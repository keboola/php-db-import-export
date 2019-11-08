<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\ImportExport\Backend\Exception\ColumnsCountNotMatchException;
use Keboola\Db\ImportExport\Backend\Exception\InvalidSourceDataException;
use Keboola\Db\ImportExport\Backend\Exception\MandatoryFileNotFoundException;
use Keboola\Db\ImportExport\Backend\Exception\NoColumnsFoundException;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\ImportResult;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\BackendHelper;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\SnowflakeDbAdapter\Connection;

class Importer implements ImporterInterface
{
    public const TIMESTAMP_COLUMN_NAME = '_timestamp';

    /** @var Connection */
    private $connection;

    /**
     * @var SqlCommandBuilder
     */
    private $sqlBuilder;

    /**
     * @var ImportState
     */
    private $importState;

    public function __construct(
        Connection $connection
    ) {
        $this->connection = $connection;
        $this->sqlBuilder = new SqlCommandBuilder();
    }

    public function importTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptions $options
    ): ImportResult {
        if (!$destination instanceof Storage\Snowflake\Table) {
            throw new \Exception(sprintf(
                'Destination "%s" is invalid only "%s" is supported.',
                get_class($destination),
                Storage\Snowflake\Table::class
            ));
        }
        $this->importState = new ImportState(BackendHelper::generateStagingTableName());
        $this->validateColumns($options, $destination);

        $this->runQuery($this->sqlBuilder->getCreateStagingTableCommand(
            $destination->getSchema(),
            $this->importState->getStagingTableName(),
            $options->getColumns()
        ));

        try {
            //import files to staging table
            $this->importToStagingTable($source, $destination, $options);
            $primaryKeys = $this->connection->getTablePrimaryKey(
                $destination->getSchema(),
                $destination->getTableName()
            );
            if ($options->isIncremental()) {
                $this->doIncrementalLoad($options, $destination, $primaryKeys);
            } else {
                $this->doNonIncrementalLoad($options, $destination, $primaryKeys);
            }
            $this->importState->setImportedColumns($options->getColumns());
        } finally {
            $this->runQuery(
                $this->sqlBuilder->getDropCommand($destination->getSchema(), $this->importState->getStagingTableName())
            );
        }

        return $this->importState->getResult();
    }

    private function validateColumns(
        ImportOptions $importOptions,
        Storage\Snowflake\Table $destination
    ): void {
        if (count($importOptions->getColumns()) === 0) {
            throw new NoColumnsFoundException(
                'No columns found in CSV file.'
            );
        }

        $tableColumns = $this->connection->getTableColumns(
            $destination->getSchema(),
            $destination->getTableName()
        );

        $moreColumns = array_diff($importOptions->getColumns(), $tableColumns);
        if (!empty($moreColumns)) {
            throw new ColumnsCountNotMatchException(
                'Columns doest not match. Non existing columns: ' . implode(', ', $moreColumns)
            );
        }
    }

    private function runQuery(string $query, ?string $timerName = null): void
    {
        if ($timerName) {
            $this->importState->startTimer($timerName);
        }
        // echo sprintf("Executing query: %s \n", $query);

        $this->connection->query($query);
        if ($timerName) {
            $this->importState->stopTimer($timerName);
        }
    }

    private function importToStagingTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptions $importOptions
    ): void {
        $adapter = $source->getBackendImportAdapter($this);
        if (!$adapter instanceof SnowflakeImportAdapterInterface) {
            throw new \Exception(sprintf(
                'Adapter "%s" must implement "SnowflakeImportAdapterInterface".',
                get_class($adapter)
            ));
        }
        $commands = $adapter->getCopyCommands($destination, $importOptions, $this->importState->getStagingTableName());
        try {
            $this->importState->addImportedRowsCount(
                $adapter->executeCopyCommands(
                    $commands,
                    $this->connection,
                    $destination,
                    $importOptions,
                    $this->importState
                )
            );
        } catch (\Throwable $e) {
            $message = 'Load error: ' . $e->getMessage();
            if (strpos($e->getMessage(), 'was not found') !== false) {
                throw new MandatoryFileNotFoundException($message, 0, $e);
            }
            throw new InvalidSourceDataException($message, 0, $e);
        }
    }

    private function doIncrementalLoad(
        ImportOptions $importOptions,
        Storage\Snowflake\Table $destination,
        array $primaryKeys
    ): void {
        $this->runQuery(
            $this->sqlBuilder->getBeginTransaction()
        );
        if (!empty($primaryKeys)) {
            $this->runQuery(
                $this->sqlBuilder->getUpdateWithPkCommand(
                    $destination,
                    $importOptions,
                    $this->importState->getStagingTableName(),
                    $primaryKeys
                ),
                'updateTargetTable'
            );
            $this->runQuery(
                $this->sqlBuilder->getDeleteOldItemsCommand(
                    $destination,
                    $this->importState->getStagingTableName(),
                    $primaryKeys
                ),
                'deleteUpdatedRowsFromStaging'
            );
            $this->importState->startTimer('dedupStaging');
            $this->dedup($importOptions, $destination, $primaryKeys);
            $this->importState->stopTimer('dedupStaging');
        }
        $this->runQuery(
            $this->sqlBuilder->getInsertFromStagingToTargetTableCommand(
                $destination,
                $importOptions,
                $this->importState->getStagingTableName()
            ),
            'insertIntoTargetFromStaging'
        );
        $this->runQuery(
            $this->sqlBuilder->getCommitTransaction()
        );
    }

    /**
     * @param ImportOptions $importOptions
     * @param array $primaryKeys
     */
    private function dedup(
        ImportOptions $importOptions,
        Storage\Snowflake\Table $destination,
        array $primaryKeys
    ): void {
        $tempTableName = BackendHelper::generateStagingTableName();
        $this->runQuery($this->sqlBuilder->getCreateStagingTableCommand(
            $destination->getSchema(),
            $tempTableName,
            $importOptions->getColumns()
        ));

        $this->runQuery(
            $this->sqlBuilder->getDedupCommand(
                $destination,
                $importOptions,
                $primaryKeys,
                $this->importState->getStagingTableName(),
                $tempTableName
            )
        );
        $this->runQuery(
            $this->sqlBuilder->getDropCommand(
                $destination->getSchema(),
                $this->importState->getStagingTableName()
            )
        );
        $this->runQuery(
            $this->sqlBuilder->getRenameTableCommand(
                $destination->getSchema(),
                $tempTableName,
                $this->importState->getStagingTableName()
            )
        );
    }

    private function doNonIncrementalLoad(
        ImportOptions $importOptions,
        Storage\Snowflake\Table $destination,
        array $primaryKeys
    ): void {
        if (!empty($primaryKeys)) {
            $this->importState->startTimer('dedup');
            $this->dedup($importOptions, $destination, $primaryKeys);
            $this->importState->stopTimer('dedup');
        }
        $this->runQuery(
            $this->sqlBuilder->getBeginTransaction()
        );
        $this->runQuery(
            $this->sqlBuilder->getTruncateTableCommand(
                $destination->getSchema(),
                $destination->getTableName()
            )
        );
        $this->runQuery(
            $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                $destination,
                $importOptions,
                $this->importState->getStagingTableName()
            ),
            'copyFromStagingToTarget'
        );
        $this->runQuery(
            $this->sqlBuilder->getCommitTransaction()
        );
    }
}
