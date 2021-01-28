<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

use Doctrine\DBAL\Connection;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Synapse\Exception\Assert;
use Keboola\Db\ImportExport\Backend\Synapse\Helper\BackendHelper;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;

class Importer implements ImporterInterface
{
    public const DEFAULT_ADAPTERS = [
        Storage\ABS\SynapseImportAdapter::class,
        Storage\Synapse\SynapseImportAdapter::class,
    ];

    /** @var string[] */
    private $adapters = self::DEFAULT_ADAPTERS;

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
        $this->sqlBuilder = new SqlCommandBuilder($this->connection);
    }

    /**
     * @param Storage\Synapse\Table $destination
     * @param SynapseImportOptions $options
     */
    public function importTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptionsInterface $options
    ): Result {
        $adapter = $this->getAdapter($source, $destination);
        Assert::assertSynapseImportOptions($options);
        Assert::assertIsSynapseTableDestination($destination);
        Assert::assertValidSource($source);

        $this->importState = new ImportState(BackendHelper::generateTempTableName());
        $destinationOptions = $this->getDestinationOptions($source, $destination);
        Assert::assertColumns($source, $destinationOptions);

        $this->runQuery($this->sqlBuilder->getCreateTempTableCommand(
            $destination->getSchema(),
            $this->importState->getStagingTableName(),
            $source->getColumnsNames(), // using provided columns to maintain order in source
            $options
        ));

        try {
            //import files to staging table
            $this->importToStagingTable($source, $destination, $options, $adapter);
            $primaryKeys = $this->sqlBuilder->getTablePrimaryKey(
                $destination->getSchema(),
                $destination->getTableName()
            );
            if ($options->isIncremental()) {
                $this->doIncrementalLoad($options, $source, $destination, $primaryKeys);
            } else {
                $this->doNonIncrementalLoad($options, $source, $destination, $primaryKeys);
            }
            $this->importState->setImportedColumns($source->getColumnsNames());
        } finally {
            $this->runQuery(
                $this->sqlBuilder->getDropCommand($destination->getSchema(), $this->importState->getStagingTableName())
            );
        }

        return $this->importState->getResult();
    }

    private function getDestinationOptions(
        Storage\SourceInterface $source,
        Storage\Synapse\Table $destination
    ): DestinationTableOptions {
        $tableRef = new SynapseTableReflection(
            $this->connection,
            $destination->getSchema(),
            $destination->getTableName()
        );

        $primaryKeysDefinition = DestinationTableOptions::PRIMARY_KEYS_DEFINITION_METADATA;
        $primaryKeys = $source->getPrimaryKeysNames();
        if ($primaryKeys === null) {
            $primaryKeysDefinition = DestinationTableOptions::PRIMARY_KEYS_DEFINITION_DB;
            $primaryKeys = $tableRef->getPrimaryKeysNames();
        }

        return new DestinationTableOptions(
            $tableRef->getColumnsNames(),
            $primaryKeys,
            $primaryKeysDefinition
        );
    }

    private function runQuery(string $query, ?string $timerName = null): void
    {
        if ($timerName) {
            $this->importState->startTimer($timerName);
        }
        $this->connection->exec($query);
        if ($timerName) {
            $this->importState->stopTimer($timerName);
        }
    }

    /**
     * @param Storage\Synapse\Table $destination
     */
    private function importToStagingTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptionsInterface $importOptions,
        SynapseImportAdapterInterface $adapter
    ): void {
        $this->importState->startTimer('copyToStaging');
        $rowsCount = $adapter->runCopyCommand(
            $source,
            $destination,
            $importOptions,
            $this->importState->getStagingTableName()
        );
        $this->importState->stopTimer('copyToStaging');

        $this->importState->addImportedRowsCount($rowsCount);
    }

    private function doIncrementalLoad(
        SynapseImportOptions $importOptions,
        Storage\SourceInterface $source,
        Storage\Synapse\Table $destination,
        array $primaryKeys
    ): void {
        $timestampValue = DateTimeHelper::getNowFormatted();

        // create temp table now, it cannot be run in transaction
        $tempTableName = $this->createTempTableForDedup($source, $destination, $importOptions);

        $this->runQuery(
            $this->sqlBuilder->getBeginTransaction()
        );
        if (!empty($primaryKeys)) {
            $this->runQuery(
                $this->sqlBuilder->getUpdateWithPkCommand(
                    $source,
                    $destination,
                    $importOptions,
                    $this->importState->getStagingTableName(),
                    $primaryKeys,
                    $timestampValue
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
            $this->dedup($source, $destination, $primaryKeys, $tempTableName);
            $this->importState->stopTimer('dedupStaging');
            $this->importState->overwriteStagingTableName($tempTableName);
        }
        $this->runQuery(
            $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                $source,
                $destination,
                $importOptions,
                $this->importState->getStagingTableName(),
                $timestampValue
            ),
            'insertIntoTargetFromStaging'
        );
        $this->runQuery(
            $this->sqlBuilder->getCommitTransaction()
        );
    }

    /**
     * @param string[] $primaryKeys
     */
    private function dedup(
        Storage\SourceInterface $source,
        Storage\Synapse\Table $destination,
        array $primaryKeys,
        string $tempTableName
    ): void {
        $this->runQuery(
            $this->sqlBuilder->getDedupCommand(
                $source,
                $destination,
                $primaryKeys,
                $this->importState->getStagingTableName(),
                $tempTableName
            )
        );

        $this->runQuery(
            $this->sqlBuilder->getTruncateTableWithDeleteCommand(
                $destination->getSchema(),
                $this->importState->getStagingTableName()
            )
        );
    }

    private function doNonIncrementalLoad(
        SynapseImportOptions $importOptions,
        Storage\SourceInterface $source,
        Storage\Synapse\Table $destination,
        array $primaryKeys
    ): void {
        $tempTableName = $this->createTempTableForDedup($source, $destination, $importOptions);

        $this->runQuery(
            $this->sqlBuilder->getBeginTransaction()
        );

        if (!empty($primaryKeys)) {
            $this->importState->startTimer('dedup');
            $this->dedup($source, $destination, $primaryKeys, $tempTableName);
            $this->importState->stopTimer('dedup');
            $this->importState->overwriteStagingTableName($tempTableName);
        }

        $this->runQuery(
            $this->sqlBuilder->getTruncateTableWithDeleteCommand(
                $destination->getSchema(),
                $destination->getTableName()
            )
        );

        $this->runQuery(
            $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                $source,
                $destination,
                $importOptions,
                $this->importState->getStagingTableName(),
                DateTimeHelper::getNowFormatted()
            ),
            'copyFromStagingToTarget'
        );
        $this->runQuery(
            $this->sqlBuilder->getCommitTransaction()
        );
    }

    public function setAdapters(array $adapters): void
    {
        $this->adapters = $adapters;
    }

    private function getAdapter(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination
    ): SynapseImportAdapterInterface {
        $adapterForUse = null;
        foreach ($this->adapters as $adapter) {
            $ref = new \ReflectionClass($adapter);
            if (!$ref->implementsInterface(SynapseImportAdapterInterface::class)) {
                throw new \Exception(
                    sprintf(
                        'Each Synapse import adapter must implement "%s".',
                        SynapseImportAdapterInterface::class
                    )
                );
            }
            if ($adapter::isSupported($source, $destination)) {
                if ($adapterForUse !== null) {
                    throw new \Exception(
                        sprintf(
                            'More than one suitable adapter found for Synapse importer with source: '
                            . '"%s", destination "%s".',
                            get_class($source),
                            get_class($destination)
                        )
                    );
                }
                $adapterForUse = new $adapter($this->connection);
            }
        }
        if ($adapterForUse === null) {
            throw new \Exception(
                sprintf(
                    'No suitable adapter found for Synapse importer with source: "%s", destination "%s".',
                    get_class($source),
                    get_class($destination)
                )
            );
        }

        return $adapterForUse;
    }

    private function createTempTableForDedup(
        Storage\SourceInterface $source,
        Storage\Synapse\Table $destination,
        SynapseImportOptions $importOptions
    ): string {
        // create temp table now, it cannot be run in transaction
        $tempTableName = BackendHelper::generateTempTableName();
        $this->runQuery($this->sqlBuilder->getCreateTempTableCommand(
            $destination->getSchema(),
            $tempTableName,
            $source->getColumnsNames(),
            $importOptions
        ));
        return $tempTableName;
    }
}
