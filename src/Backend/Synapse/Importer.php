<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DBALException;
use Keboola\Db\Import\Exception;
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
    private const OPTIMIZED_LOAD_TMP_TABLE_SUFFIX = '_tmp';
    private const OPTIMIZED_LOAD_RENAME_TABLE_SUFFIX = '_tmp_rename';

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
        $destinationOptions = $this->getDestinationOptions(
            $source,
            $destination,
            $options
        );
        Assert::assertColumns($source, $destinationOptions);

        $this->runQuery($this->sqlBuilder->getCreateTempTableCommand(
            $destination->getSchema(),
            $this->importState->getStagingTableName(),
            $source->getColumnsNames(), // using provided columns to maintain order in source
            $options,
            $destinationOptions
        ));

        try {
            //import files to staging table
            $this->importToStagingTable($source, $destination, $options, $adapter);
            if ($options->isIncremental()) {
                $this->doIncrementalLoad(
                    $options,
                    $source,
                    $destination,
                    $destinationOptions->getPrimaryKeys(),
                    $destinationOptions
                );
            } else {
                $this->doNonIncrementalLoad(
                    $options,
                    $source,
                    $destination,
                    $destinationOptions
                );
            }
            $this->importState->setImportedColumns($source->getColumnsNames());
        } catch (\Doctrine\DBAL\Exception $e) {
            throw SynapseException::covertException($e);
        } finally {
            // drop staging table
            $this->runQuery(
                $this->sqlBuilder->getDropCommand($destination->getSchema(), $this->importState->getStagingTableName())
            );
            // drop optimized load tmp table if exists
            $this->runQuery(
                $this->sqlBuilder->getDropTableIfExistsCommand(
                    $destination->getSchema(),
                    $destination->getTableName().self::OPTIMIZED_LOAD_TMP_TABLE_SUFFIX
                )
            );
            // drop optimized load rename table if exists
            $this->runQuery(
                $this->sqlBuilder->getDropTableIfExistsCommand(
                    $destination->getSchema(),
                    $destination->getTableName().self::OPTIMIZED_LOAD_RENAME_TABLE_SUFFIX
                )
            );
        }

        return $this->importState->getResult();
    }

    private function getDestinationOptions(
        Storage\SourceInterface $source,
        Storage\Synapse\Table $destination,
        SynapseImportOptions $importOptions
    ): DestinationTableOptions {
        $tableRef = new SynapseTableReflection(
            $this->connection,
            $destination->getSchema(),
            $destination->getTableName()
        );

        if ($importOptions->useOptimizedDedup()) {
            $primaryKeys = $source->getPrimaryKeysNames();
            if ($primaryKeys === null) {
                throw new \Exception(sprintf(
                    'Deduplication using CTAS query expects primary keys to be predefined.'
                ));
            }
        } else {
            $primaryKeys = $tableRef->getPrimaryKeysNames();
        }

        return new DestinationTableOptions(
            $tableRef->getColumnsNames(),
            $primaryKeys,
            new TableDistribution(
                $tableRef->getTableDistribution(),
                $tableRef->getTableDistributionColumnsNames()
            )
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
        array $primaryKeys,
        DestinationTableOptions $destinationTableOptions
    ): void {
        $timestampValue = DateTimeHelper::getNowFormatted();

        // create temp table now, it cannot be run in transaction
        $tempTableName = $this->createTempTableForDedup(
            $source,
            $destination,
            $importOptions,
            $destinationTableOptions
        );

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
        DestinationTableOptions $destinationOptions
    ): void {
        if (!empty($destinationOptions->getPrimaryKeys())) {
            if ($importOptions->useOptimizedDedup()) {
                $this->doFullLoadWithDedup(
                    $source,
                    $destination,
                    $importOptions,
                    $destinationOptions
                );
            } else {
                $this->doLegacyFullLoadWithDedup(
                    $source,
                    $destination,
                    $importOptions,
                    $destinationOptions
                );
            }
            return;
        }

        $this->doLoadFullWithoutDedup(
            $source,
            $destination,
            $importOptions
        );
    }

    private function doFullLoadWithDedup(
        Storage\SourceInterface $source,
        Storage\Synapse\Table $destination,
        SynapseImportOptions $importOptions,
        DestinationTableOptions $destinationOptions
    ): void {
        $tmpDestination = new Storage\Synapse\Table(
            $destination->getSchema(),
            $destination->getTableName() . self::OPTIMIZED_LOAD_TMP_TABLE_SUFFIX
        );

        $this->importState->startTimer('CTAS_dedup');
        $skipCasting = !$source instanceof Storage\SqlSourceInterface;
        $this->runQuery(
            $this->sqlBuilder->getCtasDedupCommand(
                $source,
                $tmpDestination,
                $this->importState->getStagingTableName(),
                $importOptions,
                DateTimeHelper::getNowFormatted(),
                $destinationOptions,
                $skipCasting
            )
        );
        $this->importState->stopTimer('CTAS_dedup');

        $tmpDestinationToRemove = new Storage\Synapse\Table(
            $destination->getSchema(),
            $destination->getTableName() . self::OPTIMIZED_LOAD_RENAME_TABLE_SUFFIX
        );

        $this->runQuery(
            $this->sqlBuilder->getRenameTableCommand(
                $destination->getSchema(),
                $destination->getTableName(),
                $tmpDestinationToRemove->getTableName()
            )
        );

        $this->runQuery(
            $this->sqlBuilder->getRenameTableCommand(
                $tmpDestination->getSchema(),
                $tmpDestination->getTableName(),
                $destination->getTableName()
            )
        );

        $this->runQuery(
            $this->sqlBuilder->getDropCommand(
                $tmpDestinationToRemove->getSchema(),
                $tmpDestinationToRemove->getTableName()
            )
        );
    }

    private function doLoadFullWithoutDedup(
        Storage\SourceInterface $source,
        Storage\Synapse\Table $destination,
        SynapseImportOptions $importOptions
    ): void {
        $this->runQuery(
            $this->sqlBuilder->getBeginTransaction()
        );

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

    private function doLegacyFullLoadWithDedup(
        Storage\SourceInterface $source,
        Storage\Synapse\Table $destination,
        SynapseImportOptions $importOptions,
        DestinationTableOptions $destinationOptions
    ): void {
        $tempTableName = $this->createTempTableForDedup(
            $source,
            $destination,
            $importOptions,
            $destinationOptions
        );

        $this->runQuery(
            $this->sqlBuilder->getBeginTransaction()
        );

        if (!empty($destinationOptions->getPrimaryKeys())) {
            $this->importState->startTimer('dedup');
            $this->dedup($source, $destination, $destinationOptions->getPrimaryKeys(), $tempTableName);
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
        SynapseImportOptions $importOptions,
        DestinationTableOptions $destinationTableOptions
    ): string {
        // create temp table now, it cannot be run in transaction
        $tempTableName = BackendHelper::generateTempTableName();
        $this->runQuery($this->sqlBuilder->getCreateTempTableCommand(
            $destination->getSchema(),
            $tempTableName,
            $source->getColumnsNames(),
            $importOptions,
            $destinationTableOptions
        ));
        return $tempTableName;
    }
}
