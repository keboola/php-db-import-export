<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Exception\JobException;
use Google\Cloud\Core\Exception\ServiceException;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryException;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\Helper\BackendHelper;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\ToFinalTableImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\TableBackendUtils\Connection\Bigquery\Session;
use Keboola\TableBackendUtils\Connection\Bigquery\SessionFactory;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Throwable;

final class FullImporter implements ToFinalTableImporterInterface
{
    private const TIMER_COPY_TO_TARGET = 'copyFromStagingToTarget';
    private const TIMER_DEDUP = 'fromStagingToTargetWithDedup';
    private const TIMER_PARTITION_RANGES = 'partitionRanges';

    private BigQueryClient $bqClient;

    private SqlBuilder $sqlBuilder;

    public function __construct(BigQueryClient $bqClient)
    {
        $this->bqClient = $bqClient;
        $this->sqlBuilder = new SqlBuilder();
    }

    private function doLoadFullWithoutDedup(
        BigqueryTableDefinition $stagingTableDefinition,
        BigqueryTableDefinition $destinationTableDefinition,
        BigqueryImportOptions $options,
        ImportState $state,
        Session $session,
    ): void {
        $state->startTimer(self::TIMER_COPY_TO_TARGET);
        $this->loadWithPartitionSplit(
            $stagingTableDefinition,
            $destinationTableDefinition,
            $options,
            $state,
            $session,
        );
        $state->stopTimer(self::TIMER_COPY_TO_TARGET);
    }

    /**
     * @param non-empty-list<string|null> $sourceConditions
     */
    private function loadIntoTruncatedTable(
        BigqueryTableDefinition $sourceTableDefinition,
        BigqueryTableDefinition $destinationTableDefinition,
        BigqueryImportOptions $options,
        Session $session,
        string $timestamp,
        array $sourceConditions,
    ): void {
        try {
            $this->bqClient->runQuery(
                $this->bqClient->query(
                    $this->sqlBuilder->getBeginTransaction(),
                    $session->getAsQueryOptions(),
                ),
            );
            $this->bqClient->runQuery(
                $this->bqClient->query(
                    $this->sqlBuilder->getTruncateTable(
                        $destinationTableDefinition->getSchemaName(),
                        $destinationTableDefinition->getTableName(),
                    ),
                    $session->getAsQueryOptions(),
                ),
            );
            foreach ($sourceConditions as $sourceCondition) {
                $sql = $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                    $sourceTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    $timestamp,
                    $sourceCondition,
                );
                $this->bqClient->runQuery(
                    $this->bqClient->query(
                        $sql,
                        $session->getAsQueryOptions(),
                    ),
                );
            }
            $this->bqClient->runQuery(
                $this->bqClient->query(
                    $this->sqlBuilder->getCommitTransaction(),
                    $session->getAsQueryOptions(),
                ),
            );
        } catch (Throwable $e) {
            RollbackTransactionHelper::rollbackTransaction(
                $this->bqClient,
                $session,
                $this->sqlBuilder,
            );
            throw $e;
        }
    }

    private function loadWithPartitionSplit(
        BigqueryTableDefinition $sourceTableDefinition,
        BigqueryTableDefinition $destinationTableDefinition,
        BigqueryImportOptions $options,
        ImportState $state,
        Session $session,
    ): void {
        $timestamp = DateTimeHelper::getNowFormatted();
        try {
            $this->loadIntoTruncatedTable($sourceTableDefinition, $destinationTableDefinition, $options, $session, $timestamp, [null]);
        } catch (JobException|ServiceException $e) {
            if (!BigqueryException::isTooManyPartitionsError($e->getMessage())) {
                throw $e;
            }
            $state->startTimer(self::TIMER_PARTITION_RANGES);
            $ranges = (new PartitionRangeResolver($this->bqClient, $this->sqlBuilder))
                ->resolve($session, $sourceTableDefinition, $destinationTableDefinition);
            $state->stopTimer(self::TIMER_PARTITION_RANGES);
            if ($ranges === null) {
                throw $e;
            }
            // The limit is per statement, so ranged inserts in one transaction keep the load atomic.
            $this->loadIntoTruncatedTable(
                $sourceTableDefinition,
                $destinationTableDefinition,
                $options,
                $session,
                $timestamp,
                $this->sqlBuilder->getPartitionRangeSourceConditions($ranges),
            );
        }
    }

    public function importToTable(
        TableDefinitionInterface $stagingTableDefinition,
        TableDefinitionInterface $destinationTableDefinition,
        ImportOptionsInterface $options,
        ImportState $state,
    ): Result {
        assert($stagingTableDefinition instanceof BigqueryTableDefinition);
        assert($destinationTableDefinition instanceof BigqueryTableDefinition);
        assert($options instanceof BigqueryImportOptions);

        $session = $options->getSession();
        if ($session === null) {
            $session = (new SessionFactory($this->bqClient))->createSession();
        }

        /** @var BigqueryTableDefinition $destinationTableDefinition */
        try {
            //import files to staging table
            if (!empty($destinationTableDefinition->getPrimaryKeysNames())) {
                $this->doFullLoadWithDedup(
                    $stagingTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    $state,
                    $session,
                );
            } else {
                $this->doLoadFullWithoutDedup(
                    $stagingTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    $state,
                    $session,
                );
            }
        } catch (JobException|ServiceException $e) {
            throw BigqueryException::covertException($e);
        }

        $state->setImportedColumns($stagingTableDefinition->getColumnsNames());

        return $state->getResult();
    }

    private function doFullLoadWithDedup(
        BigqueryTableDefinition $stagingTableDefinition,
        BigqueryTableDefinition $destinationTableDefinition,
        BigqueryImportOptions $options,
        ImportState $state,
        Session $session,
    ): void {
        $state->startTimer(self::TIMER_DEDUP);

        // 1. Create table for deduplication
        $deduplicationTableName = BackendHelper::generateTempDedupTableName();

        $importFailed = false;

        try {
            // 2 transfer data from source to dedup table with dedup process
            $this->bqClient->runQuery(
                $this->bqClient->query(
                    $this->sqlBuilder->getCreateDedupTable(
                        $stagingTableDefinition,
                        $deduplicationTableName,
                        $destinationTableDefinition->getPrimaryKeysNames(),
                    ),
                    $session->getAsQueryOptions(),
                ),
            );
            /** @var BigqueryTableDefinition $deduplicationTableDefinition */
            $deduplicationTableDefinition = (new BigqueryTableReflection(
                $this->bqClient,
                $stagingTableDefinition->getSchemaName(),
                $deduplicationTableName,
            ))->getTableDefinition();

            $this->loadWithPartitionSplit(
                $deduplicationTableDefinition,
                $destinationTableDefinition,
                $options,
                $state,
                $session,
            );
            $state->stopTimer(self::TIMER_DEDUP);
        } catch (Throwable $e) {
            $importFailed = true;
            throw $e;
        } finally {
            // 5 drop dedup table by name, so a failed reflection after the CTAS does not leak it
            try {
                $this->bqClient->runQuery(
                    $this->bqClient->query(
                        $this->sqlBuilder->getDropTableIfExistsCommand(
                            $stagingTableDefinition->getSchemaName(),
                            $deduplicationTableName,
                        ),
                        $session->getAsQueryOptions(),
                    ),
                );
            } catch (Throwable $cleanupException) {
                // never let the cleanup failure replace the original import error
                if (!$importFailed) {
                    throw $cleanupException;
                }
            }
        }
    }
}
