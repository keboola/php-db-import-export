<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable;

use DateTimeInterface;
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
use LogicException;
use Throwable;

final class IncrementalImporter implements ToFinalTableImporterInterface
{
    private const TIMER_INSERT_INTO_TARGET = 'insertIntoTargetFromStaging';
    private const TIMER_UPDATE_TARGET_TABLE = 'updateTargetTable';
    private const TIMER_DELETE_UPDATED_ROWS = 'deleteUpdatedRowsFromStaging';
    private const TIMER_DEDUP_STAGING = 'dedupStaging';
    private const TIMER_PARTITION_AWARE_IMPORT_DISTINCT_VALUES = 'partitionAwareImportDistinctValues';

    private BigQueryClient $bqClient;

    private SqlBuilder $sqlBuilder;

    private string $timestamp;

    public function __construct(
        BigQueryClient $bqClient,
        ?DateTimeInterface $timestamp = null,
    ) {
        $this->bqClient = $bqClient;
        $this->sqlBuilder = new SqlBuilder();
        if ($timestamp === null) {
            $this->timestamp = DateTimeHelper::getNowFormatted();
        } else {
            $this->timestamp = DateTimeHelper::getTimestampFormated($timestamp);
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
        // table used in getInsertAllIntoTargetTableCommand if PK's are specified, dedup table is used
        $tableToCopyFrom = $stagingTableDefinition;
        $useOptimizedImport = $options->useOptimizedImport();

        try {
            $transactionStarted = false;
            // when true, the PK branch already merged data into the destination table via a single
            // MERGE statement, so the shared UPDATE/DELETE/INSERT-and-commit sequence below is skipped
            $mergedViaOptimizedImport = false;
            if (!empty($destinationTableDefinition->getPrimaryKeysNames())) {
                // has PKs for dedup
                $deduplicationTableName = BackendHelper::generateTempDedupTableName();
                // 0. Create table deduplication table and dedup
                $state->startTimer(self::TIMER_DEDUP_STAGING);
                $this->bqClient->runQuery(
                    $this->bqClient->query(
                        $this->sqlBuilder->getCreateDedupTable(
                            $stagingTableDefinition,
                            $deduplicationTableName,
                            $destinationTableDefinition->getPrimaryKeysNames(),
                            $useOptimizedImport,
                        ),
                        $session->getAsQueryOptions(),
                    ),
                );
                $bigqueryTableReflection = new BigqueryTableReflection(
                    $this->bqClient,
                    $stagingTableDefinition->getSchemaName(),
                    $deduplicationTableName,
                );
                $deduplicationTableDefinition = $bigqueryTableReflection->getTableDefinition();
                assert($deduplicationTableDefinition instanceof BigqueryTableDefinition);

                $tableToCopyFrom = $deduplicationTableDefinition;
                $state->stopTimer(self::TIMER_DEDUP_STAGING);

                // Count unique rows in dedup table (= unique PKs from staging).
                // This is the number of rows actually being imported (updates + inserts).
                $dedupRowCount = $bigqueryTableReflection->getRowsCount();
                $state->setImportedRowsCount($dedupRowCount);

                $partitionAwareImportFilter = $this->resolvePartitionAwareImportFilter(
                    $deduplicationTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    $session,
                    $state,
                );

                if ($useOptimizedImport) {
                    // MERGE is a single atomic statement, replacing UPDATE+DELETE+INSERT;
                    // no explicit transaction is needed for the PK branch.
                    $state->startTimer(self::TIMER_UPDATE_TARGET_TABLE);
                    $this->bqClient->runQuery(
                        $this->bqClient->query(
                            $this->sqlBuilder->getMergeCommand(
                                $deduplicationTableDefinition,
                                $destinationTableDefinition,
                                $options,
                                $this->timestamp,
                                $partitionAwareImportFilter,
                            ),
                            $session->getAsQueryOptions(),
                        ),
                    );
                    $state->stopTimer(self::TIMER_UPDATE_TARGET_TABLE);
                    $mergedViaOptimizedImport = true;
                } else {
                    $this->bqClient->runQuery(
                        $this->bqClient->query(
                            $this->sqlBuilder->getBeginTransaction(),
                            $session->getAsQueryOptions(),
                        ),
                    );
                    $transactionStarted = true;

                    // 1. Run UPDATE command to update rows in final table with updated data based on PKs
                    $state->startTimer(self::TIMER_UPDATE_TARGET_TABLE);
                    $this->bqClient->runQuery(
                        $this->bqClient->query(
                            $this->sqlBuilder->getUpdateWithPkCommand(
                                $deduplicationTableDefinition,
                                $destinationTableDefinition,
                                $options,
                                $this->timestamp,
                                $partitionAwareImportFilter,
                            ),
                            $session->getAsQueryOptions(),
                        ),
                    );
                    $state->stopTimer(self::TIMER_UPDATE_TARGET_TABLE);

                    // 2. delete updated rows from staging table
                    $state->startTimer(self::TIMER_DELETE_UPDATED_ROWS);
                    $this->bqClient->runQuery(
                        $this->bqClient->query(
                            $this->sqlBuilder->getDeleteOldItemsCommand(
                                $deduplicationTableDefinition,
                                $destinationTableDefinition,
                                $options,
                                $partitionAwareImportFilter,
                            ),
                            $session->getAsQueryOptions(),
                        ),
                    );
                    $state->stopTimer(self::TIMER_DELETE_UPDATED_ROWS);
                }
            } else {
                $this->bqClient->runQuery(
                    $this->bqClient->query(
                        $this->sqlBuilder->getBeginTransaction(),
                        $session->getAsQueryOptions(),
                    ),
                );
                $transactionStarted = true;
            }

            if (!$mergedViaOptimizedImport) {
                // insert into destination table
                $state->startTimer(self::TIMER_INSERT_INTO_TARGET);
                $this->bqClient->runQuery(
                    $this->bqClient->query(
                        $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                            $tableToCopyFrom,
                            $destinationTableDefinition,
                            $options,
                            $this->timestamp,
                        ),
                        $session->getAsQueryOptions(),
                    ),
                );
                $state->stopTimer(self::TIMER_INSERT_INTO_TARGET);

                $this->bqClient->runQuery(
                    $this->bqClient->query(
                        $this->sqlBuilder->getCommitTransaction(),
                        $session->getAsQueryOptions(),
                    ),
                );
            }

            $state->setImportedColumns($stagingTableDefinition->getColumnsNames());
        } catch (JobException|ServiceException $e) {
            if ($transactionStarted) {
                RollbackTransactionHelper::rollbackTransaction(
                    $this->bqClient,
                    $session,
                    $this->sqlBuilder,
                );
            }
            throw BigqueryException::covertException($e);
        } catch (Throwable $e) {
            if ($transactionStarted) {
                RollbackTransactionHelper::rollbackTransaction(
                    $this->bqClient,
                    $session,
                    $this->sqlBuilder,
                );
            }
            throw $e;
        } finally {
            if (isset($deduplicationTableDefinition)) {
                // drop dedup table
                $this->bqClient->runQuery(
                    $this->bqClient->query(
                        $this->sqlBuilder->getDropTableIfExistsCommand(
                            $deduplicationTableDefinition->getSchemaName(),
                            $deduplicationTableDefinition->getTableName(),
                        ),
                    ),
                );
            }
        }

        return $state->getResult();
    }

    private function resolvePartitionAwareImportFilter(
        BigqueryTableDefinition $deduplicationTableDefinition,
        BigqueryTableDefinition $destinationTableDefinition,
        BigqueryImportOptions $options,
        Session $session,
        ImportState $state,
    ): ?PartitionAwareImportFilter {
        if (!$options->partitionAwareImport || $options->partitionAwareImportMaxValues === null) {
            return null;
        }
        $column = PartitionAwareImportColumn::fromDestination(
            (new BigqueryTableReflection(
                $this->bqClient,
                $destinationTableDefinition->getSchemaName(),
                $destinationTableDefinition->getTableName(),
            ))->getPartitioningConfiguration(),
            $destinationTableDefinition,
        );
        if (is_string($column)) {
            $state->setPartitionAwareImportDecision(PartitionAwareImportDecision::skipped($column));
            return null;
        }

        // one row over the threshold is enough to know pruning is off; the DISTINCT still scans the whole table
        $state->startTimer(self::TIMER_PARTITION_AWARE_IMPORT_DISTINCT_VALUES);
        $result = $this->bqClient->runQuery(
            $this->bqClient->query(
                $this->sqlBuilder->getSelectDistinctPartitionValuesCommand(
                    $deduplicationTableDefinition,
                    $column,
                    $options->partitionAwareImportMaxValues + 1,
                ),
                $session->getAsQueryOptions(),
            ),
        );
        $values = [];
        foreach ($result as $row) {
            assert(is_array($row));
            $value = $row[SqlBuilder::PARTITION_VALUE_ALIAS] ?? null;
            if (!is_string($value)) {
                throw new LogicException('Distinct partition value query must return STRING values.');
            }
            $values[] = $value;
        }
        $state->stopTimer(self::TIMER_PARTITION_AWARE_IMPORT_DISTINCT_VALUES);

        $filter = PartitionAwareImportFilter::fromDistinctValues(
            $column,
            $values,
            $options->partitionAwareImportMaxValues,
        );
        if ($filter !== null) {
            $decision = PartitionAwareImportDecision::applied($column->columnName, count($filter->values));
        } elseif ($values === []) {
            $decision = PartitionAwareImportDecision::skipped(
                PartitionAwareImportDecision::SKIPPED_NO_VALUES,
                $column->columnName,
                0,
            );
        } else {
            // the query stops at threshold + 1 values, so the count only says "over"
            $decision = PartitionAwareImportDecision::skipped(
                PartitionAwareImportDecision::SKIPPED_OVER_THRESHOLD,
                $column->columnName,
                count($values),
            );
        }
        $state->setPartitionAwareImportDecision($decision);

        return $filter;
    }
}
