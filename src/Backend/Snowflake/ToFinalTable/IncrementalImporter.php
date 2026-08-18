<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable;

use DateTimeInterface;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeException;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\ToFinalTableImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

final class IncrementalImporter implements ToFinalTableImporterInterface
{
    private const TIMER_DEDUP_TABLE_CREATE = 'dedupTableCreate';
    private const TIMER_UPDATE_TARGET_TABLE = 'updateTargetTable';
    private const TIMER_DELETE_UPDATED_ROWS = 'deleteUpdatedRowsFromStaging';
    private const TIMER_DEDUP_STAGING = 'dedupStaging';
    private const TIMER_INSERT_INTO_TARGET = 'insertIntoTargetFromStaging';
    private const TIMER_MERGE_INTO_TARGET = 'mergeIntoTargetFromStaging';

    private Connection $connection;

    private bool $forceUseCtas = false;

    private SqlBuilder $sqlBuilder;

    private string $timestamp;

    public function __construct(
        Connection $connection,
        ?DateTimeInterface $timestamp = null,
    ) {
        $this->connection = $connection;
        $this->sqlBuilder = new SqlBuilder();
        if ($timestamp === null) {
            $this->timestamp = DateTimeHelper::getNowFormatted();
        } else {
            $this->timestamp = DateTimeHelper::getTimestampFormated($timestamp);
        }
    }

    /**
     * This is used for testing purposes only.
     * method will be removed in the future.
     */
    public function tmpForceUseCtas(): void
    {
        $this->forceUseCtas = true;
    }

    public function importToTable(
        TableDefinitionInterface $stagingTableDefinition,
        TableDefinitionInterface $destinationTableDefinition,
        ImportOptionsInterface $options,
        ImportState $state,
    ): Result {
        assert($stagingTableDefinition instanceof SnowflakeTableDefinition);
        assert($destinationTableDefinition instanceof SnowflakeTableDefinition);
        assert($options instanceof SnowflakeImportOptions);

        /** @var SnowflakeTableDefinition $destinationTableDefinition */
        if ($options->useOptimizedImport()
            && !empty($destinationTableDefinition->getPrimaryKeysNames())
            && !$this->forceUseCtas
        ) {
            return $this->importWithMerge(
                $stagingTableDefinition,
                $destinationTableDefinition,
                $options,
                $state,
            );
        }

        // table used in getInsertAllIntoTargetTableCommand if PK's are specified, dedup table is used
        $tableToCopyFrom = $stagingTableDefinition;

        try {
            $this->connection->executeStatement(
                $this->sqlBuilder->getBeginTransaction(),
            );

            /** @var SnowflakeTableDefinition $destinationTableDefinition */
            if (!empty($destinationTableDefinition->getPrimaryKeysNames())
                && !$this->forceUseCtas
            ) {
                // has PKs for dedup

                // 0. Create table for deduplication
                $deduplicationTableDefinition = StageTableDefinitionFactory::createDedupTableDefinition(
                    $stagingTableDefinition,
                    $destinationTableDefinition->getPrimaryKeysNames(),
                );
                $tableToCopyFrom = $deduplicationTableDefinition;
                $qb = new SnowflakeTableQueryBuilder();
                $sql = $qb->getCreateTableCommandFromDefinition($deduplicationTableDefinition);
                $state->startTimer(self::TIMER_DEDUP_TABLE_CREATE);
                $this->connection->executeStatement($sql);
                $state->stopTimer(self::TIMER_DEDUP_TABLE_CREATE);

                // Count unique rows in staging (by PK) before any modifications.
                // This is the number of rows actually being imported (updates + inserts).
                $pkColumns = $destinationTableDefinition->getPrimaryKeysNames();
                $pkSql = implode(', ', array_map(
                    static fn(string $col) => SnowflakeQuote::quoteSingleIdentifier($col),
                    $pkColumns,
                ));
                $stagingRef = sprintf(
                    '%s.%s',
                    SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
                    SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
                );
                /** @var string $uniqueCount */
                $uniqueCount = $this->connection->fetchOne(sprintf(
                    'SELECT COUNT(*) FROM (SELECT %s FROM %s GROUP BY %s)',
                    $pkSql,
                    $stagingRef,
                    $pkSql,
                ));
                $state->setImportedRowsCount((int) $uniqueCount);

                // 1. Run UPDATE command to update rows in final table with updated data based on PKs
                $state->startTimer(self::TIMER_UPDATE_TARGET_TABLE);
                $this->connection->executeStatement(
                    $this->sqlBuilder->getUpdateWithPkCommand(
                        $stagingTableDefinition,
                        $destinationTableDefinition,
                        $options,
                        $this->timestamp,
                    ),
                );
                $state->stopTimer(self::TIMER_UPDATE_TARGET_TABLE);

                // 2. delete updated rows from staging table
                $state->startTimer(self::TIMER_DELETE_UPDATED_ROWS);
                $this->connection->executeStatement(
                    $this->sqlBuilder->getDeleteOldItemsCommand(
                        $stagingTableDefinition,
                        $destinationTableDefinition,
                        $options,
                    ),
                );
                $state->stopTimer(self::TIMER_DELETE_UPDATED_ROWS);

                // 3. dedup insert
                $state->startTimer(self::TIMER_DEDUP_STAGING);
                $this->connection->executeStatement(
                    $this->sqlBuilder->getDedupCommand(
                        $stagingTableDefinition,
                        $deduplicationTableDefinition,
                        $destinationTableDefinition->getPrimaryKeysNames(),
                    ),
                );
                $this->connection->executeStatement(
                    $this->sqlBuilder->getTruncateTable(
                        $stagingTableDefinition->getSchemaName(),
                        $stagingTableDefinition->getTableName(),
                    ),
                );
                $state->stopTimer(self::TIMER_DEDUP_STAGING);
            }

            // insert into destination table
            $state->startTimer(self::TIMER_INSERT_INTO_TARGET);
            $this->connection->executeStatement(
                $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                    $tableToCopyFrom,
                    $destinationTableDefinition,
                    $options,
                    $this->timestamp,
                ),
            );
            $state->stopTimer(self::TIMER_INSERT_INTO_TARGET);

            $this->connection->executeStatement(
                $this->sqlBuilder->getCommitTransaction(),
            );

            $state->setImportedColumns($stagingTableDefinition->getColumnsNames());
        } catch (Exception $e) {
            throw SnowflakeException::covertException($e);
        } finally {
            if (isset($deduplicationTableDefinition)) {
                // drop dedup table
                $this->connection->executeStatement(
                    $this->sqlBuilder->getDropTableIfExistsCommand(
                        $deduplicationTableDefinition->getSchemaName(),
                        $deduplicationTableDefinition->getTableName(),
                    ),
                );
            }
        }

        return $state->getResult();
    }

    /**
     * Applies the whole import with one MERGE. Being a single statement it is atomic on its own, so no
     * explicit transaction is needed — which also means no DDL can silently commit half of the import,
     * as creating a dedup table inside a transaction does. Collapsing the duplicates inline additionally
     * removes the dedup table, the second scan of staging done by the DELETE, and the extra write of
     * the data.
     */
    private function importWithMerge(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        SnowflakeImportOptions $options,
        ImportState $state,
    ): Result {
        try {
            // Count unique rows in staging (by PK) before any modifications.
            // This is the number of rows actually being imported (updates + inserts).
            $state->setImportedRowsCount(
                $this->countUniqueRowsInStaging(
                    $stagingTableDefinition,
                    $destinationTableDefinition->getPrimaryKeysNames(),
                    $options,
                ),
            );

            $state->startTimer(self::TIMER_MERGE_INTO_TARGET);
            $this->connection->executeStatement(
                $this->sqlBuilder->getMergeCommand(
                    $stagingTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    $this->timestamp,
                ),
            );
            $state->stopTimer(self::TIMER_MERGE_INTO_TARGET);

            $state->setImportedColumns($stagingTableDefinition->getColumnsNames());
        } catch (Exception $e) {
            throw SnowflakeException::covertException($e);
        }

        return $state->getResult();
    }

    /**
     * @param string[] $primaryKeys
     */
    private function countUniqueRowsInStaging(
        SnowflakeTableDefinition $stagingTableDefinition,
        array $primaryKeys,
        SnowflakeImportOptions $options,
    ): int {
        /** @var string $uniqueCount */
        $uniqueCount = $this->connection->fetchOne(
            $this->sqlBuilder->getUniquePrimaryKeyCountCommand(
                $stagingTableDefinition,
                $options,
                $primaryKeys,
            ),
        );

        return (int) $uniqueCount;
    }
}
