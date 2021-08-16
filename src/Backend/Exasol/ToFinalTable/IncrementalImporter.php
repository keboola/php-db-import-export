<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Exasol\ToFinalTable;

use Doctrine\DBAL\Connection;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\ToFinalTableImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableQueryBuilder;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

final class IncrementalImporter implements ToFinalTableImporterInterface
{
    private const TIMER_DEDUP_TABLE_CREATE = 'dedupTableCreate';
    private const TIMER_UPDATE_TARGET_TABLE = 'updateTargetTable';
    private const TIMER_DELETE_UPDATED_ROWS = 'deleteUpdatedRowsFromStaging';
    private const TIMER_DEDUP_STAGING = 'dedupStaging';
    private const TIMER_INSERT_INTO_TARGET = 'insertIntoTargetFromStaging';

    /** @var Connection */
    private $connection;

    /** @var SqlBuilder */
    private $sqlBuilder;

    public function __construct(
        Connection $connection
    ) {
        $this->connection = $connection;
        $this->sqlBuilder = new SqlBuilder();
    }

    public function importToTable(
        TableDefinitionInterface $stagingTableDefinition,
        TableDefinitionInterface $destinationTableDefinition,
        ImportOptionsInterface $options,
        ImportState $state
    ): Result {
        assert($stagingTableDefinition instanceof ExasolTableDefinition);
        assert($destinationTableDefinition instanceof ExasolTableDefinition);
        assert($options instanceof ExasolImportOptions);

        // table used in getInsertAllIntoTargetTableCommand if PK's are specified, dedup table is used
        $tableToCopyFrom = $stagingTableDefinition;

        $timestampValue = DateTimeHelper::getNowFormatted();
        /** @var ExasolTableDefinition $destinationTableDefinition */
        /** @var ExasolTableDefinition $destinationTableDefinition */
        if (!empty($destinationTableDefinition->getPrimaryKeysNames())) {
            // has PKs for dedup

            // Create table for deduplication
            $deduplicationTableDefinition = StageTableDefinitionFactory::createStagingTableDefinition(
                $stagingTableDefinition,
                $stagingTableDefinition->getColumnsNames()
            );
            $tableToCopyFrom = $deduplicationTableDefinition;
            $qb = new ExasolTableQueryBuilder();
            $sql = $qb->getCreateTableCommandFromDefinition($deduplicationTableDefinition);
            $state->startTimer(self::TIMER_DEDUP_TABLE_CREATE);
            $this->connection->executeStatement($sql);
            $state->stopTimer(self::TIMER_DEDUP_TABLE_CREATE);

            $state->startTimer(self::TIMER_UPDATE_TARGET_TABLE);
            $this->connection->executeStatement(
                $this->sqlBuilder->getUpdateWithPkCommandNull(
                    $stagingTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    $timestampValue
                )
            );
            $state->stopTimer(self::TIMER_UPDATE_TARGET_TABLE);

            $state->startTimer(self::TIMER_DELETE_UPDATED_ROWS);
            $this->connection->executeStatement(
                $this->sqlBuilder->getDeleteOldItemsCommand(
                    $stagingTableDefinition,
                    $destinationTableDefinition
                )
            );

            $state->stopTimer(self::TIMER_DELETE_UPDATED_ROWS);

            $state->startTimer(self::TIMER_DEDUP_STAGING);
            $this->connection->executeStatement(
                $this->sqlBuilder->getDedupCommand(
                    $stagingTableDefinition,
                    $deduplicationTableDefinition,
                    $destinationTableDefinition->getPrimaryKeysNames()
                )
            );
            $this->connection->executeStatement(
                $this->sqlBuilder->getTruncateTableWithDeleteCommand(
                    $stagingTableDefinition->getSchemaName(),
                    $stagingTableDefinition->getTableName()
                )
            );
            $state->stopTimer(self::TIMER_DEDUP_STAGING);
        } else {
            // TODO
        }

        // insert into.
        $state->startTimer(self::TIMER_INSERT_INTO_TARGET);
        $this->connection->executeStatement(
            $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                $tableToCopyFrom,
                $destinationTableDefinition,
                $options,
                $timestampValue
            )
        );
        $state->stopTimer(self::TIMER_INSERT_INTO_TARGET);

        $this->connection->executeStatement(
            $this->sqlBuilder->getCommitTransaction()
        );

        $state->setImportedColumns($stagingTableDefinition->getColumnsNames());

        return $state->getResult();
    }
}