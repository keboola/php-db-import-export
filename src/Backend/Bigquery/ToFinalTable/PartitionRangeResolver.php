<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable;

use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\TableBackendUtils\Connection\Bigquery\Session;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use LogicException;

final class PartitionRangeResolver
{
    public function __construct(
        private readonly BigQueryClient $bqClient,
        private readonly SqlBuilder $sqlBuilder,
    ) {
    }

    public function resolve(
        Session $session,
        BigqueryTableDefinition $sourceTableDefinition,
        BigqueryTableDefinition $destinationTableDefinition,
    ): ?PartitionRanges {
        $partitioning = (new BigqueryTableReflection(
            $this->bqClient,
            $destinationTableDefinition->getSchemaName(),
            $destinationTableDefinition->getTableName(),
        ))->getPartitioningConfiguration();
        $column = PartitionRangeColumn::fromDestination($partitioning, $destinationTableDefinition);
        if ($column === null) {
            return null;
        }

        $result = $this->bqClient->runQuery($this->bqClient->query(
            $this->sqlBuilder->getSelectPartitionRangeStartsCommand(
                $sourceTableDefinition,
                $column,
                BigqueryImportOptions::MAX_PARTITIONS_PER_STATEMENT,
            ),
            $session->getAsQueryOptions(),
        ));

        $starts = [];
        $partitionCount = 0;
        foreach ($result as $row) {
            assert(is_array($row));
            $start = $row[SqlBuilder::RANGE_START_ALIAS] ?? null;
            $count = $row[SqlBuilder::PARTITION_COUNT_ALIAS] ?? null;
            if (!is_string($start) || $start === '' || (!is_int($count) && !is_string($count))) {
                throw new LogicException('Partition range query returned an unexpected row.');
            }
            if (is_string($count) && !is_numeric($count)) {
                throw new LogicException('Partition range query returned an invalid partition count.');
            }
            $partitionCount = (int) $count;
            $starts[] = $start;
        }

        if ($starts === []) {
            return null;
        }

        $hasNullUnit = null;
        $nullResult = $this->bqClient->runQuery($this->bqClient->query(
            $this->sqlBuilder->getSelectHasNullPartitionUnitCommand($sourceTableDefinition, $column),
            $session->getAsQueryOptions(),
        ));
        foreach ($nullResult as $row) {
            assert(is_array($row));
            $hasNullUnit = $row[SqlBuilder::HAS_NULL_UNIT_ALIAS] ?? null;
        }
        if (!is_bool($hasNullUnit)) {
            throw new LogicException('Partition NULL unit query returned an unexpected row.');
        }

        return new PartitionRanges(
            $column,
            $starts,
            $partitionCount,
            $partitioning?->timePartitioningConfig?->expirationMs !== null,
            $hasNullUnit,
        );
    }
}
