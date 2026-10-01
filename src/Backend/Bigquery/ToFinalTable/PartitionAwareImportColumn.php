<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable;

use Keboola\Datatype\Definition\Bigquery;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\PartitioningConfig;

/**
 * Destination partitioning column usable to prune the PK join of an incremental import.
 */
final class PartitionAwareImportColumn
{
    public const GRANULARITY_HOUR = 'HOUR';
    public const GRANULARITY_DAY = 'DAY';
    public const GRANULARITY_MONTH = 'MONTH';
    public const GRANULARITY_YEAR = 'YEAR';

    private const TIME_PARTITION_TYPES = [
        Bigquery::TYPE_DATE,
        Bigquery::TYPE_DATETIME,
        Bigquery::TYPE_TIMESTAMP,
    ];

    private const GRANULARITIES = [
        self::GRANULARITY_HOUR,
        self::GRANULARITY_DAY,
        self::GRANULARITY_MONTH,
        self::GRANULARITY_YEAR,
    ];

    /**
     * @param Bigquery::TYPE_DATE|Bigquery::TYPE_DATETIME|Bigquery::TYPE_TIMESTAMP|Bigquery::TYPE_INT64 $type
     * @param self::GRANULARITY_*|null $granularity null for integer-range partitioning
     */
    private function __construct(
        public readonly string $columnName,
        public readonly string $type,
        public readonly ?string $granularity,
    ) {
    }

    /**
     * Null when pruning cannot be applied: no partitioning, ingestion-time partitioning,
     * or a partition column that is not part of the primary key.
     */
    public static function fromDestination(
        ?PartitioningConfig $partitioning,
        BigqueryTableDefinition $destination,
    ): ?self {
        if ($partitioning === null) {
            return null;
        }

        $range = $partitioning->rangePartitioningConfig;
        if ($range !== null) {
            if (!in_array($range->column, $destination->getPrimaryKeysNames(), true)) {
                return null;
            }
            return new self($range->column, Bigquery::TYPE_INT64, null);
        }

        $time = $partitioning->timePartitioningConfig;
        if ($time === null || $time->column === null) {
            return null;
        }
        if (!in_array($time->column, $destination->getPrimaryKeysNames(), true)) {
            return null;
        }
        $granularity = strtoupper($time->type);
        if (!in_array($granularity, self::GRANULARITIES, true)) {
            return null;
        }
        $type = self::findColumnType($destination, $time->column);
        if ($type === null || !in_array($type, self::TIME_PARTITION_TYPES, true)) {
            return null;
        }

        return new self($time->column, $type, $granularity);
    }

    private static function findColumnType(BigqueryTableDefinition $definition, string $columnName): ?string
    {
        /** @var BigqueryColumn $column */
        foreach ($definition->getColumnsDefinitions() as $column) {
            if ($column->getColumnName() === $columnName) {
                return strtoupper($column->getColumnDefinition()->getType());
            }
        }
        return null;
    }
}
