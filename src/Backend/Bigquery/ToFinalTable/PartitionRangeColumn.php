<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable;

use Keboola\Datatype\Definition\Bigquery;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\PartitioningConfig;

/**
 * Destination partition column a load can be split on.
 */
final class PartitionRangeColumn
{
    /**
     * @param Bigquery::TYPE_DATE|Bigquery::TYPE_DATETIME|Bigquery::TYPE_TIMESTAMP|Bigquery::TYPE_INT64 $type
     * @param PartitionAwareImportColumn::GRANULARITY_*|null $granularity
     */
    private function __construct(
        public readonly string $columnName,
        public readonly string $type,
        public readonly ?string $granularity,
        public readonly ?int $rangeStart,
        public readonly ?int $rangeEnd,
        public readonly ?int $rangeInterval,
    ) {
    }

    public static function fromDestination(
        ?PartitioningConfig $partitioning,
        BigqueryTableDefinition $destination,
    ): ?self {
        if ($partitioning === null) {
            return null;
        }

        $range = $partitioning->rangePartitioningConfig;
        if ($range !== null) {
            if (preg_match('/^-?\d{1,19}$/', $range->start) !== 1
                || preg_match('/^-?\d{1,19}$/', $range->end) !== 1
                || preg_match('/^-?\d{1,19}$/', $range->interval) !== 1) {
                return null;
            }
            return new self(
                $range->column,
                Bigquery::TYPE_INT64,
                null,
                (int) $range->start,
                (int) $range->end,
                (int) $range->interval,
            );
        }

        $time = $partitioning->timePartitioningConfig;
        if ($time === null || $time->column === null) {
            return null;
        }
        $granularity = strtoupper($time->type);
        if (!in_array($granularity, [
            PartitionAwareImportColumn::GRANULARITY_HOUR,
            PartitionAwareImportColumn::GRANULARITY_DAY,
            PartitionAwareImportColumn::GRANULARITY_MONTH,
            PartitionAwareImportColumn::GRANULARITY_YEAR,
        ], true)) {
            return null;
        }
        $type = self::findColumnType($destination, $time->column);
        if (!in_array($type, [
            Bigquery::TYPE_DATE,
            Bigquery::TYPE_DATETIME,
            Bigquery::TYPE_TIMESTAMP,
        ], true)) {
            return null;
        }
        if ($type === Bigquery::TYPE_DATE && $granularity === PartitionAwareImportColumn::GRANULARITY_HOUR) {
            return null;
        }

        return new self($time->column, $type, $granularity, null, null, null);
    }

    private static function findColumnType(BigqueryTableDefinition $destination, string $columnName): ?string
    {
        /** @var BigqueryColumn $column */
        foreach ($destination->getColumnsDefinitions() as $column) {
            if ($column->getColumnName() === $columnName) {
                return strtoupper($column->getColumnDefinition()->getType());
            }
        }
        return null;
    }
}
