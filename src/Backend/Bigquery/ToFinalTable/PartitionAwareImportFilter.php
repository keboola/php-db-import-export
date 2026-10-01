<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable;

use Keboola\Datatype\Definition\Bigquery;
use LogicException;

/**
 * Partition values of every imported row, rendered by SqlBuilder as a destination-only predicate
 * so BigQuery prunes the destination partitions the PK join has to read.
 */
final class PartitionAwareImportFilter
{
    // values are produced by SqlBuilder::getSelectDistinctPartitionValuesCommand() in these formats
    private const VALUE_PATTERNS = [
        PartitionAwareImportColumn::GRANULARITY_HOUR => '/^\d{4}-\d{2}-\d{2} \d{2}$/',
        PartitionAwareImportColumn::GRANULARITY_DAY => '/^\d{4}-\d{2}-\d{2}$/',
        PartitionAwareImportColumn::GRANULARITY_MONTH => '/^\d{4}-\d{2}$/',
        PartitionAwareImportColumn::GRANULARITY_YEAR => '/^\d{4}$/',
    ];
    private const DATE_PATTERN = '/^\d{4}-\d{2}-\d{2}$/';
    private const INT64_PATTERN = '/^-?\d{1,19}$/';

    /**
     * @param non-empty-list<string> $values
     */
    private function __construct(
        public readonly PartitionAwareImportColumn $column,
        public readonly array $values,
    ) {
    }

    /**
     * $values must be the distinct partition values of the WHOLE source table: a value missing here
     * would make MERGE insert a duplicate instead of updating the existing row.
     * Null (no pruning) when there is nothing to filter on or more values than $maxValues.
     *
     * @param string[] $values
     */
    public static function fromDistinctValues(
        PartitionAwareImportColumn $column,
        array $values,
        int $maxValues,
    ): ?self {
        $values = array_values(array_unique($values));
        if ($values === [] || count($values) > $maxValues) {
            return null;
        }

        $pattern = match ($column->type) {
            Bigquery::TYPE_INT64 => self::INT64_PATTERN,
            Bigquery::TYPE_DATE => self::DATE_PATTERN,
            default => self::VALUE_PATTERNS[$column->granularity ?? ''] ?? null,
        };
        if ($pattern === null) {
            throw new LogicException(sprintf('Unsupported partition granularity "%s".', $column->granularity));
        }
        foreach ($values as $value) {
            if (preg_match($pattern, $value) !== 1) {
                throw new LogicException(sprintf(
                    'Unexpected partition value "%s" for column "%s".',
                    $value,
                    $column->columnName,
                ));
            }
        }

        sort($values, $column->type === Bigquery::TYPE_INT64 ? SORT_NUMERIC : SORT_STRING);

        return new self($column, $values);
    }
}
