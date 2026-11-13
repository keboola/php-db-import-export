<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable;

use LogicException;

final class PartitionRanges
{
    /**
     * @param non-empty-list<string> $rangeStarts
     */
    public function __construct(
        public readonly PartitionRangeColumn $column,
        public readonly array $rangeStarts,
        public readonly int $partitionCount,
    ) {
        if ($rangeStarts === []) {
            throw new LogicException('Partition range starts cannot be empty.');
        }
    }
}
