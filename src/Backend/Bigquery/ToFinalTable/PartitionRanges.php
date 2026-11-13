<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable;

use LogicException;

final class PartitionRanges
{
    /** @var non-empty-list<string> */
    public readonly array $rangeStarts;

    /**
     * @param list<string> $rangeStarts
     */
    public function __construct(
        public readonly PartitionRangeColumn $column,
        array $rangeStarts,
        public readonly int $partitionCount,
    ) {
        if ($rangeStarts === []) {
            throw new LogicException('Partition range starts cannot be empty.');
        }
        $this->rangeStarts = $rangeStarts;
    }
}
