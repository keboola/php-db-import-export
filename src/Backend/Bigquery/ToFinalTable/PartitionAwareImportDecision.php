<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable;

/**
 * Whether a requested partition-aware import filtered the destination, for logging by the caller.
 */
final class PartitionAwareImportDecision
{
    public const SKIPPED_NOT_PARTITIONED = 'not partitioned';
    public const SKIPPED_INGESTION_TIME_PARTITIONING = 'ingestion-time partitioning';
    public const SKIPPED_COLUMN_NOT_IN_PRIMARY_KEY = 'partition column not in primary key';
    public const SKIPPED_UNSUPPORTED_TYPE = 'unsupported partition column type or granularity';
    public const SKIPPED_NO_VALUES = 'no partition values';
    public const SKIPPED_OVER_THRESHOLD = 'more partition values than the threshold';

    /**
     * @param self::SKIPPED_*|null $skippedReason
     */
    private function __construct(
        public readonly ?string $skippedReason,
        public readonly ?string $columnName,
        public readonly ?int $valueCount,
    ) {
    }

    public static function applied(string $columnName, int $valueCount): self
    {
        return new self(null, $columnName, $valueCount);
    }

    /**
     * @param self::SKIPPED_* $reason
     */
    public static function skipped(string $reason, ?string $columnName = null, ?int $valueCount = null): self
    {
        return new self($reason, $columnName, $valueCount);
    }

    public function isApplied(): bool
    {
        return $this->skippedReason === null;
    }

    /**
     * @return array{applied: bool, skippedReason: string|null, column: string|null, valueCount: int|null}
     */
    public function toLogContext(): array
    {
        return [
            'applied' => $this->isApplied(),
            'skippedReason' => $this->skippedReason,
            'column' => $this->columnName,
            'valueCount' => $this->valueCount,
        ];
    }
}
