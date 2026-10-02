<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery;

use InvalidArgumentException;
use Keboola\Db\ImportExport\Backend\TimestampMode;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\TableBackendUtils\Connection\Bigquery\Session;

class BigqueryImportOptions extends ImportOptions
{
    // reduces BigQuery shuffle usage: aggregation-based dedup + CLUSTER BY,
    // single MERGE instead of UPDATE+DELETE+INSERT
    public const FEATURE_OPTIMIZED_IMPORT = 'bigquery-optimized-import';

    // BigQuery rejects a job that modifies more than 4,000 partitions, so a larger filter can never apply
    public const PARTITION_AWARE_IMPORT_MAX_VALUES_LIMIT = 4000;

    private ?Session $session;

    /**
     * @param string[] $convertEmptyValuesToNull
     * @param self::USING_TYPES_* $usingTypes
     * @param string[] $importAsNull
     * @param string[] $features
     */
    public function __construct(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = self::SKIP_NO_LINE,
        string $usingTypes = self::USING_TYPES_STRING,
        ?Session $session = null,
        array $importAsNull = self::DEFAULT_IMPORT_AS_NULL,
        array $features = [],
        public readonly TimestampMode $timestampMode = TimestampMode::CurrentTime,
        public readonly bool $partitionAwareImport = false,
        public readonly ?int $partitionAwareImportMaxValues = null,
    ) {
        if ($partitionAwareImport
            && ($partitionAwareImportMaxValues === null
                || $partitionAwareImportMaxValues < 1
                || $partitionAwareImportMaxValues > self::PARTITION_AWARE_IMPORT_MAX_VALUES_LIMIT)
        ) {
            throw new InvalidArgumentException(sprintf(
                'Partition-aware import requires a partitionAwareImportMaxValues threshold between 1 and %d.',
                self::PARTITION_AWARE_IMPORT_MAX_VALUES_LIMIT,
            ));
        }
        parent::__construct(
            convertEmptyValuesToNull: $convertEmptyValuesToNull,
            isIncremental: $isIncremental,
            useTimestamp: $useTimestamp,
            numberOfIgnoredLines: $numberOfIgnoredLines,
            usingTypes: $usingTypes,
            importAsNull: $importAsNull,
            features: $features,
        );
        $this->session = $session;
    }

    public function getSession(): ?Session
    {
        return $this->session;
    }

    public function useOptimizedImport(): bool
    {
        return in_array(self::FEATURE_OPTIMIZED_IMPORT, $this->features(), true);
    }
}
