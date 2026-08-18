<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\ImportExport\ImportOptions;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

class SnowflakeImportOptions extends ImportOptions
{
    /**
     * Escape hatch back to the multi-statement import: a dedup table plus UPDATE/DELETE/INSERT for
     * incremental loads, TRUNCATE plus INSERT inside an explicit transaction for full loads. The
     * single-statement path (MERGE / INSERT OVERWRITE) is the default, so a project only needs this
     * feature if the single-statement path misbehaves for it.
     */
    public const FEATURE_LEGACY_IMPORT = 'snowflake-legacy-import';

    /** @var self::SAME_TABLES_* */
    private bool $requireSameTables;

    /** @var self::NULL_MANIPULATION_* */
    private bool $nullManipulation;

    /**
     * @param string[] $convertEmptyValuesToNull
     * @param self::SAME_TABLES_* $requireSameTables
     * @param self::NULL_MANIPULATION_* $nullManipulation
     * @param string[] $ignoreColumns
     * @param string[] $importAsNull
     * @param string[] $features
     */
    public function __construct(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = 0,
        bool $requireSameTables = self::SAME_TABLES_NOT_REQUIRED,
        bool $nullManipulation = self::NULL_MANIPULATION_ENABLED,
        array $ignoreColumns = [],
        array $importAsNull = self::DEFAULT_IMPORT_AS_NULL,
        array $features = [],
    ) {
        parent::__construct(
            $convertEmptyValuesToNull,
            $isIncremental,
            $useTimestamp,
            $numberOfIgnoredLines,
            $requireSameTables === self::SAME_TABLES_REQUIRED ? self::USING_TYPES_USER : self::USING_TYPES_STRING,
            $ignoreColumns,
            $importAsNull,
            $features,
        );
        $this->requireSameTables = $requireSameTables;
        $this->nullManipulation = $nullManipulation;
    }

    public function isRequireSameTables(): bool
    {
        return $this->requireSameTables === self::SAME_TABLES_REQUIRED;
    }

    public function isNullManipulationEnabled(): bool
    {
        return $this->nullManipulation === self::NULL_MANIPULATION_ENABLED;
    }

    public function useOptimizedImport(): bool
    {
        return !in_array(self::FEATURE_LEGACY_IMPORT, $this->features(), true);
    }

    public function getNullIfSql(): string
    {
        $nullIf = ', NULL_IF=()';
        if ($this->importAsNull() !== []) {
            $nullIf = sprintf(
                ', NULL_IF=(%s)',
                implode(
                    ',',
                    array_map(
                        fn(string $s) => SnowflakeQuote::quote($s),
                        $this->importAsNull(),
                    ),
                ),
            );
        }
        return $nullIf;
    }
}
