<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\ImportExport\ImportOptions;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

class SnowflakeImportOptions extends ImportOptions
{
    // project feature that makes ImportMethod::AUTO resolve to single_statement; read only by ImportMethodResolver
    public const FEATURE_OPTIMIZED_IMPORT = 'snowflake-optimized-import';

    /** @var self::SAME_TABLES_* */
    private bool $requireSameTables;

    /** @var self::NULL_MANIPULATION_* */
    private bool $nullManipulation;

    private ResolvedImportMethod $importMethod;

    /**
     * @param string[] $convertEmptyValuesToNull
     * @param self::SAME_TABLES_* $requireSameTables
     * @param self::NULL_MANIPULATION_* $nullManipulation
     * @param string[] $ignoreColumns
     * @param string[] $importAsNull
     * @param string[] $features
     * @param ResolvedImportMethod|null $importMethod null resolves ImportMethod::AUTO against $features
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
        ?ResolvedImportMethod $importMethod = null,
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
        $this->importMethod = $importMethod ?? ImportMethodResolver::resolve(ImportMethod::AUTO, $features);
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
        return $this->importMethod->isSingleStatement();
    }

    public function getImportMethod(): ResolvedImportMethod
    {
        return $this->importMethod;
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
