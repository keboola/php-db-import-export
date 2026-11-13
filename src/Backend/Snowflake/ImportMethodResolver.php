<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

/**
 * The only place that decides which Snowflake import method runs:
 * explicit request value > project feature (only for AUTO) > platform default.
 */
final class ImportMethodResolver
{
    public const PLATFORM_DEFAULT = ImportMethod::MULTI_STATEMENT;

    /**
     * @param string[] $projectFeatures
     */
    public static function resolve(ImportMethod $requested, array $projectFeatures): ResolvedImportMethod
    {
        if ($requested !== ImportMethod::AUTO) {
            return new ResolvedImportMethod($requested, ImportMethodSource::REQUEST);
        }
        if (in_array(SnowflakeImportOptions::FEATURE_OPTIMIZED_IMPORT, $projectFeatures, true)) {
            return new ResolvedImportMethod(ImportMethod::SINGLE_STATEMENT, ImportMethodSource::PROJECT_FEATURE);
        }

        return new ResolvedImportMethod(self::PLATFORM_DEFAULT, ImportMethodSource::DEFAULT);
    }
}
