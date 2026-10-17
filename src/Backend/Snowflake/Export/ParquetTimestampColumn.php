<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\Export;

use Keboola\Datatype\Definition\Snowflake;

/**
 * Snowflake cannot unload TIMESTAMP_LTZ/TZ to Parquet, so these columns are exported as their UTC wall-clock time.
 */
final class ParquetTimestampColumn
{
    private const ZONED_TIMESTAMP_TYPES = [
        Snowflake::TYPE_TIMESTAMP_LTZ,
        Snowflake::TYPE_TIMESTAMP_TZ,
    ];

    public static function requiresUtcConversion(string $type): bool
    {
        return in_array(strtoupper($type), self::ZONED_TIMESTAMP_TYPES, true);
    }

    /**
     * Keeps the column name, because Parquet takes the header from the select alias.
     */
    public static function createUtcSelectExpression(string $quotedColumnName): string
    {
        return sprintf(
            'CONVERT_TIMEZONE(\'UTC\', %s)::TIMESTAMP_NTZ AS %s',
            $quotedColumnName,
            $quotedColumnName,
        );
    }
}
