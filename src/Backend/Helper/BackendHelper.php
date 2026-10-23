<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Helper;

class BackendHelper
{
    // Staging and dedup tables are dropped after each import; the expiration only removes leaked ones.
    public const TEMP_TABLE_EXPIRATION_HOURS = 72;

    /**
     * @deprecated use generateStagingTableName
     */
    public static function generateTempTableName(): string
    {
        return self::generateStagingTableName();
    }

    public static function generateStagingTableName(): string
    {
        return '__temp_' . str_replace('.', '_', uniqid('csvimport', true));
    }

    public static function generateTempDedupTableName(): string
    {
        return '__temp_DEDUP_' . str_replace('.', '_', uniqid('csvimport', true));
    }

    public static function generateRandomExportPrefix(): string
    {
        return str_replace('.', '_', uniqid('csvexport', true));
    }
}
