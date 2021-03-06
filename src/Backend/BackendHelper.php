<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

final class BackendHelper
{
    public static function generateRandomExportPrefix(): string
    {
        return str_replace('.', '_', uniqid('csvexport', true));
    }
}
