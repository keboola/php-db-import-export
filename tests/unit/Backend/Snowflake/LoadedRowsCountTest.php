<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake;

use Keboola\Db\ImportExport\Backend\Snowflake\LoadedRowsCount;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class LoadedRowsCountTest extends BaseTestCase
{
    public function testCopyResultIsSummedOverFiles(): void
    {
        self::assertSame(9, LoadedRowsCount::fromCopyIntoResult([
            ['file' => 'a.csv', 'status' => 'LOADED', 'rows_parsed' => '4', 'rows_loaded' => '4'],
            ['file' => 'b.csv', 'status' => 'LOADED', 'rows_parsed' => '5', 'rows_loaded' => '5'],
        ]));
    }

    public function testPartiallyLoadedFileCountsOnlyLoadedRows(): void
    {
        self::assertSame(3, LoadedRowsCount::fromCopyIntoResult([
            [
                'file' => 'a.csv',
                'status' => 'PARTIALLY_LOADED',
                'rows_parsed' => '5',
                'rows_loaded' => '3',
                'errors_seen' => '2',
            ],
        ]));
    }

    public function testStatusOnlyCopyResultCountsAsNoRows(): void
    {
        self::assertSame(0, LoadedRowsCount::fromCopyIntoResult([
            ['status' => 'Copy executed with 0 files processed.'],
        ]));
    }

    public function testEmptyCopyResultCountsAsNoRows(): void
    {
        self::assertSame(0, LoadedRowsCount::fromCopyIntoResult([]));
    }
}
