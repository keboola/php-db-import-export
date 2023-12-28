<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\Exporter;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\ColumnsHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;

class ExportLegacyTest extends SnowflakeImportExportBaseTest
{
    public function setUp(): void
    {
        parent::setUp();
        $this->clearDestination($this->getExportDir());
    }

    public function tearDown(): void
    {
        parent::tearDown();
    }

    public function testExportGzip(): void
    {
        // import
        $file = new CsvFile(self::DATA_DIR . 'with-ts.csv');
        $source = $this->getSourceInstance('with-ts.csv', $file->getHeader());
        $destination = new Storage\Snowflake\Table(
            $this->getDestinationSchemaName(),
            'out.csv_2Cols',
        );
        $options = $this->getSimpleImportOptions();

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options,
        );

        // export
        $source = $destination;
        $options = new ExportOptions(true, ExportOptions::MANIFEST_AUTOGENERATED);
        $destination = $this->getDestinationInstance($this->getExportDir() . '/gz_test');

        $result = (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options,
        );

        $this->assertCount(1, $result);
        /** @var array<mixed> $slice */
        $slice = reset($result);

        $this->assertArrayHasKey('FILE_NAME', $slice);
        $this->assertArrayHasKey('FILE_SIZE', $slice);
        $this->assertArrayHasKey('ROW_COUNT', $slice);

        $this->assertSame('gz_test_0_0_0.csv.gz', $slice['FILE_NAME']);
        $this->assertNotEmpty($slice['FILE_SIZE']);
        $this->assertSame(2, (int) $slice['ROW_COUNT']);

        $files = $this->getFileNames($this->getExportDir() . '/gz_test', false);
        sort($files);
        $expected = [
            $this->getExportDir() . '/gz_test_0_0_0.csv.gz',
            $this->getExportDir() . '/gz_testmanifest',
        ];
        sort($expected);
        $this->assertSame($expected, $files);
    }

    public function testExportSimple(): void
    {
        // import
        $file = new CsvFile(self::DATA_DIR . 'with-ts.csv');
        $source = $this->getSourceInstance('with-ts.csv', $file->getHeader());
        $destination = new Storage\Snowflake\Table(
            $this->getDestinationSchemaName(),
            'out.csv_2Cols',
        );
        $options = $this->getSimpleImportOptions();

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options,
        );

        // export
        $source = $destination;
        $options = new ExportOptions(false, ExportOptions::MANIFEST_AUTOGENERATED);
        $destination = $this->getDestinationInstance($this->getExportDir() . '/ts_test');

        $result = (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options,
        );

        $this->assertCount(1, $result);
        /** @var array<mixed> $slice */
        $slice = reset($result);

        $this->assertArrayHasKey('FILE_NAME', $slice);
        $this->assertArrayHasKey('FILE_SIZE', $slice);
        $this->assertArrayHasKey('ROW_COUNT', $slice);

        $this->assertSame('ts_test_0_0_0.csv', $slice['FILE_NAME']);
        $this->assertNotEmpty($slice['FILE_SIZE']);
        $this->assertSame(2, (int) $slice['ROW_COUNT']);

        $files = $this->listFiles($this->getExportDir());
        $actual = $this->getCsvFileFromStorage($files);
        $expected = new CsvFile(
            self::DATA_DIR . 'with-ts.csv',
            CsvOptions::DEFAULT_DELIMITER,
            CsvOptions::DEFAULT_ENCLOSURE,
            CsvOptions::DEFAULT_ESCAPED_BY,
            1, // skip header
        );
        $this->assertCsvFilesSame($expected, $actual);

        $files = $this->getFileNames($this->getExportDir(), false);
        $this->assertContains($this->getExportDir() . '/ts_testmanifest', array_values($files));
    }

    public function assertCsvFilesSame(CsvFile $expected, CsvFile $actual): void
    {
        $this->assertArrayEqualsSorted(
            iterator_to_array($expected),
            iterator_to_array($actual),
            0,
            'Csv files are not same',
        );
    }

    public function testExportSimpleWithQuery(): void
    {
        // import
        $file = new CsvFile(self::DATA_DIR . 'tw_accounts.csv');
        $source = $this->getSourceInstance('tw_accounts.csv', $file->getHeader());
        $destination = new Storage\Snowflake\Table(
            $this->getDestinationSchemaName(),
            'accounts-3',
        );
        $options = $this->getSimpleImportOptions();

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options,
        );

        // export
        // query needed otherwise timestamp is downloaded
        $query = sprintf(
            'SELECT %s FROM %s',
            ColumnsHelper::getColumnsString($file->getHeader()),
            $destination->getQuotedTableWithScheme(),
        );
        $source = new Storage\Snowflake\SelectSource($query);
        $options = new ExportOptions(false, ExportOptions::MANIFEST_AUTOGENERATED);
        $destination = $this->getDestinationInstance($this->getExportDir() . '/tw_test');

        $result = (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options,
        );

        $this->assertCount(1, $result);
        /** @var array<mixed> $slice */
        $slice = reset($result);

        $this->assertArrayHasKey('FILE_NAME', $slice);
        $this->assertArrayHasKey('FILE_SIZE', $slice);
        $this->assertArrayHasKey('ROW_COUNT', $slice);

        $this->assertSame('tw_test_0_0_0.csv', $slice['FILE_NAME']);
        $this->assertNotEmpty($slice['FILE_SIZE']);
        $this->assertSame(3, (int) $slice['ROW_COUNT']);

        $files = $this->listFiles($this->getExportDir());
        $actual = $this->getCsvFileFromStorage($files);
        $expected = new CsvFile(
            self::DATA_DIR . 'tw_accounts.csv',
            CsvOptions::DEFAULT_DELIMITER,
            CsvOptions::DEFAULT_ENCLOSURE,
            CsvOptions::DEFAULT_ESCAPED_BY,
            1, // skip header
        );
        $this->assertCsvFilesSame($expected, $actual);

        $files = $this->getFileNames($this->getExportDir(), false);
        $this->assertContains($this->getExportDir() . '/tw_testmanifest', array_values($files));
    }
}
