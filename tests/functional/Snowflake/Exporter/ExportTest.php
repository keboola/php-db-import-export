<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake\Exporter;

use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\Export\Exporter;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\ColumnsHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\Snowflake\SnowflakeBaseTestCase;

class ExportTest extends SnowflakeBaseTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->clearDestination($this->getExportDir());
        $this->cleanSchema($this->getDestinationSchemaName());
        $this->cleanSchema($this->getSourceSchemaName());
        $this->createSchema($this->getSourceSchemaName());
        $this->createSchema($this->getDestinationSchemaName());
    }

    public function tearDown(): void
    {
        parent::tearDown();
    }

    public function testExportGzip(): void
    {
        $this->initTable(self::TABLE_OUT_CSV_2COLS);
        // import
        $file = new CsvFile(self::DATA_DIR . 'with-ts.csv');
        $source = $this->getSourceInstance('with-ts.csv', $file->getHeader());
        $destination = new Storage\Snowflake\Table(
            $this->getDestinationSchemaName(),
            self::TABLE_OUT_CSV_2COLS
        );

        $this->importTable($source, $destination);

        // export
        $source = $destination;
        $options = new ExportOptions(true);
        $destination = $this->getDestinationInstance($this->getExportDir() . '/gz_test');

        $result = (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
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
        $this->initTable(self::TABLE_OUT_CSV_2COLS);
        // import
        $file = new CsvFile(self::DATA_DIR . 'with-ts.csv');
        $source = $this->getSourceInstance('with-ts.csv', $file->getHeader());
        $destination = new Storage\Snowflake\Table(
            $this->getDestinationSchemaName(),
            self::TABLE_OUT_CSV_2COLS
        );
        $this->importTable($source, $destination);

        // export
        $source = $destination;
        $options = new ExportOptions();
        $destination = $this->getDestinationInstance($this->getExportDir() . '/ts_test');

        $result = (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
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
        self::assertNotNull($files);
        $actual = $this->getCsvFileFromStorage($files);
        $expected = new CsvFile(
            self::DATA_DIR . 'with-ts.csv',
            CsvOptions::DEFAULT_DELIMITER,
            CsvOptions::DEFAULT_ENCLOSURE,
            CsvOptions::DEFAULT_ESCAPED_BY,
            1 // skip header
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
            'Csv files are not same'
        );
    }

    public function testExportSimpleWithQuery(): void
    {
        $this->initTable(self::TABLE_ACCOUNTS_3);
        // import
        $file = new CsvFile(self::DATA_DIR . 'tw_accounts.csv');
        $source = $this->getSourceInstance('tw_accounts.csv', $file->getHeader());
        $destination = new Storage\Snowflake\Table(
            $this->getDestinationSchemaName(),
            self::TABLE_ACCOUNTS_3
        );

        $this->importTable($source, $destination);

        // export
        // query needed otherwise timestamp is downloaded
        $query = sprintf(
            'SELECT %s FROM %s',
            ColumnsHelper::getColumnsString($file->getHeader()),
            $destination->getQuotedTableWithScheme()
        );
        $source = new Storage\Snowflake\SelectSource($query);
        $options = new ExportOptions();
        $destination = $this->getDestinationInstance($this->getExportDir() . '/tw_test');

        $result = (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
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
        self::assertNotNull($files);

        $actual = $this->getCsvFileFromStorage($files);
        $expected = new CsvFile(
            self::DATA_DIR . 'tw_accounts.csv',
            CsvOptions::DEFAULT_DELIMITER,
            CsvOptions::DEFAULT_ENCLOSURE,
            CsvOptions::DEFAULT_ESCAPED_BY,
            1 // skip header
        );
        $this->assertCsvFilesSame($expected, $actual);

        $files = $this->getFileNames($this->getExportDir(), false);
        $this->assertContains($this->getExportDir() . '/tw_testmanifest', array_values($files));
    }

    private function importTable(
        Storage\SourceInterface $source,
        Storage\Snowflake\Table $destination
    ): void {
        $options = $this->getSimpleImportOptions(ImportOptions::SKIP_FIRST_LINE);
        $importer = new ToStageImporter($this->connection);
        $destinationRef = new SnowflakeTableReflection(
            $this->connection,
            $destination->getSchema(),
            $destination->getTableName()
        );
        $importDestination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $importDestination,
            $source->getColumnsNames()
        );
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new FullImporter($this->connection);

        $toFinalTableImporter->importToTable(
            $stagingTable,
            $importDestination,
            $options,
            $importState
        );
    }
}
