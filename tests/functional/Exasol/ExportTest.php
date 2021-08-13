<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Exasol;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\Backend\Exasol\Exporter;
use Keboola\Db\ImportExport\Backend\Exasol\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Exasol\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\S3;
use Keboola\Db\ImportExport\Storage\ABS;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;

class ExportTest extends ExasolBaseTestCase
{
    private const EXPORT_DIR = 'exasol_test_export';

    public function setUp(): void
    {
        parent::setUp();

        $this->clearDestination($this->getExportDir());
        $this->cleanSchema($this->getDestinationSchemaName());
        $this->cleanSchema($this->getSourceSchemaName());
        $this->createSchema($this->getSourceSchemaName());
        $this->createSchema($this->getDestinationSchemaName());
    }

    private function getExportDir(): string
    {
        $buildPrefix = '';
        if (getenv('BUILD_PREFIX') !== false) {
            $buildPrefix = getenv('BUILD_PREFIX');
        }

        return self::EXPORT_DIR
            . '-'
            . $buildPrefix
            . '-'
            . getenv('SUITE');
    }

    public function tearDown(): void
    {
        parent::tearDown();
        unset($this->client);
    }

    public function testExportGzip(): void
    {
        // import
        $schema = $this->getDestinationSchemaName();
        $this->initTable(self::TABLE_OUT_CSV_2COLS);
        $file = new CsvFile(self::DATA_DIR . 'with-ts.csv');
        $source = $this->getSourceInstance('with-ts.csv', $file->getHeader());
        $destination = new Storage\Exasol\Table(
            $schema,
            self::TABLE_OUT_CSV_2COLS
        );
        $options = $this->getSimpleImportOptions();

        $this->importTable($source, $destination, $options);

        // export
        $source = $destination;
        $options = new ExportOptions(true);
        $destination = $this->getDestinationInstance($this->getExportDir() . '/gz_test/gzip.csv');

        (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
        );

        $files = $this->listFiles($this->getExportDir());
        self::assertNotNull($files);
        self::assertCount(10, $files);
    }

    /**
     * @return ExasolImportOptions
     */
    protected function getSimpleImportOptions(
        int $skipLines = ImportOptions::SKIP_FIRST_LINE
    ): ImportOptions {
        return new ExasolImportOptions(
            [],
            false,
            true,
            $skipLines
        );
    }

    /**
     * @param Storage\Exasol\Table $destination
     * @param S3\SourceFile|ABS\SourceFile|S3\SourceDirectory|ABS\SourceDirectory $source
     * @param ExasolImportOptions $options
     * @throws \Doctrine\DBAL\Exception
     */
    private function importTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptions $options
    ): void {
        $importer = new ToStageImporter($this->connection);
        $destinationRef = new ExasolTableReflection(
            $this->connection,
            $destination->getSchema(),
            $destination->getTableName()
        );
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $source->getColumnsNames()
        );
        $qb = new ExasolTableQueryBuilder();
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
            $destination,
            $options,
            $importState
        );
    }

    public function testExportSimple(): void
    {
        // import
        $this->initTable(self::TABLE_OUT_CSV_2COLS);
        $file = new CsvFile(self::DATA_DIR . 'with-ts.csv');
        $source = $this->getSourceInstance('with-ts.csv', $file->getHeader());
        $destination = new Storage\Exasol\Table(
            $this->getDestinationSchemaName(),
            'out_csv_2Cols'
        );
        $options = $this->getSimpleImportOptions();
        $this->importTable($source, $destination, $options);

        // export
        $source = $destination;
        $options = new ExportOptions();
        $destination = $this->getDestinationInstance($this->getExportDir() . '/ts_test/ts_test');

        (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
        );

        $files = $this->listFiles($this->getExportDir());
        self::assertNotNull($files);

        $actual = $this->getCsvFileFromStorage($files);
        $expected = new CsvFile(
            self::DATA_DIR . 'with-ts.expected.exasol.csv',
            CsvOptions::DEFAULT_DELIMITER,
            CsvOptions::DEFAULT_ENCLOSURE,
            CsvOptions::DEFAULT_ESCAPED_BY,
            1 // skip header
        );
        $this->assertCsvFilesSame($expected, $actual);
    }

    public function testExportSimpleWithQuery(): void
    {
        // import
        $this->initTable(self::TABLE_ACCOUNTS_3);
        $file = new CsvFile(self::DATA_DIR . 'tw_accounts.csv');
        $source = $this->getSourceInstance('tw_accounts.csv', $file->getHeader());
        $destination = new Storage\Exasol\Table(
            $this->getDestinationSchemaName(),
            'accounts-3'
        );
        $options = $this->getSimpleImportOptions();

        $this->importTable($source, $destination, $options);

        // export
        // query needed otherwise timestamp is downloaded
        $query = sprintf(
            'SELECT %s FROM %s',
            (new SqlBuilder)->getColumnsString($file->getHeader()),
            $destination->getQuotedTableWithScheme()
        );
        $source = new Storage\Exasol\SelectSource($query);
        $options = new ExportOptions();
        $destination = $this->getDestinationInstance($this->getExportDir() . '/tw_test');

        (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
        );

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
    }
}