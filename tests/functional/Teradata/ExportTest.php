<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Teradata;

use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Teradata\Exporter;
use Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\S3;
use Keboola\Db\ImportExport\Storage\Teradata\Table;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;

class ExportTest extends TeradataBaseTestCase
{

    public function setUp(): void
    {
        parent::setUp();

        $this->clearDestination($this->getExportDir());

        $this->cleanDatabase($this->getDestinationDbName());
        $this->createDatabase($this->getDestinationDbName());

        $this->cleanDatabase($this->getSourceDbName());
        $this->createDatabase($this->getSourceDbName());
    }

    public function tearDown(): void
    {
        parent::tearDown();
        unset($this->client);
    }

    public function testExportGzip(): void
    {
        // import
        $schema = $this->getDestinationDbName();
        $this->initTable(self::BIGGER_TABLE);
        $file = new CsvFile(self::DATA_DIR . 'big_table.csv');
        /** @var S3\SourceFile $source */
        $source = $this->getSourceInstance('big_table.csv', $file->getHeader());
        $destination = new Table(
            $schema,
            self::BIGGER_TABLE
        );
        $importOptions = $this->getSimpleImportOptions(ImportOptions::SKIP_FIRST_LINE, false);

        // repeat import -> staging table will have around 16 MB
        $this->importTable($source, $destination, $importOptions, 4);

        // export
        $source = $destination;
        $exportOptions = $this->getExportOptions(true);
        $destination = $this->getDestinationInstance($this->getExportDir() . '/gz_test/gzip.csv');

        (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $exportOptions
        );

        /** @var array<int, array> $files */
        $files = $this->listFiles($this->getExportDir(). '/gz_test');
        self::assertNotNull($files);
        self::assertCount(1, $files);
        // the ~ 16M table was compressed under 1M
        self::assertTrue($files[0]['Size'] < (1024 * 1024));
    }

    /**
     * @dataProvider exportOptionsProvider
     * @param string[] $providedExportOptions
     * @param array[] $expectedFiles
     * @throws \Doctrine\DBAL\Exception
     * @throws \Keboola\Csv\InvalidArgumentException
     */
    public function testExportOptionsForSlicing(array $providedExportOptions, array $expectedFiles): void
    {
        // import
        $schema = $this->getDestinationDbName();
        $this->initTable(self::BIGGER_TABLE);
        $file = new CsvFile(self::DATA_DIR . 'big_table.csv');
        /** @var S3\SourceFile $source */
        $source = $this->getSourceInstance('big_table.csv', $file->getHeader());
        $destination = new Table(
            $schema,
            self::BIGGER_TABLE
        );
        $importOptions = $this->getSimpleImportOptions(ImportOptions::SKIP_FIRST_LINE, false);

        $this->importTable($source, $destination, $importOptions, 4);

        // export
        $source = $destination;
        $exportOptions = $this->getExportOptions(false, ...$providedExportOptions);
        $destination = $this->getDestinationInstance($this->getExportDir() . '/slice_test/gzip.csv');

        (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $exportOptions
        );

        /** @var array<int, array> $files */
        $files = $this->listFiles($this->getExportDir().'/slice_test');
        self::assertFilesMatch($expectedFiles, $files);
    }

    /**
     * @param array<int, array> $expectedFiles
     * @param array<int, array> $files
     */
    public static function assertFilesMatch(array $expectedFiles, array $files): void
    {
        self::assertCount(count($expectedFiles), $files);
        foreach ($expectedFiles as $i => $expectedFile) {
            $actualFile = $files[$i];
            self::assertContains($expectedFile['fileName'], $actualFile['Key']);
            $fileSize = (int) $actualFile['Size'];
            $expectedFileSize = ((int) $expectedFile['size']) * 1024 * 1024;
            // check that the file size is in range xMB +- 10 000B
            //  - (because I cannot really say what the exact size in bytes should be)
            // the size of the last file is ignored
            if ($expectedFileSize !== 0) {
                self::assertTrue(
                    ($expectedFileSize - 10000) < $fileSize && $fileSize < ($expectedFileSize + 10000),
                    sprintf('Actual size is %s but expected is %s', $fileSize, $expectedFileSize)
                );
            }
        }
    }

    /**
     * @param Table $destinationTable
     * @param S3\SourceFile|S3\SourceDirectory $source
     * @param TeradataImportOptions $options
     * @param int $repeatImport - dupliate data in staging table -> able to create a big table
     * @throws \Doctrine\DBAL\Exception
     */
    private function importTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destinationTable,
        ImportOptions $options,
        int $repeatImport = 0
    ): void {
        $importer = new ToStageImporter($this->connection);
        /** @var Table $destinationTable */
        $destinationRef = new TeradataTableReflection(
            $this->connection,
            $destinationTable->getSchema(),
            $destinationTable->getTableName()
        );
        /** @var TeradataTableDefinition $destination */
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $source->getColumnsNames()
        );
        $qb = new TeradataTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );

        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );

        // re-insert inserted data -> make the table BIIIG
        for ($i = 0; $i < $repeatImport; $i++) {
            $this->connection->executeStatement(sprintf(
                'INSERT INTO %s.%s SELECT * FROM %s.%s',
                TeradataQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                TeradataQuote::quoteSingleIdentifier($stagingTable->getTableName()),
                TeradataQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                TeradataQuote::quoteSingleIdentifier($stagingTable->getTableName()),
            ));
        }

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
        /** @var S3\SourceFile $source */
        $source = $this->getSourceInstance('with-ts.csv', $file->getHeader());
        $destination = new Table(
            $this->getDestinationDbName(),
            'out_csv_2Cols'
        );
        $options = $this->getSimpleImportOptions();
        $this->importTable($source, $destination, $options, 0);

        // export
        $source = $destination;
        $options = $this->getExportOptions();
        $destination = $this->getDestinationInstance($this->getExportDir() . '/ts_test/ts_test');

        (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
        );

        $files = $this->listFiles($this->getExportDir() . '/ts_test');
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
        /** @var S3\SourceFile $source */
        $source = $this->getSourceInstance('tw_accounts.csv', $file->getHeader());
        $destination = new Table(
            $this->getDestinationDbName(),
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
        $source = new Storage\Teradata\SelectSource($query);
        $options = $this->getExportOptions();
        $destination = $this->getDestinationInstance($this->getExportDir() . '/tw_test');

        (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
        );

        $files = $this->listFiles($this->getExportDir() . '/tw_test');
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

    /**
     * @return array[]
     */
    public function exportOptionsProvider(): array
    {
        /* MOS = MaxObjectSize ; BS = BufferSize
         * MOS say the max size of the target slice
         * BUT!!!
         *  - BS can be MIN 5M
         *  - At least 1 Buffer has to be written to each object
         *      -> when MOS < BS then MOS is ignored and each slice is of BS size
         *  - BS doesn't have to fill whole MOS, size of the file is BS * n where n={1,2,3,4...}.
         *      So when MOS=12, BS=5 => file will have 10M
         */
        return
            [
                // buffer can fit just once in the MOS
                'buffer 6M, max 8M, split, not single' => [
                    [
                        '6M',
                        '8M',
                    ], // options
                    [
                        ['fileName' => 'F00000', 'size' => 6],
                        ['fileName' => 'F00001', 'size' => 6],
                        ['fileName' => 'F00002', 'size' => 0],
                    ], // expected files
                ],
                // buffer can fit twice in MOS -> object has 10M
                'buffer 5, max 11m, split, not single' => [
                    [
                        '5M',
                        '11M',
                    ], // options
                    [
                        ['fileName' => 'F00000', 'size' => 10],
                        ['fileName' => 'F00001', 'size' => 0],
                    ], // expected files
                ],
                // MOS is smaller than min buffer size -> MOS is ignored and parts are of buffer size
                'buffer 5M, max 33k, split, not single' => [
                    [
                        '5M',
                        '33k',
                    ], // options
                    [
                        ['fileName' => 'F00000', 'size' => 5],
                        ['fileName' => 'F00001', 'size' => 5],
                        ['fileName' => 'F00002', 'size' => 5],
                        ['fileName' => 'F00003', 'size' => 0],
                    ], // expected files
                ],
                // whole file can it in the object
                'buffer 5M, max 100M, split, single' => [
                    [
                        '5M',
                        '100M',
                    ], // options
                    [
                        ['fileName' => 'F00000', 'size' => 0],
                    ], // expected files
                ],
                'default' => [
                    [], // options, default is 8M and 8G
                    [
                        ['fileName' => 'F00000', 'size' => 0],
                    ], // expected files
                ],
            ];
    }
}
