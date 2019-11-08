<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake;

use Keboola\Csv\CsvReader;
use Keboola\Db\ImportExport\Backend\Exception\ColumnsCountNotMatchException;
use Keboola\Db\ImportExport\Backend\Exception\MandatoryFileNotFoundException;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

class OtherImportTest extends SnowflakeImportExportBaseTest
{
    public function testCopyInvalidSourceDataShouldThrowException(): void
    {
        $options = $this->getSimpleImportOptions(['c1', 'c2']);
        $source = new Storage\Snowflake\Table(self::SNOWFLAKE_SOURCE_SCHEMA_NAME, 'names');
        $destination = new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'out.csv_2Cols');

        self::expectException(ColumnsCountNotMatchException::class);
        self::expectExceptionMessage('Columns doest not match. Non existing columns: c1, c2');
        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );
    }

    public function testImportShouldNotFailOnColumnNameRowNumber(): void
    {
        $options = $this->getSimpleImportOptions([
            'id',
            'row_number',
        ]);
        $source = $this->createABSSourceInstance('column-name-row-number.csv');
        $destination = new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'column-name-row-number');

        $result = (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );
        self::assertEquals(2, $result->getImportedRowsCount());
    }

    public function testInvalidManifestImport(): void
    {
        $initialFile = new CsvReader(self::DATA_DIR . 'tw_accounts.csv');
        $options = $this->getSimpleImportOptions($initialFile->getHeader());
        $source = $this->createABSSourceInstance('02_tw_accounts.csv.invalid.manifest', true);
        $destination = new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'accounts-3');

        self::expectException(MandatoryFileNotFoundException::class);
        self::expectExceptionMessage('Load error: Error "odbc_execute(): SQL error: Remote file');
        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );
    }

    public function testMoreColumnsShouldThrowException(): void
    {
        $options = $this->getSimpleImportOptions([
            'first',
            'second',
        ]);
        $source = $this->createABSSourceInstance('tw_accounts.csv', false);
        $destination = new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'out.csv_2Cols');

        self::expectException(ColumnsCountNotMatchException::class);
        self::expectExceptionMessage('Columns doest not match. Non existing columns: first, second');
        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );
    }

    public function testNullifyCopy(): void
    {
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS "%s"."nullify"',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."nullify" ("id" VARCHAR, "name" VARCHAR, "price" NUMERIC)',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS "%s"."nullify_src" ',
            self::SNOWFLAKE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."nullify_src" ("id" VARCHAR, "name" VARCHAR, "price" NUMERIC)',
            self::SNOWFLAKE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO "%s"."nullify_src" VALUES(\'1\', \'\', NULL), (\'2\', NULL, 500)',
            self::SNOWFLAKE_SOURCE_SCHEMA_NAME
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            ['id', 'name', 'price'],
            false,
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = new Storage\Snowflake\Table(self::SNOWFLAKE_SOURCE_SCHEMA_NAME, 'nullify_src');
        $destination = new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'nullify');

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT "id", "name", "price" FROM "%s"."nullify" ORDER BY "id" ASC',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->assertCount(2, $importedData);
        $this->assertTrue($importedData[0]['name'] === null);
        $this->assertTrue($importedData[0]['price'] === null);
        $this->assertTrue($importedData[1]['name'] === null);
    }

    public function testNullifyCopyIncremental(): void
    {
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS "%s"."nullify" ',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."nullify" ("id" VARCHAR, "name" VARCHAR, "price" NUMERIC)',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO "%s"."nullify" VALUES(\'4\', NULL, 50)',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS "%s"."nullify_src" ',
            self::SNOWFLAKE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."nullify_src" ("id" VARCHAR, "name" VARCHAR, "price" NUMERIC)',
            self::SNOWFLAKE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO "%s"."nullify_src" VALUES(\'1\', \'\', NULL), (\'2\', NULL, 500)',
            self::SNOWFLAKE_SOURCE_SCHEMA_NAME
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            ['id', 'name', 'price'],
            true, // incremetal
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = new Storage\Snowflake\Table(self::SNOWFLAKE_SOURCE_SCHEMA_NAME, 'nullify_src');
        $destination = new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'nullify');

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT "id", "name", "price" FROM "%s"."nullify" ORDER BY "id" ASC',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->assertCount(3, $importedData);
        $this->assertTrue($importedData[0]['name'] === null);
        $this->assertTrue($importedData[0]['price'] === null);
        $this->assertTrue($importedData[1]['name'] === null);
        $this->assertTrue($importedData[2]['name'] === null);
    }

    public function testNullifyCopyIncrementalWithPk(): void
    {
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS "%s"."nullify" ',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."nullify" ("id" VARCHAR, "name" VARCHAR, "price" NUMERIC, PRIMARY KEY("id"))',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO "%s"."nullify" VALUES(\'4\', \'3\', 2)',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS "%s"."nullify_src" ',
            self::SNOWFLAKE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
        // phpcs:ignore
            'CREATE TABLE "%s"."nullify_src" ("id" VARCHAR NOT NULL, "name" VARCHAR NOT NULL, "price" VARCHAR NOT NULL, PRIMARY KEY("id"))',
            self::SNOWFLAKE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO "%s"."nullify_src" VALUES(\'1\', \'\', \'\'), (\'2\', \'\', \'500\'), (\'4\', \'\', \'\')',
            self::SNOWFLAKE_SOURCE_SCHEMA_NAME
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            ['id', 'name', 'price'],
            true, // incremetal
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = new Storage\Snowflake\Table(self::SNOWFLAKE_SOURCE_SCHEMA_NAME, 'nullify_src');
        $destination = new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'nullify');

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT "id", "name", "price" FROM "%s"."nullify"',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $expectedData = [
            [
                'id' => '1',
                'name' => null,
                'price' => null,
            ],
            [
                'id' => '2',
                'name' => null,
                'price' => '500',
            ],
            [
                'id' => '4',
                'name' => null,
                'price' => null,
            ],

        ];

        $this->assertArrayEqualsSorted($expectedData, $importedData, 'id');
    }

    public function testNullifyCopyIncrementalWithPkDestinationWithNull(): void
    {
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS "%s"."nullify" ',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."nullify" ("id" VARCHAR, "name" VARCHAR, "price" NUMERIC, PRIMARY KEY("id"))',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO "%s"."nullify" VALUES(\'4\', NULL, NULL)',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS "%s"."nullify_src" ',
            self::SNOWFLAKE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
        // phpcs:ignore
            'CREATE TABLE "%s"."nullify_src" ("id" VARCHAR NOT NULL, "name" VARCHAR NOT NULL, "price" VARCHAR NOT NULL, PRIMARY KEY("id"))',
            self::SNOWFLAKE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO "%s"."nullify_src" VALUES(\'1\', \'\', \'\'), (\'2\', \'\', \'500\'), (\'4\', \'\', \'500\')',
            self::SNOWFLAKE_SOURCE_SCHEMA_NAME
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            ['id', 'name', 'price'],
            true, // incremetal
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = new Storage\Snowflake\Table(self::SNOWFLAKE_SOURCE_SCHEMA_NAME, 'nullify_src');
        $destination = new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'nullify');

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT "id", "name", "price" FROM "%s"."nullify"',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $expectedData = [
            [
                'id' => '1',
                'name' => null,
                'price' => null,
            ],
            [
                'id' => '2',
                'name' => null,
                'price' => '500',
            ],
            [
                'id' => '4',
                'name' => null,
                'price' => '500',
            ],

        ];

        $this->assertArrayEqualsSorted($expectedData, $importedData, 'id');
    }

    public function testNullifyCsv(): void
    {
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS "%s"."nullify" ',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."nullify" ("id" VARCHAR, "name" VARCHAR, "price" NUMERIC)',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            ['id', 'name', 'price'],
            false,
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = $this->createABSSourceInstance('nullify.csv');
        $destination = new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'nullify');

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT "id", "name", "price" FROM "%s"."nullify" ORDER BY "id" ASC',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->assertCount(3, $importedData);
        $this->assertTrue($importedData[1]['name'] === null);
        $this->assertTrue($importedData[2]['price'] === null);
    }

    public function testNullifyCsvIncremental(): void
    {
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS "%s"."nullify" ',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."nullify" ("id" VARCHAR, "name" VARCHAR, "price" NUMERIC)',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO "%s"."nullify" VALUES(\'4\', NULL, 50)',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            ['id', 'name', 'price'],
            true, // incremetal
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = $this->createABSSourceInstance('nullify.csv');
        $destination = new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'nullify');

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );
        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT "id", "name", "price" FROM "%s"."nullify" ORDER BY "id" ASC',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
        $this->assertCount(4, $importedData);
        $this->assertTrue($importedData[1]['name'] === null);
        $this->assertTrue($importedData[2]['price'] === null);
        $this->assertTrue($importedData[3]['name'] === null);
    }
}
