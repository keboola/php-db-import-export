<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Bigquery;

use DateTime;
use Generator;
use Google\Cloud\Core\Exception\NotFoundException;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use PHPUnit\Framework\Attributes\DataProvider;
use Throwable;

class SqlBuildTest extends BigqueryBaseTestCase
{
    public const TESTS_PREFIX = 'import_export_test_';
    public const TEST_STAGING_TABLE = 'stagingTable';
    public const TEST_STAGING_TABLE_QUOTED = '`stagingTable`';
    public const TEST_TABLE = self::TESTS_PREFIX . 'test';
    public const TEST_TABLE_QUOTED = '`' . self::TEST_TABLE . '`';

    private static function getTestDb(): string
    {
        $suffix = self::getTestObjectSuffix();
        return self::TESTS_PREFIX . ($suffix === '' ? '' : $suffix . '_') . 'schema';
    }

    private static function getTestDbQuoted(): string
    {
        return BigqueryQuote::quoteSingleIdentifier(self::getTestDb());
    }

    private static function getTestTableInDb(): string
    {
        return self::getTestDbQuoted() . '.' . self::TEST_TABLE_QUOTED;
    }

    /**
     * Rewrites the hard-coded schema literal in an expected SQL string to the
     * run-unique dataset name, so assertions keep matching once datasets are
     * isolated per CI run (see BigqueryBaseTestCase::getTestObjectSuffix()).
     */
    private static function expectedSql(string $sql): string
    {
        return str_replace(
            BigqueryQuote::quoteSingleIdentifier(self::TESTS_PREFIX . 'schema'),
            self::getTestDbQuoted(),
            $sql,
        );
    }

    private static function assertSqlEquals(mixed $expected, mixed $actual, string $message = ''): void
    {
        if (is_string($expected)) {
            $expected = self::expectedSql($expected);
        }

        self::assertEquals($expected, $actual, $message);
    }

    protected function dropTestDb(): void
    {
        $this->cleanDatabase(self::getTestDb());
    }

    protected function getBuilder(): SqlBuilder
    {
        return new SqlBuilder();
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropTestDb();
    }

    protected function createTestDb(): void
    {
        $this->createDatabase(self::getTestDb());
    }

    public function testGetDedupCommand(): void
    {
        $this->markTestSkipped('not implemented');
    }

    private function createStagingTableWithData(bool $includeEmptyValues = false): BigqueryTableDefinition
    {
        $def = $this->getStagingTableDefinition();
        $qb = new BigqueryTableQueryBuilder();
        $this->bqClient->runQuery($this->bqClient->query($qb->getCreateTableCommandFromDefinition($def)));

        $this->bqClient->runQuery(
            $this->bqClient->query(
                sprintf(
                    'INSERT INTO %s.%s(`pk1`,`pk2`,`col1`,`col2`) VALUES (\'1\',\'1\',\'1\',\'1\')',
                    self::getTestDbQuoted(),
                    self::TEST_STAGING_TABLE_QUOTED,
                ),
            ),
        );
        $this->bqClient->runQuery(
            $this->bqClient->query(
                sprintf(
                    'INSERT INTO %s.%s(`pk1`,`pk2`,`col1`,`col2`) VALUES (\'1\',\'1\',\'1\',\'1\')',
                    self::getTestDbQuoted(),
                    self::TEST_STAGING_TABLE_QUOTED,
                ),
            ),
        );
        $this->bqClient->runQuery(
            $this->bqClient->query(
                sprintf(
                    'INSERT INTO %s.%s(`pk1`,`pk2`,`col1`,`col2`) VALUES (\'2\',\'2\',\'2\',\'2\')',
                    self::getTestDbQuoted(),
                    self::TEST_STAGING_TABLE_QUOTED,
                ),
            ),
        );

        if ($includeEmptyValues) {
            $this->bqClient->runQuery(
                $this->bqClient->query(
                    sprintf(
                        'INSERT INTO %s.%s(`pk1`,`pk2`,`col1`,`col2`) VALUES (\'2\',\'2\',\'\',NULL)',
                        self::getTestDbQuoted(),
                        self::TEST_STAGING_TABLE_QUOTED,
                    ),
                ),
            );
        }

        return $def;
    }

    private function assertTableNotExists(string $schemaName, string $tableName): void
    {
        try {
            (new BigqueryTableReflection($this->bqClient, $schemaName, $tableName))->getTableStats();
            self::fail(
                sprintf(
                    'Table "%s.%s" is expected to not exist.',
                    $schemaName,
                    $tableName,
                ),
            );
        } catch (Throwable $e) {
        }
    }

    public function testGetDropTableIfExistsCommand(): void
    {
        $this->createTestDb();
        $this->assertTableNotExists(self::getTestDb(), self::TEST_TABLE);

        // check that it cannot find non-existing table
        $sql = $this->getBuilder()->getTableExistsCommand(self::getTestDb(), self::TEST_TABLE);
        self::assertSqlEquals(
        // phpcs:ignore
            "SELECT COUNT(*) AS count FROM `import_export_test_schema`.INFORMATION_SCHEMA.TABLES WHERE `table_type` != 'VIEW' AND table_name = 'import_export_test_test';", $sql
        );
        $queryResults = $this->bqClient->runQuery($this->bqClient->query($sql));
        $current = (array) $queryResults->getIterator()->current();
        $this->assertEquals(0, $current['count']);

        // try to drop not existing table
        try {
            $sql = $this->getBuilder()->getDropTableUnsafe(self::getTestDb(), self::TEST_TABLE);
            self::assertSqlEquals(
            // phpcs:ignore
                'DROP TABLE `import_export_test_schema`.`import_export_test_test`',
                $sql,
            );
            $this->bqClient->runQuery($this->bqClient->query($sql));
        } catch (NotFoundException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertStringContainsString('import_export_test_test', $e->getMessage());
        }

        // create table
        $this->initSingleTable(self::getTestDb(), self::TEST_TABLE);

        // check that the table exists already
        $sql = $this->getBuilder()->getTableExistsCommand(self::getTestDb(), self::TEST_TABLE);
        $queryResults = $this->bqClient->runQuery($this->bqClient->query($sql));
        $current = (array) $queryResults->getIterator()->current();
        $this->assertEquals(1, $current['count']);

        // drop existing table
        $sql = $this->getBuilder()->getDropTableUnsafe(self::getTestDb(), self::TEST_TABLE);
        $this->bqClient->runQuery($this->bqClient->query($sql));

        // check that the table doesn't exist anymore
        $sql = $this->getBuilder()->getTableExistsCommand(self::getTestDb(), self::TEST_TABLE);
        $queryResults = $this->bqClient->runQuery($this->bqClient->query($sql));
        $current = (array) $queryResults->getIterator()->current();
        $this->assertEquals(0, $current['count']);
    }

    public static function getInsertAllIntoTargetTableCommandProvider(): Generator
    {
        yield 'typed' => [
            BigqueryImportOptions::USING_TYPES_USER,
            // phpcs:ignore
            'INSERT INTO `import_export_test_schema`.`import_export_test_test` (`col1`, `col2`) SELECT `src`.`col1`,`src`.`col2` FROM `import_export_test_schema`.`stagingTable` AS `src`',
        ];
        yield 'string' => [
            BigqueryImportOptions::USING_TYPES_STRING,
            // phpcs:ignore
            'INSERT INTO `import_export_test_schema`.`import_export_test_test` (`col1`, `col2`) SELECT CAST(COALESCE(`src`.`col1`, \'\') as STRING) AS `col1`,CAST(COALESCE(`src`.`col2`, \'\') as STRING) AS `col2` FROM `import_export_test_schema`.`stagingTable` AS `src`',
        ];
    }

    /**
     * @param BigqueryImportOptions::USING_TYPES_* $usingTypes
     */
    #[DataProvider('getInsertAllIntoTargetTableCommandProvider')]
    public function testGetInsertAllIntoTargetTableCommand(string $usingTypes, string $expectedSql): void
    {
        $this->createTestDb();
        $destination = $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);

        // create fake stage and say that there is less columns
        $fakeStage = new BigqueryTableDefinition(
            self::getTestDb(),
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
                ],),
            [],
        );

        // no convert values no timestamp
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            new BigqueryImportOptions(
                [],
                false,
                false,
                BigqueryImportOptions::SKIP_NO_LINE,
                $usingTypes,
            ),
            '2020-01-01 00:00:00',
        );

        self::assertSqlEquals($expectedSql, $sql);

        $out = $this->bqClient->runQuery($this->bqClient->query($sql));
        self::assertEquals(4, $out->info()['numDmlAffectedRows']);

        $result = $this->bqClient->runQuery(
            $this->bqClient->query(
                sprintf(
                    'SELECT * FROM %s.%s',
                    BigqueryQuote::quoteSingleIdentifier(self::getTestDb()),
                    BigqueryQuote::quoteSingleIdentifier(self::TEST_TABLE),
                ),
            ),
        );

        /** @var array<int, array<string, mixed>> $resultArray */
        $resultArray = iterator_to_array($result->getIterator());
        self::assertEqualsCanonicalizing(
            [
            [
                'id' => null,
                'col1' => '1',
                'col2' => '1',
            ],
            [
                'id' => null,
                'col1' => '1',
                'col2' => '1',
            ],
            [
                'id' => null,
                'col1' => '2',
                'col2' => '2',
            ],
            [
                'id' => null,
                'col1' => '',
                'col2' => '',
            ],
            ],
            $resultArray,
        );
    }

    protected function createTestTableWithColumns(
        bool $includeTimestamp = false,
        bool $includePrimaryKey = false,
    ): BigqueryTableDefinition {
        $columns = [];
        $pks = [];
        if ($includePrimaryKey) {
            $pks[] = 'id';
            $columns[] = new BigqueryColumn(
                'id',
                new Bigquery(Bigquery::TYPE_INT),
            );
        } else {
            $columns[] = $this->createNullableGenericColumn('id');
        }
        $columns[] = $this->createNullableGenericColumn('col1');
        $columns[] = $this->createNullableGenericColumn('col2');

        if ($includeTimestamp) {
            $columns[] = new BigqueryColumn(
                '_timestamp',
                new Bigquery(Bigquery::TYPE_TIMESTAMP),
            );
        }

        $tableDefinition = new BigqueryTableDefinition(
            self::getTestDb(),
            self::TEST_TABLE,
            false,
            new ColumnCollection($columns),
            $pks,
        );
        $this->bqClient->runQuery(
            $this->bqClient->query(
                (new BigqueryTableQueryBuilder())->getCreateTableCommandFromDefinition($tableDefinition),
            ),
        );

        return $tableDefinition;
    }

    private function createNullableGenericColumn(string $columnName): BigqueryColumn
    {
        $definition = new Bigquery(
            Bigquery::TYPE_STRING,
            [
                'length' => '50', // should be changed to max in future
                'nullable' => true,
            ],
        );

        return new BigqueryColumn(
            $columnName,
            $definition,
        );
    }

    /**
     * @return Generator<string, array{
     *     BigqueryImportOptions::USING_TYPES_*,
     *     string
     * }>
     */
    public static function getInsertAllIntoTargetTableCommandConvertToNullProvider(): Generator
    {
        yield 'typed' => [ // nothing is converted
            BigqueryImportOptions::USING_TYPES_USER,
            // phpcs:ignore
            'INSERT INTO `import_export_test_schema`.`import_export_test_test` (`col1`, `col2`) SELECT `src`.`col1`,`src`.`col2` FROM `import_export_test_schema`.`stagingTable` AS `src`'
        ];
        yield 'string' => [
            BigqueryImportOptions::USING_TYPES_STRING,
            // phpcs:ignore
            'INSERT INTO `import_export_test_schema`.`import_export_test_test` (`col1`, `col2`) SELECT NULLIF(`src`.`col1`, \'\'),CAST(COALESCE(`src`.`col2`, \'\') as STRING) AS `col2` FROM `import_export_test_schema`.`stagingTable` AS `src`',
        ];
    }

    /**
     * @param BigqueryImportOptions::USING_TYPES_* $usingTypes
     */
    #[DataProvider('getInsertAllIntoTargetTableCommandConvertToNullProvider')]
    public function testGetInsertAllIntoTargetTableCommandConvertToNull(string $usingTypes, string $expectedSql): void
    {
        $this->createTestDb();
        $destination = $this->createTestTableWithColumns();

        $this->createStagingTableWithData(true);
        // create fake stage and say that there is less columns
        $fakeStage = new BigqueryTableDefinition(
            self::getTestDb(),
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
                ],),
            [],
        );

        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            new BigqueryImportOptions(
                ['col1'], // convert col1 to null
                false,
                false,
                BigqueryImportOptions::SKIP_NO_LINE,
                $usingTypes,
            ),
            '2020-01-01 00:00:00',
        );
        self::assertSqlEquals($expectedSql, $sql);
        $out = $this->bqClient->runQuery($this->bqClient->query($sql));
        self::assertEquals(4, $out->info()['numDmlAffectedRows']);

        $result = $this->bqClient->runQuery(
            $this->bqClient->query(
                sprintf(
                    'SELECT * FROM %s',
                    self::getTestTableInDb(),
                ),
            ),
        );

        /** @var array<int, array<string, mixed>> $resultArray */
        $resultArray = iterator_to_array($result->getIterator());
        self::assertEqualsCanonicalizing(
            [
            [
                'id' => null,
                'col1' => '1',
                'col2' => '1',
            ],
            [
                'id' => null,
                'col1' => '1',
                'col2' => '1',
            ],
            [
                'id' => null,
                'col1' => '2',
                'col2' => '2',
            ],
            [
                'id' => null,
                'col1' => null,
                'col2' => '',
            ],
            ],
            $resultArray,
        );
    }

    /**
     * @return Generator<string, array{
     *     BigqueryImportOptions::USING_TYPES_*,
     *     string
     * }>
     */
    public static function getInsertAllIntoTargetTableCommandConvertToNullWithTimestampProvider(): Generator
    {
        yield 'typed' => [ // nothing is converted
            BigqueryImportOptions::USING_TYPES_USER,
            // phpcs:ignore
            'INSERT INTO `import_export_test_schema`.`import_export_test_test` (`col1`, `col2`, `_timestamp`) SELECT `src`.`col1`,`src`.`col2`,CAST(\'2020-01-01 00:00:00\' as TIMESTAMP) FROM `import_export_test_schema`.`stagingTable` AS `src`'
        ];
        yield 'string' => [
            BigqueryImportOptions::USING_TYPES_STRING,
            // phpcs:ignore
            'INSERT INTO `import_export_test_schema`.`import_export_test_test` (`col1`, `col2`, `_timestamp`) SELECT NULLIF(`src`.`col1`, \'\'),CAST(COALESCE(`src`.`col2`, \'\') as STRING) AS `col2`,CAST(\'2020-01-01 00:00:00\' as TIMESTAMP) FROM `import_export_test_schema`.`stagingTable` AS `src`',
        ];
    }

    /**
     * @param BigqueryImportOptions::USING_TYPES_* $usingTypes
     */
    #[DataProvider('getInsertAllIntoTargetTableCommandConvertToNullWithTimestampProvider')]
    public function testGetInsertAllIntoTargetTableCommandConvertToNullWithTimestamp(
        string $usingTypes,
        string $expectedSql,
    ): void {
        $this->createTestDb();
        $destination = $this->createTestTableWithColumns(true);
        $this->createStagingTableWithData(true);
        // create fake stage and say that there is less columns
        $fakeStage = new BigqueryTableDefinition(
            self::getTestDb(),
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
                ],),
            [],
        );

        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            new BigqueryImportOptions(
                ['col1'],
                false,
                true, // use timestamp
                BigqueryImportOptions::SKIP_NO_LINE,
                $usingTypes,
            ),
            '2020-01-01 00:00:00',
        );
        self::assertSqlEquals($expectedSql, $sql);
        $out = $this->bqClient->runQuery($this->bqClient->query($sql));
        self::assertEquals(4, $out->info()['numDmlAffectedRows']);

        $result = $this->bqClient->runQuery(
            $this->bqClient->query(
                sprintf(
                    'SELECT * FROM %s',
                    self::getTestTableInDb(),
                ),
            ),
        );

        /** @var array<string, string> $item */
        foreach ($result as $item) {
            self::assertArrayHasKey('id', $item);
            self::assertArrayHasKey('col1', $item);
            self::assertArrayHasKey('col2', $item);
            self::assertArrayHasKey('_timestamp', $item);
        }
    }

    /**
     * Tests CAST(NULLIF(...) AS <type>) in INSERT when destination columns are
     * non-STRING (e.g. TIMESTAMP) but staging is STRING (cross-backend CSV load).
     */
    public function testGetInsertAllIntoTargetTableCommandConvertToNullWithTypedDestColumns(): void
    {
        $this->createTestDb();

        // Destination table with TIMESTAMP column (nullable)
        $destinationColumns = [
            $this->createNullableGenericColumn('id'),
            $this->createNullableGenericColumn('col1'),
            new BigqueryColumn('col2', new Bigquery(Bigquery::TYPE_TIMESTAMP, ['nullable' => true])),
        ];
        $destination = new BigqueryTableDefinition(
            self::getTestDb(),
            self::TEST_TABLE,
            false,
            new ColumnCollection($destinationColumns),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $this->bqClient->runQuery(
            $this->bqClient->query(
                $qb->getCreateTableCommandFromDefinition($destination),
            ),
        );

        // Staging table with STRING data (STRING_TABLE strategy)
        $this->createStagingTableWithData(true);
        $fakeStage = new BigqueryTableDefinition(
            self::getTestDb(),
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
                ],),
            [],
        );

        // Fake destination matching staging columns: col1 STRING, col2 TIMESTAMP
        $fakeDestination = new BigqueryTableDefinition(
            self::getTestDb(),
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                new BigqueryColumn('col2', new Bigquery(Bigquery::TYPE_TIMESTAMP, ['nullable' => true])),
                ],),
            [],
        );

        // convertEmptyValuesToNull on col2 (TIMESTAMP destination)
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $fakeDestination,
            new BigqueryImportOptions(
                ['col2'], // convert col2 to null when empty
                false,
                false,
                BigqueryImportOptions::SKIP_NO_LINE,
                BigqueryImportOptions::USING_TYPES_STRING,
            ),
            '2020-01-01 00:00:00',
        );

        // col1: no convert, STRING dest → CAST(COALESCE(...) as STRING)
        // col2: convert, TIMESTAMP dest → CAST(NULLIF(...) AS TIMESTAMP)
        self::assertSqlEquals(
            // phpcs:ignore
            'INSERT INTO `import_export_test_schema`.`import_export_test_test` (`col1`, `col2`) SELECT CAST(COALESCE(`src`.`col1`, \'\') as STRING) AS `col1`,CAST(NULLIF(`src`.`col2`, \'\') AS TIMESTAMP) AS `col2` FROM `import_export_test_schema`.`stagingTable` AS `src`',
            $sql,
        );

        // The staging data has col2 values '1', '2', '2', '' — '1' and '2' are not valid
        // timestamps, so we only verify the SQL is correct rather than executing it with
        // real timestamp data. The E2E test in connection covers the full data path.
    }

    public function testGetTruncateTableCommand(): void
    {
        $this->createTestDb();
        $this->createStagingTableWithData();

        $ref = new BigqueryTableReflection($this->bqClient, self::getTestDb(), self::TEST_STAGING_TABLE);
        self::assertEquals(3, $ref->getRowsCount());

        $sql = $this->getBuilder()->getTruncateTable(self::getTestDb(), self::TEST_STAGING_TABLE);
        self::assertSqlEquals(
            'TRUNCATE TABLE `import_export_test_schema`.`stagingTable`',
            $sql,
        );
        $this->bqClient->runQuery($this->bqClient->query($sql));
        $ref->refresh();
        self::assertEquals(0, $ref->getRowsCount());
    }


    private function getStagingTableDefinition(): BigqueryTableDefinition
    {
        return new BigqueryTableDefinition(
            self::getTestDb(),
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('pk1'),
                $this->createNullableGenericColumn('pk2'),
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
                ],),
            [],
        );
    }
}
