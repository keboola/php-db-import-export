<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse;

use DateTime;
use Generator;
use Keboola\Datatype\Definition\Synapse;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;
use Keboola\TableBackendUtils\Table\Synapse\TableDistributionDefinition;
use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;
use Keboola\TableBackendUtils\Utils\CaseConverter;

class SqlBuilderTest extends SynapseBaseTestCase
{
    public const TESTS_PREFIX = 'import-export-test-ng_';
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'schema';
    public const TEST_SCHEMA_QUOTED = '[' . self::TEST_SCHEMA . ']';
    public const TEST_STAGING_TABLE = '#stagingTable';
    public const TEST_TABLE = self::TESTS_PREFIX . 'test';
    public const TEST_TABLE_IN_SCHEMA = self::TEST_SCHEMA_QUOTED . '.' . self::TEST_TABLE_QUOTED;
    public const TEST_TABLE_QUOTED = '[' . self::TEST_TABLE . ']';

    protected function dropTestSchema(): void
    {
        $this->connection->exec(sprintf('DROP SCHEMA %s', self::TEST_SCHEMA_QUOTED));
    }

    protected function getBuilder(): SqlBuilder
    {
        return new SqlBuilder();
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema(self::TEST_SCHEMA);
    }

    protected function createTestSchema(): void
    {
        $this->connection->exec(sprintf('CREATE SCHEMA %s', self::TEST_SCHEMA_QUOTED));
    }

    public function testGetDedupCommand(): void
    {
        $this->createTestSchema();
        $stageDef = $this->createStagingTableWithData();

        $deduplicationDef = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            '#tempTable',
            true,
            new ColumnCollection([
                SynapseColumn::createGenericColumn('col1'),
                SynapseColumn::createGenericColumn('col2'),
            ]),
            [
                'pk1',
                'pk2',
            ],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX)
        );
        $qb = new SynapseTableQueryBuilder();
        $this->connection->exec($qb->getCreateTableCommandFromDefinition($deduplicationDef));

        $sql = $this->getBuilder()->getDedupCommand(
            $stageDef,
            $deduplicationDef,
            $deduplicationDef->getPrimaryKeysNames()
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO [IMPORT-EXPORT-TEST-NG_SCHEMA].[#TEMPTABLE] ([COL1], [COL2]) SELECT a.[COL1],a.[COL2] FROM (SELECT [COL1], [COL2], ROW_NUMBER() OVER (PARTITION BY [PK1],[PK2] ORDER BY [PK1],[PK2]) AS "_row_number_" FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]) AS a WHERE a."_row_number_" = 1',
            $sql
        );
        $this->connection->exec($sql);
        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s',
            SynapseQuote::quoteSingleIdentifier($deduplicationDef->getSchemaName()),
            SynapseQuote::quoteSingleIdentifier($deduplicationDef->getTableName())
        ));

        self::assertCount(2, $result);
    }

    private function createStagingTableWithData(bool $includeEmptyValues = false): SynapseTableDefinition
    {
        $def = $this->getStagingTableDefinition();
        $qb = new SynapseTableQueryBuilder();
        $this->connection->exec($qb->getCreateTableCommandFromDefinition($def));

        $this->connection->exec(
            sprintf(
                'INSERT INTO %s.%s([pk1],[pk2],[col1],[col2]) VALUES (1,1,\'1\',\'1\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE
            )
        );
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s.%s([pk1],[pk2],[col1],[col2]) VALUES (1,1,\'1\',\'1\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE
            )
        );
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s.%s([pk1],[pk2],[col1],[col2]) VALUES (2,2,\'2\',\'2\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE
            )
        );

        if ($includeEmptyValues) {
            $this->connection->exec(
                sprintf(
                    'INSERT INTO %s.%s([pk1],[pk2],[col1],[col2]) VALUES (2,2,\'\',NULL)',
                    self::TEST_SCHEMA_QUOTED,
                    self::TEST_STAGING_TABLE
                )
            );
        }

        return $def;
    }

    private function getDummyImportOptions(): SynapseImportOptions
    {
        return new SynapseImportOptions([]);
    }

    public function testGetDeleteOldItemsCommand(): void
    {
        $this->createTestSchema();

        $tableDefinition = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            new ColumnCollection([
                new SynapseColumn(
                    'id',
                    new Synapse(
                        Synapse::TYPE_INT
                    )
                ),
                SynapseColumn::createGenericColumn('pk1'),
                SynapseColumn::createGenericColumn('pk2'),
                SynapseColumn::createGenericColumn('col1'),
                SynapseColumn::createGenericColumn('col2'),
            ]),
            ['pk1', 'pk2'],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX)
        );
        $tableSql = sprintf(
            '%s.%s',
            SynapseQuote::quoteSingleIdentifier($tableDefinition->getSchemaName()),
            SynapseQuote::quoteSingleIdentifier($tableDefinition->getTableName())
        );
        $qb = new SynapseTableQueryBuilder();
        $this->connection->exec($qb->getCreateTableCommandFromDefinition($tableDefinition));
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id],[pk1],[pk2],[col1],[col2]) VALUES (1,1,1,\'1\',\'1\')',
                $tableSql
            )
        );
        $stagingTableDefinition = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            false,
            new ColumnCollection([
                SynapseColumn::createGenericColumn('pk1'),
                SynapseColumn::createGenericColumn('pk2'),
                SynapseColumn::createGenericColumn('col1'),
                SynapseColumn::createGenericColumn('col2'),
            ]),
            ['pk1', 'pk2'],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX)
        );
        $this->connection->exec($qb->getCreateTableCommandFromDefinition($stagingTableDefinition));
        $stagingTableSql = sprintf(
            '%s.%s',
            SynapseQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            SynapseQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName())
        );
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([pk1],[pk2],[col1],[col2]) VALUES (1,1,\'1\',\'1\')',
                $stagingTableSql
            )
        );
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([pk1],[pk2],[col1],[col2]) VALUES (2,1,\'1\',\'1\')',
                $stagingTableSql
            )
        );

        $sql = $this->getBuilder()->getDeleteOldItemsCommand(
            $stagingTableDefinition,
            $tableDefinition
        );

        $this->assertEquals(
        // phpcs:ignore
            'DELETE [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE] WHERE EXISTS (SELECT * FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WHERE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST].[PK1] = [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE].[PK1] AND [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST].[PK2] = [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE].[PK2])',
            $sql
        );
        $this->connection->exec($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            $stagingTableSql
        ));

        $this->assertCount(1, $result);
        $this->assertSame([
            [
                'PK1' => '2',
                'PK2' => '1',
                'COL1' => '1',
                'COL2' => '1',
            ],
        ], $result);
    }

    public function testGetDropCommand(): void
    {
        $this->createTestSchema();
        $this->createTestTable();
        $sql = $this->getBuilder()->getDropCommand(self::TEST_SCHEMA, self::TEST_TABLE);

        $this->assertEquals(
            'DROP TABLE [import-export-test-ng_schema].[import-export-test-ng_test]',
            $sql
        );

        $this->connection->exec($sql);

        $this->assertTableNotExists(self::TEST_SCHEMA, self::TEST_TABLE);
    }

    private function assertTableNotExists(string $schemaName, string $tableName): void
    {
        try {
            (new SynapseTableReflection($this->connection, $schemaName, $tableName))->getObjectId();
            self::fail(sprintf(
                'Table "%s.%s" is expected to not exist.',
                $schemaName,
                $tableName
            ));
        } catch (TableNotExistsReflectionException $e) {
            $this->assertEquals(sprintf(
                'Table "%s.%s" does not exist.',
                CaseConverter::stringToUpper($schemaName),
                CaseConverter::stringToUpper($tableName)
            ), $e->getMessage());
        }
    }

    public function testGetDropTableIfExistsCommand(): void
    {
        $this->assertTableNotExists(self::TEST_SCHEMA, self::TEST_TABLE);

        // try to drop not existing table
        $sql = $this->getBuilder()->getDropTableIfExistsCommand(self::TEST_SCHEMA, self::TEST_TABLE);
        $this->assertEquals(
        // phpcs:ignore
            'IF OBJECT_ID (N\'[import-export-test-ng_schema].[import-export-test-ng_test]\', N\'U\') IS NOT NULL DROP TABLE [import-export-test-ng_schema].[import-export-test-ng_test]',
            $sql
        );
        $this->connection->exec($sql);

        // create table
        $this->createTestSchema();
        $this->createTestTable();

        // try to drop not existing table
        $sql = $this->getBuilder()->getDropTableIfExistsCommand(self::TEST_SCHEMA, self::TEST_TABLE);
        $this->assertEquals(
        // phpcs:ignore
            'IF OBJECT_ID (N\'[import-export-test-ng_schema].[import-export-test-ng_test]\', N\'U\') IS NOT NULL DROP TABLE [import-export-test-ng_schema].[import-export-test-ng_test]',
            $sql
        );
        $this->connection->exec($sql);

        $this->assertTableNotExists(self::TEST_SCHEMA, self::TEST_TABLE);
    }

    protected function createTestTable(): void
    {
        $table = self::TEST_TABLE_IN_SCHEMA;
        $this->connection->exec(<<<EOT
CREATE TABLE $table (
    id int NOT NULL
)
WITH
    (
      PARTITION ( id RANGE LEFT FOR VALUES ( )),
      CLUSTERED COLUMNSTORE INDEX
    )
EOT
        );
    }

    public function testGetInsertAllIntoTargetTableCommand(): void
    {
        $this->createTestSchema();
        $destination = $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);

        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // no convert values no timestamp
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $this->getDummyImportOptions(),
            '2020-01-01 00:00:00'
        );

        $this->assertEquals(
        // phpcs:ignore
            'INSERT INTO [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] ([COL1], [COL2]) (SELECT CAST(COALESCE([COL1], \'\') as NVARCHAR(4000)) AS [COL1],CAST(COALESCE([COL2], \'\') as NVARCHAR(4000)) AS [COL2] FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE] AS [src])',
            $sql
        );

        $out = $this->connection->exec($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
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
        ], $result);
    }

    public function testGetInsertAllIntoTargetTableCommandNotString(): void
    {
        $col2 = new SynapseColumn(
            'col2',
            new Synapse(
                Synapse::TYPE_NUMERIC
            )
        );

        $this->createTestSchema();
        $destination = $this->createTestTableWithColumns(false, false, $col2);
        $this->createStagingTableWithData(true);

        // create fake stage with missing id column and numeric col2
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $col2,
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // no convert values no timestamp
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $this->getDummyImportOptions(),
            '2020-01-01 00:00:00'
        );

        $this->assertEquals(
        // phpcs:ignore
            'INSERT INTO [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] ([COL1], [COL2]) (SELECT CAST(COALESCE([COL1], \'\') as NVARCHAR(4000)) AS [COL1],CAST([COL2] as NUMERIC) AS [COL2] FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE] AS [src])',
            $sql
        );

        $out = $this->connection->exec($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
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
                'col2' => null,
            ],
        ], $result);
    }

    protected function createTestTableWithColumns(
        bool $includeTimestamp = false,
        bool $includePrimaryKey = false,
        ?SynapseColumn $overwriteColumn2 = null
    ): SynapseTableDefinition {
        $tableDefinition = $this->getTestTableWithColumnsDefinition(
            $includeTimestamp,
            $includePrimaryKey,
            $overwriteColumn2
        );
        $this->connection->exec(
            (new SynapseTableQueryBuilder())->getCreateTableCommandFromDefinition($tableDefinition)
        );

        return $tableDefinition;
    }

    private function createGenericColumn(string $columnName, bool $nullable = true): SynapseColumn
    {
        $definition = new Synapse(
            Synapse::TYPE_NVARCHAR,
            [
                'length' => '4000', // should be changed to max in future
                'nullable' => $nullable,
            ]
        );

        return new SynapseColumn(
            $columnName,
            $definition
        );
    }

    public function testGetInsertAllIntoTargetTableCommandConvertToNull(): void
    {
        $this->createTestSchema();
        $destination = $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // convert col1 to null
        $options = new SynapseImportOptions(['col1']);
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
        // phpcs:ignore
            'INSERT INTO [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] ([COL1], [COL2]) (SELECT NULLIF([COL1], \'\'),CAST(COALESCE([COL2], \'\') as NVARCHAR(4000)) AS [COL2] FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE] AS [src])',
            $sql
        );
        $out = $this->connection->exec($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
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
        ], $result);
    }

    public function testGetInsertAllIntoTargetTableCommandConvertToNullWithTimestamp(): void
    {
        $this->createTestSchema();
        $destination = $this->createTestTableWithColumns(true);
        $this->createStagingTableWithData(true);
        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // use timestamp
        $options = new SynapseImportOptions(['col1'], false, true);
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
        // phpcs:ignore
            'INSERT INTO [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] ([COL1], [COL2], [_TIMESTAMP]) (SELECT NULLIF([COL1], \'\'),CAST(COALESCE([COL2], \'\') as NVARCHAR(4000)) AS [COL2],\'2020-01-01 00:00:00\' FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE] AS [src])',
            $sql
        );
        $out = $this->connection->exec($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        foreach ($result as $item) {
            $this->assertArrayHasKey('ID', $item);
            $this->assertArrayHasKey('COL1', $item);
            $this->assertArrayHasKey('COL2', $item);
            $this->assertArrayHasKey('_TIMESTAMP', $item);
        }
    }

    public function testGetRenameTableCommand(): void
    {
        $renameTo = 'newTable';
        $this->createTestSchema();
        $this->createTestTableWithColumns();
        $sql = $this->getBuilder()->getRenameTableCommand(self::TEST_SCHEMA, self::TEST_TABLE, $renameTo);

        $this->assertEquals(
            'RENAME OBJECT [import-export-test-ng_schema].[import-export-test-ng_test] TO [newTable]',
            $sql
        );

        $this->connection->exec($sql);

        $this->assertTableNotExists(self::TEST_SCHEMA, self::TEST_TABLE);

        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, $renameTo);
        $this->assertIsString($ref->getObjectId());
    }

    public function testGetTruncateTableWithDeleteCommand(): void
    {
        $this->createTestSchema();
        $this->createStagingTableWithData();

        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        $this->assertEquals(3, $ref->getRowsCount());

        $sql = $this->getBuilder()->getTruncateTableWithDeleteCommand(self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        $this->assertEquals(
            'DELETE FROM [import-export-test-ng_schema].[#stagingTable]',
            $sql
        );
        $this->connection->exec($sql);
        $this->assertEquals(0, $ref->getRowsCount());
    }

    public function testGetUpdateWithPkCommand(): void
    {
        $this->createTestSchema();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake destination and say that there is pk on col1
        $fakeDestination = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            ['col1'],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id],[col1],[col2]) VALUES (1,\'2\',\'1\')',
                self::TEST_TABLE_IN_SCHEMA
            )
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEquals([
            [
                'ID' => '1',
                'COL1' => '2',
                'COL2' => '1',
            ],
        ], $result);

        // no convert values no timestamp
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $this->getDummyImportOptions(),
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
        // phpcs:ignore
            'UPDATE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] SET [COL2] = COALESCE([src].[COL2], \'\') FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE] AS [src] WHERE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST].[COL1] = [src].[COL1] AND (COALESCE(CAST([IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST].[COL2] AS NVARCHAR(4000)), \'\') != COALESCE([src].[COL2], \'\')) ',
            $sql
        );
        $this->connection->exec($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEquals([
            [
                'ID' => '1',
                'COL1' => '2',
                'COL2' => '2',
            ],
        ], $result);
    }

    public function testGetUpdateWithPkCommandOnlyPKs(): void
    {
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            new SynapseTableDefinition(
                self::TEST_SCHEMA,
                self::TEST_STAGING_TABLE,
                true,
                new ColumnCollection([
                    $this->createGenericColumn('id'),
                ]),
                [],
                new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
                new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
            ),
            new SynapseTableDefinition(
                self::TEST_SCHEMA,
                self::TEST_TABLE,
                true,
                new ColumnCollection([
                    $this->createGenericColumn('id'),
                ]),
                ['id'],
                new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
                new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
            ),
            $this->getDummyImportOptions(),
            '2020-01-01 00:00:00'
        );

        // only primary keys will result to empty string as there is nothing to update
        $this->assertEquals(
            '',
            $sql
        );
    }

    public function testGetUpdateWithPkCommandNotString(): void
    {
        $col2 = new SynapseColumn(
            'col2',
            new Synapse(
                Synapse::TYPE_NUMERIC
            )
        );

        $this->createTestSchema();
        $this->createTestTableWithColumns(false, false, $col2);
        $this->createStagingTableWithData(true);
        // create fake destination and say that there is pk on col1
        $fakeDestination = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $col2,
            ]),
            ['col1'],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $col2,
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id],[col1],[col2]) VALUES (1,\'2\',\'1\')',
                self::TEST_TABLE_IN_SCHEMA
            )
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEquals([
            [
                'ID' => '1',
                'COL1' => '2',
                'COL2' => '1',
            ],
        ], $result);

        // no convert values no timestamp
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $this->getDummyImportOptions(),
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
        // phpcs:ignore
            'UPDATE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] SET [COL2] = [src].[COL2] FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE] AS [src] WHERE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST].[COL1] = [src].[COL1] AND (CAST([IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST].[COL2] AS NUMERIC) != [src].[COL2]) ',
            $sql
        );
        $this->connection->exec($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEquals([
            [
                'ID' => '1',
                'COL1' => '2',
                'COL2' => '2',
            ],
        ], $result);
    }

    public function testGetUpdateWithPkCommandConvertValues(): void
    {
        $this->createTestSchema();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake destination and say that there is pk on col1
        $fakeDestination = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            ['col1'],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id],[col1],[col2]) VALUES (1,\'\',\'1\')',
                self::TEST_TABLE_IN_SCHEMA
            )
        );
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id],[col1],[col2]) VALUES (1,\'2\',\'\')',
                self::TEST_TABLE_IN_SCHEMA
            )
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
            [
                'id' => '1',
                'col1' => '',
                'col2' => '1',
            ],
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '',
            ],
        ], $result);

        $options = new SynapseImportOptions(['col1']);

        // converver values
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $options,
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
        // phpcs:ignore
            'UPDATE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] SET [COL2] = COALESCE([src].[COL2], \'\') FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE] AS [src] WHERE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST].[COL1] = [src].[COL1] AND (COALESCE(CAST([IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST].[COL2] AS NVARCHAR(4000)), \'\') != COALESCE([src].[COL2], \'\')) ',
            $sql
        );
        $this->connection->exec($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
            [
                'id' => '1',
                'col1' => null,
                'col2' => '',
            ],
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '2',
            ],
        ], $result);
    }

    public function testGetUpdateWithPkCommandConvertValuesWithTimestamp(): void
    {
        $timestampInit = new DateTime('2020-01-01 00:00:01');
        $timestampSet = new DateTime('2020-01-01 01:01:01');
        $this->createTestSchema();
        $this->createTestTableWithColumns(true);
        $this->createStagingTableWithData(true);

        // create fake destination and say that there is pk on col1
        $fakeDestination = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            ['col1'],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id],[col1],[col2],[_timestamp]) VALUES (1,\'\',\'1\',\'%s\')',
                self::TEST_TABLE_IN_SCHEMA,
                $timestampInit->format(DateTimeHelper::FORMAT)
            )
        );
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id],[col1],[col2],[_timestamp]) VALUES (1,\'2\',\'\',\'%s\')',
                self::TEST_TABLE_IN_SCHEMA,
                $timestampInit->format(DateTimeHelper::FORMAT)
            )
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
            [
                'id' => '1',
                'col1' => '',
                'col2' => '1',
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT) . '.000',
            ],
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '',
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT) . '.000',
            ],
        ], $result);

        // use timestamp
        $options = new SynapseImportOptions(['col1'], false, true);
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $options,
            $timestampSet->format(DateTimeHelper::FORMAT) . '.000'
        );

        $this->assertEquals(
        // phpcs:ignore
            'UPDATE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] SET [COL2] = COALESCE([src].[COL2], \'\'), [_TIMESTAMP] = \'2020-01-01 01:01:01.000\' FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE] AS [src] WHERE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST].[COL1] = [src].[COL1] AND (COALESCE(CAST([IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST].[COL2] AS NVARCHAR(4000)), \'\') != COALESCE([src].[COL2], \'\')) ',
            $sql
        );
        $this->connection->exec($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        foreach ($result as $item) {
            $this->assertArrayHasKey('ID', $item);
            $this->assertArrayHasKey('COL1', $item);
            $this->assertArrayHasKey('COL2', $item);
            $this->assertArrayHasKey('_TIMESTAMP', $item);
            $this->assertSame(
                $timestampSet->format(DateTimeHelper::FORMAT),
                (new DateTime($item['_TIMESTAMP']))->format(DateTimeHelper::FORMAT)
            );
        }
    }

    public function testGetUpdateWithPkCommandOnlyPksWithTimestamp(): void
    {
        $timestampInit = new DateTime('2020-01-01 00:00:01');
        $timestampSet = new DateTime('2020-01-01 01:01:01');
        $this->createTestSchema();

        // destination has only id column which is also PK and timestamp
        $destDef = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            new ColumnCollection([
                new SynapseColumn(
                    'id',
                    new Synapse(Synapse::TYPE_INT)
                ),
                new SynapseColumn(
                    '_timestamp',
                    new Synapse(Synapse::TYPE_DATETIME)
                ),
            ]),
            ['id'],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX)
        );
        $qb = new SynapseTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($destDef));

        // destination has only id column
        $stageDef = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('id'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($stageDef));

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s([id]) VALUES (1)',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE
            )
        );

        // create fake destination definition without timestamp column
        $fakeDestination = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('id'),
            ]),
            ['id'],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // insert values into destination
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s([id],[_timestamp]) VALUES (1,\'%s\')',
                self::TEST_TABLE_IN_SCHEMA,
                $timestampInit->format(DateTimeHelper::FORMAT)
            )
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s([id],[_timestamp]) VALUES (1,\'%s\')',
                self::TEST_TABLE_IN_SCHEMA,
                $timestampInit->format(DateTimeHelper::FORMAT)
            )
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
            [
                'id' => '1',
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT) . '.000',
            ],
            [
                'id' => '1',
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT) . '.000',
            ],
        ], $result);

        // use timestamp
        $options = new SynapseImportOptions([], false, true);
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $stageDef,
            $fakeDestination,
            $options,
            $timestampSet->format(DateTimeHelper::FORMAT) . '.000'
        );

        $this->assertEquals(
        // phpcs:ignore
            'UPDATE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] SET [_TIMESTAMP] = \'2020-01-01 01:01:01.000\' FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE] AS [src] WHERE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST].[ID] = [src].[ID] ',
            $sql
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        /** @var array{ID:string,_TIMESTAMP:string} $item */
        foreach ($result as $item) {
            $this->assertArrayHasKey('ID', $item);
            $this->assertArrayHasKey('_TIMESTAMP', $item);
            $this->assertSame(
                $timestampSet->format(DateTimeHelper::FORMAT),
                (new DateTime($item['_TIMESTAMP']))->format(DateTimeHelper::FORMAT)
            );
        }
    }

    public function testTransaction(): void
    {
        $this->createTestSchema();
        $this->createTestTable();

        $sql = $this->getBuilder()->getBeginTransaction();
        self::assertSame('BEGIN TRANSACTION', $sql);
        $this->connection->exec($sql);

        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id]) VALUES (1)',
                self::TEST_TABLE_IN_SCHEMA
            )
        );

        $sql = $this->getBuilder()->getCommitTransaction();
        self::assertSame('COMMIT', $sql);
        $this->connection->exec($sql);
    }

    /**
     * @return \Generator<string, array<mixed>>
     */
    public function ctasFunctionsProvider(): Generator
    {
        yield 'getCtasDedupCommand' => [
            'getCtasDedupCommand',
        ];
        yield 'getCTASInsertAllIntoTargetTableCommand' => [
            'getCTASInsertAllIntoTargetTableCommand',
        ];
    }

    /**
     * @dataProvider ctasFunctionsProvider
     */
    public function testCtasCommandFailOnMissingColumns(string $function): void
    {
        $stage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('pk1'),
                $this->createGenericColumn('pk2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        $destination = new SynapseTableDefinition(
            'schema',
            'tableDest',
            false,
            new ColumnCollection([
                $this->createGenericColumn('pk1'),
                $this->createGenericColumn('notExists'),
            ]),
            ['pk1'],
            $stage->getTableDistribution(),
            $stage->getTableIndex()
        );

        $this->expectException(Exception::class);
        $this->expectExceptionMessage(
            'Columns "PK2" can be imported as it was not found between columns "PK1, NOTEXISTS" of destination table.'
        );
        $this->getBuilder()->$function(
            $stage,
            $destination,
            new SynapseImportOptions(),
            '2020-01-01 00:00:00'
        );
    }

    /**
     * @dataProvider ctasDedupProvider
     */
    public function testGetCtasDedupCommand(
        SynapseTableDefinition $stage,
        SynapseTableDefinition $destination,
        SynapseImportOptions $options,
        string $expectedSql,
        bool $isTimestampExpected = true
    ): void {
        $this->createTestSchema();
        $this->createStagingTableWithData(true);

        $sql = $this->getBuilder()->getCtasDedupCommand(
            $stage,
            $destination,
            $options,
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
            $expectedSql,
            $sql
        );
        $out = $this->connection->executeStatement($sql);
        $this->assertEquals(2, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        foreach ($result as $item) {
            $this->assertArrayHasKey('PK1', $item);
            $this->assertArrayHasKey('PK2', $item);
            $this->assertArrayHasKey('COL1', $item);
            $this->assertArrayHasKey('COL2', $item);
            if ($isTimestampExpected) {
                $this->assertArrayHasKey('_TIMESTAMP', $item);
            } else {
                $this->assertArrayNotHasKey('_TIMESTAMP', $item);
            }
        }

        $ref = new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );
        /** @var SynapseColumn[] $timestampColumns */
        $timestampColumns = array_filter(iterator_to_array($ref->getColumnsDefinitions()), function (
            SynapseColumn $column
        ) {
            return $column->getColumnName() === '_TIMESTAMP';
        });
        if ($options->useTimestamp()) {
            self::assertCount(1, $timestampColumns);
            /** @var SynapseColumn $timestampColumn */
            $timestampColumn = array_shift($timestampColumns);
            self::assertSame(Synapse::TYPE_DATETIME2, $timestampColumn->getColumnDefinition()->getType());
        }

        $resultColumns = $ref->getColumnsDefinitions();
        $this->assertColumnsDefinitions($resultColumns, $destination, $options);
    }

    /**
     * @return \Generator<string, array{
     *  0: SynapseTableDefinition,
     *  1: SynapseTableDefinition,
     *  2: SynapseImportOptions,
     *  3: string,
     *  4?: bool
     * }>
     */
    public function ctasDedupProvider(): Generator
    {
        $stage = $this->getStagingTableDefinition();

        yield 'testGetCtasDedupCommandWithNotStringType' => [
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                $stage->getTableName(),
                $stage->isTemporary(),
                new ColumnCollection([
                    $this->createGenericColumn('pk1'),
                    $this->createGenericColumn('pk2'),
                    new SynapseColumn(
                        'col1',
                        new Synapse(
                            Synapse::TYPE_INT
                        )
                    ),
                    $this->createGenericColumn('col2'),
                ]),
                $stage->getPrimaryKeysNames(),
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                $stage->getColumnsDefinitions(),
                ['pk1', 'pk2'],
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseImportOptions(
                [],
                false,
                true,
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TABLE_TYPES_CAST
            ),
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=ROUND_ROBIN,HEAP) AS SELECT a.[PK1],a.[PK2],a.[COL1],a.[COL2],a.[_TIMESTAMP] FROM (SELECT COALESCE(CAST([PK1] as NVARCHAR(4000)), \'\') AS [PK1],COALESCE(CAST([PK2] as NVARCHAR(4000)), \'\') AS [PK2],COALESCE(CAST([COL1] as NVARCHAR(4000)), \'\') AS [COL1],COALESCE(CAST([COL2] as NVARCHAR(4000)), \'\') AS [COL2],CAST(\'2020-01-01 00:00:00\' as DATETIME2) AS [_TIMESTAMP], ROW_NUMBER() OVER (PARTITION BY [PK1],[PK2] ORDER BY [PK1],[PK2]) AS "_row_number_" FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]) AS a WHERE a."_row_number_" = 1',
        ];

        yield 'testGetCtasDedupCommandWithNotStringTypeNotNullable' => [
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                $stage->getTableName(),
                $stage->isTemporary(),
                new ColumnCollection([
                    $this->createGenericColumn('pk1'),
                    $this->createGenericColumn('pk2'),
                    new SynapseColumn(
                        'col1',
                        new Synapse(
                            Synapse::TYPE_INT
                        )
                    ),
                    $this->createGenericColumn('col2'),
                ]),
                $stage->getPrimaryKeysNames(),
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                new ColumnCollection([
                    $this->createGenericColumn('pk1'),
                    $this->createGenericColumn('pk2'),
                    $this->createGenericColumn('col1'),
                    $this->createGenericColumn('col2', false),
                ]),
                ['pk1', 'pk2'],
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseImportOptions(
                [],
                false,
                true,
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TABLE_TYPES_CAST
            ),
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=ROUND_ROBIN,HEAP) AS SELECT a.[PK1],a.[PK2],a.[COL1],a.[COL2],a.[_TIMESTAMP] FROM (SELECT COALESCE(CAST([PK1] as NVARCHAR(4000)), \'\') AS [PK1],COALESCE(CAST([PK2] as NVARCHAR(4000)), \'\') AS [PK2],COALESCE(CAST([COL1] as NVARCHAR(4000)), \'\') AS [COL1],ISNULL(CAST([COL2] as NVARCHAR(4000)), \'\') AS [COL2],CAST(\'2020-01-01 00:00:00\' as DATETIME2) AS [_TIMESTAMP], ROW_NUMBER() OVER (PARTITION BY [PK1],[PK2] ORDER BY [PK1],[PK2]) AS "_row_number_" FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]) AS a WHERE a."_row_number_" = 1',
        ];

        yield 'testGetCtasDedupCommandWithHashDistribution' => [
            $stage,
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                $stage->getColumnsDefinitions(),
                ['pk1', 'pk2'],
                new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_HASH, ['pk1']),
                $stage->getTableIndex()
            ),
            new SynapseImportOptions(
                ['col1'],
                false,
                true,            // use timestamp
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TABLE_TYPES_CAST // cast values
            ),
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=HASH([PK1]),HEAP) AS SELECT a.[PK1],a.[PK2],a.[COL1],a.[COL2],a.[_TIMESTAMP] FROM (SELECT COALESCE(CAST([PK1] as NVARCHAR(4000)), \'\') AS [PK1],COALESCE(CAST([PK2] as NVARCHAR(4000)), \'\') AS [PK2],CAST(NULLIF([COL1], \'\') as NVARCHAR(4000)) AS [COL1],COALESCE(CAST([COL2] as NVARCHAR(4000)), \'\') AS [COL2],CAST(\'2020-01-01 00:00:00\' as DATETIME2) AS [_TIMESTAMP], ROW_NUMBER() OVER (PARTITION BY [PK1],[PK2] ORDER BY [PK1],[PK2]) AS "_row_number_" FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]) AS a WHERE a."_row_number_" = 1'
        ];

        yield 'testGetCtasDedupCommandWithHashDistributionNoCasting' => [
            $stage,
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                $stage->getColumnsDefinitions(),
                ['pk1', 'pk2'],
                new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_HASH, ['pk1']),
                $stage->getTableIndex()
            ),

            new SynapseImportOptions(
                ['col1'],
                false,
                true, // use timestamp
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TABLE_TYPES_PRESERVE // dont cast values
            ),
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=HASH([PK1]),HEAP) AS SELECT a.[PK1],a.[PK2],a.[COL1],a.[COL2],a.[_TIMESTAMP] FROM (SELECT COALESCE([PK1], \'\') AS [PK1],COALESCE([PK2], \'\') AS [PK2],NULLIF([COL1], \'\') AS [COL1],COALESCE([COL2], \'\') AS [COL2],CAST(\'2020-01-01 00:00:00\' as DATETIME2) AS [_TIMESTAMP], ROW_NUMBER() OVER (PARTITION BY [PK1],[PK2] ORDER BY [PK1],[PK2]) AS "_row_number_" FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]) AS a WHERE a."_row_number_" = 1',
        ];

        yield 'testGetCtasDedupCommandWithTimestampNullConvert' => [
            $stage,
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                $stage->getColumnsDefinitions(),
                ['pk1', 'pk2'],
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseImportOptions(
                ['col1'],
                false,
                true, // use timestamp
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TABLE_TYPES_CAST // cast values
            ),
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=ROUND_ROBIN,HEAP) AS SELECT a.[PK1],a.[PK2],a.[COL1],a.[COL2],a.[_TIMESTAMP] FROM (SELECT COALESCE(CAST([PK1] as NVARCHAR(4000)), \'\') AS [PK1],COALESCE(CAST([PK2] as NVARCHAR(4000)), \'\') AS [PK2],CAST(NULLIF([COL1], \'\') as NVARCHAR(4000)) AS [COL1],COALESCE(CAST([COL2] as NVARCHAR(4000)), \'\') AS [COL2],CAST(\'2020-01-01 00:00:00\' as DATETIME2) AS [_TIMESTAMP], ROW_NUMBER() OVER (PARTITION BY [PK1],[PK2] ORDER BY [PK1],[PK2]) AS "_row_number_" FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]) AS a WHERE a."_row_number_" = 1',
        ];

        yield 'testGetCtasDedupCommandWithTimestampNullConvertNoCasting' => [
            $stage,
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                $stage->getColumnsDefinitions(),
                ['pk1', 'pk2'],
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseImportOptions(
                ['col1'],
                false,
                true, // use timestamp
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TABLE_TYPES_PRESERVE // don't cast values
            ),
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=ROUND_ROBIN,HEAP) AS SELECT a.[PK1],a.[PK2],a.[COL1],a.[COL2],a.[_TIMESTAMP] FROM (SELECT COALESCE([PK1], \'\') AS [PK1],COALESCE([PK2], \'\') AS [PK2],NULLIF([COL1], \'\') AS [COL1],COALESCE([COL2], \'\') AS [COL2],CAST(\'2020-01-01 00:00:00\' as DATETIME2) AS [_TIMESTAMP], ROW_NUMBER() OVER (PARTITION BY [PK1],[PK2] ORDER BY [PK1],[PK2]) AS "_row_number_" FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]) AS a WHERE a."_row_number_" = 1',
        ];

        yield 'testGetCtasDedupCommandNoTimestampNullConvert' => [
            $stage,
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                $stage->getColumnsDefinitions(),
                ['pk1', 'pk2'],
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseImportOptions(
                ['col1'],
                false,
                false, // don't use timestamp
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TABLE_TYPES_CAST
            ),
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=ROUND_ROBIN,HEAP) AS SELECT a.[PK1],a.[PK2],a.[COL1],a.[COL2] FROM (SELECT COALESCE(CAST([PK1] as NVARCHAR(4000)), \'\') AS [PK1],COALESCE(CAST([PK2] as NVARCHAR(4000)), \'\') AS [PK2],CAST(NULLIF([COL1], \'\') as NVARCHAR(4000)) AS [COL1],COALESCE(CAST([COL2] as NVARCHAR(4000)), \'\') AS [COL2], ROW_NUMBER() OVER (PARTITION BY [PK1],[PK2] ORDER BY [PK1],[PK2]) AS "_row_number_" FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]) AS a WHERE a."_row_number_" = 1',
            false,
        ];

        yield 'testGetCtasDedupCommandNoTimestampNullConvertNoCasting' => [
            $stage,
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                $stage->getColumnsDefinitions(),
                ['pk1', 'pk2'],
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseImportOptions(
                ['col1'],
                false,
                false, // don't use timestamp
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TABLE_TYPES_PRESERVE // don't cast
            ),
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=ROUND_ROBIN,HEAP) AS SELECT a.[PK1],a.[PK2],a.[COL1],a.[COL2] FROM (SELECT COALESCE([PK1], \'\') AS [PK1],COALESCE([PK2], \'\') AS [PK2],NULLIF([COL1], \'\') AS [COL1],COALESCE([COL2], \'\') AS [COL2], ROW_NUMBER() OVER (PARTITION BY [PK1],[PK2] ORDER BY [PK1],[PK2]) AS "_row_number_" FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]) AS a WHERE a."_row_number_" = 1',
            false,
        ];

        yield 'testGetCtasDedupCommandWithTimestampInSource' => [
            $stage,
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                // add _timestamp column to destination
                new ColumnCollection(array_merge(
                    iterator_to_array($stage->getColumnsDefinitions()),
                    [SynapseColumn::createGenericColumn('_timestamp')]
                )),
                ['pk1', 'pk2'],
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseImportOptions(
                [],
                false,
                true,
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TABLE_TYPES_CAST
            ),
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=ROUND_ROBIN,HEAP) AS SELECT a.[PK1],a.[PK2],a.[COL1],a.[COL2],a.[_TIMESTAMP] FROM (SELECT COALESCE(CAST([PK1] as NVARCHAR(4000)), \'\') AS [PK1],COALESCE(CAST([PK2] as NVARCHAR(4000)), \'\') AS [PK2],COALESCE(CAST([COL1] as NVARCHAR(4000)), \'\') AS [COL1],COALESCE(CAST([COL2] as NVARCHAR(4000)), \'\') AS [COL2],CAST(\'2020-01-01 00:00:00\' as DATETIME2) AS [_TIMESTAMP], ROW_NUMBER() OVER (PARTITION BY [PK1],[PK2] ORDER BY [PK1],[PK2]) AS "_row_number_" FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]) AS a WHERE a."_row_number_" = 1',
        ];
    }

    private function getStagingTableDefinition(): SynapseTableDefinition
    {
        return new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('pk1'),
                $this->createGenericColumn('pk2'),
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
    }

    /**
     * @return \Generator<string, array{SynapseImportOptions::TABLE_TYPES_*, string}>
     */
    public function getCTASInsertAllIntoTargetTableCommandProvider(): Generator
    {
        yield 'no type casting' => [
            SynapseImportOptions::TABLE_TYPES_PRESERVE,
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT COALESCE([COL1], \'\') AS [COL1],COALESCE([COL2], \'\') AS [COL2],ISNULL([PK1], \'\') AS [PK1] FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]',
        ];
        yield 'type casting' => [
            SynapseImportOptions::TABLE_TYPES_CAST,
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT COALESCE(CAST([COL1] as NVARCHAR(4000)), \'\') AS [COL1],COALESCE(CAST([COL2] as NVARCHAR(4000)), \'\') AS [COL2],ISNULL(CAST([PK1] as NVARCHAR(4000)), \'\') AS [PK1] FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]',
        ];
    }

    /**
     * @dataProvider getCTASInsertAllIntoTargetTableCommandProvider
     * @param SynapseImportOptions::TABLE_TYPES_* $cast
     */
    public function testGetCTASInsertAllIntoTargetTableCommand(string $cast, string $expectedSQL): void
    {
        $this->createTestSchema();
        $destination = $this->getTestTableWithColumnsDefinition(
            false,
            false,
            null,
            true
        );
        $this->createStagingTableWithData(true);

        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
                $this->createGenericColumn('pk1', false),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // no convert values no timestamp
        $importOptions = new SynapseImportOptions(
            [],
            false,
            false,
            0,
            SynapseImportOptions::CREDENTIALS_SAS,
            $cast
        );
        $sql = $this->getBuilder()->getCTASInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $importOptions,
            '2020-01-01 00:00:00'
        );

        $this->assertEquals(
        // phpcs:ignore
            $expectedSQL,
            $sql
        );

        $out = $this->connection->exec($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
            [
                'col1' => '1',
                'col2' => '1',
                'pk1' => '1',
            ],
            [
                'col1' => '1',
                'col2' => '1',
                'pk1' => '1',
            ],
            [
                'col1' => '2',
                'col2' => '2',
                'pk1' => '2',
            ],
            [
                'col1' => '',
                'col2' => '',
                'pk1' => '2',
            ],
        ], $result);

        $ref = new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );
        $resultColumns = $ref->getColumnsDefinitions();
        $this->assertColumnsDefinitions($resultColumns, $destination, $importOptions);
    }

    /**
     * @return \Generator<string, array{SynapseImportOptions::TABLE_TYPES_*, string}>
     */
    public function getCTASInsertAllIntoTargetTableCommandNotStringProvider(): Generator
    {
        yield 'no type casting' => [
            SynapseImportOptions::TABLE_TYPES_PRESERVE,
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT COALESCE([COL1], \'\') AS [COL1],[COL2] AS [COL2],ISNULL([PK1], \'\') AS [PK1] FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]',
        ];
        yield 'type casting' => [
            SynapseImportOptions::TABLE_TYPES_CAST,
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT COALESCE(CAST([COL1] as NVARCHAR(4000)), \'\') AS [COL1],CAST([COL2] as NUMERIC(18,0)) AS [COL2],ISNULL(CAST([PK1] as NVARCHAR(4000)), \'\') AS [PK1] FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]',
        ];
    }

    /**
     * @dataProvider getCTASInsertAllIntoTargetTableCommandNotStringProvider
     * @param SynapseImportOptions::TABLE_TYPES_* $cast
     */
    public function testGetCTASInsertAllIntoTargetTableCommandNotString(string $cast, string $expectedSQL): void
    {
        $col2 = new SynapseColumn(
            'col2',
            new Synapse(
                Synapse::TYPE_NUMERIC,
                [
                    'length' => '18,0',
                ]
            )
        );

        $this->createTestSchema();
        $destination = $this->getTestTableWithColumnsDefinition(
            false,
            false,
            $col2,
            true
        );
        $this->createStagingTableWithData(true);

        // create fake stage with missing id column and numeric col2
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $col2,
                $this->createGenericColumn('pk1', false),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // no convert values no timestamp
        $importOptions = new SynapseImportOptions(
            [],
            false,
            false,
            0,
            SynapseImportOptions::CREDENTIALS_SAS,
            $cast
        );
        $sql = $this->getBuilder()->getCTASInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $importOptions,
            '2020-01-01 00:00:00'
        );

        $this->assertEquals(
        // phpcs:ignore
            $expectedSQL,
            $sql
        );

        $out = $this->connection->exec($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
            [
                'col1' => '1',
                'col2' => '1',
                'pk1' => '1',
            ],
            [
                'col1' => '1',
                'col2' => '1',
                'pk1' => '1',
            ],
            [
                'col1' => '2',
                'col2' => '2',
                'pk1' => '2',
            ],
            [
                'col1' => '',
                'col2' => null,
                'pk1' => '2',
            ],
        ], $result);

        $ref = new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );
        $resultColumns = $ref->getColumnsDefinitions();
        $this->assertColumnsDefinitions($resultColumns, $destination, $importOptions);
    }

    /**
     * @return \Generator<string, array{SynapseImportOptions::TABLE_TYPES_*, string}>
     */
    public function getCTASInsertAllIntoTargetTableCommandConvertToNullProvider(): Generator
    {
        yield 'no type casting' => [
            SynapseImportOptions::TABLE_TYPES_PRESERVE,
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT NULLIF([COL1], \'\') AS [COL1],COALESCE([COL2], \'\') AS [COL2],ISNULL([PK1], \'\') AS [PK1] FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]',
        ];
        yield 'type casting' => [
            SynapseImportOptions::TABLE_TYPES_CAST,
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT CAST(NULLIF([COL1], \'\') as NVARCHAR(4000)) AS [COL1],COALESCE(CAST([COL2] as NVARCHAR(4000)), \'\') AS [COL2],ISNULL(CAST([PK1] as NVARCHAR(4000)), \'\') AS [PK1] FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]',
        ];
    }

    /**
     * @dataProvider getCTASInsertAllIntoTargetTableCommandConvertToNullProvider
     * @param SynapseImportOptions::TABLE_TYPES_* $cast
     */
    public function testGetCTASInsertAllIntoTargetTableCommandConvertToNull(string $cast, string $expectedSQL): void
    {
        $this->createTestSchema();
        $this->createStagingTableWithData(true);
        $destination = $this->getTestTableWithColumnsDefinition(
            false,
            false,
            null,
            true
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createIntColumn('col2'),
                $this->createGenericColumn('pk1', false),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // convert col1 to null
        $options = new SynapseImportOptions(
            ['col1'],
            false,
            false,
            0,
            SynapseImportOptions::CREDENTIALS_SAS,
            $cast
        );
        $sql = $this->getBuilder()->getCTASInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
            $expectedSQL,
            $sql
        );
        $out = $this->connection->exec($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
            [
                'col1' => '1',
                'col2' => '1',
                'pk1' => '1',
            ],
            [
                'col1' => '1',
                'col2' => '1',
                'pk1' => '1',
            ],
            [
                'col1' => '2',
                'col2' => '2',
                'pk1' => '2',
            ],
            [
                'col1' => null,
                'col2' => '',
                'pk1' => '2',
            ],
        ], $result);

        $ref = new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );
        $resultColumns = $ref->getColumnsDefinitions();
        $this->assertColumnsDefinitions($resultColumns, $destination, $options);
    }

    /**
     * @return \Generator<string, array{SynapseImportOptions::TABLE_TYPES_*, string}>
     */
    public function getCTASInsertAllIntoTargetTableCommandConvertToNullWithTimestampProvider(): Generator
    {
        yield 'no type casting' => [
            SynapseImportOptions::TABLE_TYPES_PRESERVE,
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT NULLIF([COL1], \'\') AS [COL1],COALESCE([COL2], \'\') AS [COL2],ISNULL([PK1], \'\') AS [PK1],CAST(\'2020-01-01 00:00:00\' AS DATETIME2) AS _TIMESTAMP FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]',
        ];
        yield 'type casting' => [
            SynapseImportOptions::TABLE_TYPES_CAST,
            // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT CAST(NULLIF([COL1], \'\') as NVARCHAR(4000)) AS [COL1],COALESCE(CAST([COL2] as NVARCHAR(4000)), \'\') AS [COL2],ISNULL(CAST([PK1] as NVARCHAR(4000)), \'\') AS [PK1],CAST(\'2020-01-01 00:00:00\' AS DATETIME2) AS _TIMESTAMP FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]',
        ];
    }

    /**
     * @dataProvider getCTASInsertAllIntoTargetTableCommandConvertToNullWithTimestampProvider
     * @param SynapseImportOptions::TABLE_TYPES_* $cast
     */
    public function testGetCTASInsertAllIntoTargetTableCommandConvertToNullWithTimestamp(
        string $cast,
        string $expectedSQL
    ): void {
        $this->createTestSchema();
        $destination = $this->getTestTableWithColumnsDefinition(
            true,
            false,
            null,
            true
        );
        $this->createStagingTableWithData(true);
        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createIntColumn('col2'),
                $this->createGenericColumn('pk1', false),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // use timestamp
        $options = new SynapseImportOptions(
            ['col1'],
            false,
            true,
            0,
            SynapseImportOptions::CREDENTIALS_SAS,
            $cast
        );
        $sql = $this->getBuilder()->getCTASInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
            $expectedSQL,
            $sql
        );
        $out = $this->connection->exec($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        foreach ($result as $item) {
            $this->assertArrayHasKey('COL1', $item);
            $this->assertArrayHasKey('COL2', $item);
            $this->assertArrayHasKey('_TIMESTAMP', $item);
        }

        $ref = new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );
        $resultColumns = $ref->getColumnsDefinitions();
        $this->assertColumnsDefinitions($resultColumns, $destination, $options);
    }

    public function testGetCTASInsertAllIntoTargetTableCommandSourceToDestinationNumeric(): void
    {
        $this->createTestSchema();
        $qb = new SynapseTableQueryBuilder();
        $columns = [
            'pk1',
            'pk2',
            'colNumericNull',
            'colNumeric',
            'colInteger',
            'colFloat',
            'colBool',
            'colDate',
            'colTimestamp',
        ];
        $stage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection(array_map(
                function ($name) {
                    return $this->createGenericColumn($name);
                },
                $columns
            )),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($stage));
        $records = [
            [1, 1, '\'1\'', '\'1\'', '\'1\'', '\'1.1\'', '\'1\'', '\'2020-02-02\'', '\'2016-12-21 00:00:00.0000000\''],
            [1, 1, '\'1\'', '\'1\'', '\'1\'', '\'1.1\'', '\'1\'', '\'2020-02-02\'', '\'2016-12-21 00:00:00.0000000\''],
            [2, 2, '\'2\'', '\'2\'', '\'2\'', '\'2.1\'', '\'0\'', '\'2020-02-02\'', '\'2016-12-22 00:00:00.0000000\''],
            [2, 2, '\'0\'', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL'],
        ];
        foreach ($records as $record) {
            $this->connection->executeStatement(
                sprintf(
                    'INSERT INTO %s.%s(%s) VALUES (%s)',
                    self::TEST_SCHEMA_QUOTED,
                    self::TEST_STAGING_TABLE,
                    implode(',', $columns),
                    implode(',', $record)
                )
            );
        }

        $destination = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            new ColumnCollection([
                $this->createGenericColumn('pk1'),
                new SynapseColumn(
                    'colNumericNull',
                    new Synapse(
                        Synapse::TYPE_NUMERIC, //<-- use numeric
                        [
                            'nullable' => true,
                            'length' => '18,0',
                        ]
                    )
                ),
                new SynapseColumn(
                    'colNumeric',
                    new Synapse(
                        Synapse::TYPE_NUMERIC, //<-- use numeric
                        [
                            'nullable' => true,
                            'length' => '18,0',
                        ]
                    )
                ),
                new SynapseColumn(
                    'colInteger',
                    new Synapse(
                        Synapse::TYPE_INT,
                        [
                            'nullable' => true,
                        ]
                    )
                ),
                new SynapseColumn(
                    'colFloat',
                    new Synapse(
                        Synapse::TYPE_FLOAT,
                        [
                            'nullable' => true,
                        ]
                    )
                ),
                new SynapseColumn(
                    'colBool',
                    new Synapse(
                        Synapse::TYPE_BIT,
                        [
                            'nullable' => true,
                        ]
                    )
                ),
                new SynapseColumn(
                    'colDate',
                    new Synapse(
                        Synapse::TYPE_DATE,
                        [
                            'nullable' => true,
                        ]
                    )
                ),
                new SynapseColumn(
                    'colTimestamp',
                    new Synapse(
                        Synapse::TYPE_DATETIME2,
                        [
                            'nullable' => true,
                            'length' => '7',
                        ]
                    )
                ),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX)
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('colNumericNull'),
                $this->createGenericColumn('colNumeric'),
                $this->createGenericColumn('colInteger'),
                $this->createGenericColumn('colFloat'),
                $this->createGenericColumn('colBool'),
                $this->createGenericColumn('colDate'),
                $this->createGenericColumn('colTimestamp'),
                $this->createGenericColumn('pk1', false),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // convert colNumericNull to null
        $options = new SynapseImportOptions(
            ['colNumericNull'],
            false,
            false,
            0,
            SynapseImportOptions::CREDENTIALS_SAS,
            SynapseImportOptions::TABLE_TYPES_CAST
        );
        $sql = $this->getBuilder()->getCTASInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
        // phpcs:ignore
            'CREATE TABLE [IMPORT-EXPORT-TEST-NG_SCHEMA].[IMPORT-EXPORT-TEST-NG_TEST] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT CAST([COLNUMERICNULL] as NUMERIC(18,0)) AS [COLNUMERICNULL],CAST([COLNUMERIC] as NUMERIC(18,0)) AS [COLNUMERIC],CAST([COLINTEGER] as INT) AS [COLINTEGER],CAST([COLFLOAT] as FLOAT) AS [COLFLOAT],CAST([COLBOOL] as BIT) AS [COLBOOL],CAST([COLDATE] as DATE) AS [COLDATE],CAST([COLTIMESTAMP] as DATETIME2(7)) AS [COLTIMESTAMP],COALESCE(CAST([PK1] as NVARCHAR(4000)), \'\') AS [PK1] FROM [IMPORT-EXPORT-TEST-NG_SCHEMA].[#STAGINGTABLE]',
            $sql
        );
        $out = $this->connection->executeStatement($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
            [
                'colNumericNull' => '1',
                'colNumeric' => '1',
                'pk1' => '1',
                'colInteger' => '1',
                'colFloat' => '1.1000000000000001',
                'colBool' => '1',
                'colDate' => '2020-02-02',
                'colTimestamp' => '2016-12-21 00:00:00.0000000',
            ],
            [
                'colNumericNull' => '1',
                'colNumeric' => '1',
                'pk1' => '1',
                'colInteger' => '1',
                'colFloat' => '1.1000000000000001',
                'colBool' => '1',
                'colDate' => '2020-02-02',
                'colTimestamp' => '2016-12-21 00:00:00.0000000',
            ],
            [
                'colNumericNull' => '2',
                'colNumeric' => '2',
                'pk1' => '2',
                'colInteger' => '2',
                'colFloat' => '2.1000000000000001',
                'colBool' => '0',
                'colDate' => '2020-02-02',
                'colTimestamp' => '2016-12-22 00:00:00.0000000',
            ],
            [
                'colNumericNull' => null,
                'colNumeric' => '0',
                'pk1' => '2',
                'colInteger' => null,
                'colFloat' => null,
                'colBool' => null,
                'colDate' => null,
                'colTimestamp' => null,
            ],
        ], $result);

        $ref = new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );
        $resultColumns = $ref->getColumnsDefinitions();
        $this->assertColumnsDefinitions($resultColumns, $destination, $options);
    }

    private function createIntColumn(string $columnName): SynapseColumn
    {
        $definition = new Synapse(
            Synapse::TYPE_INT,
            [
                'nullable' => false,
            ]
        );

        return new SynapseColumn(
            $columnName,
            $definition
        );
    }

    protected function getTestTableWithColumnsDefinition(
        bool $includeTimestamp = false,
        bool $includePrimaryKey = false,
        ?SynapseColumn $overwriteColumn2 = null,
        bool $includeNotNullableColumn = false
    ): SynapseTableDefinition {
        $columns = [];
        $pks = [];
        if ($includePrimaryKey) {
            $pks[] = 'id';
            $columns[] = new SynapseColumn(
                'id',
                new Synapse(Synapse::TYPE_INT)
            );
        } else {
            $columns[] = $this->createGenericColumn('id');
        }
        $columns[] = $this->createGenericColumn('col1');
        if ($overwriteColumn2 === null) {
            $columns[] = $this->createGenericColumn('col2');
        } else {
            $columns[] = $overwriteColumn2;
        }

        if ($includeTimestamp) {
            $columns[] = new SynapseColumn(
                '_timestamp',
                new Synapse(Synapse::TYPE_DATETIME)
            );
        }

        if ($includeNotNullableColumn) {
            $columns[] = $this->createGenericColumn('pk1', false);
        }

        $tableDefinition = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            new ColumnCollection($columns),
            $pks,
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX)
        );
        return $tableDefinition;
    }

    private function assertColumnsDefinitions(
        ColumnCollection $resultColumns,
        SynapseTableDefinition $destination,
        SynapseImportOptions $options
    ): void {
        /** @var SynapseColumn $resultCol */
        foreach ($resultColumns as $resultCol) {
            $timestampUpper = CaseConverter::stringToUpper(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME);
            if ($resultCol->getColumnName() === $timestampUpper) {
                continue;
            }
            $expectedCol = null;
            /** @var SynapseColumn $col */
            foreach ($destination->getColumnsDefinitions() as $col) {
                if ($col->getColumnName() === $resultCol->getColumnName()) {
                    $expectedCol = $col;
                    break;
                }
            }
            $this->assertNotNull($expectedCol);
            if ($options->getCastValueTypes()) {
                $this->assertSame(
                    $expectedCol->getColumnDefinition()->getSQLDefinition(),
                    $resultCol->getColumnDefinition()->getSQLDefinition(),
                    sprintf('Column "%s" definition not match.', $resultCol->getColumnName())
                );
            } else {
                $this->assertSame(
                    $expectedCol->getColumnDefinition()->isNullable(),
                    $resultCol->getColumnDefinition()->isNullable(),
                    sprintf('Column "%s" nullability not match.', $resultCol->getColumnName())
                );
            }
        }
    }
}
