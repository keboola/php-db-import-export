<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\ToStage;

use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\FromTableInsertIntoAdapter;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\MockDbalConnectionTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class FromTableInsertIntoAdapterTest extends BaseTestCase
{
    use MockDbalConnectionTrait;

    public function testGetCopyCommands(): void
    {
        $source = new Storage\Snowflake\Table('test_schema', 'test_table', ['col1', 'col2']);

        $conn = $this->mockConnection();
        $conn->expects(self::never())->method('executeQuery');
        $conn->expects(self::once())->method('executeStatement')->with(
        // phpcs:ignore
            'INSERT INTO "test_schema"."stagingTable" ("col1", "col2") SELECT "col1", "col2" FROM "test_schema"."test_table"'
        )->willReturn(10);

        $destination = new SnowflakeTableDefinition(
            'test_schema',
            'stagingTable',
            true,
            new ColumnCollection([
                SnowflakeColumn::createGenericColumn('col1'),
                SnowflakeColumn::createGenericColumn('col2'),
                ],),
            [],
        );
        $options = new SnowflakeImportOptions([]);
        $adapter = new FromTableInsertIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        self::assertEquals(10, $count);
    }

    public function testGetCopyCommandsSelectSource(): void
    {
        $source = new Storage\Snowflake\SelectSource(
            'SELECT * FROM "test_schema"."test_table"',
            ['bind' => 'val'],
            ['col1', 'col2'],
            [],
            ['1'],
        );

        $conn = $this->mockConnection();
        $conn->expects(self::never())->method('executeQuery');
        $conn->expects(self::once())->method('executeStatement')->with(
        // phpcs:ignore
            'INSERT INTO "test_schema"."stagingTable" ("col1", "col2") SELECT * FROM "test_schema"."test_table"',
            ['bind' => 'val'],
            [1],
        )->willReturn(10);

        $destination = new SnowflakeTableDefinition(
            'test_schema',
            'stagingTable',
            true,
            new ColumnCollection([
                SnowflakeColumn::createGenericColumn('col1'),
                SnowflakeColumn::createGenericColumn('col2'),
                ],),
            [],
        );
        $options = new SnowflakeImportOptions([]);
        $adapter = new FromTableInsertIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        self::assertEquals(10, $count);
    }

    public function testUnreportedAffectedRowsAreCountedOnTheStagingTable(): void
    {
        $source = new Storage\Snowflake\SelectSource(
            'SELECT * FROM "test_schema"."test_table"',
            [],
            ['col1', 'col2'],
            [],
        );

        $conn = $this->mockConnection();
        // odbc_num_rows() reports -1 when the driver cannot tell how many rows the INSERT affected.
        $conn->expects(self::once())->method('executeStatement')->willReturn(-1);
        $conn->expects(self::once())
            ->method('fetchAllAssociative')
            ->with('SHOW TABLES LIKE \'stagingTable\' IN SCHEMA "test_schema"')
            ->willReturn([
                ['name' => 'stagingTable', 'kind' => 'TEMPORARY', 'rows' => '7', 'bytes' => '128'],
            ]);

        $destination = new SnowflakeTableDefinition(
            'test_schema',
            'stagingTable',
            true,
            new ColumnCollection([
                SnowflakeColumn::createGenericColumn('col1'),
                SnowflakeColumn::createGenericColumn('col2'),
            ]),
            [],
        );
        $adapter = new FromTableInsertIntoAdapter($conn);

        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            new SnowflakeImportOptions([]),
        );

        self::assertSame(7, $count);
    }
}
