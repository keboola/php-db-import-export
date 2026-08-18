<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\ToFinalTable;

use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable\SqlBuilder;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use PHPUnit\Framework\TestCase;

/**
 * Covers the single-statement import path (MERGE / INSERT OVERWRITE) and guards that the
 * multi-statement path reachable through the `snowflake-legacy-import` feature keeps emitting
 * byte-identical SQL, since both share the column expression builders.
 */
class SqlBuilderTest extends TestCase
{
    private const SCHEMA = 'import_export_test_schema';
    private const STAGE_TABLE = '__temp_stagingTable';
    private const DEST_TABLE = 'import_export_test_test';

    private function getBuilder(): SqlBuilder
    {
        return new SqlBuilder();
    }

    private function genericColumn(string $columnName): SnowflakeColumn
    {
        return new SnowflakeColumn(
            $columnName,
            new Snowflake(
                Snowflake::TYPE_VARCHAR,
                [
                    'length' => '4000',
                    'nullable' => true,
                ],
            ),
        );
    }

    private function typedColumn(string $columnName, string $type): SnowflakeColumn
    {
        return new SnowflakeColumn($columnName, new Snowflake($type, ['nullable' => true]));
    }

    /**
     * @param SnowflakeColumn[] $columns
     * @param string[] $primaryKeys
     */
    private function table(
        string $tableName,
        array $columns,
        array $primaryKeys = [],
    ): SnowflakeTableDefinition {
        return new SnowflakeTableDefinition(
            self::SCHEMA,
            $tableName,
            true,
            new ColumnCollection($columns),
            $primaryKeys,
        );
    }

    /**
     * @param string[] $primaryKeys
     */
    private function genericStage(array $primaryKeys = []): SnowflakeTableDefinition
    {
        return $this->table(
            self::STAGE_TABLE,
            [
                $this->genericColumn('col1'),
                $this->genericColumn('col2'),
            ],
            $primaryKeys,
        );
    }

    /**
     * @param string[] $primaryKeys
     */
    private function genericDestination(array $primaryKeys = []): SnowflakeTableDefinition
    {
        return $this->table(
            self::DEST_TABLE,
            [
                $this->genericColumn('col1'),
                $this->genericColumn('col2'),
            ],
            $primaryKeys,
        );
    }

    /**
     * The multi-statement path shares its column expression builder with the MERGE, so this pins the
     * SQL it has always produced (expectation copied from the functional SqlBuilderTest).
     */
    public function testGetInsertAllIntoTargetTableCommandIsUnchanged(): void
    {
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $this->genericStage(),
            $this->genericDestination(),
            new SnowflakeImportOptions(ignoreColumns: ['id']),
            '2020-01-01 00:00:00',
        );

        self::assertSame(
            // phpcs:ignore Generic.Files.LineLength.MaxExceeded
            'INSERT INTO "import_export_test_schema"."import_export_test_test" ("col1", "col2") (SELECT COALESCE("col1", \'\') AS "col1",COALESCE("col2", \'\') AS "col2" FROM "import_export_test_schema"."__temp_stagingTable" AS "src")',
            $sql,
        );
    }

    /**
     * Same guard for the UPDATE, which shares the SET and change-detection builders with the MERGE
     * (expectation copied from the functional SqlBuilderTest).
     */
    public function testGetUpdateWithPkCommandIsUnchanged(): void
    {
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $this->genericStage(),
            $this->genericDestination(['col1']),
            new SnowflakeImportOptions(['col1'], false, true),
            '2020-01-01 01:01:01',
        );

        self::assertSame(
            // phpcs:ignore Generic.Files.LineLength.MaxExceeded
            'UPDATE "import_export_test_schema"."import_export_test_test" AS "dest" SET "col1" = IFF("src"."col1" = \'\', NULL, "src"."col1"), "col2" = COALESCE("src"."col2", \'\'), "_timestamp" = \'2020-01-01 01:01:01\' FROM "import_export_test_schema"."__temp_stagingTable" AS "src" WHERE "dest"."col1" = COALESCE("src"."col1", \'\')  AND (COALESCE(TO_VARCHAR("dest"."col1"), \'\') != COALESCE("src"."col1", \'\') OR COALESCE(TO_VARCHAR("dest"."col2"), \'\') != COALESCE("src"."col2", \'\'))',
            $sql,
        );
    }

    public function testGetInsertOverwriteWithoutPrimaryKeysHasNoQualify(): void
    {
        $sql = $this->getBuilder()->getInsertOverwriteAllIntoTargetTableCommand(
            $this->genericStage(),
            $this->genericDestination(),
            new SnowflakeImportOptions(useTimestamp: true),
            '2020-01-01 00:00:00',
        );

        self::assertSame(
            // phpcs:ignore Generic.Files.LineLength.MaxExceeded
            'INSERT OVERWRITE INTO "import_export_test_schema"."import_export_test_test" ("col1", "col2", "_timestamp") (SELECT COALESCE("col1", \'\') AS "col1",COALESCE("col2", \'\') AS "col2",\'2020-01-01 00:00:00\' FROM "import_export_test_schema"."__temp_stagingTable" AS "src")',
            $sql,
        );
    }

    public function testGetInsertOverwriteWithPrimaryKeysDedupesInline(): void
    {
        $sql = $this->getBuilder()->getInsertOverwriteAllIntoTargetTableCommand(
            $this->genericStage(),
            $this->genericDestination(['col1']),
            new SnowflakeImportOptions(useTimestamp: true),
            '2020-01-01 00:00:00',
            ['col1'],
        );

        self::assertSame(
            // phpcs:ignore Generic.Files.LineLength.MaxExceeded
            'INSERT OVERWRITE INTO "import_export_test_schema"."import_export_test_test" ("col1", "col2", "_timestamp") (SELECT COALESCE("col1", \'\') AS "col1",COALESCE("col2", \'\') AS "col2",\'2020-01-01 00:00:00\' FROM "import_export_test_schema"."__temp_stagingTable" AS "src" QUALIFY ROW_NUMBER() OVER (PARTITION BY "src"."col1" ORDER BY "src"."col1") = 1)',
            $sql,
        );
    }

    public function testGetInsertOverwriteDedupesOnAllPrimaryKeyColumns(): void
    {
        $stage = $this->table(
            self::STAGE_TABLE,
            [
                $this->genericColumn('pk1'),
                $this->genericColumn('pk2'),
                $this->genericColumn('col1'),
            ],
        );
        $destination = $this->table(
            self::DEST_TABLE,
            [
                $this->genericColumn('pk1'),
                $this->genericColumn('pk2'),
                $this->genericColumn('col1'),
            ],
            ['pk1', 'pk2'],
        );

        $sql = $this->getBuilder()->getInsertOverwriteAllIntoTargetTableCommand(
            $stage,
            $destination,
            new SnowflakeImportOptions(),
            '2020-01-01 00:00:00',
            ['pk1', 'pk2'],
        );

        self::assertStringContainsString(
            'QUALIFY ROW_NUMBER() OVER (PARTITION BY "src"."pk1", "src"."pk2" ORDER BY "src"."pk1", "src"."pk2") = 1',
            $sql,
        );
    }

    /**
     * A non-typed table keeps the null manipulation: primary keys join through COALESCE, values are
     * coalesced to empty strings and the change detection compares as text.
     */
    public function testGetMergeCommandForNonTypedTable(): void
    {
        $sql = $this->getBuilder()->getMergeCommand(
            $this->genericStage(),
            $this->genericDestination(['col1']),
            new SnowflakeImportOptions(useTimestamp: true),
            '2020-01-01 01:01:01',
        );

        self::assertSame(
            'MERGE INTO "import_export_test_schema"."import_export_test_test" AS "dest"'
            . ' USING (SELECT "col1", "col2" FROM "import_export_test_schema"."__temp_stagingTable" AS "src"'
            . ' QUALIFY ROW_NUMBER() OVER (PARTITION BY COALESCE("src"."col1", \'\')'
            . ' ORDER BY COALESCE("src"."col1", \'\')) = 1) AS "src"'
            . ' ON "dest"."col1" = COALESCE("src"."col1", \'\') '
            . ' WHEN MATCHED AND (COALESCE(TO_VARCHAR("dest"."col1"), \'\') != COALESCE("src"."col1", \'\')'
            . ' OR COALESCE(TO_VARCHAR("dest"."col2"), \'\') != COALESCE("src"."col2", \'\'))'
            . ' THEN UPDATE SET "col1" = COALESCE("src"."col1", \'\'), "col2" = COALESCE("src"."col2", \'\'),'
            . ' "_timestamp" = \'2020-01-01 01:01:01\''
            . ' WHEN NOT MATCHED THEN INSERT ("col1", "col2", "_timestamp")'
            . ' VALUES (COALESCE("src"."col1", \'\'),COALESCE("src"."col2", \'\'),\'2020-01-01 01:01:01\')',
            $sql,
        );
    }

    /**
     * A typed table skips the null manipulation: the join compares the primary key directly and the
     * change detection uses IS DISTINCT FROM instead of a text comparison.
     */
    public function testGetMergeCommandForTypedTable(): void
    {
        $stage = $this->table(
            self::STAGE_TABLE,
            [
                $this->typedColumn('id', Snowflake::TYPE_INT),
                $this->typedColumn('name', Snowflake::TYPE_VARCHAR),
            ],
        );
        $destination = $this->table(
            self::DEST_TABLE,
            [
                $this->typedColumn('id', Snowflake::TYPE_INT),
                $this->typedColumn('name', Snowflake::TYPE_VARCHAR),
            ],
            ['id'],
        );

        $sql = $this->getBuilder()->getMergeCommand(
            $stage,
            $destination,
            new SnowflakeImportOptions(
                requireSameTables: SnowflakeImportOptions::SAME_TABLES_REQUIRED,
                nullManipulation: SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
            ),
            '2020-01-01 01:01:01',
        );

        // no null manipulation, so the dedup key stays the raw column, matching the ON condition
        self::assertStringContainsString(
            'QUALIFY ROW_NUMBER() OVER (PARTITION BY "src"."id" ORDER BY "src"."id") = 1',
            $sql,
        );
        self::assertStringContainsString('ON "dest"."id" = "src"."id"', $sql);
        self::assertStringContainsString(
            'WHEN MATCHED AND ("dest"."id" IS DISTINCT FROM "src"."id"'
            . ' OR "dest"."name" IS DISTINCT FROM "src"."name")',
            $sql,
        );
        self::assertStringContainsString(
            'THEN UPDATE SET "id" = "src"."id", "name" = "src"."name"',
            $sql,
        );
        self::assertStringContainsString(
            'WHEN NOT MATCHED THEN INSERT ("id", "name") VALUES ("src"."id","src"."name")',
            $sql,
        );
        // no timestamp requested, so the merge must not touch the timestamp column
        self::assertStringNotContainsString('"_timestamp"', $sql);
    }

    /**
     * VECTOR arrives from a file as its serialized JSON text, so the merge has to restore it in both
     * branches - a plain assignment would write the text into a VECTOR column.
     */
    public function testGetMergeCommandCastsAutoCastTypesInBothBranches(): void
    {
        $stage = $this->table(
            self::STAGE_TABLE,
            [
                $this->typedColumn('id', Snowflake::TYPE_INT),
                $this->genericColumn('vec'),
            ],
        );
        $destination = $this->table(
            self::DEST_TABLE,
            [
                $this->typedColumn('id', Snowflake::TYPE_INT),
                new SnowflakeColumn('vec', new Snowflake(Snowflake::TYPE_VECTOR, ['length' => 'INT,3'])),
            ],
            ['id'],
        );

        $sql = $this->getBuilder()->getMergeCommand(
            $stage,
            $destination,
            new SnowflakeImportOptions(
                requireSameTables: SnowflakeImportOptions::SAME_TABLES_REQUIRED,
                nullManipulation: SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
            ),
            '2020-01-01 01:01:01',
        );

        self::assertStringContainsString(
            '"vec" = CAST(PARSE_JSON("src"."vec") AS ARRAY)::VECTOR (INT,3)',
            $sql,
        );
        self::assertStringContainsString(
            'VALUES ("src"."id",CAST(PARSE_JSON("src"."vec") AS ARRAY)::VECTOR (INT,3))',
            $sql,
        );
    }
}
