<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Bigquery\ToFinalTable;

use Keboola\Datatype\Definition\Bigquery;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\TimestampMode;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use PHPUnit\Framework\TestCase;

/**
 * Covers the bigquery-optimized-import feature gate: with the feature OFF, SqlBuilder must
 * produce byte-identical SQL to the pre-existing implementation. With the feature ON, it must
 * produce the aggregation-based dedup / CLUSTER BY / MERGE SQL.
 */
class SqlBuilderTest extends TestCase
{
    private function getInstance(): SqlBuilder
    {
        return new SqlBuilder();
    }

    /**
     * @param array{nullable?: bool, default?: string|null} $options
     */
    private function col(string $name, string $type, array $options = []): BigqueryColumn
    {
        return new BigqueryColumn($name, new Bigquery($type, $options));
    }

    /**
     * @param BigqueryColumn[] $columns
     * @param string[] $primaryKeys
     */
    private function table(
        string $schema,
        string $tableName,
        array $columns,
        array $primaryKeys = [],
    ): BigqueryTableDefinition {
        return new BigqueryTableDefinition($schema, $tableName, false, new ColumnCollection($columns), $primaryKeys);
    }

    public function testGetCreateDedupTableOffIsUnchanged(): void
    {
        $staging = $this->table('in.c-main', 'stage', [
            $this->col('id', Bigquery::TYPE_STRING),
            $this->col('name', Bigquery::TYPE_STRING),
        ]);

        $sql = $this->getInstance()->getCreateDedupTable($staging, 'dedup_1', ['id']);

        // phpcs:disable Generic.Files.LineLength.MaxExceeded
        self::assertSame(
            <<<SQL
            CREATE OR REPLACE TABLE `in.c-main`.`dedup_1` AS
            SELECT a.`id`,a.`name` FROM (
                SELECT src.`id`, src.`name`, ROW_NUMBER() OVER (PARTITION BY src.`id` ORDER BY src.`id`) AS `_row_number_`
                FROM `in.c-main`.`stage` as src
            ) AS a
                WHERE a.`_row_number_` = 1
            SQL,
            $sql,
        );
        // phpcs:enable Generic.Files.LineLength.MaxExceeded

        // explicit false must match the default (no behavior change)
        self::assertSame($sql, $this->getInstance()->getCreateDedupTable($staging, 'dedup_1', ['id'], false));
    }

    public function testGetCreateDedupTableOptimizedSingleClusterablePk(): void
    {
        $staging = $this->table('in.c-main', 'stage', [
            $this->col('id', Bigquery::TYPE_STRING),
            $this->col('name', Bigquery::TYPE_STRING),
        ]);

        $sql = $this->getInstance()->getCreateDedupTable($staging, 'dedup_1', ['id'], true);

        self::assertSame(
            <<<SQL
            CREATE OR REPLACE TABLE `in.c-main`.`dedup_1`
            CLUSTER BY `id` AS
            SELECT `a`.`id`, `a`.`name` FROM (
                SELECT ANY_VALUE(`src`) AS `a` FROM `in.c-main`.`stage` AS `src` GROUP BY src.`id`
            )
            SQL,
            $sql,
        );
    }

    public function testGetCreateDedupTableOptimizedSkipsClusterByForNonClusterablePkType(): void
    {
        $staging = $this->table('in.c-main', 'stage', [
            $this->col('id', Bigquery::TYPE_INT64),
            $this->col('geo', Bigquery::TYPE_GEOGRAPHY),
            $this->col('name', Bigquery::TYPE_STRING),
        ]);

        $sql = $this->getInstance()->getCreateDedupTable($staging, 'dedup_2', ['id', 'geo'], true);

        // no CLUSTER BY clause: one PK column (geo) is not a clusterable type
        self::assertSame(
            <<<SQL
            CREATE OR REPLACE TABLE `in.c-main`.`dedup_2`
            AS
            SELECT `a`.`id`, `a`.`geo`, `a`.`name` FROM (
                SELECT ANY_VALUE(`src`) AS `a` FROM `in.c-main`.`stage` AS `src` GROUP BY src.`id`, src.`geo`
            )
            SQL,
            $sql,
        );
    }

    public function testGetCreateDedupTableOptimizedCapsClusterByAtFourColumns(): void
    {
        $staging = $this->table('in.c-main', 'stage', [
            $this->col('pk1', Bigquery::TYPE_STRING),
            $this->col('pk2', Bigquery::TYPE_STRING),
            $this->col('pk3', Bigquery::TYPE_STRING),
            $this->col('pk4', Bigquery::TYPE_STRING),
            $this->col('pk5', Bigquery::TYPE_STRING),
            $this->col('val', Bigquery::TYPE_STRING),
        ]);

        $sql = $this->getInstance()->getCreateDedupTable(
            $staging,
            'dedup_3',
            ['pk1', 'pk2', 'pk3', 'pk4', 'pk5'],
            true,
        );

        // CLUSTER BY is capped at 4 columns, but GROUP BY still dedups on the full PK
        // phpcs:disable Generic.Files.LineLength.MaxExceeded
        self::assertSame(
            <<<SQL
            CREATE OR REPLACE TABLE `in.c-main`.`dedup_3`
            CLUSTER BY `pk1`, `pk2`, `pk3`, `pk4` AS
            SELECT `a`.`pk1`, `a`.`pk2`, `a`.`pk3`, `a`.`pk4`, `a`.`pk5`, `a`.`val` FROM (
                SELECT ANY_VALUE(`src`) AS `a` FROM `in.c-main`.`stage` AS `src` GROUP BY src.`pk1`, src.`pk2`, src.`pk3`, src.`pk4`, src.`pk5`
            )
            SQL,
            $sql,
        );
        // phpcs:enable Generic.Files.LineLength.MaxExceeded
    }

    public function testGetMergeCommandTypedTable(): void
    {
        $destination = $this->table('out.c-main', 'dest', [
            $this->col('id', Bigquery::TYPE_INT64),
            $this->col('name', Bigquery::TYPE_STRING),
        ], ['id']);
        $dedup = $this->table('in.c-main', 'dedup_4', [
            $this->col('id', Bigquery::TYPE_INT64),
            $this->col('name', Bigquery::TYPE_STRING),
        ]);
        $options = new BigqueryImportOptions(
            isIncremental: true,
            useTimestamp: true,
            usingTypes: BigqueryImportOptions::USING_TYPES_USER,
        );

        $sql = $this->getInstance()->getMergeCommand($dedup, $destination, $options, '2024-01-01 00:00:00');

        self::assertSame(
            'MERGE `out.c-main`.`dest` AS `dest` USING `in.c-main`.`dedup_4` AS `src` ON `dest`.`id` = `src`.`id`  '
            . 'WHEN MATCHED AND (`dest`.`id` IS DISTINCT FROM `src`.`id` '
            . 'OR `dest`.`name` IS DISTINCT FROM `src`.`name`) '
            . 'THEN UPDATE SET `id` = `src`.`id`, `name` = `src`.`name`, `_timestamp` = \'2024-01-01 00:00:00\' '
            . 'WHEN NOT MATCHED THEN INSERT (`id`, `name`, `_timestamp`) '
            . 'VALUES (`src`.`id`,`src`.`name`,CAST(\'2024-01-01 00:00:00\' as TIMESTAMP))',
            $sql,
        );
    }

    public function testGetMergeCommandStringTableWithConvertEmptyValuesToNullAndCrossBackendCast(): void
    {
        $destination = $this->table('out.c-main', 'dest', [
            $this->col('id', Bigquery::TYPE_INT64),
            $this->col('name', Bigquery::TYPE_STRING),
            $this->col('price', Bigquery::TYPE_NUMERIC),
        ], ['id']);
        // staging/dedup columns are STRING (e.g. cross-backend CSV load), destination is typed
        $dedup = $this->table('in.c-main', 'dedup_5', [
            $this->col('id', Bigquery::TYPE_STRING),
            $this->col('name', Bigquery::TYPE_STRING),
            $this->col('price', Bigquery::TYPE_STRING),
        ]);
        $options = new BigqueryImportOptions(
            convertEmptyValuesToNull: ['name'],
            isIncremental: true,
            useTimestamp: true,
        );

        $sql = $this->getInstance()->getMergeCommand($dedup, $destination, $options, '2024-01-01 00:00:00');

        self::assertSame(
            'MERGE `out.c-main`.`dest` AS `dest` USING `in.c-main`.`dedup_5` AS `src` '
            . 'ON CAST(`dest`.`id` AS STRING) = COALESCE(`src`.`id`, \'\')  '
            . 'WHEN MATCHED AND (CAST(`dest`.`id` AS STRING) != COALESCE(`src`.`id`, \'\') '
            . 'OR `dest`.`name` != COALESCE(`src`.`name`, \'\') '
            . 'OR CAST(`dest`.`price` AS STRING) != COALESCE(`src`.`price`, \'\')) '
            . 'THEN UPDATE SET `id` = CAST(COALESCE(`src`.`id`, \'\') AS INT64), '
            . '`name` = IF(`src`.`name` = \'\', NULL, `src`.`name`), '
            . '`price` = CAST(COALESCE(`src`.`price`, \'\') AS NUMERIC), '
            . '`_timestamp` = \'2024-01-01 00:00:00\' '
            . 'WHEN NOT MATCHED THEN INSERT (`id`, `name`, `price`, `_timestamp`) '
            // no `AS col` aliases here: MERGE ... VALUES rejects them (INSERT ... SELECT allows them)
            . 'VALUES (CAST(COALESCE(`src`.`id`, \'\') as INT64),'
            . 'NULLIF(`src`.`name`, \'\'),'
            . 'CAST(COALESCE(`src`.`price`, \'\') as NUMERIC),'
            . 'CAST(\'2024-01-01 00:00:00\' as TIMESTAMP))',
            $sql,
        );
    }

    /**
     * Pre-existing behavior carried over unchanged from getUpdateWithPkCommand(): for typed tables,
     * usingUserDefinedTypes() short-circuits getColumnsComparisonSql() before the FromSource check,
     * so _timestamp IS still compared here despite TimestampMode::FromSource. The exclusion only
     * applies on the untyped (string-table) branch.
     */
    public function testGetMergeCommandTimestampModeFromSourceWithUserTypesStillComparesTimestamp(): void
    {
        $destination = $this->table('out.c-main', 'dest', [
            $this->col('id', Bigquery::TYPE_INT64),
            $this->col('name', Bigquery::TYPE_STRING),
            $this->col('_timestamp', Bigquery::TYPE_TIMESTAMP),
        ], ['id']);
        $dedup = $this->table('in.c-main', 'dedup_6', [
            $this->col('id', Bigquery::TYPE_INT64),
            $this->col('name', Bigquery::TYPE_STRING),
            $this->col('_timestamp', Bigquery::TYPE_TIMESTAMP),
        ]);
        $options = new BigqueryImportOptions(
            isIncremental: true,
            useTimestamp: false,
            usingTypes: BigqueryImportOptions::USING_TYPES_USER,
            timestampMode: TimestampMode::FromSource,
        );

        $sql = $this->getInstance()->getMergeCommand($dedup, $destination, $options, '2024-01-01 00:00:00');

        self::assertSame(
            'MERGE `out.c-main`.`dest` AS `dest` USING `in.c-main`.`dedup_6` AS `src` ON `dest`.`id` = `src`.`id`  '
            . 'WHEN MATCHED AND (`dest`.`id` IS DISTINCT FROM `src`.`id` '
            . 'OR `dest`.`name` IS DISTINCT FROM `src`.`name` '
            . 'OR `dest`.`_timestamp` IS DISTINCT FROM `src`.`_timestamp`) '
            . 'THEN UPDATE SET `id` = `src`.`id`, `name` = `src`.`name`, `_timestamp` = `src`.`_timestamp` '
            . 'WHEN NOT MATCHED THEN INSERT (`id`, `name`, `_timestamp`) '
            . 'VALUES (`src`.`id`,`src`.`name`,`src`.`_timestamp`)',
            $sql,
        );
    }

    /**
     * Regression check: extracting getColumnsSetForUpdate()/getColumnsComparisonSql() out of
     * getUpdateWithPkCommand() (to share with getMergeCommand()) must not change its output.
     */
    public function testGetUpdateWithPkCommandUnchangedAfterRefactor(): void
    {
        $destination = $this->table('out.c-main', 'dest', [
            $this->col('id', Bigquery::TYPE_INT64),
            $this->col('name', Bigquery::TYPE_STRING),
        ], ['id']);
        $dedup = $this->table('in.c-main', 'dedup_4', [
            $this->col('id', Bigquery::TYPE_INT64),
            $this->col('name', Bigquery::TYPE_STRING),
        ]);
        $options = new BigqueryImportOptions(
            isIncremental: true,
            useTimestamp: true,
            usingTypes: BigqueryImportOptions::USING_TYPES_USER,
        );

        $sql = $this->getInstance()->getUpdateWithPkCommand($dedup, $destination, $options, '2024-01-01 00:00:00');

        self::assertSame(
            'UPDATE `out.c-main`.`dest` AS `dest` SET `id` = `src`.`id`, `name` = `src`.`name`, '
            . '`_timestamp` = \'2024-01-01 00:00:00\' FROM `in.c-main`.`dedup_4` AS `src` '
            . 'WHERE `dest`.`id` = `src`.`id`  AND '
            . '(`dest`.`id` IS DISTINCT FROM `src`.`id` OR `dest`.`name` IS DISTINCT FROM `src`.`name`)',
            $sql,
        );
    }

    /**
     * Regression check: extracting getInsertColumnsAndValues() out of
     * getInsertAllIntoTargetTableCommand() (to share with getMergeCommand()) must not change its output.
     */
    public function testGetInsertAllIntoTargetTableCommandUnchangedAfterRefactor(): void
    {
        $destination = $this->table('out.c-main', 'dest', [
            $this->col('id', Bigquery::TYPE_INT64),
            $this->col('name', Bigquery::TYPE_STRING),
        ], ['id']);
        $dedup = $this->table('in.c-main', 'dedup_4', [
            $this->col('id', Bigquery::TYPE_INT64),
            $this->col('name', Bigquery::TYPE_STRING),
        ]);
        $options = new BigqueryImportOptions(
            isIncremental: true,
            useTimestamp: true,
            usingTypes: BigqueryImportOptions::USING_TYPES_USER,
        );

        $sql = $this->getInstance()->getInsertAllIntoTargetTableCommand(
            $dedup,
            $destination,
            $options,
            '2024-01-01 00:00:00',
        );

        self::assertSame(
            'INSERT INTO `out.c-main`.`dest` (`id`, `name`, `_timestamp`) '
            . 'SELECT `src`.`id`,`src`.`name`,CAST(\'2024-01-01 00:00:00\' as TIMESTAMP) '
            . 'FROM `in.c-main`.`dedup_4` AS `src`',
            $sql,
        );
    }
}
