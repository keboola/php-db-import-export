<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable;

use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\SourceDestinationColumnMap;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;

class SqlBuilder
{
    private const AUTO_CASTING_TYPES = [
        Snowflake::TYPE_VARIANT,
        Snowflake::TYPE_OBJECT,
        Snowflake::TYPE_ARRAY,
        Snowflake::TYPE_VECTOR,
    ];
    public const SRC_ALIAS = 'src';

    public function getBeginTransaction(): string
    {
        return 'BEGIN TRANSACTION';
    }

    public function getCommitTransaction(): string
    {
        return 'COMMIT';
    }

    /**
     * @param string[] $primaryKeys
     */
    public function getDedupCommand(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $deduplicationTableDefinition,
        array $primaryKeys,
    ): string {
        if (empty($primaryKeys)) {
            return '';
        }

        $pkSql = $this->getColumnsString(
            $primaryKeys,
            ',',
        );

        $stage = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
        );

        $depudeSql = sprintf(
            'SELECT %s FROM ('
            . 'SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS "_row_number_" '
            . 'FROM %s'
            . ') AS a '
            . 'WHERE a."_row_number_" = 1',
            $this->getColumnsString($deduplicationTableDefinition->getColumnsNames(), ',', 'a'),
            $this->getColumnsString($deduplicationTableDefinition->getColumnsNames(), ', '),
            $pkSql,
            $pkSql,
            $stage,
        );

        $deduplication = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($deduplicationTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($deduplicationTableDefinition->getTableName()),
        );

        return sprintf(
            'INSERT INTO %s (%s) %s',
            $deduplication,
            $this->getColumnsString($deduplicationTableDefinition->getColumnsNames()),
            $depudeSql,
        );
    }

    /**
     * @param string[] $columns
     */
    public function getColumnsString(
        array $columns,
        string $delimiter = ', ',
        ?string $tableAlias = null,
    ): string {
        return implode(
            $delimiter,
            array_map(
                static function ($columns) use (
                    $tableAlias,
                ) {
                    $alias = $tableAlias === null ? '' : $tableAlias . '.';
                    return $alias . SnowflakeQuote::quoteSingleIdentifier($columns);
                },
                $columns,
            ),
        );
    }

    public function getDeleteOldItemsCommand(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        SnowflakeImportOptions $importOptions,
    ): string {
        $stagingTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
        );

        $destinationTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName()),
        );

        return sprintf(
            'DELETE FROM %s "src" USING %s AS "dest" WHERE %s',
            $stagingTable,
            $destinationTable,
            $this->getPrimayKeyWhereConditions(
                $destinationTableDefinition->getPrimaryKeysNames(),
                $importOptions,
            ),
        );
    }

    /**
     * @param string[] $primaryKeys
     */
    private function getPrimayKeyWhereConditions(
        array $primaryKeys,
        SnowflakeImportOptions $importOptions,
    ): string {
        $pkWhereSql = array_map(
            function (string $col) use ($importOptions) {
                $str = '"dest".%s = COALESCE("src".%s, \'\')';
                if (!$importOptions->isNullManipulationEnabled()) {
                    $str = '"dest".%s = "src".%s';
                }
                return sprintf(
                    $str,
                    QuoteHelper::quoteIdentifier($col),
                    QuoteHelper::quoteIdentifier($col),
                );
            },
            $primaryKeys,
        );

        return implode(' AND ', $pkWhereSql) . ' ';
    }

    public function getDropTableIfExistsCommand(
        string $schema,
        string $tableName,
    ): string {
        return sprintf(
            'DROP TABLE IF EXISTS %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($schema),
            SnowflakeQuote::quoteSingleIdentifier($tableName),
        );
    }

    public function getInsertAllIntoTargetTableCommand(
        SnowflakeTableDefinition $sourceTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        SnowflakeImportOptions $importOptions,
        string $timestamp,
    ): string {
        [$insColumns, $columnsSetSql] = $this->getInsertColumnsAndValues(
            $sourceTableDefinition,
            $destinationTableDefinition,
            $importOptions,
            $timestamp,
            false,
        );

        return sprintf(
            'INSERT INTO %s (%s) (SELECT %s FROM %s.%s AS %s)',
            $this->getTableReference($destinationTableDefinition),
            $this->getColumnsString($insColumns),
            implode(',', $columnsSetSql),
            SnowflakeQuote::quoteSingleIdentifier($sourceTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($sourceTableDefinition->getTableName()),
            SnowflakeQuote::quoteSingleIdentifier(self::SRC_ALIAS),
        );
    }

    /**
     * Replaces the whole destination content in one statement. Snowflake truncates the target as part
     * of the INSERT, so neither an explicit transaction nor a separate TRUNCATE is needed, and unlike a
     * CTAS the table object — and with it the grants on it — is kept. Passing $dedupPrimaryKeys drops
     * duplicate rows inline instead of through an intermediate dedup table.
     *
     * @param string[] $dedupPrimaryKeys
     */
    public function getInsertOverwriteAllIntoTargetTableCommand(
        SnowflakeTableDefinition $sourceTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        SnowflakeImportOptions $importOptions,
        string $timestamp,
        array $dedupPrimaryKeys = [],
    ): string {
        [$insColumns, $columnsSetSql] = $this->getInsertColumnsAndValues(
            $sourceTableDefinition,
            $destinationTableDefinition,
            $importOptions,
            $timestamp,
            false,
        );

        return sprintf(
            'INSERT OVERWRITE INTO %s (%s) (SELECT %s FROM %s.%s AS %s%s)',
            $this->getTableReference($destinationTableDefinition),
            $this->getColumnsString($insColumns),
            implode(',', $columnsSetSql),
            SnowflakeQuote::quoteSingleIdentifier($sourceTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($sourceTableDefinition->getTableName()),
            SnowflakeQuote::quoteSingleIdentifier(self::SRC_ALIAS),
            $this->getDedupQualifyClause($dedupPrimaryKeys),
        );
    }

    /**
     * Applies the whole incremental import as one atomic statement: matched rows are updated in place,
     * unmatched rows inserted, and duplicates in the source are collapsed inline. The source subquery
     * exposes the raw staging columns, so the UPDATE and INSERT branches apply the null/cast handling
     * themselves, exactly as the separate UPDATE and INSERT statements do.
     */
    public function getMergeCommand(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        SnowflakeImportOptions $importOptions,
        string $timestamp,
    ): string {
        $columnsSet = $this->getColumnsSetForUpdate(
            $stagingTableDefinition,
            $destinationTableDefinition,
            $importOptions,
            $timestamp,
        );
        $columnsComparisonSql = $this->getColumnsComparisonSql(
            $stagingTableDefinition,
            $destinationTableDefinition,
            $importOptions,
        );
        [$insColumns, $insValues] = $this->getInsertColumnsAndValues(
            $stagingTableDefinition,
            $destinationTableDefinition,
            $importOptions,
            $timestamp,
            true,
        );

        $source = sprintf(
            '(SELECT %s FROM %s.%s AS %s%s)',
            $this->getColumnsString($stagingTableDefinition->getColumnsNames()),
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
            SnowflakeQuote::quoteSingleIdentifier(self::SRC_ALIAS),
            // dedup on the same key the ON condition joins by: with null manipulation a NULL and an
            // empty string collapse to the same key, and two source rows matching one target row is
            // what ERROR_ON_NONDETERMINISTIC_MERGE rejects
            $this->getDedupQualifyClause(
                $destinationTableDefinition->getPrimaryKeysNames(),
                $importOptions->isNullManipulationEnabled(),
            ),
        );

        // an empty comparison list means there is nothing that could have changed, so the update
        // applies unconditionally - the same fallback getUpdateWithPkCommand() uses
        $matchedCondition = $columnsComparisonSql === []
            ? ''
            : sprintf(' AND (%s)', implode(' OR ', $columnsComparisonSql));

        return sprintf(
            'MERGE INTO %s AS "dest" USING %s AS %s ON %s'
            . ' WHEN MATCHED%s THEN UPDATE SET %s'
            . ' WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)',
            $this->getTableReference($destinationTableDefinition),
            $source,
            SnowflakeQuote::quoteSingleIdentifier(self::SRC_ALIAS),
            $this->getPrimayKeyWhereConditions(
                $destinationTableDefinition->getPrimaryKeysNames(),
                $importOptions,
            ),
            $matchedCondition,
            implode(', ', $columnsSet),
            $this->getColumnsString($insColumns),
            implode(',', $insValues),
        );
    }

    /**
     * Keeps one row per primary key, picked the same way the dedup table picks it: ROW_NUMBER() ordered
     * by the primary key, i.e. an arbitrary but single row among duplicates. The window references the
     * columns through the source alias so it partitions on the raw values rather than on the SELECT
     * list expressions, matching what deduplicating into a separate table does.
     *
     * $normalizeNullsToEmptyString makes NULL and an empty string one key, for callers that go on to
     * join by COALESCE(pk, '') and would otherwise let two source rows reach the same target row.
     *
     * @param string[] $primaryKeys
     */
    private function getDedupQualifyClause(
        array $primaryKeys,
        bool $normalizeNullsToEmptyString = false,
    ): string {
        if ($primaryKeys === []) {
            return '';
        }

        // the source alias is quoted in the FROM clause, so the references must be quoted too -
        // an unquoted `src` would be resolved as SRC and would not match the quoted alias
        $sourceAlias = SnowflakeQuote::quoteSingleIdentifier(self::SRC_ALIAS);
        $pkSql = implode(', ', array_map(
            static function (string $columnName) use ($sourceAlias, $normalizeNullsToEmptyString): string {
                $reference = $sourceAlias . '.' . SnowflakeQuote::quoteSingleIdentifier($columnName);
                return $normalizeNullsToEmptyString
                    ? sprintf('COALESCE(%s, \'\')', $reference)
                    : $reference;
            },
            $primaryKeys,
        ));

        return sprintf(
            ' QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 1',
            $pkSql,
            $pkSql,
        );
    }

    private function getTableReference(SnowflakeTableDefinition $tableDefinition): string
    {
        return sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($tableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($tableDefinition->getTableName()),
        );
    }

    /**
     * Builds the destination column list and the matching value expressions for an insert.
     *
     * Shared by the INSERT ... SELECT builders and by the WHEN NOT MATCHED branch of getMergeCommand().
     * A MERGE VALUES list accepts no column aliases, and an unqualified column name there is ambiguous
     * because the target exposes the same names, so $forMergeValues drops the `AS <column>` suffix and
     * qualifies every column reference with the source alias.
     *
     * @return array{0: string[], 1: string[]}
     */
    private function getInsertColumnsAndValues(
        SnowflakeTableDefinition $sourceTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        SnowflakeImportOptions $importOptions,
        string $timestamp,
        bool $forMergeValues,
    ): array {
        $columnMap = SourceDestinationColumnMap::createForTables(
            $sourceTableDefinition,
            $destinationTableDefinition,
            $importOptions->ignoreColumns(),
            SourceDestinationColumnMap::MODE_MAP_BY_NAME,
        );

        $sourcePrefix = $forMergeValues
            ? SnowflakeQuote::quoteSingleIdentifier(self::SRC_ALIAS) . '.'
            : '';
        $ref = static fn(string $columnName): string
            => $sourcePrefix . SnowflakeQuote::quoteSingleIdentifier($columnName);
        $as = static fn(string $columnName): string => $forMergeValues
            ? ''
            : ' AS ' . SnowflakeQuote::quoteSingleIdentifier($columnName);

        $insColumns = [];
        $columnsSetSql = [];

        /** @var SnowflakeColumn $sourceColumn */
        foreach ($sourceTableDefinition->getColumnsDefinitions() as $sourceColumn) {
            $insColumns[] = $sourceColumn->getColumnName();

            // output mapping same tables are required do not convert nulls to empty strings
            if (!$importOptions->isNullManipulationEnabled()
                && !in_array($sourceColumn->getColumnName(), $importOptions->ignoreColumns(), true)
            ) {
                $destinationColumn = $columnMap->getDestination($sourceColumn);
                $type = $destinationColumn->getColumnDefinition()->getType();
                $useAutoCast = in_array($type, self::AUTO_CASTING_TYPES, true);
                $isSameType = $type === $sourceColumn->getColumnDefinition()->getType();
                if ($useAutoCast && !$isSameType) {
                    if ($type === Snowflake::TYPE_OBJECT) {
                        // object can't be casted from string but can be casted from variant
                        $columnsSetSql[] = sprintf(
                            'CAST(TO_VARIANT(%s) AS %s)%s',
                            $ref($sourceColumn->getColumnName()),
                            $destinationColumn->getColumnDefinition()->getSQLDefinition(),
                            $as($destinationColumn->getColumnName()),
                        );
                        continue;
                    }
                    if ($type === Snowflake::TYPE_ARRAY) {
                        $columnsSetSql[] = sprintf(
                            'CAST(PARSE_JSON(%s) AS %s)%s',
                            $ref($sourceColumn->getColumnName()),
                            $destinationColumn->getColumnDefinition()->getSQLDefinition(),
                            $as($destinationColumn->getColumnName()),
                        );
                        continue;
                    }
                    if ($type === Snowflake::TYPE_VECTOR) {
                        $columnsSetSql[] = sprintf(
                            'CAST(PARSE_JSON(%s) AS ARRAY)::%s%s',
                            $ref($sourceColumn->getColumnName()),
                            $destinationColumn->getColumnDefinition()->getSQLDefinition(),
                            $as($destinationColumn->getColumnName()),
                        );
                        continue;
                    }
                    $columnsSetSql[] = sprintf(
                        'CAST(%s AS %s)%s',
                        $ref($sourceColumn->getColumnName()),
                        $destinationColumn->getColumnDefinition()->getSQLDefinition(),
                        $as($destinationColumn->getColumnName()),
                    );
                    continue;
                }
                $columnsSetSql[] = $ref($sourceColumn->getColumnName());
                continue;
            }

            // Input mapping convert empty values to null
            // empty strings '' are converted to null values
            if (in_array($sourceColumn->getColumnName(), $importOptions->getConvertEmptyValuesToNull(), true)) {
                // use nullif only for string base type
                if ($sourceColumn->getColumnDefinition()->getBasetype() === BaseType::STRING) {
                    $columnsSetSql[] = sprintf(
                        'IFF(%s = \'\', NULL, %s)',
                        $ref($sourceColumn->getColumnName()),
                        $ref($sourceColumn->getColumnName()),
                    );
                    continue;
                }
                // if tables is not typed column could be other than string in this case we skip conversion
                $columnsSetSql[] = $ref($sourceColumn->getColumnName());
                continue;
            }

            // for string base type convert null values to empty string ''
            //phpcs:ignore
            if (!$importOptions->usingUserDefinedTypes() && $sourceColumn->getColumnDefinition()->getBasetype() === BaseType::STRING) {
                $columnsSetSql[] = sprintf(
                    'COALESCE(%s, \'\')%s',
                    $ref($sourceColumn->getColumnName()),
                    $as($sourceColumn->getColumnName()),
                );
                continue;
            }
            // on columns other than string dont use COALESCE
            // this will fail if the column is not null, but this is expected
            $columnsSetSql[] = $ref($sourceColumn->getColumnName());
        }

        $useTimestamp = !in_array(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME, $insColumns, true)
            && $importOptions->useTimestamp();

        if ($useTimestamp) {
            $insColumns[] = ToStageImporterInterface::TIMESTAMP_COLUMN_NAME;
            $columnsSetSql[] = SnowflakeQuote::quote($timestamp);
        }

        return [$insColumns, $columnsSetSql];
    }

    public function getTruncateTable(
        string $schema,
        string $tableName,
    ): string {
        return sprintf(
            'TRUNCATE TABLE %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($schema),
            SnowflakeQuote::quoteSingleIdentifier($tableName),
        );
    }

    /**
     * Generates a CREATE TABLE AS SELECT (CTAS) command to create the destination table from the staging table.
     * Adds a _timestamp column with the current timestamp value.
     */
    public function getCTASInsertAllIntoTargetTableCommand(
        SnowflakeTableDefinition $sourceTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        string $timestamp,
    ): string {
        $timestampColumn = sprintf(
            '\'%s\' AS %s',
            $timestamp,
            SnowflakeQuote::quoteSingleIdentifier(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME),
        );

        // Build the source table reference
        $sourceTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($sourceTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($sourceTableDefinition->getTableName()),
        );

        // Build the destination table reference
        $destinationTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName()),
        );

        // Map destination columns by name so source values can be coerced to the destination
        // column type. A non-typed (string) destination registers its columns as a length-less
        // VARCHAR, while the workspace CTAS output (the source) may carry explicit lengths or
        // non-string types (e.g. TRY_CAST(... AS INTEGER)). A plain `CREATE OR REPLACE ... AS
        // SELECT` would copy those source types verbatim; casting to the destination type keeps
        // such columns string-typed and matching the registered schema (DMD-1575).
        $destinationColumnsByName = [];
        foreach ($destinationTableDefinition->getColumnsDefinitions() as $destinationColumn) {
            $destinationColumnsByName[$destinationColumn->getColumnName()] = $destinationColumn;
        }

        $columns = [];
        foreach ($sourceTableDefinition->getColumnsNames() as $columnName) {
            if ($columnName === ToStageImporterInterface::TIMESTAMP_COLUMN_NAME) {
                continue;
            }
            $quotedColumn = SnowflakeQuote::quoteSingleIdentifier($columnName);
            $destinationColumn = $destinationColumnsByName[$columnName] ?? null;
            // A generic non-typed (string) destination column (length-less VARCHAR NOT NULL
            // DEFAULT '') is coerced to its registered type so the recreated table keeps the
            // VARCHAR typing. Other length-less types (e.g. TIMESTAMP_NTZ) keep their source value.
            if ($destinationColumn instanceof SnowflakeColumn && $destinationColumn->isGenericColumn()) {
                $columns[] = sprintf(
                    'CAST(%s AS %s) AS %s',
                    $quotedColumn,
                    $destinationColumn->getColumnDefinition()->getType(),
                    $quotedColumn,
                );
                continue;
            }
            $columns[] = $quotedColumn;
        }

        // Create the CTAS command
        return sprintf(
            'CREATE OR REPLACE TABLE %s AS SELECT %s FROM %s',
            $destinationTable,
            implode(',', [...$columns, $timestampColumn]),
            $sourceTable,
        );
    }

    public function getUpdateWithPkCommand(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationDefinition,
        SnowflakeImportOptions $importOptions,
        string $timestamp,
    ): string {
        $columnsSet = $this->getColumnsSetForUpdate(
            $stagingTableDefinition,
            $destinationDefinition,
            $importOptions,
            $timestamp,
        );
        $columnsComparisonSql = $this->getColumnsComparisonSql(
            $stagingTableDefinition,
            $destinationDefinition,
            $importOptions,
        );

        $dest = $this->getTableReference($destinationDefinition);

        if ($columnsComparisonSql === []) {
            return sprintf(
                'UPDATE %s AS "dest" SET %s FROM %s.%s AS "src" WHERE %s',
                $dest,
                implode(', ', $columnsSet),
                QuoteHelper::quoteIdentifier($stagingTableDefinition->getSchemaName()),
                QuoteHelper::quoteIdentifier($stagingTableDefinition->getTableName()),
                $this->getPrimayKeyWhereConditions($destinationDefinition->getPrimaryKeysNames(), $importOptions),
            );
        }

        return sprintf(
            'UPDATE %s AS "dest" SET %s FROM %s.%s AS "src" WHERE %s AND (%s)',
            $dest,
            implode(', ', $columnsSet),
            QuoteHelper::quoteIdentifier($stagingTableDefinition->getSchemaName()),
            QuoteHelper::quoteIdentifier($stagingTableDefinition->getTableName()),
            $this->getPrimayKeyWhereConditions($destinationDefinition->getPrimaryKeysNames(), $importOptions),
            implode(' OR ', $columnsComparisonSql),
        );
    }

    /**
     * Builds the `SET` assignments of an update. The expressions reference the source through the
     * "src" alias, so they fit both the UPDATE ... FROM form and the WHEN MATCHED branch of a MERGE.
     *
     * @return string[]
     */
    private function getColumnsSetForUpdate(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationDefinition,
        SnowflakeImportOptions $importOptions,
        string $timestamp,
    ): array {
        $columnMap = SourceDestinationColumnMap::createForTables(
            $stagingTableDefinition,
            $destinationDefinition,
            $importOptions->ignoreColumns(),
            SourceDestinationColumnMap::MODE_MAP_BY_NAME,
        );
        $columnsSet = [];

        foreach ($stagingTableDefinition->getColumnsDefinitions() as $sourceColumn) {
            if (!$importOptions->isNullManipulationEnabled()) {
                $destinationColumn = $columnMap->getDestination($sourceColumn);
                $type = $destinationColumn->getColumnDefinition()->getType();
                $useAutoCast = in_array($type, self::AUTO_CASTING_TYPES, true);
                $isSameType = $type === $sourceColumn->getColumnDefinition()->getType();
                if ($useAutoCast && !$isSameType) {
                    if ($type === Snowflake::TYPE_OBJECT) {
                        // object can't be casted from string but can be casted from variant
                        $columnsSet[] = sprintf(
                            '%s = CAST(TO_VARIANT("src".%s) AS %s)',
                            SnowflakeQuote::quoteSingleIdentifier($destinationColumn->getColumnName()),
                            SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                            $destinationColumn->getColumnDefinition()->getSQLDefinition(),
                        );
                        continue;
                    }
                    if ($type === Snowflake::TYPE_ARRAY) {
                        $columnsSet[] = sprintf(
                            '%s = CAST(PARSE_JSON("src".%s) AS %s)',
                            SnowflakeQuote::quoteSingleIdentifier($destinationColumn->getColumnName()),
                            SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                            $destinationColumn->getColumnDefinition()->getSQLDefinition(),
                        );
                        continue;
                    }
                    if ($type === Snowflake::TYPE_VECTOR) {
                        $columnsSet[] = sprintf(
                            '%s = CAST(PARSE_JSON("src".%s) AS ARRAY)::%s',
                            SnowflakeQuote::quoteSingleIdentifier($destinationColumn->getColumnName()),
                            SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                            $destinationColumn->getColumnDefinition()->getSQLDefinition(),
                        );
                        continue;
                    }
                    $columnsSet[] = sprintf(
                        '%s = CAST("src".%s AS %s)',
                        SnowflakeQuote::quoteSingleIdentifier($destinationColumn->getColumnName()),
                        SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                        $destinationColumn->getColumnDefinition()->getSQLDefinition(),
                    );
                    continue;
                }

                $columnsSet[] = sprintf(
                    '%s = "src".%s',
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                );
                continue;
            }
            if (in_array($sourceColumn->getColumnName(), $importOptions->getConvertEmptyValuesToNull(), true)) {
                $columnsSet[] = sprintf(
                    '%s = IFF("src".%s = \'\', NULL, "src".%s)',
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                );
            } else {
                $columnsSet[] = sprintf(
                    '%s = COALESCE("src".%s, \'\')',
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                );
            }
        }

        if ($importOptions->useTimestamp()) {
            $columnsSet[] = sprintf(
                '%s = \'%s\'',
                SnowflakeQuote::quoteSingleIdentifier(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME),
                $timestamp,
            );
        }

        return $columnsSet;
    }

    /**
     * Builds the per-column "did this row actually change" predicates, OR-ed together by the caller to
     * skip rewriting rows whose values are identical. Both sides are referenced through the "dest" and
     * "src" aliases, so the result fits an UPDATE ... FROM as well as a MERGE ... WHEN MATCHED AND.
     *
     * @return string[]
     */
    private function getColumnsComparisonSql(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationDefinition,
        SnowflakeImportOptions $importOptions,
    ): array {
        if ($importOptions->isNullManipulationEnabled()) {
            // update only changed rows - mysql TIMESTAMP ON UPDATE behaviour simulation
            return array_map(
                static function ($columnName) {
                    return sprintf(
                        'COALESCE(TO_VARCHAR("dest".%s), \'\') != COALESCE("src".%s, \'\')',
                        SnowflakeQuote::quoteSingleIdentifier($columnName),
                        SnowflakeQuote::quoteSingleIdentifier($columnName),
                    );
                },
                $stagingTableDefinition->getColumnsNames(),
            );
        }

        $columnMap = SourceDestinationColumnMap::createForTables(
            $stagingTableDefinition,
            $destinationDefinition,
            $importOptions->ignoreColumns(),
            SourceDestinationColumnMap::MODE_MAP_BY_NAME,
        );
        $columnsComparisonSql = [];
        foreach ($stagingTableDefinition->getColumnsDefinitions() as $sourceColumn) {
            $destinationColumn = $columnMap->getDestination($sourceColumn);
            if (in_array(
                $destinationColumn->getColumnDefinition()->getType(),
                [
                Snowflake::TYPE_GEOGRAPHY,
                Snowflake::TYPE_GEOMETRY,
                ],
                true,
            )
            ) {
                $columnsComparisonSql[] = sprintf(
                    'ST_ASEWKT("dest".%s) IS DISTINCT FROM ST_ASEWKT("src".%s)',
                    SnowflakeQuote::quoteSingleIdentifier($destinationColumn->getColumnName()),
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                );
            } else {
                $columnsComparisonSql[] = sprintf(
                    '"dest".%s IS DISTINCT FROM "src".%s',
                    SnowflakeQuote::quoteSingleIdentifier($destinationColumn->getColumnName()),
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                );
            }
        }

        return $columnsComparisonSql;
    }
}
