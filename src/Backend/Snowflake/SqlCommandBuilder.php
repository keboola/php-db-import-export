<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\ImportExport\Backend\Snowflake\Helper\ColumnsHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\Snowflake\Table;
use Keboola\SnowflakeDbAdapter\QueryBuilder;

class SqlCommandBuilder
{
    public function getBeginTransaction(): string
    {
        return 'BEGIN TRANSACTION';
    }

    public function getCommitTransaction(): string
    {
        return 'COMMIT';
    }

    public function getCreateStagingTableCommand(
        string $schema,
        string $tableName,
        array $columns
    ): string {
        $columnsSql = array_map(function ($column) {
            return sprintf('%s varchar', QueryBuilder::quoteIdentifier($column));
        }, $columns);
        return sprintf(
            'CREATE TEMPORARY TABLE %s.%s (%s)',
            QueryBuilder::quoteIdentifier($schema),
            QueryBuilder::quoteIdentifier($tableName),
            implode(', ', $columnsSql)
        );
    }

    public function getDedupCommand(
        Table $destination,
        ImportOptions $importOptions,
        array $primaryKeys,
        string $stagingTableName,
        string $tempTableName
    ): string {
        if (empty($primaryKeys)) {
            return '';
        }

        $pkSql = ColumnsHelper::getColumnsString(
            $primaryKeys,
            ','
        );

        $depudeSql = sprintf(
            'SELECT %s FROM ('
            .'SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS "_row_number_"'
            .'FROM %s.%s'
            .') AS a '
            .'WHERE a."_row_number_" = 1',
            ColumnsHelper::getColumnsString($importOptions->getColumns(), ',', 'a'),
            ColumnsHelper::getColumnsString($importOptions->getColumns(), ', '),
            $pkSql,
            $pkSql,
            QueryBuilder::quoteIdentifier($destination->getSchema()),
            QueryBuilder::quoteIdentifier($stagingTableName)
        );

        return sprintf(
            'INSERT INTO %s.%s (%s) %s',
            QueryBuilder::quoteIdentifier($destination->getSchema()),
            QueryBuilder::quoteIdentifier($tempTableName),
            ColumnsHelper::getColumnsString($importOptions->getColumns()),
            $depudeSql
        );
    }

    public function getDeleteOldItemsCommand(
        Table $destination,
        string $stagingTableName,
        array $primaryKeys
    ): string {
        // Delete updated rows from staging table
        return sprintf(
            'DELETE FROM %s.%s "src" USING %s AS "dest" WHERE %s',
            QueryBuilder::quoteIdentifier($destination->getSchema()),
            QueryBuilder::quoteIdentifier($stagingTableName),
            $destination->getQuotedTableWithScheme(),
            $this->getPrimayKeyWhereConditions($primaryKeys)
        );
    }

    private function getPrimayKeyWhereConditions(
        array $primaryKeys
    ): string {
        $pkWhereSql = array_map(function (string $col) {
            return sprintf(
                '"dest".%s = COALESCE("src".%s, \'\')',
                QueryBuilder::quoteIdentifier($col),
                QueryBuilder::quoteIdentifier($col)
            );
        }, $primaryKeys);

        return implode(' AND ', $pkWhereSql) . ' ';
    }

    public function getDropCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'DROP TABLE %s.%s',
            QueryBuilder::quoteIdentifier($schema),
            QueryBuilder::quoteIdentifier($tableName)
        );
    }

    public function getInsertAllIntoTargetTableCommand(
        Table $destination,
        ImportOptions $importOptions,
        string $stagingTableName
    ): string {
        $columnsSetSqlSelect = implode(', ', array_map(function ($column) use (
            $importOptions
        ) {
            if (in_array($column, $importOptions->getConvertEmptyValuesToNull())) {
                return sprintf(
                    'IFF(%s = \'\', NULL, %s)',
                    QueryBuilder::quoteIdentifier($column),
                    QueryBuilder::quoteIdentifier($column)
                );
            }

            return sprintf(
                "COALESCE(%s, '') AS %s",
                QueryBuilder::quoteIdentifier($column),
                QueryBuilder::quoteIdentifier($column)
            );
        }, $importOptions->getColumns()));

        if (in_array(Importer::TIMESTAMP_COLUMN_NAME, $importOptions->getColumns())
            || $importOptions->useTimestamp() === false
        ) {
            return sprintf(
                'INSERT INTO %s (%s) (SELECT %s FROM %s.%s)',
                $destination->getQuotedTableWithScheme(),
                ColumnsHelper::getColumnsString($importOptions->getColumns()),
                $columnsSetSqlSelect,
                QueryBuilder::quoteIdentifier($destination->getSchema()),
                QueryBuilder::quoteIdentifier($stagingTableName)
            );
        }

        return sprintf(
            'INSERT INTO %s (%s, "%s") (SELECT %s, \'%s\' FROM %s.%s)',
            $destination->getQuotedTableWithScheme(),
            ColumnsHelper::getColumnsString($importOptions->getColumns()),
            Importer::TIMESTAMP_COLUMN_NAME,
            $columnsSetSqlSelect,
            DateTimeHelper::getNowFormatted(),
            QueryBuilder::quoteIdentifier($destination->getSchema()),
            QueryBuilder::quoteIdentifier($stagingTableName)
        );
    }

    public function getInsertFromStagingToTargetTableCommand(
        Table $destination,
        ImportOptions $importOptions,
        string $stagingTableName
    ): string {
        if ($importOptions->useTimestamp()) {
            $insColumns = array_merge($importOptions->getColumns(), [Importer::TIMESTAMP_COLUMN_NAME]);
        } else {
            $insColumns = $importOptions->getColumns();
        }

        $columnsSetSql = [];

        foreach ($importOptions->getColumns() as $columnName) {
            if (in_array($columnName, $importOptions->getConvertEmptyValuesToNull())) {
                $columnsSetSql[] = sprintf(
                    'IFF("src".%s = \'\', NULL, %s)',
                    QueryBuilder::quoteIdentifier($columnName),
                    QueryBuilder::quoteIdentifier($columnName)
                );
            } else {
                $columnsSetSql[] = sprintf(
                    'COALESCE("src".%s, \'\')',
                    QueryBuilder::quoteIdentifier($columnName)
                );
            }
        }

        if ($importOptions->useTimestamp()) {
            $columnsSetSql[] = sprintf('\'%s\'', DateTimeHelper::getNowFormatted());
        }

        return sprintf(
            'INSERT INTO %s (%s) SELECT %s FROM %s.%s AS "src"',
            $destination->getQuotedTableWithScheme(),
            ColumnsHelper::getColumnsString($insColumns),
            implode(',', $columnsSetSql),
            QueryBuilder::quoteIdentifier($destination->getSchema()),
            QueryBuilder::quoteIdentifier($stagingTableName)
        );
    }

    public function getRenameTableCommand(
        string $schema,
        string $sourceTableName,
        string $targetTable
    ): string {
        return sprintf(
            'ALTER TABLE %s.%s RENAME TO %s.%s',
            QueryBuilder::quoteIdentifier($schema),
            QueryBuilder::quoteIdentifier($sourceTableName),
            QueryBuilder::quoteIdentifier($schema),
            QueryBuilder::quoteIdentifier($targetTable)
        );
    }

    public function getTruncateTableCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'TRUNCATE %s.%s',
            QueryBuilder::quoteIdentifier($schema),
            QueryBuilder::quoteIdentifier($tableName)
        );
    }

    public function getUpdateWithPkCommand(
        Table $destination,
        ImportOptions $importOptions,
        string $stagingTableName,
        array $primaryKeys
    ): string {
        $columnsSet = [];
        foreach ($importOptions->getColumns() as $columnName) {
            if (in_array($columnName, $importOptions->getConvertEmptyValuesToNull())) {
                $columnsSet[] = sprintf(
                    '%s = IFF("src".%s = \'\', NULL, "src".%s)',
                    QueryBuilder::quoteIdentifier($columnName),
                    QueryBuilder::quoteIdentifier($columnName),
                    QueryBuilder::quoteIdentifier($columnName)
                );
            } else {
                $columnsSet[] = sprintf(
                    '%s = COALESCE("src".%s, \'\')',
                    QueryBuilder::quoteIdentifier($columnName),
                    QueryBuilder::quoteIdentifier($columnName)
                );
            }
        }

        if ($importOptions->useTimestamp()) {
            $columnsSet[] = sprintf(
                '%s = \'%s\'',
                QueryBuilder::quoteIdentifier(Importer::TIMESTAMP_COLUMN_NAME),
                DateTimeHelper::getNowFormatted()
            );
        }

        // update only changed rows - mysql TIMESTAMP ON UPDATE behaviour simulation
        $columnsComparsionSql = array_map(
            function ($columnName) {
                return sprintf(
                    'COALESCE(TO_VARCHAR("dest".%s), \'\') != COALESCE("src".%s, \'\')',
                    QueryBuilder::quoteIdentifier($columnName),
                    QueryBuilder::quoteIdentifier($columnName)
                );
            },
            $importOptions->getColumns()
        );

        $sql = sprintf(
            'UPDATE %s AS "dest" SET %s FROM %s.%s AS "src" WHERE %s AND (%s) ',
            $destination->getQuotedTableWithScheme(),
            implode(', ', $columnsSet),
            QueryBuilder::quoteIdentifier($destination->getSchema()),
            QueryBuilder::quoteIdentifier($stagingTableName),
            $this->getPrimayKeyWhereConditions($primaryKeys),
            implode(' OR ', $columnsComparsionSql)
        );

        return $sql;
    }
}
