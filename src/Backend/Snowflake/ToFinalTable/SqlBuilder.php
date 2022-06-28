<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable;

use Keboola\Datatype\Definition\BaseType;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;

class SqlBuilder
{
    public const SRC_ALIAS = 'src';

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
        array $primaryKeys
    ): string {
        if (empty($primaryKeys)) {
            return '';
        }


        $pkSql = $this->getColumnsString(
            $primaryKeys,
            ','
        );

        $stage = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName())
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
            $stage
        );

        $deduplication = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($deduplicationTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($deduplicationTableDefinition->getTableName())
        );

        return sprintf(
            'INSERT INTO %s (%s) %s',
            $deduplication,
            $this->getColumnsString($deduplicationTableDefinition->getColumnsNames()),
            $depudeSql
        );
    }

    /**
     * @param string[] $columns
     */
    public function getColumnsString(
        array $columns,
        string $delimiter = ', ',
        ?string $tableAlias = null
    ): string {
        return implode($delimiter, array_map(static function ($columns) use (
            $tableAlias
        ) {
            $alias = $tableAlias === null ? '' : $tableAlias . '.';
            return $alias . SnowflakeQuote::quoteSingleIdentifier($columns);
        }, $columns));
    }

    public function getDeleteOldItemsCommand(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition
    ): string {
        $stagingTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName())
        );

        $destinationTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName())
        );

        return sprintf(
            'DELETE FROM %s "src" USING %s AS "dest" WHERE %s',
            $stagingTable,
            $destinationTable,
            $this->getPrimayKeyWhereConditions($destinationTableDefinition->getPrimaryKeysNames())
        );
    }

    /**
     * @param string[] $primaryKeys
     */
    private function getPrimayKeyWhereConditions(
        array $primaryKeys
    ): string {
        $pkWhereSql = array_map(function (string $col) {
            return sprintf(
                '"dest".%s = COALESCE("src".%s, \'\')',
                QuoteHelper::quoteIdentifier($col),
                QuoteHelper::quoteIdentifier($col)
            );
        }, $primaryKeys);

        return implode(' AND ', $pkWhereSql) . ' ';
    }


    public function getDropTableIfExistsCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'DROP TABLE IF EXISTS %s.%s CASCADE',
            SnowflakeQuote::quoteSingleIdentifier($schema),
            SnowflakeQuote::quoteSingleIdentifier($tableName)
        );
    }

    public function getInsertAllIntoTargetTableCommand(
        SnowflakeTableDefinition $sourceTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        SnowflakeImportOptions $importOptions,
        string $timestamp
    ): string {
        $destinationTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName())
        );

        $insColumns = $sourceTableDefinition->getColumnsNames();
        $useTimestamp = !in_array(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME, $insColumns, true)
            && $importOptions->useTimestamp();

        if ($useTimestamp) {
            $insColumns = array_merge(
                $sourceTableDefinition->getColumnsNames(),
                [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME]
            );
        }

        $columnsSetSql = [];

        /** @var SnowflakeColumn $columnDefinition */
        foreach ($sourceTableDefinition->getColumnsDefinitions() as $columnDefinition) {
            if (in_array($columnDefinition->getColumnName(), $importOptions->getConvertEmptyValuesToNull(), true)) {
                // use nullif only for string base type
                if ($columnDefinition->getColumnDefinition()->getBasetype() === BaseType::STRING) {
                    $columnsSetSql[] = sprintf(
                        'IFF(%s = \'\', NULL, %s)',
                        SnowflakeQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                        SnowflakeQuote::quoteSingleIdentifier($columnDefinition->getColumnName())
                    );
                } else {
                    $columnsSetSql[] = SnowflakeQuote::quoteSingleIdentifier($columnDefinition->getColumnName());
                }
            } else {
                $columnsSetSql[] = sprintf(
                    'CAST(COALESCE(%s, \'\') AS %s) AS %s',
                    SnowflakeQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    $columnDefinition->getColumnDefinition()->buildDefinitionString(),
                    SnowflakeQuote::quoteSingleIdentifier($columnDefinition->getColumnName())
                );
            }
        }

        if ($useTimestamp) {
            $columnsSetSql[] = SnowflakeQuote::quote($timestamp);
        }

        return sprintf(
            'INSERT INTO %s (%s) (SELECT %s FROM %s.%s AS %s)',
            $destinationTable,
            $this->getColumnsString($insColumns),
            implode(',', $columnsSetSql),
            SnowflakeQuote::quoteSingleIdentifier($sourceTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($sourceTableDefinition->getTableName()),
            SnowflakeQuote::quoteSingleIdentifier(self::SRC_ALIAS)
        );
    }

    public function getTruncateTableWithDeleteCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'DELETE FROM %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($schema),
            SnowflakeQuote::quoteSingleIdentifier($tableName)
        );
    }
}