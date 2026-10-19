<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\Export;

use Doctrine\DBAL\Connection;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;

/**
 * Snowflake cannot unload TIMESTAMP_LTZ/TZ to Parquet, so these columns are exported as their UTC wall-clock time.
 */
final class ParquetTimestampColumn
{
    private const ZONED_TIMESTAMP_TYPES = [
        Snowflake::TYPE_TIMESTAMP_LTZ,
        Snowflake::TYPE_TIMESTAMP_TZ,
    ];

    /**
     * Accepts a base type name or a full definition such as TIMESTAMP_LTZ(9).
     */
    public static function requiresUtcConversion(string $type): bool
    {
        return in_array(strtoupper(self::baseTypeName($type)), self::ZONED_TIMESTAMP_TYPES, true);
    }

    /**
     * Physical column types: a column declared as TIMESTAMP is LTZ or TZ under the account TIMESTAMP_TYPE_MAPPING,
     * which table metadata does not record. DESC TABLE also describes views and other table-like objects the table
     * reflection rejects, and full column definitions are not parsed, because they reject structured types such as
     * ARRAY(NUMBER).
     *
     * @return array<string, string> type as described by Snowflake, by column name in table order
     */
    public static function describeColumnTypes(Connection $connection, string $schemaName, string $tableName): array
    {
        /** @var array<array{name: string, kind: string, type: string}> $describedColumns */
        $describedColumns = $connection->fetchAllAssociative(sprintf(
            'DESC TABLE %s',
            SnowflakeQuote::createQuotedIdentifierFromParts([$schemaName, $tableName]),
        ));
        $columnTypes = [];
        foreach ($describedColumns as $describedColumn) {
            if (in_array($describedColumn['kind'], SnowflakeTableReflection::RECOGNIZED_COLUMN_KINDS, true)) {
                $columnTypes[$describedColumn['name']] = $describedColumn['type'];
            }
        }

        return $columnTypes;
    }

    /**
     * @return list<string>
     */
    public static function findColumnsRequiringUtcConversion(
        Connection $connection,
        string $schemaName,
        string $tableName,
    ): array {
        $columns = [];
        foreach (self::describeColumnTypes($connection, $schemaName, $tableName) as $column => $type) {
            if (self::requiresUtcConversion($type)) {
                // PHP turns a numeric column name used as an array key into an int.
                $columns[] = (string) $column;
            }
        }

        return $columns;
    }

    /**
     * Keeps the column name, because Parquet takes the header from the select alias.
     */
    public static function createUtcSelectExpression(string $quotedColumnName): string
    {
        return sprintf(
            'CONVERT_TIMEZONE(\'UTC\', %s)::TIMESTAMP_NTZ AS %s',
            $quotedColumnName,
            $quotedColumnName,
        );
    }

    private static function baseTypeName(string $type): string
    {
        return preg_match('/^\w+/', $type, $matches) === 1 ? $matches[0] : $type;
    }
}
