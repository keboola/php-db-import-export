<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\ToStage;

use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\Helper\BackendHelper;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

final class StageTableDefinitionFactory
{
    /**
     * @param string[] $sourceColumnsNames
     * @param bool $stageVectorAsVarchar When the data is loaded from a file, a VECTOR column arrives
     *     as its serialized JSON-array text (Snowflake cannot COPY a VECTOR directly from a file -
     *     see SnowflakeQueryBuilder, DMD-1680). Stage such columns as VARCHAR so the load succeeds;
     *     the ToFinalTable importer restores them via CAST(PARSE_JSON(..) AS ARRAY)::VECTOR. For
     *     table-to-table imports the source already holds a native VECTOR, so this must stay false.
     */
    public static function createStagingTableDefinition(
        TableDefinitionInterface $destination,
        array $sourceColumnsNames,
        bool $stageVectorAsVarchar = false,
    ): SnowflakeTableDefinition {
        /** @var SnowflakeTableDefinition $destination */
        $newDefinitions = [];
        // create staging table for source columns in order
        // but with types from destination
        // also maintain source columns order
        foreach ($sourceColumnsNames as $columnName) {
            /** @var SnowflakeColumn $definition */
            foreach ($destination->getColumnsDefinitions() as $definition) {
                if ($definition->getColumnName() === $columnName) {
                    if ($stageVectorAsVarchar
                        && $definition->getColumnDefinition()->getType() === Snowflake::TYPE_VECTOR
                    ) {
                        $newDefinitions[] = self::createNvarcharColumn($columnName);
                        continue 2;
                    }
                    // if column exists in destination set destination type
                    $newDefinitions[] = new SnowflakeColumn(
                        $columnName,
                        new Snowflake(
                            $definition->getColumnDefinition()->getType(),
                            [
                                'length' => $definition->getColumnDefinition()->getLength(),
                                'nullable' => true, // set all columns to be nullable
                                'default' => $definition->getColumnDefinition()->getDefault(),
                            ],
                        ),
                    );
                    continue 2;
                }
            }
            // if column doesn't exists in destination set default type
            $newDefinitions[] = self::createNvarcharColumn($columnName);
        }

        return new SnowflakeTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateStagingTableName(),
            true,
            new ColumnCollection($newDefinitions),
            $destination->getPrimaryKeysNames(),
        );
    }

    /**
     * @param string[] $sourceColumnsNames
     */
    public static function createVarcharStagingTableDefinition(
        string $schemaName,
        array $sourceColumnsNames,
    ): SnowflakeTableDefinition {
        $newDefinitions = [];
        // create staging table for source columns in order
        foreach ($sourceColumnsNames as $columnName) {
            $newDefinitions[] = self::createNvarcharColumn($columnName);
        }

        return new SnowflakeTableDefinition(
            $schemaName,
            BackendHelper::generateStagingTableName(),
            true,
            new ColumnCollection($newDefinitions),
            [],
        );
    }

    private static function createNvarcharColumn(string $columnName): SnowflakeColumn
    {
        return new SnowflakeColumn(
            $columnName,
            new Snowflake(
                Snowflake::TYPE_VARCHAR,
                [
                    'length' => (string) Snowflake::DEFAULT_VARCHAR_LENGTH,
                    'nullable' => true, // set all columns to be nullable
                ],
            ),
        );
    }

    /**
     * @param string[] $pkNames
     */
    public static function createDedupTableDefinition(
        SnowflakeTableDefinition $destination,
        array $pkNames,
    ): SnowflakeTableDefinition {
        return new SnowflakeTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateTempDedupTableName(),
            true,
            $destination->getColumnsDefinitions(),
            $pkNames,
        );
    }
}
