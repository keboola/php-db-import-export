<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\ToStage;

use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\StageTableDefinitionFactory;
use Keboola\TableBackendUtils\Column\ColumnCollection;

use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class StageTableDefinitionFactoryTest extends BaseTestCase
{
    public function testCreateStagingTableDefinitionWithTypes(): void
    {
        $definition = new SnowflakeTableDefinition(
            'schema',
            'table',
            false,
            new ColumnCollection([
                new SnowflakeColumn('name', new Snowflake(Snowflake::TYPE_DATE)),
                SnowflakeColumn::createGenericColumn('id'),
                ],),
            [],
        );
        $stageDefinition = StageTableDefinitionFactory::createStagingTableDefinition(
            $definition,
            ['id', 'name', 'notInDef'],
        );

        self::assertSame('schema', $stageDefinition->getSchemaName());
        self::assertStringStartsWith('__temp_', $stageDefinition->getTableName());
        self::assertTrue($stageDefinition->isTemporary());
        // order same as source
        self::assertSame(['id', 'name', 'notInDef'], $stageDefinition->getColumnsNames());
        /** @var SnowflakeColumn[] $definitions */
        $definitions = iterator_to_array($stageDefinition->getColumnsDefinitions());
        // id is NVARCHAR
        self::assertSame(Snowflake::TYPE_VARCHAR, $definitions[0]->getColumnDefinition()->getType());
        // name is DATE
        self::assertSame(Snowflake::TYPE_DATE, $definitions[1]->getColumnDefinition()->getType());
        // notInDef has default NVARCHAR
        self::assertSame(
            Snowflake::TYPE_VARCHAR,
            $definitions[2]->getColumnDefinition()->getType(),
        );
    }

    public function testCreateStagingTableDefinitionKeepsVectorTypeByDefault(): void
    {
        // Default (e.g. table-to-table load): the source holds a native VECTOR, so the staging
        // column must keep the VECTOR type and be copied as-is.
        $definition = new SnowflakeTableDefinition(
            'schema',
            'table',
            false,
            new ColumnCollection([
                new SnowflakeColumn('vector', new Snowflake(Snowflake::TYPE_VECTOR, ['length' => 'INT,3'])),
                new SnowflakeColumn('array', new Snowflake(Snowflake::TYPE_ARRAY)),
            ]),
            [],
        );
        $stageDefinition = StageTableDefinitionFactory::createStagingTableDefinition(
            $definition,
            ['vector', 'array'],
        );

        /** @var SnowflakeColumn[] $definitions */
        $definitions = iterator_to_array($stageDefinition->getColumnsDefinitions());
        self::assertSame('vector', $definitions[0]->getColumnName());
        self::assertSame(Snowflake::TYPE_VECTOR, $definitions[0]->getColumnDefinition()->getType());
        self::assertSame('array', $definitions[1]->getColumnName());
        self::assertSame(Snowflake::TYPE_ARRAY, $definitions[1]->getColumnDefinition()->getType());
    }

    public function testCreateStagingTableDefinitionStagesVectorAsVarcharForFileImport(): void
    {
        // File import: Snowflake cannot COPY a VECTOR column from a file - it arrives as JSON-array
        // text and must be staged as VARCHAR so the load succeeds; ToFinalTable then restores it via
        // PARSE_JSON::VECTOR. Other types keep the destination type.
        $definition = new SnowflakeTableDefinition(
            'schema',
            'table',
            false,
            new ColumnCollection([
                new SnowflakeColumn('vector', new Snowflake(Snowflake::TYPE_VECTOR, ['length' => 'INT,3'])),
                new SnowflakeColumn('array', new Snowflake(Snowflake::TYPE_ARRAY)),
            ]),
            [],
        );
        $stageDefinition = StageTableDefinitionFactory::createStagingTableDefinition(
            $definition,
            ['vector', 'array'],
            stageVectorAsVarchar: true,
        );

        /** @var SnowflakeColumn[] $definitions */
        $definitions = iterator_to_array($stageDefinition->getColumnsDefinitions());
        // vector is staged as VARCHAR (cannot be loaded from file directly)
        self::assertSame('vector', $definitions[0]->getColumnName());
        self::assertSame(Snowflake::TYPE_VARCHAR, $definitions[0]->getColumnDefinition()->getType());
        // other semi-structured types keep the destination type
        self::assertSame('array', $definitions[1]->getColumnName());
        self::assertSame(Snowflake::TYPE_ARRAY, $definitions[1]->getColumnDefinition()->getType());
    }

    public function testCreateStagingTableDefinitionWithText(): void
    {
        $columns = ['id', 'name', 'number', 'notInDef'];
        $stageDefinition = StageTableDefinitionFactory::createVarcharStagingTableDefinition(
            'schema',
            $columns,
        );

        self::assertSame('schema', $stageDefinition->getSchemaName());
        self::assertStringStartsWith('__temp_', $stageDefinition->getTableName());
        self::assertTrue($stageDefinition->isTemporary());
        // order same as source
        self::assertSame($columns, $stageDefinition->getColumnsNames());
        /** @var SnowflakeColumn[] $definitions */
        $definitions = iterator_to_array($stageDefinition->getColumnsDefinitions());
        foreach ($columns as $i => $columnName) {
            self::assertSame(Snowflake::TYPE_VARCHAR, $definitions[$i]->getColumnDefinition()->getType());
            self::assertSame($columnName, $definitions[$i]->getColumnName());
        }
    }
}
