<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Synapse\ToStage;

use Keboola\Datatype\Definition\Synapse;
use Keboola\Db\ImportExport\Backend\Synapse\Helper\BackendHelper;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\StageTableDefinitionFactory;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\Table\Synapse\TableDistributionDefinition;
use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class StageTableDefinitionFactoryTest extends BaseTestCase
{
    public function testCreateStagingTableDefinition(): void
    {
        $definition = new SynapseTableDefinition(
            'schema',
            'table',
            false,
            new ColumnCollection([
                new SynapseColumn('name', new Synapse(Synapse::TYPE_DATE)),
                SynapseColumn::createGenericColumn('id'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
        $stageDefinition = StageTableDefinitionFactory::createStagingTableDefinition(
            $definition,
            ['id', 'name', 'notInDef']
        );

        self::assertSame('schema', $stageDefinition->getSchemaName());
        self::assertStringStartsWith('#__temp_csvimport', $stageDefinition->getTableName());
        self::assertTrue($stageDefinition->isTemporary());
        // order same as source
        self::assertSame(['id', 'name', 'notInDef'], $stageDefinition->getColumnsNames());
        /** @var SynapseColumn[] $definitions */
        $definitions = iterator_to_array($stageDefinition->getColumnsDefinitions());
        // id is NVARCHAR
        self::assertSame(Synapse::TYPE_NVARCHAR, $definitions[0]->getColumnDefinition()->getType());
        // name is DATE
        self::assertSame(Synapse::TYPE_DATE, $definitions[1]->getColumnDefinition()->getType());
        // notInDef has default NVARCHAR
        self::assertSame(Synapse::TYPE_NVARCHAR, $definitions[2]->getColumnDefinition()->getType());
        self::assertSame(
            TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN,
            $stageDefinition->getTableDistribution()->getDistributionName()
        );
        // index is heap
        self::assertSame(
            TableIndexDefinition::TABLE_INDEX_TYPE_HEAP,
            $stageDefinition->getTableIndex()->getIndexType()
        );
    }

    public function testCreateStagingTableDefinitionWithIndex(): void
    {
        $definition = new SynapseTableDefinition(
            'schema',
            'table',
            false,
            new ColumnCollection([
                new SynapseColumn('name', new Synapse(Synapse::TYPE_DATE)),
                SynapseColumn::createGenericColumn('id'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
        $stageDefinition = StageTableDefinitionFactory::createStagingTableDefinition(
            $definition,
            ['id', 'name', 'notInDef'],
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX)
        );

        self::assertSame('schema', $stageDefinition->getSchemaName());
        self::assertStringStartsWith('#__temp_csvimport', $stageDefinition->getTableName());
        self::assertTrue($stageDefinition->isTemporary());
        // order same as source
        self::assertSame(['id', 'name', 'notInDef'], $stageDefinition->getColumnsNames());
        /** @var SynapseColumn[] $definitions */
        $definitions = iterator_to_array($stageDefinition->getColumnsDefinitions());
        // id is NVARCHAR
        self::assertSame(Synapse::TYPE_NVARCHAR, $definitions[0]->getColumnDefinition()->getType());
        // name is DATE
        self::assertSame(Synapse::TYPE_DATE, $definitions[1]->getColumnDefinition()->getType());
        // notInDef has default NVARCHAR
        self::assertSame(Synapse::TYPE_NVARCHAR, $definitions[2]->getColumnDefinition()->getType());
        self::assertSame(
            TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN,
            $stageDefinition->getTableDistribution()->getDistributionName()
        );
        // index is heap
        self::assertSame(
            TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX,
            $stageDefinition->getTableIndex()->getIndexType()
        );
    }

    public function testCreateStagingTableDefinitionWithText(): void
    {
        $definition = new SynapseTableDefinition(
            'schema',
            'table',
            false,
            new ColumnCollection([
                new SynapseColumn('name', new Synapse(Synapse::TYPE_DATE)),
                SynapseColumn::createGenericColumn('id'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
        $stageDefinition = StageTableDefinitionFactory::createStagingTableDefinitionWithText(
            $definition,
            ['id', 'name', 'notInDef']
        );

        self::assertSame('schema', $stageDefinition->getSchemaName());
        self::assertStringStartsWith('#__temp_csvimport', $stageDefinition->getTableName());
        self::assertTrue($stageDefinition->isTemporary());
        // order same as source
        self::assertSame(['id', 'name', 'notInDef'], $stageDefinition->getColumnsNames());
        /** @var SynapseColumn[] $definitions */
        $definitions = iterator_to_array($stageDefinition->getColumnsDefinitions());
        self::assertSame(Synapse::TYPE_NVARCHAR, $definitions[0]->getColumnDefinition()->getType());
        // name is in table as NVARCHAR
        self::assertSame(Synapse::TYPE_NVARCHAR, $definitions[1]->getColumnDefinition()->getType());
        self::assertSame(Synapse::TYPE_NVARCHAR, $definitions[2]->getColumnDefinition()->getType());
        self::assertSame(
            TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN,
            $stageDefinition->getTableDistribution()->getDistributionName()
        );
        // index is heap
        self::assertSame(
            TableIndexDefinition::TABLE_INDEX_TYPE_HEAP,
            $stageDefinition->getTableIndex()->getIndexType()
        );
    }

    public function testCreateStagingTableDefinitionWithTextWithIndex(): void
    {
        $definition = new SynapseTableDefinition(
            'schema',
            'table',
            false,
            new ColumnCollection([
                new SynapseColumn('name', new Synapse(Synapse::TYPE_DATE)),
                SynapseColumn::createGenericColumn('id'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
        $stageDefinition = StageTableDefinitionFactory::createStagingTableDefinitionWithText(
            $definition,
            ['id', 'name', 'notInDef'],
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX)
        );

        self::assertSame('schema', $stageDefinition->getSchemaName());
        self::assertStringStartsWith('#__temp_csvimport', $stageDefinition->getTableName());
        self::assertTrue($stageDefinition->isTemporary());
        // order same as source
        self::assertSame(['id', 'name', 'notInDef'], $stageDefinition->getColumnsNames());
        /** @var SynapseColumn[] $definitions */
        $definitions = iterator_to_array($stageDefinition->getColumnsDefinitions());
        self::assertSame(Synapse::TYPE_NVARCHAR, $definitions[0]->getColumnDefinition()->getType());
        // name is in table as NVARCHAR
        self::assertSame(Synapse::TYPE_NVARCHAR, $definitions[1]->getColumnDefinition()->getType());
        self::assertSame(Synapse::TYPE_NVARCHAR, $definitions[2]->getColumnDefinition()->getType());
        self::assertSame(
            TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN,
            $stageDefinition->getTableDistribution()->getDistributionName()
        );
        // index is heap
        self::assertSame(
            TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX,
            $stageDefinition->getTableIndex()->getIndexType()
        );
    }
}
