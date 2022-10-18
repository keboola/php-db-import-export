<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake\ToFinal;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Datatype\Definition\Synapse;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Utils\FakeDriver\FakeConnection;
use Keboola\Db\ImportExport\Utils\FakeDriver\FakeConnectionFactory;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\Table\Synapse\TableDistributionDefinition;
use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Tests\Keboola\Db\ImportExportFunctional\Synapse\SynapseBaseTestCase;

class FullImportCustomQueryFakeTest extends SynapseBaseTestCase
{

    protected function setUp(): void
    {
    }

    protected function tearDown(): void
    {
    }

    public function testLoadFromAbsToFinalTableWithoutDedup(): void
    {
        $sourceCol1 = uniqid('sourceCol1');
        $params = [
            'sourceFiles' => [
                'sourceFile1' => uniqid('sourceFile1'),
            ],
            'sourceContainerUrl' => uniqid('sourceContainerUrl'),
            'sourceSasToken' => uniqid('sourceSasToken'),

            'stageSchemaName' => uniqid('stageSchemaName'),
            'stageTableName' => uniqid('__temp_stageTableName'),
            'stageColumns' => [
                'sourceCol1' => new \Keboola\TableBackendUtils\Column\SynapseColumn(
                    $sourceCol1,
                    new Synapse(Synapse::TYPE_INT)
                ),
            ],

            'destSchemaName' => uniqid('destSchemaName'),
            'destTableName' => uniqid('destTableName'),
            'destColumns' => [
                'destCol1' => new SynapseColumn(
                    $sourceCol1,
                    new Synapse(Synapse::TYPE_INT)
                ),
                'destColTimestamp' => new SynapseColumn(
                    uniqid('_timestamp'),
                    new Synapse(Synapse::TYPE_DATETIME2)
                ),
            ],
        ];

        $sourceColumnsNames = [];
        /** @var ColumnInterface $column */
        foreach ($params['stageColumns'] as $column) {
            $sourceColumnsNames[] = $column->getColumnName();
        }

        $queries = [];

        // fake connection
        $conn = FakeConnectionFactory::getConnection();

        // mock file source
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects(self::atLeastOnce())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::atLeastOnce())->method('getManifestEntries')->willReturn($params['sourceFiles']);
        $source->expects(self::atLeastOnce())->method('getColumnsNames')->willReturn($sourceColumnsNames);
        // ABS specific
        $source->expects(self::atLeastOnce())->method('getContainerUrl')->willReturn($params['sourceContainerUrl']);
        $source->expects(self::atLeastOnce())->method('getSasToken')->willReturn($params['sourceSasToken']);

        // fake staging table
        $stagingTable = new SynapseTableDefinition(
            $params['stageSchemaName'],
            $params['stageTableName'],
            true,
            new ColumnCollection($params['stageColumns']),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_HASH, ['id']),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX, ['id'])
        );

        // fake options
        $options = new SynapseImportOptions(
            [],
            false,
            false,
            1
        );
        // fake destination
        $destination = new SynapseTableDefinition(
            $params['destSchemaName'],
            $params['destTableName'],
            false,
            new ColumnCollection($params['destColumns']),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_HASH, ['id']),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX, ['id'])
        );

        // mock importer
        $importer = new ToStageImporter($conn);


        // init query builder
        $qb = new SynapseTableQueryBuilder();

        // ACTION: create stage table
        $conn->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );

        // ACTION: create final table
        $conn->executeStatement(
            $qb->getCreateTableCommandFromDefinition($destination)
        );

        // ACTION: import to stage table
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );

        // ACTION: import to final table
        $toFinalTableImporter = new FullImporter($conn);
        $result = $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        // result
        dump('=== queries');
        /** @var FakeConnection $wrappedConn */
        $wrappedConn = $conn->getWrappedConnection();
        $queries = $wrappedConn->getPreparedQueries();
        foreach ($queries as $query) {
            dump($this->replaceParamsInQuery($query, $params, '', ''));
            dump('---------');
        }

        $this->assertCount(8, $queries);
    }

    /**
     * Try to replace params back...
     */
    private function replaceParamsInQuery(string $query, array $params, string $prefix = '{{ ', string $suffix = ' }}'): string
    {
        foreach ($params as $paramKey => $param) {
            if (is_array($param)) {
                foreach ($param as $itemKey => $item) {
                    if ($item instanceof ColumnInterface) {
                        $query = $this->replaceParamInQuery($query, $item->getColumnName(), $itemKey, $prefix, $suffix);
                    } else {
                        $query = $this->replaceParamInQuery($query, $item, $itemKey, $prefix, $suffix);
                    }
                }
            } else {
                $query = $this->replaceParamInQuery($query, $param, $paramKey, $prefix, $suffix);
            }
        }
        return $query;
    }

    private function replaceParamInQuery(string $query, string $search, string $replace, string $prefix, string $suffix): string
    {
        return str_replace(
            $search,
            sprintf(
                '%s%s%s',
                $prefix,
                $replace,
                $suffix
            ),
            $query
        );
    }
}
