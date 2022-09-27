<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake\ToFinal;

use Doctrine\DBAL\Connection;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Tests\Keboola\Db\ImportExportFunctional\Snowflake\SnowflakeBaseTestCase;

class FullImportCustomQueryTest extends SnowflakeBaseTestCase
{
    protected function setUp(): void
    {}

    protected function tearDown(): void
    {}

    public function testLoadFromAbsToFinalTableWithoutDedup(): void
    {
        $params = [
            'sourceFiles' => [
                'sourceFile1' => uniqid('sourceFile1'),
            ],
            'sourceContainerUrl' => uniqid('sourceContainerUrl'),
            'sourceSasToken' => uniqid('sourceSasToken'),

            'stageSchemaName' => uniqid('stageSchemaName'),
            'stageTableName' => uniqid('__temp_stageTableName'),
            'stageColumns' => [
                'sourceCol1' => new SnowflakeColumn(
                    uniqid('sourceCol1'),
                    new Snowflake(Snowflake::TYPE_INT)
                ),
            ],

            'destSchemaName' => uniqid('destSchemaName'),
            'destTableName' => uniqid('destTableName'),
            'destColumns' => [
                'destCol1' => new SnowflakeColumn(
                    uniqid('destColumn1'),
                    new Snowflake(Snowflake::TYPE_INT)
                ),
                'destColTimestamp' => new SnowflakeColumn(
                    uniqid('_timestamp'),
                    new Snowflake(Snowflake::TYPE_TIMESTAMP)
                ),
            ],
        ];

        $sourceColumnsNames = [];
        /** @var ColumnInterface $column */
        foreach ($params['stageColumns'] as $column) {
            $sourceColumnsNames[] = $column->getColumnName();
        }

        $queries = [];

        // mock connection
        $conn = $this->createMock(Connection::class);
        $conn->expects(self::atLeastOnce())->method('executeStatement')->willReturnCallback(
            static function (...$values) use (&$queries) {
                $queries[] = $values[0];
                return 0;
            }
        );

        // mock file source
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects(self::atLeastOnce())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::atLeastOnce())->method('getManifestEntries')->willReturn($params['sourceFiles']);
        $source->expects(self::atLeastOnce())->method('getColumnsNames')->willReturn($sourceColumnsNames);
        // ABS specific
        $source->expects(self::atLeastOnce())->method('getContainerUrl')->willReturn($params['sourceContainerUrl']);
        $source->expects(self::atLeastOnce())->method('getSasToken')->willReturn($params['sourceSasToken']);

        // fake staging table
        $stagingTable = new SnowflakeTableDefinition(
            $params['stageSchemaName'],
            $params['stageTableName'],
            true,
            new ColumnCollection($params['stageColumns']),
            []
        );
        // fake options
        $options = new SnowflakeImportOptions(
            [],
            false,
            false,
            1
        );
        // fake destination
        $destination = new SnowflakeTableDefinition(
            $params['destSchemaName'],
            $params['destTableName'],
            false,
            new ColumnCollection($params['destColumns']),
            []
        );

        // mock importer
        $importer = new ToStageImporter($conn);

        // init query builder
        $qb = new SnowflakeTableQueryBuilder();

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
        foreach ($queries as $query) {
//            dump($query);
//            dump('---');
            dump($this->replaceParamsInQuery($query, $params, '', ''));
            dump('---------');
        }

        $this->assertCount(7, $queries);
    }

    public function testLoadFromS3ToFinalTableWithoutDedup(): void
    {
        $params = [
            'sourceFiles' => [
                'sourceFile1' => uniqid('sourceFile1'),
            ],
            'sourceS3Prefix' => uniqid('sourceS3Prefix'),
            'sourceKey' => uniqid('sourceKey'),
            'sourceSecret' => uniqid('sourceSecret'),
            'sourceRegion' => uniqid('sourceRegion'),

            'stageSchemaName' => uniqid('stageSchemaName'),
            'stageTableName' => uniqid('__temp_stageTableName'),
            'stageColumns' => [
                'sourceCol1' => new SnowflakeColumn(
                    uniqid('sourceCol1'),
                    new Snowflake(Snowflake::TYPE_INT)
                ),
                'sourceCol2' => new SnowflakeColumn(
                    uniqid('sourceCol2'),
                    new Snowflake(Snowflake::TYPE_VARCHAR)
                ),
            ],

            'destSchemaName' => uniqid('destSchemaName'),
            'destTableName' => uniqid('destTableName'),
            'destColumns' => [
                'destCol1' => new SnowflakeColumn(
                    uniqid('destColumn1'),
                    new Snowflake(Snowflake::TYPE_INT)
                ),
                'destCol2' => new SnowflakeColumn(
                    uniqid('destColumn2'),
                    new Snowflake(Snowflake::TYPE_VARCHAR)
                ),
                'destColTimestamp' => new SnowflakeColumn(
                    uniqid('_timestamp'),
                    new Snowflake(Snowflake::TYPE_TIMESTAMP)
                ),
            ],
        ];

        $sourceColumnsNames = [];
        /** @var ColumnInterface $column */
        foreach ($params['stageColumns'] as $column) {
            $sourceColumnsNames[] = $column->getColumnName();
        }

        $queries = [];

        // mock connection
        $conn = $this->createMock(Connection::class);
        $conn->expects(self::atLeastOnce())->method('executeStatement')->willReturnCallback(
            static function (...$values) use (&$queries) {
                $queries[] = $values[0];
                return 0;
            }
        );

        // mock file source
        $source = $this->createMock(Storage\S3\SourceFile::class);
        $source->expects(self::atLeastOnce())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::atLeastOnce())->method('getManifestEntries')->willReturn($params['sourceFiles']);
        $source->expects(self::atLeastOnce())->method('getColumnsNames')->willReturn($sourceColumnsNames);
        // S3 scpecific
        $source->expects(self::atLeastOnce())->method('getS3Prefix')->willReturn($params['sourceS3Prefix']);
        $source->expects(self::atLeastOnce())->method('getKey')->willReturn($params['sourceKey']);
        $source->expects(self::atLeastOnce())->method('getSecret')->willReturn($params['sourceSecret']);
        $source->expects(self::atLeastOnce())->method('getRegion')->willReturn($params['sourceRegion']);

        // fake staging table
        $stagingTable = new SnowflakeTableDefinition(
            $params['stageSchemaName'],
            $params['stageTableName'],
            true,
            new ColumnCollection($params['stageColumns']),
            []
        );
        // fake options
        $options = new SnowflakeImportOptions(
            [],
            false,
            false,
            1
        );
        // fake destination
        $destination = new SnowflakeTableDefinition(
            $params['destSchemaName'],
            $params['destTableName'],
            false,
            new ColumnCollection($params['destColumns']),
            []
        );

        // mock importer
        $importer = new ToStageImporter($conn);

        // init query builder
        $qb = new SnowflakeTableQueryBuilder();

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
        foreach ($queries as $query) {
//            dump($query);
//            dump('---');
            dump($this->replaceParamsInQuery($query, $params, '', ''));
            dump('---------');
        }

        $this->assertCount(7, $queries);
    }

    public function testLoadFromAbsToFinalTableWithDedupWithSinglePK(): void
    {
        $sourceCol1Name = uniqid('sourceCol1');
        $destCol1Name = uniqid('destColumn1');
        $params = [
            'sourceFiles' => [
                'sourceFile1' => uniqid('sourceFile1'),
            ],
            'sourceContainerUrl' => uniqid('sourceContainerUrl'),
            'sourceSasToken' => uniqid('sourceSasToken'),

            'stageSchemaName' => uniqid('stageSchemaName'),
            'stageTableName' => uniqid('__temp_stageTableName'),
            'stageColumns' => [
                'sourceCol1' => new SnowflakeColumn(
                    $sourceCol1Name,
                    new Snowflake(Snowflake::TYPE_INT)
                ),
                'sourceCol2' => new SnowflakeColumn(
                    uniqid('sourceCol2'),
                    new Snowflake(Snowflake::TYPE_VARCHAR)
                ),
            ],
            'stagePrimaryKeys' => [
                'sourceCol1' => $sourceCol1Name,
            ],

            'destSchemaName' => uniqid('destSchemaName'),
            'destTableName' => uniqid('destTableName'),
            'destColumns' => [
                'destCol1' => new SnowflakeColumn(
                    $destCol1Name,
                    new Snowflake(Snowflake::TYPE_INT)
                ),
                'destCol2' => new SnowflakeColumn(
                    uniqid('destColumn2'),
                    new Snowflake(Snowflake::TYPE_VARCHAR)
                ),
                'destColTimestamp' => new SnowflakeColumn(
                    uniqid('_timestamp'),
                    new Snowflake(Snowflake::TYPE_TIMESTAMP)
                ),
            ],
            'destPrimaryKeys' => [
                'destPrimaryKey1' => $destCol1Name,
            ]
        ];

        $sourceColumnsNames = [];
        /** @var ColumnInterface $column */
        foreach ($params['stageColumns'] as $column) {
            $sourceColumnsNames[] = $column->getColumnName();
        }

        $queries = [];

        // mock connection
        $conn = $this->createMock(Connection::class);
        $conn->expects(self::atLeastOnce())->method('executeStatement')->willReturnCallback(
            static function (...$values) use (&$queries) {
                $queries[] = $values[0];
                return 0;
            }
        );

        // mock file source
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects(self::atLeastOnce())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::atLeastOnce())->method('getManifestEntries')->willReturn($params['sourceFiles']);
        $source->expects(self::atLeastOnce())->method('getColumnsNames')->willReturn($sourceColumnsNames);
        // ABS specific
        $source->expects(self::atLeastOnce())->method('getContainerUrl')->willReturn($params['sourceContainerUrl']);
        $source->expects(self::atLeastOnce())->method('getSasToken')->willReturn($params['sourceSasToken']);

        // fake staging table
        $stagingTable = new SnowflakeTableDefinition(
            $params['stageSchemaName'],
            $params['stageTableName'],
            true,
            new ColumnCollection($params['stageColumns']),
            $params['stagePrimaryKeys']
        );
        // fake options
        $options = new SnowflakeImportOptions(
            [],
            false,
            false,
            1
        );
        // fake destination
        $destination = new SnowflakeTableDefinition(
            $params['destSchemaName'],
            $params['destTableName'],
            false,
            new ColumnCollection($params['destColumns']),
            $params['destPrimaryKeys']
        );

        // mock importer
        $importer = new ToStageImporter($conn);

        // init query builder
        $qb = new SnowflakeTableQueryBuilder();

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
        dump('=== queries:');
        foreach ($queries as $query) {
//            dump($query);
//            dump('---');
            dump($this->replaceParamsInQuery($query, $params, '', ''));
            dump('---------');
        }

        $this->assertCount(10, $queries);
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
