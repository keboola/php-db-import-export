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
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
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
        // 'foo'  = identifier
        // '#foo' = value
        $sourceCol1Name = uniqid('sourceCol1');
        $destCol1Name = uniqid('destColumn1');
        $params = [
            'sourceFiles' => [
                '#sourceFile1' => uniqid('sourceFile1'),
            ],
            '#sourceContainerUrl' => uniqid('sourceContainerUrl'),
            '#sourceSasToken' => uniqid('sourceSasToken'),

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
        $source->expects(self::atLeastOnce())->method('getContainerUrl')->willReturn($params['#sourceContainerUrl']);
        $source->expects(self::atLeastOnce())->method('getSasToken')->willReturn($params['#sourceSasToken']);

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
            dump($this->replaceParamsInQuery($query, $params));
            dump('---------');
        }

        $this->assertCount(10, $queries);
    }

    public function testReplaceParamsInQuery(): void
    {
        $input = <<<SQL
            COPY INTO "stageSchemaName6336e8dda7606"."stageTableName6336e8dda7607"
            FROM 'sourceContainerUrl6336ebdee0b80'
            CREDENTIALS=(AZURE_SAS_TOKEN='sourceSasToken6336ebdee0b81')
            FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
            FILES = ('sourceFile16336ebdee0b7f')
        SQL;
        $params = [
            'stageSchemaName' => 'stageSchemaName6336e8dda7606',
            '#sourceContainerUrl' => 'sourceContainerUrl6336ebdee0b80',
            '#sourceSasToken' => 'sourceSasToken6336ebdee0b81',
            'testIdInArray' => [
                'stageTableName' => 'stageTableName6336e8dda7607',
            ],
            'testValueInArray' => [
                '#sourceFile1' => 'sourceFile16336ebdee0b7f',
            ],
        ];

        $output = $this->replaceParamsInQuery($input, $params);

        $expected = <<<SQL
            COPY INTO {{ id(stageSchemaName) }}.{{ id(stageTableName) }}
            FROM {{ sourceContainerUrl }}
            CREDENTIALS=(AZURE_SAS_TOKEN={{ sourceSasToken }})
            FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
            FILES = ({{ sourceFile1 }})
        SQL;
        $this->assertSame($expected, $output);
    }

    /**
     * @dataProvider replaceParamInQueryProvider
     */
    public function testReplaceParamInQuery(string $input, string $key, string $value, ?string $prefix, ?string $suffix, string $expectedOutput): void
    {

        $output = $this->replaceParamInQuery(
            $input,
            $value,
            $key,
            $prefix,
            $suffix,
        );
        $this->assertSame($expectedOutput, $output);
    }

    public function replaceParamInQueryProvider(): array
    {
        $defaultQuery = <<<SQL
            COPY INTO "stageSchemaName6336e8dda7606"."stageTableName6336e8dda7607"
            FROM 'sourceContainerUrl6336ebdee0b80'
        SQL;

        return [
            'test id' => [
                $defaultQuery,
                'keyInOutput' => 'stageSchemaName',
                'valueInQuery' => 'stageSchemaName6336e8dda7606',
                '{{ ',
                ' }}',
                'output' => <<<SQL
                    COPY INTO {{ id(stageSchemaName) }}."stageTableName6336e8dda7607"
                    FROM 'sourceContainerUrl6336ebdee0b80'
                SQL,
            ],
            'test value' => [
                $defaultQuery,
                '#sourceContainerUrl',
                'sourceContainerUrl6336ebdee0b80',
                '{{ ',
                ' }}',
                <<<SQL
                    COPY INTO "stageSchemaName6336e8dda7606"."stageTableName6336e8dda7607"
                    FROM {{ sourceContainerUrl }}
                SQL,
            ],
            'test id with other prefix+suffix' => [
                $defaultQuery,
                'stageSchemaName',
                'stageSchemaName6336e8dda7606',
                '[',
                ']',
                <<<SQL
                    COPY INTO [id(stageSchemaName)]."stageTableName6336e8dda7607"
                    FROM 'sourceContainerUrl6336ebdee0b80'
                SQL
            ],
            'test value with other prefix+suffix' => [
                $defaultQuery,
                '#sourceContainerUrl',
                'sourceContainerUrl6336ebdee0b80',
                '[',
                ']',
                <<<SQL
                    COPY INTO "stageSchemaName6336e8dda7606"."stageTableName6336e8dda7607"
                    FROM [sourceContainerUrl]
                SQL
            ],
    /**
     * Try to replace params back...
     */
    private function replaceParamsInQuery(string $query, array $params, string $outputPrefix = '{{ ', string $outputSuffix = ' }}'): string
    {
        foreach ($params as $key => $value) {
            if (is_array($value)) {
                foreach ($value as $keyInArray => $valueInArray) {
                    if ($valueInArray instanceof ColumnInterface) {
                        $query = $this->replaceParamInQuery($query, $valueInArray->getColumnName(), $keyInArray, $outputPrefix, $outputSuffix);
                    } else {
                        $query = $this->replaceParamInQuery($query, $valueInArray, $keyInArray, $outputPrefix, $outputSuffix);
                    }
                }
            } else {
                $query = $this->replaceParamInQuery($query, $value, $key, $outputPrefix, $outputSuffix);
            }
        }
        return $query;
    }

    private function replaceParamInQuery(string $query, string $valueInQuery, string $keyInOutput, string $outputPrefix = '{{ ', string $outputSuffix = ' }}'): string
    {
        if (strpos($keyInOutput, '#') === 0) {
            // replace values
            $valueInQuery = SnowflakeQuote::quote($valueInQuery);
            $keyInOutput = substr($keyInOutput, 1);
        } else {
            // replace identifiers
            $valueInQuery = SnowflakeQuote::quoteSingleIdentifier($valueInQuery);
            $keyInOutput = sprintf('id(%s)', $keyInOutput);
        }
        return str_replace(
            $valueInQuery,
            sprintf(
                '%s%s%s',
                $outputPrefix,
                $keyInOutput,
                $outputSuffix
            ),
            $query
        );
    }
}
