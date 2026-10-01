<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Bigquery\ToFinal;

use DateTimeImmutable;
use DateTimeZone;
use Generator;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\IncrementalImporter;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\PartitionAwareImportColumn;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\PartitionAwareImportFilter;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Bigquery\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Bigquery\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\TimestampMode;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use PHPUnit\Framework\Attributes\DataProvider;
use Tests\Keboola\Db\ImportExportFunctional\Bigquery\BigqueryBaseTestCase;

class IncrementalImportTest extends BigqueryBaseTestCase
{
    protected static function getBigqueryIncrementalImportOptions(
        int $skipLines = ImportOptions::SKIP_FIRST_LINE,
    ): BigqueryImportOptions {
        return new BigqueryImportOptions(
            [],
            true,
            true,
            $skipLines,
            BigqueryImportOptions::USING_TYPES_STRING,
        );
    }

    protected static function getBigqueryIncrementalImportOptionsOptimized(
        int $skipLines = ImportOptions::SKIP_FIRST_LINE,
    ): BigqueryImportOptions {
        return new BigqueryImportOptions(
            convertEmptyValuesToNull: [],
            isIncremental: true,
            useTimestamp: true,
            numberOfIgnoredLines: $skipLines,
            usingTypes: BigqueryImportOptions::USING_TYPES_STRING,
            features: [BigqueryImportOptions::FEATURE_OPTIMIZED_IMPORT],
        );
    }

    protected static function getSimpleImportOptionsOptimized(
        int $skipLines = ImportOptions::SKIP_FIRST_LINE,
    ): BigqueryImportOptions {
        return new BigqueryImportOptions(
            convertEmptyValuesToNull: [],
            isIncremental: false,
            useTimestamp: true,
            numberOfIgnoredLines: $skipLines,
            usingTypes: BigqueryImportOptions::USING_TYPES_STRING,
            features: [BigqueryImportOptions::FEATURE_OPTIMIZED_IMPORT],
        );
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanDatabase($this->getDestinationDbName());
        $this->createDatabase($this->getDestinationDbName());

        $this->cleanDatabase($this->getSourceDbName());
        $this->createDatabase($this->getSourceDbName());
    }

    /**
     * @return Generator<string, array<mixed>>
     */
    public static function incrementalImportData(): Generator
    {
        $accountsStub = static::getParseCsvStub('expectation.tw_accounts.increment.csv');
        $accountsNoDedupStub = static::getParseCsvStub('expectation.tw_accounts.increment.nodedup.csv');
        $multiPKStub = static::getParseCsvStub('expectation.multi-pk_not-null.increment.csv');

        yield 'simple no dedup' => [
            static::getSourceInstance(
                'tw_accounts.csv',
                $accountsNoDedupStub->getColumns(),
                false,
                false,
                ['id'],
            ),
            static::getSimpleImportOptions(),
            static::getSourceInstance(
                'tw_accounts.increment.csv',
                $accountsNoDedupStub->getColumns(),
                false,
                false,
                ['id'],
            ),
            static::getBigqueryIncrementalImportOptions(),
            [static::getDestinationDbName(), 'accounts-3'],
            $accountsNoDedupStub->getRows(),
            4,
            self::TABLE_ACCOUNTS_3,
            [],
        ];
        yield 'simple' => [
            static::getSourceInstance(
                'tw_accounts.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id'],
            ),
            static::getSimpleImportOptions(),
            static::getSourceInstance(
                'tw_accounts.increment.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id'],
            ),
            static::getBigqueryIncrementalImportOptions(),
            [static::getDestinationDbName(), 'accounts-3'],
            $accountsStub->getRows(),
            3, // 4 rows in CSV but id=18 is duplicated, so 3 unique PKs
            self::TABLE_ACCOUNTS_3,
            ['id'],
        ];
        yield 'simple no timestamp' => [
            static::getSourceInstance(
                'tw_accounts.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id'],
            ),
            new BigqueryImportOptions(
                [],
                false,
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE,
            ),
            static::getSourceInstance(
                'tw_accounts.increment.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id'],
            ),
            new BigqueryImportOptions(
                [],
                true, // incremental
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE,
            ),
            [static::getDestinationDbName(), self::TABLE_ACCOUNTS_WITHOUT_TS],
            $accountsStub->getRows(),
            3, // 4 rows in CSV but id=18 is duplicated, so 3 unique PKs
            self::TABLE_ACCOUNTS_WITHOUT_TS,
            ['id'],
        ];
        yield 'multi pk' => [
            static::getSourceInstance(
                'multi-pk_not-null.csv',
                $multiPKStub->getColumns(),
                false,
                false,
                ['VisitID', 'Value', 'MenuItem'],
            ),
            static::getSimpleImportOptions(),
            static::getSourceInstance(
                'multi-pk_not-null.increment.csv',
                $multiPKStub->getColumns(),
                false,
                false,
                ['VisitID', 'Value', 'MenuItem'],
            ),
            static::getBigqueryIncrementalImportOptions(),
            [static::getDestinationDbName(), self::TABLE_MULTI_PK_WITH_TS],
            $multiPKStub->getRows(),
            3,
            self::TABLE_MULTI_PK_WITH_TS,
            ['VisitID', 'Value', 'MenuItem'],
        ];
    }

    /**
     * Same scenarios as incrementalImportData(), but with the bigquery-optimized-import feature
     * enabled: aggregation-based dedup + CLUSTER BY on the dedup table, and a single MERGE
     * replacing UPDATE+DELETE+INSERT. Must produce identical final table contents.
     *
     * @return Generator<string, array<mixed>>
     */
    public static function incrementalImportDataOptimized(): Generator
    {
        $accountsStub = static::getParseCsvStub('expectation.tw_accounts.increment.csv');
        $multiPKStub = static::getParseCsvStub('expectation.multi-pk_not-null.increment.csv');

        yield 'simple optimized' => [
            static::getSourceInstance(
                'tw_accounts.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id'],
            ),
            static::getSimpleImportOptionsOptimized(),
            static::getSourceInstance(
                'tw_accounts.increment.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id'],
            ),
            static::getBigqueryIncrementalImportOptionsOptimized(),
            [static::getDestinationDbName(), 'accounts-3'],
            $accountsStub->getRows(),
            3, // 4 rows in CSV but id=18 is duplicated, so 3 unique PKs
            self::TABLE_ACCOUNTS_3,
            ['id'],
        ];
        yield 'multi pk optimized' => [
            static::getSourceInstance(
                'multi-pk_not-null.csv',
                $multiPKStub->getColumns(),
                false,
                false,
                ['VisitID', 'Value', 'MenuItem'],
            ),
            static::getSimpleImportOptionsOptimized(),
            static::getSourceInstance(
                'multi-pk_not-null.increment.csv',
                $multiPKStub->getColumns(),
                false,
                false,
                ['VisitID', 'Value', 'MenuItem'],
            ),
            static::getBigqueryIncrementalImportOptionsOptimized(),
            [static::getDestinationDbName(), self::TABLE_MULTI_PK_WITH_TS],
            $multiPKStub->getRows(),
            3,
            self::TABLE_MULTI_PK_WITH_TS,
            ['VisitID', 'Value', 'MenuItem'],
        ];
    }

    /**
     * @param string[]     $table
     * @param string[]     $dedupCols
     * @param array<mixed> $expected
     */
    #[DataProvider('incrementalImportData')]
    #[DataProvider('incrementalImportDataOptimized')]
    public function testIncrementalImport(
        Storage\SourceInterface $fullLoadSource,
        BigqueryImportOptions $fullLoadOptions,
        Storage\SourceInterface $incrementalSource,
        BigqueryImportOptions $incrementalOptions,
        array $table,
        array $expected,
        int $expectedImportedRowCount,
        string $tablesToInit,
        array $dedupCols,
    ): void {
        $this->initTable($tablesToInit);

        [$schemaName, $tableName] = $table;
        /** @var BigqueryTableDefinition $destination */
        $destination = (new BigqueryTableReflection(
            $this->bqClient,
            $schemaName,
            $tableName,
        ))->getTableDefinition();
        // update PK
        $destination = $this->cloneDefinitionWithDedupCol($destination, $dedupCols);

        $toStageImporter = new ToStageImporter($this->bqClient);
        $fullImporter = new FullImporter($this->bqClient);
        $incrementalImporter = new IncrementalImporter($this->bqClient);

        $fullLoadStagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $fullLoadSource->getColumnsNames(),
        );
        $incrementalLoadStagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $incrementalSource->getColumnsNames(),
        );

        try {
            // full load
            $qb = new BigqueryTableQueryBuilder();
            $this->bqClient->runQuery(
                $this->bqClient->query(
                    $qb->getCreateTableCommandFromDefinition($fullLoadStagingTable),
                ),
            );

            $importState = $toStageImporter->importToStagingTable(
                $fullLoadSource,
                $fullLoadStagingTable,
                $fullLoadOptions,
            );
            $fullImporter->importToTable(
                $fullLoadStagingTable,
                $destination,
                $fullLoadOptions,
                $importState,
            );
            // incremental load
            $qb = new BigqueryTableQueryBuilder();
            $this->bqClient->runQuery(
                $this->bqClient->query(
                    $qb->getCreateTableCommandFromDefinition($incrementalLoadStagingTable),
                ),
            );
            $importState = $toStageImporter->importToStagingTable(
                $incrementalSource,
                $incrementalLoadStagingTable,
                $incrementalOptions,
            );
            $result = $incrementalImporter->importToTable(
                $incrementalLoadStagingTable,
                $destination,
                $incrementalOptions,
                $importState,
            );
        } finally {
            $this->bqClient->runQuery(
                $this->bqClient->query(
                    (new SqlBuilder())->getDropTableIfExistsCommand(
                        $fullLoadStagingTable->getSchemaName(),
                        $fullLoadStagingTable->getTableName(),
                    ),
                ),
            );
            $this->bqClient->runQuery(
                $this->bqClient->query(
                    (new SqlBuilder())->getDropTableIfExistsCommand(
                        $incrementalLoadStagingTable->getSchemaName(),
                        $incrementalLoadStagingTable->getTableName(),
                    ),
                ),
            );
        }

        self::assertEquals($expectedImportedRowCount, $result->getImportedRowsCount());

        /** @var BigqueryTableDefinition $destination */
        $this->assertBigqueryTableEqualsExpected(
            $fullLoadSource,
            $destination,
            $incrementalOptions,
            $expected,
            0,
        );
    }

    public static function incrementalImportTimestampBehavior(): Generator
    {
        yield 'import typed table, timestamp update always `no feature`' => [
            'features' => [],
            'expectedContent' => [
                [
                    'id'=> 1,
                    'name'=> 'change',
                    'price'=> '100',
                    'isDeleted'=> 0,
                    '_timestamp'=> '2022-02-02 00:00:00',
                ],
                [
                    'id'=> 2,
                    'name'=> 'test2',
                    'price'=> '200',
                    'isDeleted'=> 0,
                    '_timestamp'=> '2021-01-01 00:00:00',
                ],
                [
                    'id'=> 3,
                    'name'=> 'test3',
                    'price'=> '300',
                    'isDeleted'=> 0,
                    '_timestamp'=> '2021-01-01 00:00:00', // no change, no timestamp update
                ],
                [
                    'id'=> 4,
                    'name'=> 'test4',
                    'price'=> '400',
                    'isDeleted'=> 0,
                    '_timestamp'=> '2022-02-02 00:00:00',
                ],
            ],
        ];
    }

    /**
     * @param string[]     $features
     * @param array<mixed> $expectedContent
     */
    #[DataProvider('incrementalImportTimestampBehavior')]
    public function testImportTimestampBehavior(array $features, array $expectedContent): void
    {
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'CREATE TABLE %s.%s
            (
              `id` INT64 NOT NULL,
              `name` STRING(50),
              `price` DECIMAL,
              `isDeleted` INT64,
              `_timestamp` TIMESTAMP
           )',
            BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
            BigqueryQuote::quoteSingleIdentifier(self::TABLE_TRANSLATIONS),
        ),),);
        $this->bqClient->dataset($this->getDestinationDbName())->table(self::TABLE_TRANSLATIONS)->update(
            [
                'tableConstraints' => [
                    'primaryKey' => [
                        'columns' => 'id',
                    ],
                ],
            ],
        );
        $this->initTable(self::TABLE_TRANSLATIONS, $this->getSourceDbName());
        $destination = (new BigqueryTableReflection(
            $this->bqClient,
            $this->getDestinationDbName(),
            self::TABLE_TRANSLATIONS,
        ))->getTableDefinition();
        $source = (new BigqueryTableReflection(
            $this->bqClient,
            $this->getSourceDbName(),
            self::TABLE_TRANSLATIONS,
        ))->getTableDefinition();

        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            <<<SQL
INSERT INTO %s.%s (`id`, `name`, `price`, `isDeleted`) VALUES
(1, 'change', 100, 0),
(3, 'test3', 300, 0),
(4, 'test4', 400, 0)
SQL,
            BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
            BigqueryQuote::quoteSingleIdentifier(self::TABLE_TRANSLATIONS),
        ),),);
        $this->bqClient->runQuery(
            $this->bqClient->query(
                sprintf(
                    <<<SQL
INSERT INTO %s.%s (`id`, `name`, `price`, `isDeleted`, `_timestamp`) VALUES
(1, 'test', 100, 0, '2021-01-01 00:00:00'),
(2, 'test2', 200, 0, '2021-01-01 00:00:00'),
(3, 'test3', 300, 0, '2021-01-01 00:00:00')
SQL,
                    BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                    BigqueryQuote::quoteSingleIdentifier(self::TABLE_TRANSLATIONS),
                ),
            ),
        );

        $state = new ImportState($destination->getTableName());
        (new IncrementalImporter(
            $this->bqClient,
            new DateTimeImmutable('2022-02-02 00:00:00', new DateTimeZone('UTC')),
        )
        )->importToTable(
            $source,
            $destination,
            new BigqueryImportOptions(
                isIncremental: true,
                useTimestamp: true,
                usingTypes: BigqueryImportOptions::USING_TYPES_USER,
                features: $features,
            ),
            $state,
        );

        $destinationContent = $this->fetchTable($this->getDestinationDbName(), self::TABLE_TRANSLATIONS);
        $this->assertEqualsCanonicalizing($expectedContent, $destinationContent);
    }

    /**
     * Test documenting non-deterministic deduplication behavior with duplicate primary keys.
     *
     * KNOWN LIMITATION: When source table contains multiple rows with identical PK values,
     * the deduplication uses ORDER BY on PK columns only, which provides no tie-breaker.
     * This results in non-deterministic selection of which duplicate row is kept.
     *
     * This test verifies that:
     * 1. Deduplication does occur (only unique PK rows remain)
     * 2. The behavior is currently non-deterministic (may pick different rows on different runs)
     *
     * To fix this issue, the ORDER BY clause in getDedupSelect() should include all columns,
     * not just primary key columns.
     */
    public function testDeduplicationWithDuplicatePKsIsNonDeterministic(): void
    {
        $tableName = 'test_dedup_nondeterministic';

        // Create destination table with PK
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'CREATE TABLE %s.%s
            (
              `id` INT64 NOT NULL,
              `name` STRING(50) NOT NULL,
              `value` STRING(100),
              `_timestamp` TIMESTAMP
           )',
            BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        ),),);
        $this->bqClient->dataset($this->getDestinationDbName())->table($tableName)->update(
            [
                'tableConstraints' => [
                    'primaryKey' => [
                        'columns' => ['id', 'name'],
                    ],
                ],
            ],
        );

        // Create source table with duplicate PK rows
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'CREATE TABLE %s.%s
            (
              `id` INT64,
              `name` STRING(50),
              `value` STRING(100)
           )',
            BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        ),),);

        // Insert data with duplicate PKs - different non-PK values
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            <<<SQL
INSERT INTO %s.%s (`id`, `name`, `value`) VALUES
(1, 'Alice', 'value1'),
(2, 'Bob', 'value2'),
(1, 'Alice', 'value1_duplicate'),
(2, 'Bob', 'value2_duplicate')
SQL,
            BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        ),),);

        $destination = (new BigqueryTableReflection(
            $this->bqClient,
            $this->getDestinationDbName(),
            $tableName,
        ))->getTableDefinition();
        $source = (new BigqueryTableReflection(
            $this->bqClient,
            $this->getSourceDbName(),
            $tableName,
        ))->getTableDefinition();

        $importOptions = new BigqueryImportOptions(
            isIncremental: true,
            useTimestamp: true,
            usingTypes: BigqueryImportOptions::USING_TYPES_USER,
        );

        $state = new ImportState($destination->getTableName());
        $result = (new IncrementalImporter($this->bqClient))->importToTable(
            $source,
            $destination,
            $importOptions,
            $state,
        );

        // Verify deduplication occurred - should have exactly 2 rows (one per unique PK)
        $destinationData = $this->bqClient->runQuery(
            $this->bqClient->query(
                sprintf(
                    'SELECT `id`, `name`, `value` FROM %s.%s ORDER BY `id`, `name`',
                    BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                    BigqueryQuote::quoteSingleIdentifier($tableName),
                ),
            ),
        );

        $rows = [];
        foreach ($destinationData as $row) {
            assert(is_array($row));
            $rows[] = $row;
        }

        // Verify exactly 2 rows remain (deduplication worked)
        $this->assertCount(2, $rows, 'Deduplication should reduce 4 rows to 2 unique PK rows');

        // Verify correct PK combinations exist
        $this->assertEquals(1, $rows[0]['id']);
        $this->assertEquals('Alice', $rows[0]['name']);
        $this->assertEquals(2, $rows[1]['id']);
        $this->assertEquals('Bob', $rows[1]['name']);

        // Note: We do NOT assert which 'value' was selected (value1 vs value1_duplicate)
        // because the current implementation is non-deterministic
        $this->assertContains(
            $rows[0]['value'],
            ['value1', 'value1_duplicate'],
            'Value should be one of the duplicate options (non-deterministic selection)',
        );
        $this->assertContains(
            $rows[1]['value'],
            ['value2', 'value2_duplicate'],
            'Value should be one of the duplicate options (non-deterministic selection)',
        );
    }

    public function testIncrementalLoadWithTimestampFromSource(): void
    {
        $tableName = 'test_timestamp_from_source';

        // 1. Create destination table with _timestamp column (typed table)
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'CREATE TABLE %s.%s
            (
              `id` INT64 NOT NULL,
              `name` STRING(50),
              `value` STRING(100),
              `_timestamp` TIMESTAMP
           )',
            BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        ),),);
        $this->bqClient->dataset($this->getDestinationDbName())->table($tableName)->update(
            [
            'tableConstraints' => [
                'primaryKey' => ['columns' => 'id'],
            ],
            ],
        );

        // 2. Pre-populate destination with initial data
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            <<<SQL
INSERT INTO %s.%s (`id`, `name`, `value`, `_timestamp`) VALUES
(1, 'row1', 'old1', '2020-01-01 00:00:00'),
(2, 'row2', 'old2', '2020-01-01 00:00:00'),
(3, 'row3', 'old3', '2020-01-01 00:00:00')
SQL,
            BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        ),),);

        // 3. Create source table WITH _timestamp column
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'CREATE TABLE %s.%s
            (
              `id` INT64,
              `name` STRING(50),
              `value` STRING(100),
              `_timestamp` TIMESTAMP
           )',
            BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        ),),);

        // 4. Populate source with explicit timestamps (NOT current time)
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            <<<SQL
INSERT INTO %s.%s (`id`, `name`, `value`, `_timestamp`) VALUES
(1, 'row1', 'new1', '2023-06-15 12:00:00'),
(3, 'row3', 'old3', '2023-06-15 12:00:00'),
(4, 'row4', 'val4', '2023-06-15 12:00:00')
SQL,
            BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        ),),);

        // 5. Get table definitions
        $destination = (new BigqueryTableReflection(
            $this->bqClient,
            $this->getDestinationDbName(),
            $tableName,
        ))->getTableDefinition();
        $source = (new BigqueryTableReflection(
            $this->bqClient,
            $this->getSourceDbName(),
            $tableName,
        ))->getTableDefinition();

        // 6. Run IncrementalImporter with TimestampMode::FromSource
        $state = new ImportState($destination->getTableName());
        (new IncrementalImporter($this->bqClient))->importToTable(
            $source,
            $destination,
            new BigqueryImportOptions(
                isIncremental: true,
                useTimestamp: false,
                usingTypes: BigqueryImportOptions::USING_TYPES_USER,
                timestampMode: TimestampMode::FromSource,
            ),
            $state,
        );

        // 7. Verify results
        // Row 1: data changed (old1 -> new1), timestamp from source
        // Row 2: not in source, keeps original timestamp
        // Row 3: data unchanged, but timestamp column IS compared (USING_TYPES_USER compares all columns),
        //        so row is updated with source timestamp
        // Row 4: new row, inserted with source timestamp
        $expectedContent = [
            ['id' => 1, 'name' => 'row1', 'value' => 'new1', '_timestamp' => '2023-06-15 12:00:00'],
            ['id' => 2, 'name' => 'row2', 'value' => 'old2', '_timestamp' => '2020-01-01 00:00:00'],
            ['id' => 3, 'name' => 'row3', 'value' => 'old3', '_timestamp' => '2023-06-15 12:00:00'],
            ['id' => 4, 'name' => 'row4', 'value' => 'val4', '_timestamp' => '2023-06-15 12:00:00'],
        ];

        $destinationContent = $this->fetchTable($this->getDestinationDbName(), $tableName);
        $this->assertEqualsCanonicalizing($expectedContent, $destinationContent);
    }

    /**
     * @return Generator<string, array{string[], BigqueryImportOptions::USING_TYPES_*}>
     */
    public static function partitionAwareImportProvider(): Generator
    {
        yield 'legacy, typed source' => [[], BigqueryImportOptions::USING_TYPES_USER];
        yield 'optimized, typed source' => [
            [BigqueryImportOptions::FEATURE_OPTIMIZED_IMPORT],
            BigqueryImportOptions::USING_TYPES_USER,
        ];
        yield 'legacy, string source' => [[], BigqueryImportOptions::USING_TYPES_STRING];
        yield 'optimized, string source' => [
            [BigqueryImportOptions::FEATURE_OPTIMIZED_IMPORT],
            BigqueryImportOptions::USING_TYPES_STRING,
        ];
    }

    /**
     * Partition-aware import only narrows which destination partitions the PK join reads; the rows
     * after the import must be the same with it off, on, and on but above the threshold.
     *
     * @param string[] $features
     * @param BigqueryImportOptions::USING_TYPES_* $usingTypes
     */
    #[DataProvider('partitionAwareImportProvider')]
    public function testPartitionAwareImportKeepsIncrementalImportResult(array $features, string $usingTypes): void
    {
        $sourceDayType = $usingTypes === BigqueryImportOptions::USING_TYPES_USER ? 'DATE' : 'STRING';
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'CREATE TABLE %s.`source` (`id` STRING, `day` %s, `value` STRING)',
            BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
            $sourceDayType,
        )));
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'INSERT INTO %1$s.`source` (`id`, `day`, `value`) VALUES '
            . "('a', CAST('2024-01-01' AS %2\$s), 'new'), "
            . "('b', CAST('2024-01-02' AS %2\$s), 'new'), "
            . "('c', CAST('2024-01-04' AS %2\$s), 'same id, new day'), "
            . "('d', CAST('2024-01-05' AS %2\$s), 'inserted')",
            BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
            $sourceDayType,
        )));
        $source = (new BigqueryTableReflection($this->bqClient, $this->getSourceDbName(), 'source'))
            ->getTableDefinition();
        assert($source instanceof BigqueryTableDefinition);

        $rowsByRun = [];
        foreach (['off' => null, 'on' => 1000, 'above threshold' => 1] as $run => $maxValues) {
            $tableName = 'partitioned_' . str_replace(' ', '_', $run);
            $destinationTable = sprintf(
                '%s.%s',
                BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                BigqueryQuote::quoteSingleIdentifier($tableName),
            );
            $this->bqClient->runQuery($this->bqClient->query(sprintf(
                'CREATE TABLE %s (`id` STRING NOT NULL, `day` DATE NOT NULL, `value` STRING) PARTITION BY `day`',
                $destinationTable,
            )));
            $this->bqClient->runQuery($this->bqClient->query(sprintf(
                'INSERT INTO %s (`id`, `day`, `value`) VALUES'
                . " ('a', DATE '2024-01-01', 'old'), ('a', DATE '2024-01-02', 'old, other day'),"
                . " ('b', DATE '2024-01-02', 'old'), ('c', DATE '2024-01-03', 'old')",
                $destinationTable,
            )));
            $destination = (new BigqueryTableReflection($this->bqClient, $this->getDestinationDbName(), $tableName))
                ->getTableDefinition();
            assert($destination instanceof BigqueryTableDefinition);
            $destination = $this->cloneDefinitionWithDedupCol($destination, ['id', 'day']);

            (new IncrementalImporter($this->bqClient))->importToTable(
                $source,
                $destination,
                new BigqueryImportOptions(
                    isIncremental: true,
                    usingTypes: $usingTypes,
                    features: $features,
                    partitionAwareImport: $maxValues !== null,
                    partitionAwareImportMaxValues: $maxValues,
                ),
                new ImportState($tableName),
            );

            $rowsByRun[$run] = iterator_to_array($this->bqClient->runQuery($this->bqClient->query(sprintf(
                'SELECT `id`, CAST(`day` AS STRING) AS `day`, `value` FROM %s ORDER BY `id`, `day`',
                $destinationTable,
            ))), false);
        }

        $expected = [
            ['id' => 'a', 'day' => '2024-01-01', 'value' => 'new'],
            ['id' => 'a', 'day' => '2024-01-02', 'value' => 'old, other day'],
            ['id' => 'b', 'day' => '2024-01-02', 'value' => 'new'],
            ['id' => 'c', 'day' => '2024-01-03', 'value' => 'old'],
            ['id' => 'c', 'day' => '2024-01-04', 'value' => 'same id, new day'],
            ['id' => 'd', 'day' => '2024-01-05', 'value' => 'inserted'],
        ];
        self::assertSame($expected, $rowsByRun['off']);
        self::assertSame($expected, $rowsByRun['on']);
        self::assertSame($expected, $rowsByRun['above threshold']);
    }

    /**
     * @return Generator<string, array{string, string, string, string[]}>
     */
    public static function partitionedDestinationProvider(): Generator
    {
        // column type, PARTITION BY clause, value of row n (1..20) in partition d (0..59), filter values of d 10 and 11
        yield 'DATE, day' => [
            'DATE',
            'PARTITION BY `part`',
            'DATE_ADD(DATE \'2024-01-01\', INTERVAL d DAY)',
            ['2024-01-11', '2024-01-12'],
        ];
        yield 'TIMESTAMP, day: OR of half-open ranges' => [
            'TIMESTAMP',
            'PARTITION BY TIMESTAMP_TRUNC(`part`, DAY)',
            'TIMESTAMP_ADD(TIMESTAMP \'2024-01-01 00:00:00+00\', INTERVAL d * 24 + n HOUR)',
            ['2024-01-11', '2024-01-12'],
        ];
        yield 'INT64, integer range: IN list' => [
            'INT64',
            'PARTITION BY RANGE_BUCKET(`part`, GENERATE_ARRAY(0, 600, 10))',
            'd * 10 + MOD(n, 10)',
            ['101', '102', '103', '111', '112', '113'],
        ];
    }

    /**
     * Guards that the rendered filter is one BigQuery can prune on (bare column, typed literals), which
     * SQL-text assertions cannot prove: a CAST(dest.col AS STRING) or IN (SELECT ...) filter reads every partition.
     *
     * @param string[] $expectedFilterValues
     */
    #[DataProvider('partitionedDestinationProvider')]
    public function testPartitionAwareImportFilterPrunesDestinationPartitions(
        string $columnType,
        string $partitionBy,
        string $valueExpression,
        array $expectedFilterValues,
    ): void {
        $this->createPartitionedDestination('partitioned_dest', $columnType, $partitionBy, $valueExpression);
        $this->createPartitionedSource($columnType, $valueExpression);

        $destinationReflection = new BigqueryTableReflection(
            $this->bqClient,
            $this->getDestinationDbName(),
            'partitioned_dest',
        );
        $destination = $destinationReflection->getTableDefinition();
        assert($destination instanceof BigqueryTableDefinition);
        $destination = $this->cloneDefinitionWithDedupCol($destination, ['id', 'part']);
        $source = (new BigqueryTableReflection($this->bqClient, $this->getSourceDbName(), 'source'))
            ->getTableDefinition();
        assert($source instanceof BigqueryTableDefinition);

        $sqlBuilder = new SqlBuilder();
        $column = PartitionAwareImportColumn::fromDestination(
            $destinationReflection->getPartitioningConfiguration(),
            $destination,
        );
        self::assertNotNull($column);
        $values = [];
        $result = $this->bqClient->runQuery($this->bqClient->query(
            $sqlBuilder->getSelectDistinctPartitionValuesCommand($source, $column, 1001),
        ));
        foreach ($result as $row) {
            assert(is_array($row));
            $value = $row[SqlBuilder::PARTITION_VALUE_ALIAS] ?? null;
            if (!is_string($value)) {
                self::fail('Distinct partition value query must return STRING values.');
            }
            $values[] = $value;
        }
        $filter = PartitionAwareImportFilter::fromDistinctValues($column, $values, 1000);
        self::assertNotNull($filter);
        self::assertSame($expectedFilterValues, $filter->values);

        $options = new BigqueryImportOptions(isIncremental: true, usingTypes: BigqueryImportOptions::USING_TYPES_USER);
        $timestamp = '2024-01-01 00:00:00';
        $statements = [
            'MERGE' => static fn(?PartitionAwareImportFilter $f) => $sqlBuilder->getMergeCommand(
                $source,
                $destination,
                $options,
                $timestamp,
                $f,
            ),
            'UPDATE' => static fn(?PartitionAwareImportFilter $f) => $sqlBuilder->getUpdateWithPkCommand(
                $source,
                $destination,
                $options,
                $timestamp,
                $f,
            ),
            'DELETE' => static fn(?PartitionAwareImportFilter $f) => $sqlBuilder->getDeleteOldItemsCommand(
                $source,
                $destination,
                $options,
                $f,
            ),
        ];
        foreach ($statements as $name => $buildSql) {
            $unfiltered = $this->getDryRunBytesProcessed($buildSql(null));
            $filtered = $this->getDryRunBytesProcessed($buildSql($filter));
            // pruned reads 2 of 60 equally sized partitions (~30x less) plus the small source; 10x leaves margin
            self::assertGreaterThan(0, $filtered, $name);
            self::assertLessThanOrEqual(
                intdiv($unfiltered, 10),
                $filtered,
                sprintf('%s: filtered %d bytes, unfiltered %d bytes', $name, $filtered, $unfiltered),
            );
        }
    }

    /**
     * @return Generator<string, array{string[], string[]}>
     */
    public static function importerPathProvider(): Generator
    {
        yield 'legacy UPDATE + DELETE' => [[], ['DELETE', 'UPDATE']];
        yield 'optimized MERGE' => [[BigqueryImportOptions::FEATURE_OPTIMIZED_IMPORT], ['MERGE']];
    }

    /**
     * SqlBuilder tests cannot catch the importer computing the filter but not passing it on, so this
     * compares the bytes BigQuery actually processed for the importer's own DML jobs with the option off and on.
     *
     * @param string[] $features
     * @param string[] $expectedStatementTypes
     */
    #[DataProvider('importerPathProvider')]
    public function testIncrementalImporterAppliesPartitionAwareFilter(
        array $features,
        array $expectedStatementTypes,
    ): void {
        $columnType = 'DATE';
        $partitionBy = 'PARTITION BY `part`';
        $valueExpression = 'DATE_ADD(DATE \'2024-01-01\', INTERVAL d DAY)';
        $this->createPartitionedSource($columnType, $valueExpression);
        $source = (new BigqueryTableReflection($this->bqClient, $this->getSourceDbName(), 'source'))
            ->getTableDefinition();
        assert($source instanceof BigqueryTableDefinition);
        // job listing has second precision and a shared project may skew clocks: filter by the unique table instead
        $minCreationTime = (int) (microtime(true) * 1000) - 60_000;
        $runId = substr(md5(uniqid('', true)), 0, 8);

        $bytesByRun = [];
        foreach (['off' => false, 'on' => true] as $run => $partitionAwareImport) {
            $tableName = sprintf('importer_%s_%s', $run, $runId);
            $this->createPartitionedDestination($tableName, $columnType, $partitionBy, $valueExpression);
            $destination = (new BigqueryTableReflection($this->bqClient, $this->getDestinationDbName(), $tableName))
                ->getTableDefinition();
            assert($destination instanceof BigqueryTableDefinition);

            (new IncrementalImporter($this->bqClient))->importToTable(
                $source,
                $this->cloneDefinitionWithDedupCol($destination, ['id', 'part']),
                new BigqueryImportOptions(
                    isIncremental: true,
                    usingTypes: BigqueryImportOptions::USING_TYPES_USER,
                    features: $features,
                    partitionAwareImport: $partitionAwareImport,
                    partitionAwareImportMaxValues: $partitionAwareImport ? 1000 : null,
                ),
                new ImportState($tableName),
            );

            $bytesByRun[$run] = $this->getDmlBytesProcessedByStatementType(
                $minCreationTime,
                sprintf(
                    '%s.%s',
                    BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                    BigqueryQuote::quoteSingleIdentifier($tableName),
                ),
            );
            self::assertSame($expectedStatementTypes, array_keys($bytesByRun[$run]), $run);
        }

        foreach ($expectedStatementTypes as $statementType) {
            // same data as the dry-run test: 2 of 60 partitions read, ~30x less; 10x leaves margin
            self::assertLessThanOrEqual(
                intdiv($bytesByRun['off'][$statementType], 10),
                $bytesByRun['on'][$statementType],
                sprintf(
                    '%s: on %d bytes, off %d bytes',
                    $statementType,
                    $bytesByRun['on'][$statementType],
                    $bytesByRun['off'][$statementType],
                ),
            );
        }
    }

    private function createPartitionedDestination(
        string $tableName,
        string $columnType,
        string $partitionBy,
        string $valueExpression,
    ): void {
        $table = sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        );
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'CREATE TABLE %s (`id` STRING NOT NULL, `part` %s NOT NULL, `value` STRING) %s',
            $table,
            $columnType,
            $partitionBy,
        )));
        // 60 partitions with 20 rows each
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'INSERT INTO %s (`id`, `part`, `value`)'
            . ' SELECT CONCAT(\'row-\', CAST(d AS STRING), \'-\', CAST(n AS STRING)), %s, REPEAT(\'x\', 100)'
            . ' FROM UNNEST(GENERATE_ARRAY(0, 59)) AS d, UNNEST(GENERATE_ARRAY(1, 20)) AS n',
            $table,
            $valueExpression,
        )));
    }

    private function createPartitionedSource(string $columnType, string $valueExpression): void
    {
        $table = sprintf('%s.`source`', BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()));
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'CREATE TABLE %s (`id` STRING, `part` %s, `value` STRING)',
            $table,
            $columnType,
        )));
        // updates for rows of 2 of the 60 destination partitions
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'INSERT INTO %s (`id`, `part`, `value`)'
            . ' SELECT CONCAT(\'row-\', CAST(d AS STRING), \'-\', CAST(n AS STRING)), %s, \'updated\''
            . ' FROM UNNEST([10, 11]) AS d, UNNEST(GENERATE_ARRAY(1, 3)) AS n',
            $table,
            $valueExpression,
        )));
    }

    /**
     * Bytes processed by this test's own finished UPDATE/DELETE/MERGE jobs whose SQL references $tableReference.
     * The importer runs each statement as a separate session query job, so no script child jobs are involved.
     *
     * @return array<string, int> statement type => bytes, sorted by statement type
     */
    private function getDmlBytesProcessedByStatementType(int $minCreationTime, string $tableReference): array
    {
        $bytes = [];
        $jobs = $this->bqClient->jobs([
            'minCreationTime' => $minCreationTime,
            'stateFilter' => 'done',
            'projection' => 'full',
        ]);
        foreach ($jobs as $job) {
            $query = self::arrayPath($job->info(), 'configuration', 'query', 'query');
            if (!is_string($query) || !str_contains($query, $tableReference)) {
                continue;
            }
            // jobs.list returns partial statistics; jobs.get has statementType and totalBytesProcessed
            $statistics = self::arrayPath($job->reload(), 'statistics', 'query');
            $statementType = is_array($statistics) ? ($statistics['statementType'] ?? null) : null;
            if (!in_array($statementType, ['UPDATE', 'DELETE', 'MERGE'], true)) {
                continue;
            }
            $processed = is_array($statistics) ? ($statistics['totalBytesProcessed'] ?? null) : null;
            if (!is_numeric($processed)) {
                self::fail(sprintf('Job %s has no totalBytesProcessed.', $job->id()));
            }
            $bytes[$statementType] = ($bytes[$statementType] ?? 0) + (int) $processed;
        }
        ksort($bytes);
        return $bytes;
    }

    /**
     * @param array<mixed> $data
     */
    private static function arrayPath(array $data, string ...$keys): mixed
    {
        $value = $data;
        foreach ($keys as $key) {
            if (!is_array($value) || !array_key_exists($key, $value)) {
                return null;
            }
            $value = $value[$key];
        }
        return $value;
    }

    private function getDryRunBytesProcessed(string $sql): int
    {
        // dry run only plans the statement: no data change, nothing billed
        $query = $this->bqClient->query($sql);
        $query->dryRun(true);
        $job = $this->bqClient->startQuery($query);
        $statistics = $job->info()['statistics'] ?? null;
        $bytes = is_array($statistics) ? ($statistics['totalBytesProcessed'] ?? null) : null;
        if (!is_numeric($bytes)) {
            self::fail('Dry run returned no totalBytesProcessed for: ' . $sql);
        }
        return (int) $bytes;
    }
}
