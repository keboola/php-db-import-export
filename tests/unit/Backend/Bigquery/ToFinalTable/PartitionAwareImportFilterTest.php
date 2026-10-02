<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Bigquery\ToFinalTable;

use Generator;
use InvalidArgumentException;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\PartitionAwareImportColumn;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\PartitionAwareImportDecision;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\PartitionAwareImportFilter;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\PartitioningConfig;
use Keboola\TableBackendUtils\Table\Bigquery\RangePartitioningConfig;
use Keboola\TableBackendUtils\Table\Bigquery\TimePartitioningConfig;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class PartitionAwareImportFilterTest extends TestCase
{
    /**
     * @param string[] $primaryKeys
     */
    private static function destination(array $primaryKeys): BigqueryTableDefinition
    {
        return new BigqueryTableDefinition('out.c-main', 'dest', false, new ColumnCollection([
            new BigqueryColumn('id', new Bigquery(Bigquery::TYPE_STRING)),
            new BigqueryColumn('day', new Bigquery(Bigquery::TYPE_DATE)),
            new BigqueryColumn('ts', new Bigquery(Bigquery::TYPE_TIMESTAMP)),
            new BigqueryColumn('bucket', new Bigquery(Bigquery::TYPE_INT64)),
        ]), $primaryKeys);
    }

    private static function timePartitioning(string $type, ?string $column): PartitioningConfig
    {
        return new PartitioningConfig(new TimePartitioningConfig($type, null, $column), null, false);
    }

    /**
     * @return Generator<string, array{?PartitioningConfig, string[], PartitionAwareImportDecision::SKIPPED_*}>
     */
    public static function notApplicableProvider(): Generator
    {
        yield 'not partitioned' => [null, ['id', 'day'], PartitionAwareImportDecision::SKIPPED_NOT_PARTITIONED];
        yield 'no partitioning in config' => [
            new PartitioningConfig(null, null, false),
            ['id', 'day'],
            PartitionAwareImportDecision::SKIPPED_NOT_PARTITIONED,
        ];
        yield 'ingestion-time partitioning' => [
            self::timePartitioning('DAY', null),
            ['id', 'day'],
            PartitionAwareImportDecision::SKIPPED_INGESTION_TIME_PARTITIONING,
        ];
        yield 'time partition column not in PK' => [
            self::timePartitioning('DAY', 'day'),
            ['id'],
            PartitionAwareImportDecision::SKIPPED_COLUMN_NOT_IN_PRIMARY_KEY,
        ];
        yield 'range partition column not in PK' => [
            new PartitioningConfig(null, new RangePartitioningConfig('bucket', '0', '100', '10'), false),
            ['id'],
            PartitionAwareImportDecision::SKIPPED_COLUMN_NOT_IN_PRIMARY_KEY,
        ];
        yield 'time partitioning on a column of unsupported type' => [
            self::timePartitioning('DAY', 'id'),
            ['id'],
            PartitionAwareImportDecision::SKIPPED_UNSUPPORTED_TYPE,
        ];
    }

    /**
     * @param string[] $primaryKeys
     */
    #[DataProvider('notApplicableProvider')]
    public function testColumnIsNotResolvedWhenPartitionAwareImportCannotApply(
        ?PartitioningConfig $partitioning,
        array $primaryKeys,
        string $expectedSkippedReason,
    ): void {
        self::assertSame(
            $expectedSkippedReason,
            PartitionAwareImportColumn::fromDestination($partitioning, self::destination($primaryKeys)),
        );
    }

    public function testColumnResolvesTypeAndGranularityFromDestination(): void
    {
        $time = PartitionAwareImportColumn::fromDestination(
            self::timePartitioning('HOUR', 'ts'),
            self::destination(['id', 'ts']),
        );
        self::assertInstanceOf(PartitionAwareImportColumn::class, $time);
        self::assertSame(
            ['ts', Bigquery::TYPE_TIMESTAMP, 'HOUR'],
            [$time->columnName, $time->type, $time->granularity],
        );

        $range = PartitionAwareImportColumn::fromDestination(
            new PartitioningConfig(null, new RangePartitioningConfig('bucket', '0', '100', '10'), false),
            self::destination(['id', 'bucket']),
        );
        self::assertInstanceOf(PartitionAwareImportColumn::class, $range);
        self::assertSame(
            ['bucket', Bigquery::TYPE_INT64, null],
            [$range->columnName, $range->type, $range->granularity],
        );
    }

    public function testFilterIsDroppedAboveThresholdAndKeptAtIt(): void
    {
        $column = PartitionAwareImportColumn::fromDestination(
            self::timePartitioning('DAY', 'day'),
            self::destination(['id', 'day']),
        );
        self::assertInstanceOf(PartitionAwareImportColumn::class, $column);
        $values = ['2024-01-03', '2024-01-01', '2024-01-02'];

        self::assertNull(PartitionAwareImportFilter::fromDistinctValues($column, $values, 2));
        self::assertNull(PartitionAwareImportFilter::fromDistinctValues($column, [], 2));

        $filter = PartitionAwareImportFilter::fromDistinctValues($column, $values, 3);
        self::assertNotNull($filter);
        self::assertSame(['2024-01-01', '2024-01-02', '2024-01-03'], $filter->values);
    }

    public function testFilterRejectsValueThatIsNotAPartitionValue(): void
    {
        $column = PartitionAwareImportColumn::fromDestination(
            self::timePartitioning('DAY', 'day'),
            self::destination(['id', 'day']),
        );
        self::assertInstanceOf(PartitionAwareImportColumn::class, $column);

        $this->expectException(LogicException::class);
        PartitionAwareImportFilter::fromDistinctValues($column, ["2024-01-01') OR (TRUE"], 10);
    }

    /**
     * @return Generator<string, array{int|null}>
     */
    public static function invalidThresholdProvider(): Generator
    {
        yield 'missing' => [null];
        yield 'zero' => [0];
        yield 'above the BigQuery 4,000 modified partitions limit' => [4001];
    }

    #[DataProvider('invalidThresholdProvider')]
    public function testOptionsRejectInvalidThresholdWhenPartitionAwareImportIsRequested(?int $maxValues): void
    {
        $this->expectException(InvalidArgumentException::class);
        new BigqueryImportOptions(
            isIncremental: true,
            partitionAwareImport: true,
            partitionAwareImportMaxValues: $maxValues,
        );
    }

    public function testOptionsAcceptThresholdAtTheLimit(): void
    {
        $options = new BigqueryImportOptions(
            isIncremental: true,
            partitionAwareImport: true,
            partitionAwareImportMaxValues: BigqueryImportOptions::PARTITION_AWARE_IMPORT_MAX_VALUES_LIMIT,
        );
        self::assertSame(4000, $options->partitionAwareImportMaxValues);
    }
}
