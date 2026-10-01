<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Bigquery\ToFinalTable;

use Generator;
use InvalidArgumentException;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\PartitionPruningColumn;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\PartitionPruningFilter;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\PartitioningConfig;
use Keboola\TableBackendUtils\Table\Bigquery\RangePartitioningConfig;
use Keboola\TableBackendUtils\Table\Bigquery\TimePartitioningConfig;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class PartitionPruningFilterTest extends TestCase
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
     * @return Generator<string, array{?PartitioningConfig, string[]}>
     */
    public static function notApplicableProvider(): Generator
    {
        yield 'not partitioned' => [null, ['id', 'day']];
        yield 'no partitioning in config' => [new PartitioningConfig(null, null, false), ['id', 'day']];
        yield 'ingestion-time partitioning' => [self::timePartitioning('DAY', null), ['id', 'day']];
        yield 'time partition column not in PK' => [self::timePartitioning('DAY', 'day'), ['id']];
        yield 'range partition column not in PK' => [
            new PartitioningConfig(null, new RangePartitioningConfig('bucket', '0', '100', '10'), false),
            ['id'],
        ];
    }

    /**
     * @param string[] $primaryKeys
     */
    #[DataProvider('notApplicableProvider')]
    public function testColumnIsNotResolvedWhenPruningCannotApply(
        ?PartitioningConfig $partitioning,
        array $primaryKeys,
    ): void {
        self::assertNull(PartitionPruningColumn::fromDestination($partitioning, self::destination($primaryKeys)));
    }

    public function testColumnResolvesTypeAndGranularityFromDestination(): void
    {
        $time = PartitionPruningColumn::fromDestination(
            self::timePartitioning('HOUR', 'ts'),
            self::destination(['id', 'ts']),
        );
        self::assertNotNull($time);
        self::assertSame(['ts', Bigquery::TYPE_TIMESTAMP, 'HOUR'], [$time->columnName, $time->type, $time->granularity]);

        $range = PartitionPruningColumn::fromDestination(
            new PartitioningConfig(null, new RangePartitioningConfig('bucket', '0', '100', '10'), false),
            self::destination(['id', 'bucket']),
        );
        self::assertNotNull($range);
        self::assertSame(['bucket', Bigquery::TYPE_INT64, null], [$range->columnName, $range->type, $range->granularity]);
    }

    public function testFilterIsDroppedAboveThresholdAndKeptAtIt(): void
    {
        $column = PartitionPruningColumn::fromDestination(
            self::timePartitioning('DAY', 'day'),
            self::destination(['id', 'day']),
        );
        self::assertNotNull($column);
        $values = ['2024-01-03', '2024-01-01', '2024-01-02'];

        self::assertNull(PartitionPruningFilter::fromDistinctValues($column, $values, 2));
        self::assertNull(PartitionPruningFilter::fromDistinctValues($column, [], 2));

        $filter = PartitionPruningFilter::fromDistinctValues($column, $values, 3);
        self::assertNotNull($filter);
        self::assertSame(['2024-01-01', '2024-01-02', '2024-01-03'], $filter->values);
    }

    public function testFilterRejectsValueThatIsNotAPartitionValue(): void
    {
        $column = PartitionPruningColumn::fromDestination(
            self::timePartitioning('DAY', 'day'),
            self::destination(['id', 'day']),
        );
        self::assertNotNull($column);

        $this->expectException(LogicException::class);
        PartitionPruningFilter::fromDistinctValues($column, ["2024-01-01') OR (TRUE"], 10);
    }

    public function testOptionsRequireThresholdWhenPruningIsRequested(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new BigqueryImportOptions(isIncremental: true, partitionPruning: true);
    }
}
