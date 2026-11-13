<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Bigquery\ToFinalTable;

use Generator;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\PartitionRangeColumn;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\PartitioningConfig;
use Keboola\TableBackendUtils\Table\Bigquery\RangePartitioningConfig;
use Keboola\TableBackendUtils\Table\Bigquery\TimePartitioningConfig;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class PartitionRangeColumnTest extends TestCase
{
    private static function destination(): BigqueryTableDefinition
    {
        return new BigqueryTableDefinition('out.c-main', 'dest', false, new ColumnCollection([
            new BigqueryColumn('id', new Bigquery(Bigquery::TYPE_STRING)),
            new BigqueryColumn('name', new Bigquery(Bigquery::TYPE_STRING)),
            new BigqueryColumn('day', new Bigquery(Bigquery::TYPE_DATE)),
            new BigqueryColumn('dt', new Bigquery(Bigquery::TYPE_DATETIME)),
            new BigqueryColumn('ts', new Bigquery(Bigquery::TYPE_TIMESTAMP)),
            new BigqueryColumn('bucket', new Bigquery(Bigquery::TYPE_INT64)),
        ]), ['id']);
    }

    /**
     * @return Generator<string, array{?PartitioningConfig, ?array{string, string, ?string, ?int, ?int, ?int}}>
     */
    public static function fromDestinationProvider(): Generator
    {
        yield 'DATE by day' => [
            new PartitioningConfig(new TimePartitioningConfig('DAY', null, 'day'), null, false),
            ['day', Bigquery::TYPE_DATE, 'DAY', null, null, null],
        ];
        yield 'DATE by month' => [
            new PartitioningConfig(new TimePartitioningConfig('MONTH', null, 'day'), null, false),
            ['day', Bigquery::TYPE_DATE, 'MONTH', null, null, null],
        ];
        yield 'DATETIME by hour' => [
            new PartitioningConfig(new TimePartitioningConfig('HOUR', null, 'dt'), null, false),
            ['dt', Bigquery::TYPE_DATETIME, 'HOUR', null, null, null],
        ];
        yield 'TIMESTAMP by year' => [
            new PartitioningConfig(new TimePartitioningConfig('YEAR', null, 'ts'), null, false),
            ['ts', Bigquery::TYPE_TIMESTAMP, 'YEAR', null, null, null],
        ];
        yield 'integer range' => [
            new PartitioningConfig(null, new RangePartitioningConfig('bucket', '0', '100', '10'), false),
            ['bucket', Bigquery::TYPE_INT64, null, 0, 100, 10],
        ];
        yield 'STRING column' => [
            new PartitioningConfig(new TimePartitioningConfig('DAY', null, 'name'), null, false),
            null,
        ];
        yield 'not partitioned' => [null, null];
        yield 'no partitioning in config' => [new PartitioningConfig(null, null, false), null];
        yield 'ingestion time' => [new PartitioningConfig(new TimePartitioningConfig('DAY', null, null), null, false), null];
        yield 'missing column' => [new PartitioningConfig(new TimePartitioningConfig('DAY', null, 'missing'), null, false), null];
        yield 'unknown granularity' => [new PartitioningConfig(new TimePartitioningConfig('WEEK', null, 'day'), null, false), null];
        yield 'DATE by hour' => [new PartitioningConfig(new TimePartitioningConfig('HOUR', null, 'day'), null, false), null];
        yield 'integer range with non-numeric bound' => [
            new PartitioningConfig(null, new RangePartitioningConfig('bucket', 'oops', '100', '10'), false),
            null,
        ];
    }

    /**
     * @param ?array{string, string, ?string, ?int, ?int, ?int} $expected
     */
    #[DataProvider('fromDestinationProvider')]
    public function testFromDestination(?PartitioningConfig $partitioning, ?array $expected): void
    {
        $column = PartitionRangeColumn::fromDestination($partitioning, self::destination());
        if ($expected === null) {
            self::assertNull($column);
            return;
        }
        self::assertInstanceOf(PartitionRangeColumn::class, $column);
        self::assertSame($expected, [
            $column->columnName,
            $column->type,
            $column->granularity,
            $column->rangeStart,
            $column->rangeEnd,
            $column->rangeInterval,
        ]);
    }
}
