<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Synapse;

use Keboola\Db\ImportExport\Backend\Synapse\DestinationTableOptions;
use Keboola\Db\ImportExport\Backend\Synapse\TableDistribution;
use PHPUnit\Framework\TestCase;

class DestinationTableOptionsTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $options = new DestinationTableOptions(
            ['pk1', 'pk1', 'col1', 'col2'],
            ['pk1', 'pk1'],
            new TableDistribution()
        );
        self::assertEquals(['PK1', 'PK1', 'COL1', 'COL2'], $options->getColumnNamesInOrder());
        self::assertEquals(['PK1', 'PK1'], $options->getPrimaryKeys());
        self::assertEquals('ROUND_ROBIN', $options->getDistribution()->getDistributionName());
        self::assertEquals([], $options->getDistribution()->getDistributionColumnsNames());
    }

    public function testDistributionValues(): void
    {
        $options = new DestinationTableOptions(
            ['pk1', 'pk1', 'col1', 'col2'],
            ['pk1', 'pk1'],
            new TableDistribution(
                TableDistribution::TABLE_DISTRIBUTION_HASH,
                ['pk1']
            )
        );
        self::assertEquals(['PK1', 'PK1', 'COL1', 'COL2'], $options->getColumnNamesInOrder());
        self::assertEquals(['PK1', 'PK1'], $options->getPrimaryKeys());
        self::assertEquals('HASH', $options->getDistribution()->getDistributionName());
        self::assertEquals(['PK1'], $options->getDistribution()->getDistributionColumnsNames());
    }
}
