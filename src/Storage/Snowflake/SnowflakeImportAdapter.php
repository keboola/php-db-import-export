<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Snowflake;

use Generator;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportAdapterInterface;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;

class SnowflakeImportAdapter implements SnowflakeImportAdapterInterface
{
    /**
     * @var Table
     */
    private $source;

    /**
     * @param Table $source
     */
    public function __construct(SourceInterface $source)
    {
        $this->source = $source;
    }

    /**
     * @param Table $destination
     */
    public function executeCopyCommands(
        Generator $commands,
        Connection $connection,
        DestinationInterface $destination,
        ImportOptions $importOptions,
        ImportState $importState
    ): int {
        $importState->startTimer('copyToStaging');
        $connection->query($commands->current());
        $rows = $connection->fetchAll(sprintf(
            'SELECT COUNT(*) AS "count" FROM %s.%s',
            QueryBuilder::quoteIdentifier($destination->getSchema()),
            QueryBuilder::quoteIdentifier($importState->getStagingTableName())
        ));
        $importState->stopTimer('copyToStaging');
        return (int) $rows[0]['count'];
    }

    /**
     * @param Table $destination
     */
    public function getCopyCommands(
        DestinationInterface $destination,
        ImportOptions $importOptions,
        string $stagingTableName
    ): Generator {
        $quotedColumns = array_map(function ($column) {
            return QueryBuilder::quoteIdentifier($column);
        }, $importOptions->getColumns());

        $sql = sprintf(
            'INSERT INTO %s.%s (%s)',
            QueryBuilder::quoteIdentifier($destination->getSchema()),
            QueryBuilder::quoteIdentifier($stagingTableName),
            implode(', ', $quotedColumns)
        );

        $sql .= sprintf(
            ' SELECT %s FROM %s.%s',
            implode(', ', $quotedColumns),
            QueryBuilder::quoteIdentifier($this->source->getSchema()),
            QueryBuilder::quoteIdentifier($this->source->getTableName()),
        );

        yield $sql;
    }
}
