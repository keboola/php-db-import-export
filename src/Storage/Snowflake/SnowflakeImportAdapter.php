<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Snowflake;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\SqlCommandBuilder;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

class SnowflakeImportAdapter implements SnowflakeImportAdapterInterface
{
    private Connection $connection;

    private SqlCommandBuilder $sqlBuilder;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->sqlBuilder = new SqlCommandBuilder();
    }

    public static function isSupported(Storage\SourceInterface $source, Storage\DestinationInterface $destination): bool
    {
        if (!$destination instanceof Table) {
            return false;
        }

        if (!$source instanceof Table && !$source instanceof SelectSource) {
            return false;
        }

        return true;
    }

    /**
     * @param Table|SelectSource $source
     * @param Table $destination
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptionsInterface $importOptions,
        string $stagingTableName,
    ): int {
        $quotedColumns = array_map(
            function ($column) {
                return SnowflakeQuote::quoteSingleIdentifier($column);
            },
            $source->getColumnsNames(),
        );

        $sql = sprintf(
            'INSERT INTO %s.%s (%s) %s',
            SnowflakeQuote::quoteSingleIdentifier($destination->getSchema()),
            SnowflakeQuote::quoteSingleIdentifier($stagingTableName),
            implode(', ', $quotedColumns),
            $source->getFromStatement(),
        );

        $this->connection->executeStatement(
            $sql,
            $source instanceof SelectSource ? $source->getQueryBindings() : [],
        );

        /** @var array<array{count: int|numeric-string}> $rows */
        $rows = $this->connection->fetchAllAssociative(
            $this->sqlBuilder->getTableItemsCountCommand(
                $destination->getSchema(),
                $stagingTableName,
            ),
        );

        return (int) $rows[0]['count'];
    }
}
