<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Snowflake;

use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ExporterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer as SnowflakeImporter;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\NoBackendAdapterException;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\SnowflakeDbAdapter\QueryBuilder;

class Table implements SourceInterface, DestinationInterface
{
    /**
     * @var string
     */
    private $schema;

    /**
     * @var string
     */
    private $tableName;

    public function __construct(string $schema, string $tableName)
    {
        $this->schema = $schema;
        $this->tableName = $tableName;
    }

    public function getBackendExportAdapter(
        ExporterInterface $exporter
    ): BackendExportAdapterInterface {
        throw new NoBackendAdapterException();
    }

    public function getBackendImportAdapter(
        ImporterInterface $importer
    ): BackendImportAdapterInterface {
        switch (true) {
            case $importer instanceof SnowflakeImporter:
                return new SnowflakeImportAdapter($this);
            default:
                throw new NoBackendAdapterException();
        }
    }

    public function getQuotedTableWithScheme(): string
    {
        return sprintf(
            '%s.%s',
            QueryBuilder::quoteIdentifier($this->schema),
            QueryBuilder::quoteIdentifier($this->tableName),
        );
    }

    public function getSchema(): string
    {
        return $this->schema;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }
}
