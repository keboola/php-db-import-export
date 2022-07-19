<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\GCS;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\CopyCommandCsvOptionsHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\SqlCommandBuilder;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

class SnowflakeImportAdapter implements SnowflakeImportAdapterInterface
{
    private const SLICED_FILES_CHUNK_SIZE = 1000;

    private Connection $connection;

    private SqlCommandBuilder $sqlBuilder;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->sqlBuilder = new SqlCommandBuilder();
    }
    
    public static function isSupported(Storage\SourceInterface $source, Storage\DestinationInterface $destination): bool
    {
        if (!$source instanceof Storage\Gcs\SourceFile) {
            return false;
        }
        if (!$destination instanceof Storage\Snowflake\Table) {
            return false;
        }

        return true;
    }

    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptionsInterface $importOptions,
        string $stagingTableName
    ): int
    {
        $commands = $this->getCommands($source, $destination, $importOptions, $stagingTableName);

        foreach ($commands as $sql) {
            $this->connection->query($sql);
        }

        $rows = $this->connection->fetchAll($this->sqlBuilder->getTableItemsCountCommand(
            $destination->getSchema(),
            $stagingTableName
        ));

        return (int) $rows[0]['count'];
    }

    /**
     * @param string[]
     */
    private function getCommands(
        Storage\GCS\SourceFile $source,
        Storage\Snowflake\Table $destination,
        ImportOptionsInterface $importOptions,
        string $stagingTableName
    ): array {
        $filesToImport = $source->getManifestEntries();
        $commands = [];
        $gcsPrefix = $source->getGcsPrefix() . '/';
        foreach (array_chunk($filesToImport, ImporterInterface::SLICED_FILES_CHUNK_SIZE) as $entries) {
            $quotedFiles = array_map(
                static function ($entry) use ($gcsPrefix) {
                    return QuoteHelper::quote(strtr($entry, [$gcsPrefix => '']));
                },
                $entries
            );

            $commands[] = sprintf(
                'COPY INTO %s.%s FROM %s
                STORAGE_INTEGRATION = %s, 
                FILE_FORMAT = (TYPE=CSV %s)
                FILES = (%s)',
                $this->connection->quoteIdentifier($destination->getSchema()),
                $this->connection->quoteIdentifier($stagingTableName),
                QuoteHelper::quote($source->getGcsPrefix()),
                SnowflakeQuote::quoteSingleIdentifier($source->getStorageIntegrationName()),
                implode(' ', CopyCommandCsvOptionsHelper::getCsvCopyCommandOptions(
                    $importOptions,
                    $source->getCsvOptions()
                )),
                implode(', ', $quotedFiles)
            );
        }

        return $commands;
    }
}
