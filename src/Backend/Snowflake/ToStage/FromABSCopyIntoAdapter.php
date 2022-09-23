<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Helper\QueryTemplate;
use Keboola\Db\ImportExport\Backend\Helper\QueryTemplateCollection;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\CopyCommandCsvOptionsHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeException;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\ABS\BaseFile;
use Keboola\Db\ImportExport\Storage\ABS\SourceFile;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Throwable;

class FromABSCopyIntoAdapter implements CopyAdapterInterface
{
    private Connection $connection;

    private const SLICED_FILES_CHUNK_SIZE = 1000;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * @param SourceFile $source
     * @param SnowflakeTableDefinition $destination
     * @param SnowflakeImportOptions $importOptions
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destination,
        ImportOptionsInterface $importOptions
    ): int {
        try {
            $files = $source->getManifestEntries();
            foreach (array_chunk($files, self::SLICED_FILES_CHUNK_SIZE) as $files) {
                $cmd = $this->getCopyCommand(
                    $source,
                    $destination,
                    $importOptions,
                    $files
                );
                $this->connection->executeStatement(
                    $cmd
                );
            }
        } catch (Throwable $e) {
            throw SnowflakeException::covertException($e);
        }

        $ref = new SnowflakeTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );

        return $ref->getRowsCount();
    }

    /**
     * @param string[] $files
     */
    private function getCopyCommand(
        Storage\ABS\SourceFile $source,
        SnowflakeTableDefinition $destination,
        SnowflakeImportOptions $importOptions,
        array $files
    ): string {
        $quotedFiles = array_map(
            static function ($entry) use ($source) {
                return QuoteHelper::quote(
                    strtr(
                        $entry,
                        [$source->getContainerUrl(BaseFile::PROTOCOL_AZURE) => '']
                    )
                );
            },
            $files
        );

        $query = new QueryTemplate(
            'COPY INTO :schema.:table
FROM :url
CREDENTIALS=(AZURE_SAS_TOKEN=\':token\')
FILE_FORMAT = (TYPE=CSV :format)
FILES = (:files)'
        );
        $query->setParams([
            'schema' => SnowflakeQuote::quoteSingleIdentifier($destination->getSchemaName()),
            'table' => SnowflakeQuote::quoteSingleIdentifier($destination->getTableName()),
            'url' => QuoteHelper::quote($source->getContainerUrl(BaseFile::PROTOCOL_AZURE)),
            'token' => $source->getSasToken(),
            'format' => implode(
                ' ',
                CopyCommandCsvOptionsHelper::getCsvCopyCommandOptions(
                    $importOptions,
                    $source->getCsvOptions()
                )
            ),
            'files' => implode(', ', $quotedFiles)
        ]);
        QueryTemplateCollection::add($query, 'copy from ABS into Snowflake');
        return $query->toSql();
    }
}
