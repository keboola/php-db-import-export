<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\S3;

use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Storage\CopyCommandHelper;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\Snowflake\Table;
use Keboola\Db\ImportExport\Storage\SourceInterface;

class SnowflakeImportAdapter implements BackendImportAdapterInterface
{
    /**
     * @var SourceFile
     */
    private $source;

    /**
     * @param SourceFile $source
     */
    public function __construct(SourceInterface $source)
    {
        $this->source = $source;
    }

    /**
     * @param Table $destination
     */
    public function getCopyCommands(
        DestinationInterface $destination,
        ImportOptions $importOptions,
        string $stagingTableName
    ): array {
        $filesToImport = $this->source->getManifestEntries();
        $commands = [];
        foreach (array_chunk($filesToImport, ImporterInterface::SLICED_FILES_CHUNK_SIZE) as $entries) {
            $commands[] = sprintf(
                'COPY INTO %s.%s
FROM %s 
CREDENTIALS = (AWS_KEY_ID = %s AWS_SECRET_KEY = %s)
REGION = %s
FILE_FORMAT = (TYPE=CSV %s)
FILES = (%s)',
                QuoteHelper::quoteIdentifier($destination->getSchema()),
                QuoteHelper::quoteIdentifier($stagingTableName),
                QuoteHelper::quote($this->source->getS3Prefix()),
                QuoteHelper::quote($this->source->getKey()),
                QuoteHelper::quote($this->source->getSecret()),
                QuoteHelper::quote($this->source->getRegion()),
                implode(' ', CopyCommandHelper::getCsvCopyCommandOptions(
                    $importOptions,
                    $this->source->getCsvOptions()
                )),
                implode(
                    ', ',
                    array_map(
                        function ($entry) {
                            return QuoteHelper::quote(strtr($entry, [$this->source->getS3Prefix().'/' => '']));
                        },
                        $entries
                    )
                )
            );
        }
        return $commands;
    }
}
