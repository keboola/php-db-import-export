<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Generator;
use Keboola\Csv\CsvOptions;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportAdapterInterface;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\Snowflake\Table;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;

class SnowflakeImportAdapter implements SnowflakeImportAdapterInterface
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
     * @inheritDoc
     * @param Table $destination
     */
    public function executeCopyCommands(
        Generator $commands,
        Connection $connection,
        DestinationInterface $destination,
        ImportOptions $importOptions,
        ImportState $importState
    ): int {
        $timerName = sprintf('copyToStaging-%s', $this->source->getFilePath());
        $importState->startTimer($timerName);
        $rowsCount = 0;
        foreach ($commands as $command) {
            $results = $connection->fetchAll($command);
            foreach ($results as $result) {
                $rowsCount += (int) $result['rows_loaded'];
            }
        }
        $importState->stopTimer($timerName);

        return $rowsCount;
    }

    /**
     * @param Table $destination
     */
    public function getCopyCommands(
        DestinationInterface $destination,
        ImportOptions $importOptions,
        string $stagingTableName
    ): Generator {
        $filesToImport = $this->source->getManifestEntries();
        foreach (array_chunk($filesToImport, ImporterInterface::SLICED_FILES_CHUNK_SIZE) as $entries) {
            yield sprintf(
                'COPY INTO %s.%s 
FROM %s
CREDENTIALS=(AZURE_SAS_TOKEN=\'%s\')
FILE_FORMAT = (TYPE=CSV %s)
FILES = (%s)',
                QueryBuilder::quoteIdentifier($destination->getSchema()),
                QueryBuilder::quoteIdentifier($stagingTableName),
                QueryBuilder::quote($this->source->getContainerUrl()),
                $this->source->getSasToken(),
                implode(' ', $this->getCsvCopyCommandOptions($importOptions, $this->source->getCsvOptions())),
                implode(
                    ', ',
                    array_map(
                        function ($entry) {
                            return QueryBuilder::quote(strtr($entry, [$this->source->getContainerUrl() => '']));
                        },
                        $entries
                    )
                )
            );
        }
    }

    private function getCsvCopyCommandOptions(
        ImportOptions $importOptions,
        CsvOptions $csvOptions
    ): array {
        $options = [
            sprintf('FIELD_DELIMITER = %s', QueryBuilder::quote($csvOptions->getDelimiter())),
        ];

        if ($importOptions->getNumberOfIgnoredLines() > 0) {
            $options[] = sprintf('SKIP_HEADER = %d', $importOptions->getNumberOfIgnoredLines());
        }

        if ($csvOptions->getEnclosure()) {
            $options[] = sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', QueryBuilder::quote($csvOptions->getEnclosure()));
            $options[] = 'ESCAPE_UNENCLOSED_FIELD = NONE';
        } elseif ($csvOptions->getEscapedBy()) {
            $options[] = sprintf('ESCAPE_UNENCLOSED_FIELD = %s', QueryBuilder::quote($csvOptions->getEscapedBy()));
        }
        return $options;
    }
}
