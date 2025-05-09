<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\Export;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\ExportOptionsInterface;
use Keboola\Db\ImportExport\Storage;

class GcsExportAdapter implements BackendExportAdapterInterface
{
    private Connection $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public static function isSupported(Storage\SourceInterface $source, Storage\DestinationInterface $destination): bool
    {
        if (!$source instanceof Storage\SqlSourceInterface) {
            return false;
        }
        if (!$destination instanceof Storage\GCS\DestinationFile) {
            return false;
        }
        return true;
    }

    /**
     * @param Storage\SqlSourceInterface $source
     * @param Storage\GCS\DestinationFile $destination
     * @param ExportOptions $exportOptions
     * @return array<mixed>
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptionsInterface $exportOptions,
    ): array {
        $timestampFormat = 'YYYY-MM-DD HH24:MI:SS';
        if (in_array(Exporter::FEATURE_FRACTIONAL_SECONDS, $exportOptions->features(), true)) {
            $timestampFormat = 'YYYY-MM-DD HH24:MI:SS.FF9';
        }
        $sql = sprintf(
            'COPY INTO \'%s/%s\'
FROM (%s)
STORAGE_INTEGRATION = "%s"
FILE_FORMAT = (
    TYPE = \'CSV\'
    FIELD_DELIMITER = \',\'
    FIELD_OPTIONALLY_ENCLOSED_BY = \'\"\'
    %s
    TIMESTAMP_FORMAT = \'%s\',
    NULL_IF = ()
)
MAX_FILE_SIZE=%d
DETAILED_OUTPUT = TRUE',
            $destination->getGcsPrefix(),
            $destination->getFilePath(),
            $source->getFromStatement(),
            $destination->getStorageIntegrationName(),
            $exportOptions->isCompressed() ? "COMPRESSION='GZIP'" : "COMPRESSION='NONE'",
            $timestampFormat,
            Exporter::DEFAULT_SLICE_SIZE,
        );

        $this->connection->executeQuery($sql, $source->getQueryBindings());

        /** @var array<array{FILE_NAME: string, FILE_SIZE: string, ROW_COUNT: string}> $unloadedFiles */
        $unloadedFiles = $this->connection->fetchAllAssociative('select * from table(result_scan(last_query_id()));');

        if ($exportOptions->generateManifest()) {
            (new Storage\GCS\ManifestGenerator\GcsSlicedManifestFromUnloadQueryResultGenerator(
                $destination->getClient(),
            ))
                ->generateAndSaveManifest($destination->getRelativePath(), $unloadedFiles);
        }

        return $unloadedFiles;
    }
}
