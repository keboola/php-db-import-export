<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeExportAdapterInterface;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;

class SnowflakeExportAdapter implements SnowflakeExportAdapterInterface
{
    /** @var Connection */
    private $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public static function isSupported(Storage\SourceInterface $source, Storage\DestinationInterface $destination): bool
    {
        if (!$source instanceof Storage\ABS\DestinationFile) {
            return false;
        }
        if (!$destination instanceof Storage\SqlSourceInterface) {
            return false;
        }
        return true;
    }

    /**
     * @param Storage\SqlSourceInterface $source
     * @param Storage\ABS\DestinationFile $destination
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptions $exportOptions
    ): void {
        if (!$source instanceof Storage\SqlSourceInterface) {
            throw new \Exception(sprintf(
                'Source "%s" must implement "%s".',
                get_class($source),
                Storage\SqlSourceInterface::class
            ));
        }

        $compression = $exportOptions->isCompresed() ? "COMPRESSION='GZIP'" : "COMPRESSION='NONE'";

        $from = $source->getFromStatement();

        $sql = sprintf(
            'COPY INTO \'%s%s\' 
FROM %s
CREDENTIALS=(AZURE_SAS_TOKEN=\'%s\')
FILE_FORMAT = (
    TYPE = \'CSV\'
    FIELD_DELIMITER = \',\'
    FIELD_OPTIONALLY_ENCLOSED_BY = \'\"\'
    %s
    TIMESTAMP_FORMAT = \'YYYY-MM-DD HH24:MI:SS\'
)
MAX_FILE_SIZE=50000000',
            $destination->getContainerUrl(BaseFile::PROTOCOL_AZURE),
            $destination->getFilePath(),
            $from,
            $destination->getSasToken(),
            $compression
        );

        $this->connection->fetchAll($sql, $source->getQueryBindings());
    }
}
