<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\SQLServerPlatform;
use Keboola\Db\ImportExport\Backend\BackendHelper;
use Keboola\Db\ImportExport\Backend\Synapse\PolyBaseCommandBuilder;
use Keboola\Db\ImportExport\Backend\Synapse\SqlCommandBuilder;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseExportOptions;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportAdapterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;

class SynapsePolyBaseImportAdapter implements SynapseImportAdapterInterface
{
    /** @var Connection */
    private $connection;

    /** @var \Doctrine\DBAL\Platforms\AbstractPlatform|SQLServerPlatform */
    private $platform;

    /** @var SqlCommandBuilder */
    private $sqlBuilder;

    /** @var PolyBaseCommandBuilder */
    private $polyBase;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->platform = $connection->getDatabasePlatform();
        $this->sqlBuilder = new SqlCommandBuilder($this->connection);
        $this->polyBase = new PolyBaseCommandBuilder($connection);
    }

    public static function isSupported(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination
    ): bool {
        if (!$source instanceof Storage\ABS\SourceFile) {
            return false;
        }
        if (!$destination instanceof Storage\Synapse\Table) {
            return false;
        }
        return true;
    }

    /**
     * @param Storage\ABS\SourceFile $source
     * @param Storage\Synapse\Table $destination
     * @param SynapseImportOptions $importOptions
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptionsInterface $importOptions,
        string $stagingTableName
    ): int {
        $dateFormat = 'yyyy-MM-dd HH:mm:ss';
        $exportId = BackendHelper::generateRandomExportPrefix();
        $blobMasterKey = $importOptions->getBlobMasterKey();
        $containerUrl = $source->getPolyBaseUrl(SynapseExportOptions::CREDENTIALS_MASTER_KEY);
        $credentialsId = $exportId . '_StorageCredential';
        $dataSourceId = $exportId . '_StorageSource';
        $fileFormatId = $exportId . '_StorageFileFormat';
        $tableId = $exportId . '_StorageExternalTable';

        try {
            $sql = $this->polyBase->getCredentialsQuery(
                $credentialsId,
                SynapseExportOptions::CREDENTIALS_MASTER_KEY,
                $blobMasterKey
            );
            $this->connection->exec($sql);
            $sql = $this->polyBase->getDataSourceQuery($dataSourceId, $containerUrl, $credentialsId);
            $this->connection->exec($sql);
            $fileFormatIdQuoted = $this->platform->quoteSingleIdentifier($fileFormatId);
            $firstRow = '';
            if ($importOptions->getNumberOfIgnoredLines() !== 0) {
                $firstRow = sprintf(',First_Row=%s', $importOptions->getNumberOfIgnoredLines() + 1);
            }
            $enclosure = $this->connection->quote('0x'.bin2hex($source->getCsvOptions()->getEnclosure()));
            $fieldDelimiter = $this->connection->quote($source->getCsvOptions()->getDelimiter());
            $sql = <<<EOT
CREATE EXTERNAL FILE FORMAT $fileFormatIdQuoted
WITH
(
    FORMAT_TYPE = DelimitedText,
    FORMAT_OPTIONS
    (
        FIELD_TERMINATOR = $fieldDelimiter,
        STRING_DELIMITER = $enclosure,
        DATE_FORMAT = '$dateFormat',
        USE_TYPE_DEFAULT = FALSE
        $firstRow
    )
);
EOT;
            $this->connection->exec($sql);

            $sql = $this->getCreateTempTableCommand(
                $tableId,
                $source->getColumnsNames(),
                $dataSourceId,
                $fileFormatIdQuoted
            );

            $this->connection->exec($sql);

            $sql = $this->sqlBuilder->getDropCommand($destination->getSchema(), $stagingTableName);
            $this->connection->exec($sql);

            $destinationSchema = $this->platform->quoteSingleIdentifier($destination->getSchema());
            $destinationTable = $this->platform->quoteSingleIdentifier($stagingTableName);
            $tableIdQuoted = $this->platform->quoteSingleIdentifier($tableId);

            $sql = <<< EOT
CREATE TABLE $destinationSchema.$destinationTable
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN)
AS
SELECT * FROM [dbo].$tableIdQuoted;
EOT;

            $this->connection->exec($sql);
        } catch (\Throwable $e) {
            //exception is saved for later while we try to clean created resources
            $exception = $e;
        }


        foreach ($this->polyBase->getPolyBaseCleanUpQueries(
            $fileFormatId,
            $dataSourceId,
            $credentialsId,
            $tableId
        ) as $sql
        ) {
            try {
                $this->connection->exec($sql);
            } catch (\Throwable $e) {
                // we want to perform whole clean up
            }
        }

        if ($exception !== null) {
            throw $exception;
        }

        $rows = $this->connection->fetchAll($this->sqlBuilder->getTableItemsCountCommand(
            $destination->getSchema(),
            $stagingTableName
        ));

        return (int) $rows[0]['count'];
    }

    private function getCreateTempTableCommand(
        string $tableName,
        array $columns,
        string $dataSourceId,
        string $fileFormatId
    ): string {
        $columnsSql = array_map(function ($column) {
            return sprintf('%s nvarchar(max)', $this->platform->quoteSingleIdentifier($column));
        }, $columns);
        return sprintf(
            'CREATE EXTERNAL TABLE [dbo].%s (%s) WITH (LOCATION=\'../\',DATA_SOURCE=%s,FILE_FORMAT=%s)',
            $this->platform->quoteSingleIdentifier($tableName),
            implode(', ', $columnsSql),
            $dataSourceId,
            $fileFormatId
        );
    }
}

