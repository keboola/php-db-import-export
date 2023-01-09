<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataException;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\S3\SourceFile;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Throwable;

class FromS3NOSAdapter implements CopyAdapterInterface
{
    private Connection $connection;

    private const SLICED_FILES_CHUNK_SIZE = 10;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * @param SourceFile $source
     * @param TeradataTableDefinition $destination
     * @param TeradataImportOptions $importOptions
     */
    public function runCopyCommand(
        Storage\SourceInterface  $source,
        TableDefinitionInterface $destination,
        ImportOptionsInterface   $importOptions
    ): int
    {
        try {
            $files = $source->getManifestEntries();

            foreach (array_chunk($files, self::SLICED_FILES_CHUNK_SIZE) as $filesChunk) {
                $partialImportCommand = $this->getCopyCommand(
                    $source,
                    $destination,
                    $importOptions,
                    $filesChunk
                );
                $cmd = sprintf(
                    'INSERT INTO %s.%s %s;',
                    TeradataQuote::quoteSingleIdentifier($destination->getSchemaName()),
                    TeradataQuote::quoteSingleIdentifier($destination->getTableName()),
                    $partialImportCommand);
                $this->connection->executeStatement($cmd);
            }

        } catch (Throwable $e) {
            throw TeradataException::covertException($e);
        }

        $ref = new TeradataTableReflection(
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
        Storage\S3\SourceFile    $source,
        TeradataTableDefinition $destination,
        TeradataImportOptions   $importOptions,
        array                    $files
    ): string
    {
        $s3Prefix = $source->getS3Prefix();
        $csvOptions = $source->getCsvOptions();

        $singleFileImports = [];
        foreach ($files as $file){
            $singleFileImports[] = sprintf(
                "
        SELECT %s FROM READ_NOS ( USING
 LOCATION (%s)
 ACCESS_ID (%s)
 ACCESS_KEY (%s)
 MANIFEST('FALSE')
 HEADER('%s')
 RETURNTYPE('NOSREAD_RECORD')
 -- ROWFORMAT(%s)
 ) AS d
",
                implode(
                    ',',
                    array_map(
                        fn($columnName) => TeradataQuote::quoteSingleIdentifier($columnName),
                        $destination->getColumnsNames()
                    )
                ),
                TeradataQuote::quote(str_replace('https://', '/S3/', $source->getBucketURL() . '/' . $source->getFilePath())),
                TeradataQuote::quote($source->getKey()),
                TeradataQuote::quote($source->getSecret()),
                $importOptions->getNumberOfIgnoredLines() === ImportOptionsInterface::SKIP_FIRST_LINE ? 'TRUE' : 'FALSE',
                TeradataQuote::quote(json_encode(['field_delimiter' => $csvOptions->getDelimiter(), 'record_delimiter' => "\n"])),
            );
        }
        return implode("\nUNION ALL\n", $singleFileImports);

    }
}
