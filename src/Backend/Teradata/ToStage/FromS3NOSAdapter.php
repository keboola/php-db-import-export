<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataException;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\S3\SourceFile;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
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
        Storage\SourceInterface $source,
        TableDefinitionInterface $destination,
        ImportOptionsInterface $importOptions
    ): int {
        try {
            $files = $source->getManifestEntries();

            $useHeader = false;
            if ($importOptions->getNumberOfIgnoredLines() === ImportOptionsInterface::SKIP_FIRST_LINE) {
                // CSV contains header
                $columns = $this->getColumnsFromCSV($source, $files[0]);
                $useHeader = true;
            } else {
                // if no header is provided, NOS will set it as Col1, Col2...
                $columns = array_map(fn($number) => 'Col' . $number, range(1, count($destination->getColumnsNames())));
            }
            foreach (array_chunk($files, self::SLICED_FILES_CHUNK_SIZE) as $filesChunk) {
                $partialImportCommand = $this->getCopyCommand(
                    $source,
                    $filesChunk,
                    $columns,
                    $useHeader
                );
                $cmd = sprintf(
                    'INSERT INTO %s.%s %s;',
                    TeradataQuote::quoteSingleIdentifier($destination->getSchemaName()),
                    TeradataQuote::quoteSingleIdentifier($destination->getTableName()),
                    $partialImportCommand
                );
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
     * @param string[] $columns
     */
    private function getCopyCommand(
        Storage\S3\SourceFile $source,
        array $files,
        array $columns,
        bool $useHeader
    ): string {

        $singleFileImports = [];
        foreach ($files as $file) {
            $singleFileImports[] = sprintf(
                "
        SELECT %s FROM READ_NOS ( USING
 LOCATION (%s)
 ACCESS_ID (%s)
 ACCESS_KEY (%s)
 MANIFEST('FALSE')
 HEADER('%s')
 RETURNTYPE('NOSREAD_RECORD')
 ROWFORMAT('{\"field_delimiter\":\"%s\", \"character_set\":\"UTF8\"}')
 ) AS d
",
                implode(
                    ',',
                    array_map(
                        fn($columnName) => TeradataQuote::quoteSingleIdentifier($columnName),
                        $columns
                    )
                ),
                TeradataQuote::quote('/S3/' . $source->getS3PathFromRelative($file)),
                TeradataQuote::quote($source->getKey()),
                TeradataQuote::quote($source->getSecret()),
                $useHeader ? 'TRUE' : 'FALSE',
                $source->getCsvOptions()->getDelimiter()
            );
        }
        return implode("\nUNION ALL\n", $singleFileImports);
    }

    /**
     * @return string[]
     */
    private function getColumnsFromCSV(
        Storage\S3\SourceFile $source,
        string $firstFile
    ): array {
        $firstFile = str_replace('s3://', '', $firstFile);
        $firstFileSuffix = str_replace($source->getBucket(), '', $firstFile);
        $sql =
            sprintf(
                "
        SELECT * FROM READ_NOS ( USING
 LOCATION (%s)
 ACCESS_ID (%s)
 ACCESS_KEY (%s)
 MANIFEST('FALSE')
 RETURNTYPE('NOSREAD_SCHEMA')
 ROWFORMAT('{\"field_delimiter\":\"%s\", \"character_set\":\"UTF8\"}')
 ) AS d
",
                TeradataQuote::quote('/S3/' . $source->getS3Path($firstFileSuffix)),
                TeradataQuote::quote($source->getKey()),
                TeradataQuote::quote($source->getSecret()),
                $source->getCsvOptions()->getDelimiter()
            );
        $columns = $this->connection->fetchAllAssociative($sql);

        return array_map(fn($row) => $row['Name'], $columns);
    }
}
