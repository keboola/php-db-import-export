<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Exasol\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage\S3\SourceDirectory;
use Keboola\Db\ImportExport\Storage\S3\SourceFile;
use Keboola\Db\ImportExport\Storage;
use Keboola\FileStorage\LineEnding\StringLineEndingDetectorHelper;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

class FromS3CopyIntoAdapter implements CopyAdapterInterface
{
    private const SLICED_FILES_CHUNK_SIZE = 50;
    /** @var Connection */
    private $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * @param SourceFile $source
     * @param ExasolTableDefinition $destination
     * @param ExasolImportOptions $importOptions
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destination,
        ImportOptionsInterface $importOptions
    ): int {
        assert($source instanceof SourceFile);
        assert($destination instanceof ExasolTableDefinition);
        assert($importOptions instanceof ExasolImportOptions);

        foreach ($this->getCopyCommand($source, $destination, $importOptions) as $sql) {
            if ($sql === null) {
                break;
            }
            $this->connection->executeStatement($sql);
        }

        $ref = new ExasolTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );

        return $ref->getRowsCount();
    }

    /**
     * @return \Generator<string|null>
     */
    private function getCopyCommand(
        Storage\S3\SourceFile $source,
        ExasolTableDefinition $destination,
        ExasolImportOptions $importOptions
    ): \Generator {
        $destinationSchema = ExasolQuote::quoteSingleIdentifier($destination->getSchemaName());
        $destinationTable = ExasolQuote::quoteSingleIdentifier($destination->getTableName());

        // first row (skippping some lines)
        $firstRow = '';
        if ($importOptions->getNumberOfIgnoredLines() !== 0) {
            $firstRow = sprintf('SKIP=%d', $importOptions->getNumberOfIgnoredLines());
        }

        // get files from single file, directory or manifest
        /** @var SourceDirectory|SourceFile $source */
        $filesToImport = $source->getManifestEntries();
        $s3Prefix = $source->getS3Prefix() . '/';

        if (count($filesToImport) === 0) {
            yield null;
        }

        foreach (array_chunk($filesToImport, self::SLICED_FILES_CHUNK_SIZE) as $entries) {
            $entries = array_map(
                static function ($entry) use ($s3Prefix) {
                    return 'FILE ' . ExasolQuote::quote(strtr($entry, [$s3Prefix => '']));
                },
                $entries
            );

            // EXA COLUMN SEPARATOR = string between values
            // EXA COLUMN DELIMITER = enclosure -> quote to quote values aaa -> "aaa"
            // ESCAPED BY is not supported yet
            yield sprintf(
                '
IMPORT INTO %s.%s FROM CSV AT %s
USER %s IDENTIFIED BY %s
%s --- files
--- file_opt
%s
COLUMN SEPARATOR=%s
COLUMN DELIMITER=%s
',
                $destinationSchema,
                $destinationTable,
                ExasolQuote::quote($source->getBucketURL()),
                ExasolQuote::quote($source->getKey()),
                ExasolQuote::quote($source->getSecret()),
                implode("\n", $entries),
                $firstRow,
                ExasolQuote::quote($source->getCsvOptions()->getDelimiter()),
                ExasolQuote::quote($source->getCsvOptions()->getEnclosure())
            );
        }
    }
}
