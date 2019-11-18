<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\ABS;

use Keboola\Csv\CsvOptions;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\ABS\SnowflakeImportAdapter;
use Keboola\Db\ImportExport\Storage;
use Keboola\SnowflakeDbAdapter\Connection;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SnowflakeImportAdapterTest extends BaseTestCase
{
    public function testExecuteCopyCommands(): void
    {
        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = self::createMock(Storage\ABS\SourceFile::class);
        $source->expects(self::once())->method('getFilePath')->willReturn('file.csv');
        /** @var Connection|MockObject $connection */
        $connection = self::createMock(Connection::class);
        $connection->expects(self::exactly(2))->method('fetchAll')->willReturn([['rows_loaded' => 1]]);
        /** @var ImportState|MockObject $state */
        $state = self::createMock(ImportState::class);
        $state->expects(self::once())->method('startTimer');
        $state->expects(self::once())->method('stopTimer');
        /** @var ImportOptions|MockObject $options */
        $options = self::createMock(ImportOptions::class);

        $adapter = new SnowflakeImportAdapter($source);
        $rows = $adapter->executeCopyCommands(
            ['cmd1', 'cmd2'],
            $connection,
            new Storage\Snowflake\Table('', ''),
            $options,
            $state
        );

        self::assertEquals(2, $rows);
    }

    public function testGetCopyCommands(): void
    {
        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = self::createMock(Storage\ABS\SourceFile::class);
        $source->expects(self::once())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')->willReturn(['azure://url']);
        $source->expects(self::exactly(2))->method('getContainerUrl')->willReturn('containerUrl');
        $source->expects(self::once())->method('getSasToken')->willReturn('sasToken');

        $destination = new Storage\Snowflake\Table('schema', 'table');
        $options = new ImportOptions();
        $adapter = new SnowflakeImportAdapter($source);
        $commands = $adapter->getCopyCommands(
            $destination,
            $options,
            'stagingTable'
        );

        self::assertSame([
            <<<EOT
COPY INTO "schema"."stagingTable" 
FROM 'containerUrl'
CREDENTIALS=(AZURE_SAS_TOKEN='sasToken')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
FILES = ('azure://url')
EOT
            ,
        ], $commands);
    }

    public function testGetCopyCommandsChunk(): void
    {
        $files = [];
        for ($i = 1; $i <= 1500; $i++) {
            $files[] = 'azure://url' . $i;
        }

        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = self::createMock(Storage\ABS\SourceFile::class);
        $source->expects(self::exactly(2))->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::exactly(1))->method('getManifestEntries')->willReturn($files);
        $source->expects(self::exactly(1502/*Called for each entry plus 2times*/))
            ->method('getContainerUrl')->willReturn('containerUrl');
        $source->expects(self::exactly(2))->method('getSasToken')->willReturn('sasToken');

        $destination = new Storage\Snowflake\Table('schema', 'table');
        $options = new ImportOptions();
        $adapter = new SnowflakeImportAdapter($source);
        $commands = $adapter->getCopyCommands(
            $destination,
            $options,
            'stagingTable'
        );

        [$cmd1Files, $cmd2Files] = array_chunk($files, 1000);

        $cmd1Files = implode(', ', array_map(function ($file) {
            return sprintf("'%s'", $file);
        }, $cmd1Files));
        $cmd2Files = implode(', ', array_map(function ($file) {
            return sprintf("'%s'", $file);
        }, $cmd2Files));

        self::assertSame([
            <<<EOT
COPY INTO "schema"."stagingTable" 
FROM 'containerUrl'
CREDENTIALS=(AZURE_SAS_TOKEN='sasToken')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
FILES = ($cmd1Files)
EOT
            ,
            <<<EOT
COPY INTO "schema"."stagingTable" 
FROM 'containerUrl'
CREDENTIALS=(AZURE_SAS_TOKEN='sasToken')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
FILES = ($cmd2Files)
EOT
            ,
        ], $commands);
    }
}
