<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\ToStage;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\FromABSCopyIntoAdapter;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\MockDbalConnectionTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class FromABSCopyIntoAdapterTest extends BaseTestCase
{
    use MockDbalConnectionTrait;

    public function testGetCopyCommands(): void
    {
        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects(self::any())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')->willReturn(['https://url']);

        $conn = $this->mockConnection();
        $conn->expects(self::once())->method('executeStatement')->with(
            <<<EOT
COPY INTO . 
FROM ''
CREDENTIALS=(AZURE_SAS_TOKEN='')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
FILES = ('https://url')
EOT
        );

        $conn->expects(self::once())->method('fetchOne')
            ->with('SELECT COUNT(*) AS NumberOfRows FROM "schema"."stagingTable"')
            ->willReturn(10);

        $destination = new SnowflakeTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            []
        );
        $options = new SnowflakeImportOptions();
        $adapter = new FromABSCopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options
        );

        self::assertEquals(10, $count);
    }

    public function testGetCopyCommandsRowSkip(): void
    {
        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects(self::any())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')->willReturn(['https://url']);

        $conn = $this->mockConnection();
        // @codingStandardsIgnoreStart
        $conn->expects(self::once())->method('executeStatement')->with(
            <<<EOT
COPY INTO . 
FROM ''
CREDENTIALS=(AZURE_SAS_TOKEN='')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' SKIP_HEADER = 3 FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
FILES = ('https://url')
EOT
        );
        // @codingStandardsIgnoreEnd

        $conn->expects(self::once())->method('fetchOne')
            ->with('SELECT COUNT(*) AS NumberOfRows FROM "schema"."stagingTable"')
            ->willReturn(7);

        $destination = new SnowflakeTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            []
        );
        $options = new SnowflakeImportOptions([], false, false, 3);
        $adapter = new FromABSCopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options
        );

        self::assertEquals(7, $count);
    }

    public function testGetCopyCommandWithMoreChunksOfFiles(): void
    {
        $entries = [];
        $entriesWithoutBucket = [];

        // limit for snflk files in one query is 1000
        for ($i = 1; $i < 1005; $i++) {
            $entries[] = "file{$i}";
            $entriesWithoutBucket[] = "'file{$i}'";
        }

        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects(self::any())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')->willReturn($entries);

        $conn = $this->mockConnection();

        $qTemplate = <<<EOT
COPY INTO . 
FROM ''
CREDENTIALS=(AZURE_SAS_TOKEN='')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
FILES = (%s)
EOT;
        $q1 = sprintf($qTemplate, implode(', ', array_slice($entriesWithoutBucket, 0, 1000)));
        $q2 = sprintf($qTemplate, implode(', ', array_slice($entriesWithoutBucket, 1000, 5)));
        $conn->expects(self::exactly(2))->method('executeStatement')->withConsecutive([$q1], [$q2]);

        $conn->expects(self::once())->method('fetchOne')
            ->with('SELECT COUNT(*) AS NumberOfRows FROM "schema"."stagingTable"')
            ->willReturn(7);

        $destination = new SnowflakeTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            []
        );
        $options = new SnowflakeImportOptions();
        $adapter = new FromABSCopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options
        );

        self::assertEquals(7, $count);
    }
}