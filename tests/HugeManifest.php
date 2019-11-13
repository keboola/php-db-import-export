<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExport;

use Keboola\Csv\CsvOptions;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Temp\Temp;
use MicrosoftAzure\Storage\Common\Internal\Resources;
use Symfony\Component\Stopwatch\Stopwatch;
use Webmozart\Assert\Assert;

class HugeManifest
{
    private const MANIFEST_FILE_NAME = 'hugeManifest.json';
    // 500k slices + something extra to test slices count
    private const SLICES_TOTAL = (500 * ImporterInterface::SLICED_FILES_CHUNK_SIZE) + 10;
    use ABSSourceTrait;

    /**
     * @var string
     */
    private $manifestFile;

    /**
     * @var AbsLoader
     */
    private $loader;

    /**
     * @var Stopwatch
     */
    private $stopwatch;

    /**
     * @var Temp
     */
    private $temp;

    /**
     * @var string
     */
    private $containerName;

    /**
     * @var string
     */
    private $accountName;

    public function __construct(
        string $accountName,
        string $containerName
    ) {
        $this->accountName = $accountName;
        $this->containerName = $containerName;
        $this->loader = new AbsLoader($accountName, $containerName);
    }

    public function run(): void
    {
        $this->temp = new Temp();
        $this->temp->initRunFolder();

        $this->stopwatch = new Stopwatch();
        $this->stopwatch->start('test');
        $this->loader->deleteContainer();
        $this->loader->createContainer();

        $this->stopwatch->start('upload');
        $this->uploadManifestAndSlices();
        $event = $this->stopwatch->stop('upload');
        echo 'max memory upload: ' . $this->getMemoryForHuman($event->getMemory()) . PHP_EOL;

        $this->stopwatch->start('commands');
        $this->generateCommands();
        $event = $this->stopwatch->stop('commands');
        echo 'max memory commands: ' . $this->getMemoryForHuman($event->getMemory()) . PHP_EOL;

        $this->loader->deleteContainer();
        $event = $this->stopwatch->stop('test');
        echo 'max memory: ' . $this->getMemoryForHuman($event->getMemory()) . PHP_EOL;
    }

    private function uploadManifestAndSlices(): void
    {
        $this->printMemory();
        $manifest = $this->openManifestFile();

        echo 'Generating manifest' . PHP_EOL;
        for ($i = 0; $i <= self::SLICES_TOTAL; $i++) {
            $sliceName = sprintf('my_awesome_long_name_slice.csv_%d', $i);
            fwrite($manifest, sprintf(
                '{"url":"%s"}%s' . PHP_EOL,
                $this->getAbsUrl($sliceName),
                $i === self::SLICES_TOTAL ? '' : ','
            ));
        }

        $this->closeManifestFile($manifest);

        echo PHP_EOL;

        echo 'Uploading manifest' . PHP_EOL;

        $this->loader->getBlobService()->createBlockBlob(
            $this->containerName,
            self::MANIFEST_FILE_NAME,
            file_get_contents($this->getManifestFileName())
        );

        echo sprintf(
            'Manifest file size: %s',
            $this->getMemoryForHuman(filesize($this->getManifestFileName()))
        ) . PHP_EOL;
        $this->printMemory();
    }

    private function printMemory(): void
    {
        $memUsage = memory_get_usage(true);

        echo $this->getMemoryForHuman($memUsage);

        echo PHP_EOL;
    }

    private function getMemoryForHuman(int $memUsage): string
    {
        if ($memUsage < 1024) {
            return $memUsage . ' bytes';
        } elseif ($memUsage < 1048576) {
            return round($memUsage / 1024, 2) . ' kilobytes';
        } else {
            return round($memUsage / 1048576, 2) . ' megabytes';
        }
    }

    /**
     * @return false|resource
     */
    private function openManifestFile()
    {
        file_put_contents($this->getManifestFileName(), '{"entries":[' . PHP_EOL);
        return fopen($this->getManifestFileName(), 'a');
    }

    private function getManifestFileName(): string
    {
        if ($this->manifestFile === null) {
            $this->manifestFile = $this->temp->getTmpFolder() . '/' . self::MANIFEST_FILE_NAME;
        }

        return $this->manifestFile;
    }

    private function getAbsUrl(string $fileName): string
    {
        return sprintf(
            'azure://%s.%s/%s/%s',
            $this->accountName,
            Resources::BLOB_BASE_DNS_NAME,
            $this->containerName,
            $fileName
        );
    }

    /**
     * @param resource $resource
     */
    private function closeManifestFile($resource): void
    {
        fwrite($resource, ']}');
        fclose($resource);
    }

    private function generateCommands(): void
    {
        $source = new Storage\ABS\SourceFile(
            $this->containerName,
            self::MANIFEST_FILE_NAME,
            $this->getCredentialsForAzureContainer($this->containerName),
            $this->accountName,
            new CsvOptions,
            true
        );
        $destination = new Storage\Snowflake\Table('schema', 'table');
        $options = new ImportOptions();
        $adapter = new Storage\ABS\SnowflakeImportAdapter($source);
        echo 'Generating commands' . PHP_EOL;
        $commandsCount = 0;
        foreach ($adapter->getCopyCommands(
            $destination,
            $options,
            'stagingTable'
        ) as $index => $cmd) {
            $commandsCount++;
            $this->stopwatch->lap('commands');
        };

        Assert::eq(501, $commandsCount);
    }
}
