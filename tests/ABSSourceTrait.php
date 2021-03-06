<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExport;

use DateTime;
use Keboola\CsvOptions\CsvOptions;
use MicrosoftAzure\Storage\Blob\BlobSharedAccessSignatureHelper;
use MicrosoftAzure\Storage\Common\Internal\Resources;
use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Storage;

trait ABSSourceTrait
{
    protected function createDummyABSSourceInstance(
        string $file,
        bool $isSliced = false
    ): Storage\ABS\SourceFile {
        return new Storage\ABS\SourceFile(
            'absContainer',
            $file,
            'azureCredentials',
            'absAccount',
            new CsvOptions(),
            $isSliced,
            []
        );
    }

    protected function createABSSourceDestinationInstance(
        string $filePath
    ): Storage\ABS\DestinationFile {
        return new Storage\ABS\DestinationFile(
            (string) getenv('ABS_CONTAINER_NAME'),
            $filePath,
            $this->getCredentialsForAzureContainer((string) getenv('ABS_CONTAINER_NAME'), 'rwla'),
            (string) getenv('ABS_ACCOUNT_NAME'),
            (string) getenv('ABS_ACCOUNT_KEY')
        );
    }

    protected function createABSSourceInstance(
        string $filePath,
        array $columns = [],
        bool $isSliced = false,
        bool $isDirectory = false,
        ?array $primaryKeys = null
    ): Storage\ABS\SourceFile {
        return $this->createABSSourceInstanceFromCsv(
            $filePath,
            new CsvOptions(),
            $columns,
            $isSliced,
            $isDirectory,
            $primaryKeys
        );
    }

    protected function createABSSourceInstanceFromCsv(
        string $filePath,
        CsvOptions $options,
        array $columns = [],
        bool $isSliced = false,
        bool $isDirectory = false,
        ?array $primaryKeys = null
    ): Storage\ABS\SourceFile {
        if ($isDirectory) {
            $class = Storage\ABS\SourceDirectory::class;
        } else {
            $class = Storage\ABS\SourceFile::class;
        }
        return new $class(
            (string) getenv('ABS_CONTAINER_NAME'),
            $filePath,
            $this->getCredentialsForAzureContainer((string) getenv('ABS_CONTAINER_NAME')),
            (string) getenv('ABS_ACCOUNT_NAME'),
            $options,
            $isSliced,
            $columns,
            $primaryKeys
        );
    }

    protected function getCredentialsForAzureContainer(
        string $container,
        string $permissions = 'rwl'
    ): string {
        $sasHelper = new BlobSharedAccessSignatureHelper(
            (string) getenv('ABS_ACCOUNT_NAME'),
            (string) getenv('ABS_ACCOUNT_KEY')
        );
        $expirationDate = (new DateTime())->modify('+1hour');
        return $sasHelper->generateBlobServiceSharedAccessSignatureToken(
            Resources::RESOURCE_TYPE_CONTAINER,
            $container,
            $permissions,
            $expirationDate,
            (new DateTime())
        );
    }
}
