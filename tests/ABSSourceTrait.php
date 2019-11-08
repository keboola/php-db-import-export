<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExport;

use DateTime;
use Keboola\Csv\CsvOptions;
use MicrosoftAzure\Storage\Blob\BlobSharedAccessSignatureHelper;
use MicrosoftAzure\Storage\Common\Internal\Resources;
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
            $isSliced
        );
    }

    protected function createABSSourceDestinationInstance(
        string $filePath
    ): Storage\ABS\DestinationFile {
        return new Storage\ABS\DestinationFile(
            (string) getenv('ABS_CONTAINER_NAME'),
            $filePath,
            $this->getCredentialsForAzureContainer((string) getenv('ABS_CONTAINER_NAME')),
            (string) getenv('ABS_ACCOUNT_NAME')
        );
    }

    protected function createABSSourceInstance(
        string $filePath,
        bool $isSliced = false
    ): Storage\ABS\SourceFile {
        return $this->createABSSourceInstanceFromCsv($filePath, new CsvOptions(), $isSliced);
    }

    protected function createABSSourceInstanceFromCsv(
        string $filePath,
        CsvOptions $options,
        bool $isSliced = false
    ): Storage\ABS\SourceFile {
        return new Storage\ABS\SourceFile(
            (string) getenv('ABS_CONTAINER_NAME'),
            $filePath,
            $this->getCredentialsForAzureContainer((string) getenv('ABS_CONTAINER_NAME')),
            (string) getenv('ABS_ACCOUNT_NAME'),
            $options,
            $isSliced
        );
    }

    protected function getCredentialsForAzureContainer(
        string $container
    ): string {
        $sasHelper = new BlobSharedAccessSignatureHelper(
            (string) getenv('ABS_ACCOUNT_NAME'),
            (string) getenv('ABS_ACCOUNT_KEY')
        );
        $expirationDate = (new DateTime())->modify('+1hour');
        return $sasHelper->generateBlobServiceSharedAccessSignatureToken(
            Resources::RESOURCE_TYPE_CONTAINER,
            $container,
            'rwl',
            $expirationDate,
            (new DateTime())
        );
    }
}
