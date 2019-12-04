<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Storage;

trait S3SourceTrait
{
    protected function createS3SourceInstance(
        string $filePath,
        bool $isSliced = false
    ): Storage\S3\SourceFile {
        return $this->createS3SourceInstanceFromCsv($filePath, new CsvOptions(), $isSliced);
    }

    protected function createS3SourceInstanceFromCsv(
        string $filePath,
        CsvOptions $options,
        bool $isSliced = false
    ): Storage\S3\SourceFile {
        return new Storage\S3\SourceFile(
            (string) getenv('AWS_ACCESS_KEY_ID'),
            (string) getenv('AWS_SECRET_ACCESS_KEY'),
            (string) getenv('AWS_REGION'),
            (string) getenv('AWS_S3_BUCKET'),
            $filePath,
            $options,
            $isSliced
        );
    }
}
