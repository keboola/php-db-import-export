<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\S3;

use Aws\Exception\AwsException;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer as SnowflakeImporter;
use Keboola\Db\ImportExport\Storage\NoBackendAdapterException;
use Keboola\Db\ImportExport\Storage\SourceInterface;

class SourceFile implements SourceInterface
{
    /**
     * @var bool
     */
    private $isSliced;

    /**
     * @var CsvOptions
     */
    private $csvOptions;

    /**
     * @var string
     */
    private $key;

    /**
     * @var string
     */
    private $secret;

    /**
     * @var string
     */
    private $region;

    /**
     * @var string
     */
    private $bucket;

    /**
     * @var string
     */
    private $filePath;

    public function __construct(
        string $key,
        string $secret,
        string $region,
        string $bucket,
        string $filePath,
        CsvOptions $csvOptions,
        bool $isSliced
    ) {
        $this->isSliced = $isSliced;
        $this->csvOptions = $csvOptions;
        $this->key = $key;
        $this->secret = $secret;
        $this->region = $region;
        $this->bucket = $bucket;
        $this->filePath = $filePath;
    }

    public function getBackendImportAdapter(
        ImporterInterface $importer
    ): BackendImportAdapterInterface {
        switch (true) {
            case $importer instanceof SnowflakeImporter:
                return new SnowflakeImportAdapter($this);
            default:
                throw new NoBackendAdapterException();
        }
    }

    public function getCsvOptions(): CsvOptions
    {
        return $this->csvOptions;
    }

    public function getKey(): string
    {
        return $this->key;
    }

    public function getManifestEntries(): array
    {
        if (!$this->isSliced) {
            return [$this->getS3Prefix() . $this->filePath];
        }

        $client = new \Aws\S3\S3Client([
            'credentials' => [
                'key' => $this->key,
                'secret' => $this->secret,
            ],
            'region' => $this->region,
            'version' => '2006-03-01',
        ]);

        try {
            $response = $client->getObject([
                'Bucket' => $this->bucket,
                'Key' => ltrim($this->filePath, '/'),
            ]);
        } catch (AwsException $e) {
            throw new Exception('Unable to download file from S3: ' . $e->getMessage());
        }

        $manifest = json_decode((string) $response['Body'], true);
        return array_map(static function ($entry) {
            return $entry['url'];
        }, $manifest['entries']);
    }

    public function getRegion(): string
    {
        return $this->region;
    }

    public function getS3Prefix(): string
    {
        return sprintf('s3://%s', $this->bucket);
    }

    public function getSecret(): string
    {
        return $this->secret;
    }
}
