<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\GCS\ManifestGenerator;

use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageObject;
use JsonException;
use Keboola\Db\ImportExport\Storage\SlicedManifestGeneratorInterface;
use Keboola\FileStorage\Path\RelativePathInterface;

/**
 * Generates manifest file by iterating files in a gcs path
 */
class GcsSlicedManifestFromFolderGenerator implements SlicedManifestGeneratorInterface
{
    public function __construct(
        private readonly StorageClient $client,
        private readonly WriteStreamFactory $writeStreamFactory,
    ) {
    }

    /**
     * @throws JsonException
     */
    public function generateAndSaveManifest(RelativePathInterface $path): void
    {
        $bucket = $this->client->bucket($path->getRoot());
        $objects = $bucket->objects(['prefix' => $path->getPathnameWithoutRoot()]);

        $entries = [];
        /** @var StorageObject $object */
        foreach ($objects as $object) {
            $entries[] = [
                'url' => sprintf('gs://%s/%s', $bucket->name(), $object->name()),
                'mandatory' => true,
            ];
        }

        /** @var string $encodedManifest */
        $encodedManifest = json_encode([
            'entries' => $entries,
        ], JSON_THROW_ON_ERROR);

        $writeStream = $this->writeStreamFactory->createWriteStream();
        $uploader = $bucket->getStreamableUploader($writeStream, [
            'name' => $path->getPathnameWithoutRoot() . 'manifest',
        ]);
        $writeStream->setUploader($uploader);
        $stream = fopen('data://text/plain;base64,' . base64_encode($encodedManifest), 'rb');
        if ($stream !== false) {
            while (($line = stream_get_line($stream, self::CHUNK_SIZE_256_KB)) !== false) {
                $writeStream->write($line);
            }
        }
        $writeStream->close();
    }
}
