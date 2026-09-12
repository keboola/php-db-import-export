<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportCommon\Cleanup;

/**
 * Recognises a path that belongs to a CI run's fixture prefix (BUILD_PREFIX,
 * `ci-<run id>`), so cleanup never touches anything else living in the shared
 * bucket or container.
 */
final class StaleFixturePrefix
{
    private const RUN_PREFIX_PATTERN = '~^ci-\d+/~';

    /**
     * Matches fixtures written at the storage root - the ABS and GCS layout
     * (see FixturePath::in).
     */
    public static function isFixtureOfSomeRun(string $path): bool
    {
        return preg_match(self::RUN_PREFIX_PATTERN, $path) === 1;
    }

    /**
     * S3 fixtures live under AWS_S3_KEY (see FixturePath::inS3), so an object
     * outside that subtree is not ours even when it carries a run prefix. An
     * unset key means the fixtures sit at the bucket root, same as the others.
     */
    public static function isS3FixtureOfSomeRun(string $path): bool
    {
        $key = trim((string) getenv('AWS_S3_KEY'), " \t\n\r\0\x0B/");
        if ($key === '') {
            return self::isFixtureOfSomeRun($path);
        }

        $subtree = $key . '/';
        if (!str_starts_with($path, $subtree)) {
            return false;
        }

        return self::isFixtureOfSomeRun(substr($path, strlen($subtree)));
    }
}
