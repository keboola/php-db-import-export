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

    public static function isFixtureOfSomeRun(string $path): bool
    {
        $key = trim((string) getenv('AWS_S3_KEY'), " \t\n\r\0\x0B/");
        if ($key !== '' && str_starts_with($path, $key . '/')) {
            $path = substr($path, strlen($key) + 1);
        }

        return preg_match(self::RUN_PREFIX_PATTERN, $path) === 1;
    }
}
