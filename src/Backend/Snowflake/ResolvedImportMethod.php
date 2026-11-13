<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use InvalidArgumentException;

/**
 * A concrete import method together with its origin. Never AUTO: importers branch on it directly.
 */
final readonly class ResolvedImportMethod
{
    public function __construct(
        public ImportMethod $method,
        public ImportMethodSource $source,
    ) {
        if ($method === ImportMethod::AUTO) {
            throw new InvalidArgumentException('Resolved import method must not be "auto".');
        }
    }

    public function isSingleStatement(): bool
    {
        return $this->method === ImportMethod::SINGLE_STATEMENT;
    }
}
