<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport;

interface ImportOptionsInterface
{
    /** @return string[] */
    public function getConvertEmptyValuesToNull(): array;

    public function getNumberOfIgnoredLines(): int;

    public function isIncremental(): bool;

    public function useTimestamp(): bool;
}
