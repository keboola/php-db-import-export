<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

class ImportResult
{
    /** @var array */
    private $results;

    public function __construct(array $results)
    {
        $this->results = $results;
    }

    public function getImportedColumns(): array
    {
        return (array) $this->getKeyValue('importedColumns', []);
    }

    public function getImportedRowsCount(): int
    {
        return (int) $this->getKeyValue('importedRowsCount');
    }

    public function getTimers(): array
    {
        return (array) $this->getKeyValue('timers', []);
    }

    public function getWarnings(): array
    {
        return (array) $this->getKeyValue('warnings', []);
    }

    /**
     * @param string $keyName
     * @param mixed|null $default
     * @return mixed|null
     */
    public function getKeyValue(string $keyName, $default = null)
    {
        return isset($this->results[$keyName]) ? $this->results[$keyName] : $default;
    }
}
