<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\Import\Result;
use Tracy\Debugger;

class ImportState
{
    /** @var array<mixed> */
    protected array $warnings = [];

    protected int $importedRowsCount = 0;

    /** @var array<mixed> */
    private array $timers = [];

    /** @var string[] */
    private array $importedColumns = [];

    private string $stagingTableName = '';

    public function __construct(string $stagingTableName)
    {
        $this->stagingTableName = $stagingTableName;
    }

    public function addImportedRowsCount(int $count): void
    {
        $this->importedRowsCount += $count;
    }

    public function getResult(): Result
    {
        return new Result([
            'warnings' => $this->warnings,
            'timers' => array_values($this->timers), // convert to indexed array
            'importedRowsCount' => $this->importedRowsCount,
            'importedColumns' => $this->importedColumns,
        ]);
    }

    public function getStagingTableName(): string
    {
        return $this->stagingTableName;
    }

    public function overwriteStagingTableName(string $stagingTableName): void
    {
        $this->stagingTableName = $stagingTableName;
    }

    /**
     * @param string[] $importedColumns
     */
    public function setImportedColumns(array $importedColumns): void
    {
        $this->importedColumns = $importedColumns;
    }

    public function startTimer(string $timerName): void
    {
        Debugger::timer($timerName);
        $this->timers[$timerName] = [
            'name' => $timerName,
            'durationSeconds' => null,
        ];
    }

    public function stopTimer(string $timerName): void
    {
        $this->timers[$timerName]['durationSeconds'] = Debugger::timer($timerName);
    }
}
