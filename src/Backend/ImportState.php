<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Symfony\Component\Stopwatch\Stopwatch;

class ImportState
{
    /** @var array */
    protected $warnings = [];

    /** @var int */
    protected $importedRowsCount = 0;

    /** @var array */
    private $timers = [];

    /** @var array */
    private $importedColumns = [];

    /** @var string */
    private $stagingTableName = '';

    /**
     * @var Stopwatch
     */
    private $stopwatch;

    public function __construct(string $stagingTableName)
    {
        $this->stagingTableName = $stagingTableName;
        $this->stopwatch = new Stopwatch();
    }

    public function addImportedRowsCount(int $count): void
    {
        $this->importedRowsCount += $count;
    }

    public function getResult(): ImportResult
    {
        return new ImportResult([
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

    public function setImportedColumns(array $importedColumns): void
    {
        $this->importedColumns = $importedColumns;
    }

    public function startTimer(string $timerName): void
    {
        $this->stopwatch->start($timerName);
        $this->timers[$timerName] = [
            'name' => $timerName,
            'durationSeconds' => null,
        ];
    }

    public function stopTimer(string $timerName): void
    {
        $miliseconds = $this->stopwatch->stop($timerName)->getDuration();
        $this->timers[$timerName]['durationSeconds'] = $miliseconds / 1000;
    }
}
