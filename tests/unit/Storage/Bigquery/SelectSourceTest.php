<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\Bigquery;

use Keboola\Db\ImportExport\Storage;
use PHPUnit\Framework\TestCase;

class SelectSourceTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $source = new Storage\Bigquery\SelectSource('SELECT * FROM `SCHEMA`.`TABLE`', ['prop' => 1], ['col1']);

        $this->assertEquals('SELECT * FROM `SCHEMA`.`TABLE`', $source->getFromStatement());
        $this->assertEquals('SELECT * FROM `SCHEMA`.`TABLE`', $source->getFromStatementWithStringCasting());
        $this->assertEquals('SELECT * FROM `SCHEMA`.`TABLE`', $source->getQuery());
        $this->assertSame(['prop' => 1], $source->getQueryBindings());
        $this->assertSame(['col1'], $source->getColumnsNames());
        $this->assertNull($source->getPrimaryKeysNames());
    }
}
