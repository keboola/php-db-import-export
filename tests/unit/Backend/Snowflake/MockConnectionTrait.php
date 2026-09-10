<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use PHPUnit\Framework\MockObject\MockObject;

trait MockConnectionTrait
{
    /**
     * @return Connection&MockObject
     */
    private function mockConnection(): Connection
    {
        /** @var Connection&MockObject $mock */
        $mock = $this->createMock(Connection::class);

        $mock->method('quoteIdentifier')->willReturnCallback(
            static function (string $input) {
                return QuoteHelper::quoteIdentifier($input);
            },
        );

        return $mock;
    }
}
