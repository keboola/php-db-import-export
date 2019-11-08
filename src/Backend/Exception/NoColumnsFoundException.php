<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Exception;

use Keboola\SnowflakeDbAdapter\Exception\ExceptionInterface;

class NoColumnsFoundException extends \Exception implements ExceptionInterface
{
}
