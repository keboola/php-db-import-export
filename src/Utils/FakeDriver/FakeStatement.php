<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Utils\FakeDriver;

use Doctrine\DBAL\Cache\ArrayResult;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\ParameterType;
use Keboola\TableBackendUtils\Connection\Exception\DriverException;

class FakeStatement implements Statement
{
    /**
     * @var resource
     */
    private $dbh;

    /**
     * @var resource
     */
    private $stmt;

    /**
     * @var array<mixed>
     */
    private array $params = [];

    private string $query;

    /**
     * @param resource $dbh database handle
     */
    public function __construct($dbh, string $query)
    {
        $this->dbh = $dbh;
        $this->query = $query;
//        $this->stmt = $this->prepare();
    }

    /**
     * @return resource
     */
//    private function prepare()
//    {
//        $stmt = @odbc_prepare($this->dbh, $this->query);
//        if (!$stmt) {
//            throw DriverException::newFromHandle($this->dbh);
//        }
//        return $stmt;
//    }

    /**
     * @inheritDoc
     */
    public function bindValue($param, $value, $type = ParameterType::STRING): bool
    {
        return $this->bindParam($param, $value, $type);
    }

    /**
     * @inheritDoc
     */
    public function bindParam($param, &$variable, $type = ParameterType::STRING, $length = null): bool
    {
        $this->params[$param] = &$variable;
        return true;
    }

    /**
     * @inheritDoc
     */
    public function execute($params = null): ArrayResult
    {
        return new ArrayResult([]);
    }

    /**
     * Avoid odbc file open http://php.net/manual/en/function.odbc-execute.php
     *
     * @param array<mixed> $bind
     * @return array<mixed>
     */
    private function repairBinding(array $bind): array
    {
        return array_map(function ($value) {
            if (!is_string($value)) {
                return $value;
            }
            if (preg_match("/^'.*'$/", $value)) {
                return " {$value} ";
            }

            return $value;
        }, $bind);
    }
}
