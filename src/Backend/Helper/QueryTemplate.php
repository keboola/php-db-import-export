<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Helper;

class QueryTemplate implements \Stringable
{
    private string $query;
    private array $params;

    public function __construct(string $query, array $params = [])
    {
        $this->query = $query;
        $this->params = $params;
    }

    public function getQuery(): string
    {
        return $this->query;
    }

    public function setParams(array $params): void
    {
        $this->params = $params;
    }

    public function getParams(): array
    {
        return $this->params;
    }

    public function clearParams(): void
    {
        $this->params = [];
    }

    public function clearParamsValues(): void
    {
        $this->params = array_fill_keys(
            array_keys($this->params),
            null
        );
    }

    public function __toString(): string
    {
        return $this->toSql();
    }

    public function toSql(): string
    {
        return $this->fillQueryWithParams($this->query, $this->params);
    }

    private function fillQueryWithParams(string $query, array $params = []): string
    {
        foreach ($params as $key => $value) {
            $query = preg_replace(
                sprintf('/:%s\b/', $key),
                $value,
                $query,
            );
        }
        return $query;
    }

    public function replaceParams(string $prefix = '{{ ', string $suffix = ' }}'): string
    {
        return preg_replace(
            '/:(\w+)\b/',
            sprintf('%s\1%s', $prefix, $suffix),
            $this->query,
        );
    }
}
