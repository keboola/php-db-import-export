<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Helper;

class QueryTemplateCollection
{
    private const KEY_TEMPLATE = 'template';
    private const KEY_NAME = 'name';

    /** @var array{template: QueryTemplate, name: string} */
    static private array $items;

    public static function add(QueryTemplate $template, string $name = ''): void
    {
        self::$items[] = [
            self::KEY_NAME => $name,
            self::KEY_TEMPLATE => $template,
        ];
    }

    public static function clear(): void
    {
        self::$items = [];
    }

    public static function getAll($withParamsValues = false): array
    {
        if ($withParamsValues === false) {
            foreach (self::$items as &$item) {
                // clear params from query template
                $item[self::KEY_TEMPLATE]->clearParamsValues();
            }
        }
        return self::$items;
    }
}
