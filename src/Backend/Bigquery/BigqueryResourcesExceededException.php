<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery;

/**
 * import exceeded BigQuery's shuffle disk/memory limit, retrying cannot help
 */
class BigqueryResourcesExceededException extends BigqueryInputDataException
{
    public function __construct()
    {
        parent::__construct(
            'The import exceeded the maximum disk and memory limit available '
            . 'for BigQuery shuffle operations. This usually happens when an incremental load has to deduplicate '
            . 'and merge a large volume of rows against a large destination table; reduce the amount of loaded '
            . 'data, apply retention on the destination table, or switch to an append-only strategy '
            . '(e.g. "delete_where" followed by an append without primary keys).',
        );
    }
}
