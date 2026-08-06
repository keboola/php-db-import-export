<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery;

use Throwable;

/**
 * import exceeded BigQuery's shuffle disk/memory limit, retrying cannot help
 */
final class BigqueryResourcesExceededException extends BigqueryInputDataException
{
    private const MESSAGE = 'The import exceeded the maximum disk and memory limit available '
        . 'for BigQuery shuffle operations. This usually happens when an incremental load has to deduplicate '
        . 'and merge a large volume of rows against a large destination table; reduce the amount of loaded '
        . 'data, apply retention on the destination table, or switch to an append-only strategy '
        . '(e.g. "delete_where" followed by an append without primary keys).';

    public function __construct(string $bigqueryMessage, ?Throwable $previous = null)
    {
        parent::__construct(self::MESSAGE . ' ' . $bigqueryMessage, 0, $previous);
    }
}
