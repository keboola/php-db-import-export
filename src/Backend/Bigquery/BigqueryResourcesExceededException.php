<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery;

/**
 * import exceeded BigQuery's shuffle disk/memory limit, retrying cannot help
 */
final class BigqueryResourcesExceededException extends BigqueryInputDataException
{
}
