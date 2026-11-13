<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

/**
 * Where a resolved import method came from; reported in import telemetry.
 */
enum ImportMethodSource: string
{
    case REQUEST = 'request';
    case PROJECT_FEATURE = 'projectFeature';
    case DEFAULT = 'default';
}
