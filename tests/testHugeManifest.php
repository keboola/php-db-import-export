<?php

declare(strict_types=1);

/**
 * Test huge manifest 500k files
 */

use Tests\Keboola\Db\ImportExport\HugeManifest;

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', '1');
error_reporting(E_ALL);

$basedir = dirname(__DIR__);

require_once $basedir . '/vendor/autoload.php';
require_once 'HugeManifest.php';

$test = new HugeManifest(
    (string) getenv('ABS_ACCOUNT_NAME'),
    (string) getenv('ABS_CONTAINER_NAME')
);
$test->run();
