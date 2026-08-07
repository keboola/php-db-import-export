<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery;

use Google\Cloud\BigQuery\Exception\JobException;
use Google\Cloud\Core\Exception\ServiceException;
use Keboola\Db\Import\Exception;
use Throwable;

class BigqueryException extends Exception
{
    private const MAX_MESSAGES_IN_ERROR_MESSAGE = 10;

    private const RESOURCES_EXCEEDED_MESSAGE = 'Reduce the loaded volume, apply retention on the destination '
        . 'table, or use an append-only load.';

    public static function covertException(JobException|ServiceException $e): Throwable
    {
        if ($e instanceof ServiceException) {
            if (preg_match('/.*Required field .+ cannot be null.*/m', $e->getMessage(), $output_array) === 1) {
                return new BigqueryInputDataException($e->getMessage());
            }
            if (preg_match('/Bad \w+ value/m', $e->getMessage()) === 1) {
                return new BigqueryInputDataException($e->getMessage());
            }
            if (self::isResourcesExceededError($e->getMessage())) {
                return new BigqueryResourcesExceededException(
                    self::extractErrorMessage($e->getMessage()) . ' ' . self::RESOURCES_EXCEEDED_MESSAGE,
                    0,
                    $e,
                );
            }
            return new self($e->getMessage());
        }
        return $e;
    }

    /**
     * @param array{
     *     status: array{
     *         errorResult?: array{message: string},
     *         errors?: array<int, array{message: string, reason: string}>,
     *     },
     *     jobReference: array{jobId: string},
     * } $jobInfo
     */
    public static function createExceptionFromJobResult(array $jobInfo): Throwable
    {
        $errorMessage = $jobInfo['status']['errorResult']['message'] ?? 'Unknown error';
        $jobErrors = $jobInfo['status']['errors'] ?? [];

        // detecting missing required column. Record with `Required column value is missing` substring contains
        // much better information for enduser
        foreach ($jobErrors as $error) {
            if (str_contains($error['message'], 'Required column value is missing')) {
                $errorMessage = $error['message'];
                return new BigqueryInputDataException($errorMessage);
            }
        }

        $filteredJobErrors = array_filter(
            $jobErrors,
            function ($error) use ($jobInfo) {
                // the errorResult is the first in list of errors as well
                return $error['message'] !== ($jobInfo['status']['errorResult']['message'] ?? '');
            },
        );
        $countOfErrors = count($filteredJobErrors);
        $parsingErrors = array_filter(
            $filteredJobErrors,
            function ($error) {
                return self::isUserError($error['message'], $error['reason']);
            },
        );
        if (count($parsingErrors) > 0) {
            // filter parsing errors
            $areExtraErrors = count($parsingErrors) !== $countOfErrors;
            return new BigqueryInputDataException(
                self::getErrorMessageForErrorList($parsingErrors, $areExtraErrors, $jobInfo['jobReference']['jobId']),
            );
        }

        // last resort: the per-row parse errors above are more actionable, so they keep precedence
        foreach ($jobErrors as $error) {
            if (self::isResourcesExceededError($error['message'])
                || (array_key_exists('reason', $error) && $error['reason'] === 'resourcesExceeded')
            ) {
                return new BigqueryResourcesExceededException(sprintf(
                    '%s %s For more information check job "%s" in Google Cloud Console.',
                    $error['message'],
                    self::RESOURCES_EXCEEDED_MESSAGE,
                    $jobInfo['jobReference']['jobId'],
                ));
            }
        }

        return new self($errorMessage);
    }

    /**
     * ServiceException carries the raw JSON response body, the human sentence is in `error.message`.
     */
    private static function extractErrorMessage(string $message): string
    {
        $decoded = json_decode($message, true);
        if (is_array($decoded)
            && is_array($decoded['error'] ?? null)
            && is_string($decoded['error']['message'] ?? null)
            && $decoded['error']['message'] !== ''
        ) {
            return $decoded['error']['message'];
        }

        return $message;
    }

    private static function isResourcesExceededError(string $message): bool
    {
        return str_contains($message, 'resourcesExceeded')
            || str_contains($message, 'maximum disk and memory limit available for shuffle operations');
    }

    private static function isUserError(string $message, string $reason): bool
    {
        if (str_contains($message, 'Required column value is missing')) {
            return true;
        }
        if (str_contains($message, 'Could not parse')) {
            return true;
        }
        return false;
    }

    /**
     * @param array<int, array{message: string, reason: string}> $parsingErrors
     */
    private static function getErrorMessageForErrorList(
        array $parsingErrors,
        bool $areExtraErrors,
        string $jobId,
    ): string {
        $count = count($parsingErrors);
        if ($count > self::MAX_MESSAGES_IN_ERROR_MESSAGE) {
            return sprintf(
                'There were too many errors during the import. For more information check job "%s"'
                . 'in Google Cloud Console.',
                $jobId,
            );
        }

        $messages = array_map(
            function ($error): string {
                return $error['message'];
            },
            $parsingErrors,
        );
        $message = implode(
            PHP_EOL,
            $messages,
        );
        if ($areExtraErrors) {
            $message .= PHP_EOL . sprintf(
                'There were additional errors during the import. For more information check job "%s"'
                . 'in Google Cloud Console.',
                $jobId,
            );
        }

        return $message;
    }
}
