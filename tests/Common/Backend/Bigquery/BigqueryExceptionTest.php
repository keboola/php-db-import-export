<?php

declare(strict_types = 1);

namespace Tests\Keboola\Db\ImportExportCommon\Backend\Bigquery;

use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryException;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryInputDataException;
use PHPUnit\Framework\TestCase;

class BigqueryExceptionTest extends TestCase
{

    /**
     * @dataProvider provideJobAndExpectedError
     * @param mixed[] $job
     * @param callable(string $message): void $expectedMessagesAssertion
     */
    public function testCreateExceptionFromJobResult(array $job, callable $expectedMessagesAssertion): void
    {
        $e = BigqueryException::createExceptionFromJobResult($job);
        $this->assertInstanceOf(BigqueryInputDataException::class, $e);
        $expectedMessagesAssertion($e->getMessage());
    }

    public function provideJobAndExpectedError()
    {
        yield 'single error' => [
            [
                'kind' => 'bigquery#job',
                'etag' => '/ty8yus/A/JoTZZvf1qgFQ==',
                'id' => 'tf2-56:US.eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                'selfLink' => 'https://bigquery.googleapis.com/bigquery/v2/projects/tf2-56/jobs/eb64d133-213d-4e99-9cc0-4e37d0c18de9?location=US',
                'user_email' => 'tf2-56@617348738050.iam.gserviceaccount.com',
                'configuration' => [
                    'load' => [
                        'sourceUris' => [
                            0 => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        'schema' => [
                            'fields' => [
                                [
                                    'name' => 'id',
                                    'type' => 'INTEGER',
                                ],
                                [
                                    'name' => 'timestamp',
                                    'type' => 'TIMESTAMP',
                                ],
                            ],
                        ],
                        'destinationTable' => [
                            'projectId' => 'tf2-56',
                            'datasetId' => 'in_c_API_tests_58bf9bd5cc965e125d1c6abf53b6d291ab5c34e8',
                            'tableId' => '__temp_csvimport655f304cc39c67_90384747',
                        ],
                        'fieldDelimiter' => ',',
                        'skipLeadingRows' => 1,
                        'quote' => '"',
                        'allowQuotedNewlines' => true,
                        'sourceFormat' => 'CSV',
                        'autodetect' => false,
                        'preserveAsciiControlCharacters' => true,
                    ],
                    'labels' => [
                        'run_id' => '3118',
                    ],
                    'jobType' => 'LOAD',
                ],
                'jobReference' => [
                    'projectId' => 'tf2-56',
                    'jobId' => 'eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                    'location' => 'US',
                ],
                'statistics' => [
                    'creationTime' => '1700737120686',
                    'startTime' => '1700737121028',
                    'endTime' => '1700737123803',
                    'completionRatio' => 0,
                    'totalSlotMs' => '2172',
                    'reservation_id' => 'default-pipeline',
                ],
                'status' => [
                    'errorResult' => [
                        'reason' => 'invalid',
                        'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 1; errors: 1; max bad: 0; error percent: 0',
                    ],
                    'errors' => [
                        [
                            'reason' => 'invalid',
                            'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 1; errors: 1; max bad: 0; error percent: 0',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 2 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                    ],
                    'state' => 'DONE',
                ],
                'principal_subject' => 'serviceAccount:tf2-56@617348738050.iam.gserviceaccount.com',
                'jobCreationReason' => [
                    'code' => 'REQUESTED',
                ],
            ],
            function (string $message) {
                $this->assertStringStartsWith(
                    'CSV processing failed:'
                    . PHP_EOL
                    . 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD '
                    . 'HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 2 byte_offset_to_start_of_line: 17 column_index: 1 '
                    . 'column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File:', $message
                );
            },
        ];
        yield 'multiple errors' => [
            [
                'kind' => 'bigquery#job',
                'etag' => '/ty8yus/A/JoTZZvf1qgFQ==',
                'id' => 'tf2-56:US.eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                'selfLink' => 'https://bigquery.googleapis.com/bigquery/v2/projects/tf2-56/jobs/eb64d133-213d-4e99-9cc0-4e37d0c18de9?location=US',
                'user_email' => 'tf2-56@617348738050.iam.gserviceaccount.com',
                'configuration' => [
                    'load' => [
                        'sourceUris' => [
                            0 => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        'schema' => [
                            'fields' => [
                                [
                                    'name' => 'id',
                                    'type' => 'INTEGER',
                                ],
                                [
                                    'name' => 'timestamp',
                                    'type' => 'TIMESTAMP',
                                ],
                            ],
                        ],
                        'destinationTable' => [
                            'projectId' => 'tf2-56',
                            'datasetId' => 'in_c_API_tests_58bf9bd5cc965e125d1c6abf53b6d291ab5c34e8',
                            'tableId' => '__temp_csvimport655f304cc39c67_90384747',
                        ],
                        'fieldDelimiter' => ',',
                        'skipLeadingRows' => 1,
                        'quote' => '"',
                        'allowQuotedNewlines' => true,
                        'sourceFormat' => 'CSV',
                        'autodetect' => false,
                        'preserveAsciiControlCharacters' => true,
                    ],
                    'labels' => [
                        'run_id' => '3118',
                    ],
                    'jobType' => 'LOAD',
                ],
                'jobReference' => [
                    'projectId' => 'tf2-56',
                    'jobId' => 'eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                    'location' => 'US',
                ],
                'statistics' => [
                    'creationTime' => '1700737120686',
                    'startTime' => '1700737121028',
                    'endTime' => '1700737123803',
                    'completionRatio' => 0,
                    'totalSlotMs' => '2172',
                    'reservation_id' => 'default-pipeline',
                ],
                'status' => [
                    'errorResult' => [
                        'reason' => 'invalid',
                        'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 2; errors: 2; max bad: 0; error percent: 0',
                    ],
                    'errors' => [
                        [
                            'reason' => 'invalid',
                            'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 2; errors: 2; max bad: 0; error percent: 0',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 2 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                    ],
                    'state' => 'DONE',
                ],
                'principal_subject' => 'serviceAccount:tf2-56@617348738050.iam.gserviceaccount.com',
                'jobCreationReason' => [
                    'code' => 'REQUESTED',
                ],
            ],
            function (string $message) {
                $this->assertStringStartsWith(
                    'CSV processing failed:'
                    . PHP_EOL
                    . 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD '
                    . 'HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 2 byte_offset_to_start_of_line: 17 column_index: 1 '
                    . 'column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File:', $message
                );
                $this->assertStringContainsString(
                // different line number
                    'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD '
                    . 'HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 '
                    . 'column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File:', $message
                );
            },
        ];
        yield 'too many errors' => [
            [
                'kind' => 'bigquery#job',
                'etag' => '/ty8yus/A/JoTZZvf1qgFQ==',
                'id' => 'tf2-56:US.eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                'selfLink' => 'https://bigquery.googleapis.com/bigquery/v2/projects/tf2-56/jobs/eb64d133-213d-4e99-9cc0-4e37d0c18de9?location=US',
                'user_email' => 'tf2-56@617348738050.iam.gserviceaccount.com',
                'configuration' => [
                    'load' => [
                        'sourceUris' => [
                            0 => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        'schema' => [
                            'fields' => [
                                [
                                    'name' => 'id',
                                    'type' => 'INTEGER',
                                ],
                                [
                                    'name' => 'timestamp',
                                    'type' => 'TIMESTAMP',
                                ],
                            ],
                        ],
                        'destinationTable' => [
                            'projectId' => 'tf2-56',
                            'datasetId' => 'in_c_API_tests_58bf9bd5cc965e125d1c6abf53b6d291ab5c34e8',
                            'tableId' => '__temp_csvimport655f304cc39c67_90384747',
                        ],
                        'fieldDelimiter' => ',',
                        'skipLeadingRows' => 1,
                        'quote' => '"',
                        'allowQuotedNewlines' => true,
                        'sourceFormat' => 'CSV',
                        'autodetect' => false,
                        'preserveAsciiControlCharacters' => true,
                    ],
                    'labels' => [
                        'run_id' => '3118',
                    ],
                    'jobType' => 'LOAD',
                ],
                'jobReference' => [
                    'projectId' => 'tf2-56',
                    'jobId' => 'eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                    'location' => 'US',
                ],
                'statistics' => [
                    'creationTime' => '1700737120686',
                    'startTime' => '1700737121028',
                    'endTime' => '1700737123803',
                    'completionRatio' => 0,
                    'totalSlotMs' => '2172',
                    'reservation_id' => 'default-pipeline',
                ],
                'status' => [
                    'errorResult' => [
                        'reason' => 'invalid',
                        'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 2000; errors: 1500; max bad: 0; error percent: 0',
                    ],
                    'errors' => [
                        [
                            'reason' => 'invalid',
                            'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 2000; errors: 1500; max bad: 0; error percent: 0',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 2 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],

                    ],
                    'state' => 'DONE',
                ],
                'principal_subject' => 'serviceAccount:tf2-56@617348738050.iam.gserviceaccount.com',
                'jobCreationReason' => [
                    'code' => 'REQUESTED',
                ],
            ],
            function (string $message) {
                $this->assertStringStartsWith(
                    'CSV processing failed with too many errors (for details see job detail https://bigquery.googleapis.com/bigquery/', $message
                );

                $this->assertCount(11, explode(PHP_EOL, $message));
            },
        ];
    }
}
