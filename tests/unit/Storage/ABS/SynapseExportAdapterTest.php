<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\ABS;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\PDOSqlsrv;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseExportOptions;
use Keboola\Db\ImportExport\Storage\ABS\DestinationFile;
use PHPUnit\Framework\MockObject\MockObject;
use Keboola\Db\ImportExport\Storage;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SynapseExportAdapterTest extends BaseTestCase
{
    public function testGetCopyCommandQuery(): void
    {
        $destination = $this->getDestinationMock();
        $options = $this->getOptionsMock(true);
        $conn = $this->getConnectionMock();

        $conn->expects($this->once())->method('executeQuery')->with(
            <<<EOT
CREATE EXTERNAL TABLE [random_export_id_StorageExternalTable]
WITH 
(
    LOCATION='/path/to/export',
    DATA_SOURCE = [random_export_id_StorageSource],
    FILE_FORMAT = [random_export_id_StorageFileFormat]
)
AS
SELECT * FROM "schema"."table" WHERE id = ?
EOT
        );

        $conn->expects($this->exactly(7))->method('exec')->withConsecutive(
            [
                <<<EOT
CREATE DATABASE SCOPED CREDENTIAL [random_export_id_StorageCredential]
WITH
    IDENTITY = 'user',
    SECRET = 'masterKey'
;
EOT
                ,
            ],
            [
                <<<EOT
CREATE EXTERNAL DATA SOURCE [random_export_id_StorageSource]
WITH 
(
    TYPE = HADOOP,
    LOCATION = 'wasbs://container@account.blob.core.windows.net/',
    CREDENTIAL = [random_export_id_StorageCredential]
);
EOT
                ,
            ],
            [
                <<<EOT
CREATE EXTERNAL FILE FORMAT [random_export_id_StorageFileFormat]
WITH
(
    FORMAT_TYPE = DelimitedText,
    FORMAT_OPTIONS 
    (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        DATE_FORMAT = 'yyyy-MM-dd HH:mm:ss',
        USE_TYPE_DEFAULT = FALSE
    )
    ,DATA_COMPRESSION = 'org.apache.hadoop.io.compress.GzipCodec'
);
EOT
                ,
            ],
            [
                <<<EOT
DROP EXTERNAL TABLE [random_export_id_StorageExternalTable]
EOT
                ,
            ],
            [
                <<<EOT
DROP EXTERNAL FILE FORMAT [random_export_id_StorageFileFormat]
EOT
                ,
            ],
            [
                <<<EOT
DROP EXTERNAL DATA SOURCE [random_export_id_StorageSource]
EOT
                ,
            ],
            [
                <<<EOT
DROP DATABASE SCOPED CREDENTIAL [random_export_id_StorageCredential]
EOT
                ,
            ]
        );

        $source = new Storage\Synapse\SelectSource('SELECT * FROM "schema"."table" WHERE id = ?', [1]);
        $adapter = new Storage\ABS\SynapseExportAdapter($conn);

        $this->assertIsArray($adapter->runCopyCommand(
            $source,
            $destination,
            $options
        ));
    }

    private function getDestinationMock(): DestinationFile
    {
        return new DestinationFile('container', '/path/to/export', 'sasToken', 'account', 'masterKey');
    }

    /**
     * @return SynapseExportOptions|MockObject
     */
    private function getOptionsMock(bool $compressed, string $credentialsType = 'MASTER_KEY')
    {
        /** @var SynapseExportOptions|MockObject $destination */
        $options = $this->createMock(SynapseExportOptions::class);
        $options->expects($this->once())->method('isCompressed')->willReturn($compressed);
        $options->expects($this->once())->method('getExportId')->willReturn('random_export_id');
        $options->expects($this->exactly(2))->method('getExportCredentialsType')->willReturn($credentialsType);
        return $options;
    }

    /**
     * @return Connection|MockObject
     */
    private function getConnectionMock()
    {
        $driverConnectionMock = $this->createMock(PDOSqlsrv\Connection::class);
        $driverConnectionMock->expects($this->any())
            ->method('quote')
            ->willReturnCallback(function ($string) {
                return sprintf('\'%s\'', $string);
            });

        $driverMock = $this->createMock(PDOSqlsrv\Driver::class);
        $driverMock->expects($this->any())
            ->method('connect')
            ->willReturn($driverConnectionMock);

        /** @var Connection|MockObject $conn */
        $conn = $this->getMockBuilder(Connection::class)
            ->setConstructorArgs([['platform' => new SQLServer2012Platform()], $driverMock])
            ->disableOriginalClone()
            ->disableArgumentCloning()
            ->disallowMockingUnknownTypes()
            ->setMethods([
                'exec',
                'prepare',
                'query',
                'execute',
                'executeQuery',
            ])
            ->getMock();
        return $conn;
    }

    public function testRunCopyCommand(): void
    {
        $destination = $this->getDestinationMock();
        $options = $this->getOptionsMock(false);
        $conn = $this->getConnectionMock();

        $conn->expects($this->once())->method('executeQuery')->with(
            <<<EOT
CREATE EXTERNAL TABLE [random_export_id_StorageExternalTable]
WITH 
(
    LOCATION='/path/to/export',
    DATA_SOURCE = [random_export_id_StorageSource],
    FILE_FORMAT = [random_export_id_StorageFileFormat]
)
AS
SELECT * FROM [schema].[table]
EOT
        );

        $conn->expects($this->exactly(7))->method('exec')->withConsecutive(
            [
                <<<EOT
CREATE DATABASE SCOPED CREDENTIAL [random_export_id_StorageCredential]
WITH
    IDENTITY = 'user',
    SECRET = 'masterKey'
;
EOT
                ,
            ],
            [
                <<<EOT
CREATE EXTERNAL DATA SOURCE [random_export_id_StorageSource]
WITH 
(
    TYPE = HADOOP,
    LOCATION = 'wasbs://container@account.blob.core.windows.net/',
    CREDENTIAL = [random_export_id_StorageCredential]
);
EOT
                ,
            ],
            [
                <<<EOT
CREATE EXTERNAL FILE FORMAT [random_export_id_StorageFileFormat]
WITH
(
    FORMAT_TYPE = DelimitedText,
    FORMAT_OPTIONS 
    (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        DATE_FORMAT = 'yyyy-MM-dd HH:mm:ss',
        USE_TYPE_DEFAULT = FALSE
    )
    
);
EOT
                ,
            ],
            [
                <<<EOT
DROP EXTERNAL TABLE [random_export_id_StorageExternalTable]
EOT
                ,
            ],
            [
                <<<EOT
DROP EXTERNAL FILE FORMAT [random_export_id_StorageFileFormat]
EOT
                ,
            ],
            [
                <<<EOT
DROP EXTERNAL DATA SOURCE [random_export_id_StorageSource]
EOT
                ,
            ],
            [
                <<<EOT
DROP DATABASE SCOPED CREDENTIAL [random_export_id_StorageCredential]
EOT
                ,
            ]
        );

        $source = new Storage\Synapse\Table('schema', 'table');
        $adapter = new Storage\ABS\SynapseExportAdapter($conn);

        $this->assertIsArray($adapter->runCopyCommand(
            $source,
            $destination,
            $options
        ));
    }

    public function testRunCopyCommandCompressed(): void
    {
        $destination = $this->getDestinationMock();
        $options = $this->getOptionsMock(true);
        $conn = $this->getConnectionMock();

        $conn->expects($this->once())->method('executeQuery')->with(
            <<<EOT
CREATE EXTERNAL TABLE [random_export_id_StorageExternalTable]
WITH 
(
    LOCATION='/path/to/export',
    DATA_SOURCE = [random_export_id_StorageSource],
    FILE_FORMAT = [random_export_id_StorageFileFormat]
)
AS
SELECT * FROM [schema].[table]
EOT
        );

        $conn->expects($this->exactly(7))->method('exec')->withConsecutive(
            [
                <<<EOT
CREATE DATABASE SCOPED CREDENTIAL [random_export_id_StorageCredential]
WITH
    IDENTITY = 'user',
    SECRET = 'masterKey'
;
EOT
                ,
            ],
            [
                <<<EOT
CREATE EXTERNAL DATA SOURCE [random_export_id_StorageSource]
WITH 
(
    TYPE = HADOOP,
    LOCATION = 'wasbs://container@account.blob.core.windows.net/',
    CREDENTIAL = [random_export_id_StorageCredential]
);
EOT
                ,
            ],
            [
                <<<EOT
CREATE EXTERNAL FILE FORMAT [random_export_id_StorageFileFormat]
WITH
(
    FORMAT_TYPE = DelimitedText,
    FORMAT_OPTIONS 
    (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        DATE_FORMAT = 'yyyy-MM-dd HH:mm:ss',
        USE_TYPE_DEFAULT = FALSE
    )
    ,DATA_COMPRESSION = 'org.apache.hadoop.io.compress.GzipCodec'
);
EOT
                ,
            ],
            [
                <<<EOT
DROP EXTERNAL TABLE [random_export_id_StorageExternalTable]
EOT
                ,
            ],
            [
                <<<EOT
DROP EXTERNAL FILE FORMAT [random_export_id_StorageFileFormat]
EOT
                ,
            ],
            [
                <<<EOT
DROP EXTERNAL DATA SOURCE [random_export_id_StorageSource]
EOT
                ,
            ],
            [
                <<<EOT
DROP DATABASE SCOPED CREDENTIAL [random_export_id_StorageCredential]
EOT
                ,
            ]
        );

        $source = new Storage\Synapse\Table('schema', 'table');
        $adapter = new Storage\ABS\SynapseExportAdapter($conn);

        $this->assertIsArray($adapter->runCopyCommand(
            $source,
            $destination,
            $options
        ));
    }

    public function testRunCopyCommandManagedIdentity(): void
    {
        $destination = $this->getDestinationMock();
        $options = $this->getOptionsMock(false, 'MANAGED_IDENTITY');
        $conn = $this->getConnectionMock();

        $conn->expects($this->once())->method('executeQuery')->with(
            <<<EOT
CREATE EXTERNAL TABLE [random_export_id_StorageExternalTable]
WITH 
(
    LOCATION='/path/to/export',
    DATA_SOURCE = [random_export_id_StorageSource],
    FILE_FORMAT = [random_export_id_StorageFileFormat]
)
AS
SELECT * FROM [schema].[table]
EOT
        );

        $conn->expects($this->exactly(7))->method('exec')->withConsecutive(
            [
                <<<EOT
CREATE DATABASE SCOPED CREDENTIAL [random_export_id_StorageCredential]
WITH
    IDENTITY = 'Managed Service Identity'
;
EOT
                ,
            ],
            [
                <<<EOT
CREATE EXTERNAL DATA SOURCE [random_export_id_StorageSource]
WITH 
(
    TYPE = HADOOP,
    LOCATION = 'abfss://container@account.dfs.core.windows.net/',
    CREDENTIAL = [random_export_id_StorageCredential]
);
EOT
                ,
            ],
            [
                <<<EOT
CREATE EXTERNAL FILE FORMAT [random_export_id_StorageFileFormat]
WITH
(
    FORMAT_TYPE = DelimitedText,
    FORMAT_OPTIONS 
    (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        DATE_FORMAT = 'yyyy-MM-dd HH:mm:ss',
        USE_TYPE_DEFAULT = FALSE
    )
    
);
EOT
                ,
            ],
            [
                <<<EOT
DROP EXTERNAL TABLE [random_export_id_StorageExternalTable]
EOT
                ,
            ],
            [
                <<<EOT
DROP EXTERNAL FILE FORMAT [random_export_id_StorageFileFormat]
EOT
                ,
            ],
            [
                <<<EOT
DROP EXTERNAL DATA SOURCE [random_export_id_StorageSource]
EOT
                ,
            ],
            [
                <<<EOT
DROP DATABASE SCOPED CREDENTIAL [random_export_id_StorageCredential]
EOT
                ,
            ]
        );

        $source = new Storage\Synapse\Table('schema', 'table');
        $adapter = new Storage\ABS\SynapseExportAdapter($conn);

        $this->assertIsArray($adapter->runCopyCommand(
            $source,
            $destination,
            $options
        ));
    }
}
