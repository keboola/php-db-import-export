<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport;

use Keboola\Db\ImportExport\Backend\Helper\BackendHelper;

class ExportOptions implements ExportOptionsInterface
{
    public const MANIFEST_AUTOGENERATED = true;
    public const MANIFEST_SKIP = false;

    private bool $isCompressed;

    private string $exportId;

    private bool $generateManifest;

    public function __construct(
        bool $isCompressed = false,
        bool $generateManifest = self::MANIFEST_SKIP
    ) {
        $this->isCompressed = $isCompressed;
        $this->exportId = BackendHelper::generateRandomExportPrefix();
        $this->generateManifest = $generateManifest;
    }

    public function getExportId(): string
    {
        return $this->exportId;
    }

    public function isCompressed(): bool
    {
        return $this->isCompressed;
    }

    public function generateManifest(): bool
    {
        return $this->generateManifest === self::MANIFEST_AUTOGENERATED;
    }
}
