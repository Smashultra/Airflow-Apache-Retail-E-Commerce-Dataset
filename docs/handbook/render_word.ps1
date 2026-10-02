param(
    [string]$DocumentPath,
    [string]$PdfPath,
    [string]$StatsPath
)
$ErrorActionPreference = 'Stop'
$resolvedDocument = (Resolve-Path -LiteralPath $DocumentPath).Path
$resolvedPdf = [System.IO.Path]::GetFullPath($PdfPath)
$resolvedStats = [System.IO.Path]::GetFullPath($StatsPath)
$wordApplication = $null
$openedDocument = $null
$documentsBefore = 0
$previousAlerts = $null
$previousSecurity = $null
try {
    $wordApplication = New-Object -ComObject Word.Application
    $documentsBefore = $wordApplication.Documents.Count
    $previousAlerts = $wordApplication.DisplayAlerts
    $previousSecurity = $wordApplication.AutomationSecurity
    if ($documentsBefore -eq 0) { $wordApplication.Visible = $false }
    $wordApplication.DisplayAlerts = 0
    $wordApplication.AutomationSecurity = 3
    $openedDocument = $wordApplication.Documents.Open($resolvedDocument, $false, $false)
    $openedDocument.Repaginate()
    $openedDocument.Fields.Update() | Out-Null
    foreach ($toc in $openedDocument.TablesOfContents) { $toc.Update() }
    $openedDocument.Repaginate()
    foreach ($toc in $openedDocument.TablesOfContents) { $toc.UpdatePageNumbers() }
    $openedDocument.Save()
    $openedDocument.ExportAsFixedFormat($resolvedPdf, 17)
    $stats = [ordered]@{
        pages = $openedDocument.ComputeStatistics(2)
        words = $openedDocument.ComputeStatistics(0)
        tables = $openedDocument.Tables.Count
        figures = $openedDocument.InlineShapes.Count
        toc_count = $openedDocument.TablesOfContents.Count
        word_version = $wordApplication.Version
        docx = $resolvedDocument
        pdf = $resolvedPdf
    }
    $stats | ConvertTo-Json | Set-Content -LiteralPath $resolvedStats -Encoding UTF8
    $stats | ConvertTo-Json
}
finally {
    if ($null -ne $openedDocument) {
        $openedDocument.Close(0)
        [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($openedDocument)
    }
    if ($null -ne $wordApplication) {
        if ($documentsBefore -eq 0 -and $wordApplication.Documents.Count -eq 0) {
            $wordApplication.Quit()
        }
        elseif ($null -ne $previousAlerts) {
            $wordApplication.DisplayAlerts = $previousAlerts
            $wordApplication.AutomationSecurity = $previousSecurity
        }
        [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($wordApplication)
    }
    [GC]::Collect()
    [GC]::WaitForPendingFinalizers()
}
