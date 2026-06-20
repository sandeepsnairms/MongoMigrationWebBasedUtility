# Stand-alone harness that mirrors CalculatePartitioningStrategy + SamplePartitioner
# constants exactly. Runs a fixed scenario matrix so we can sanity-check the planner
# against the worked-example tables in docs/PartitionerMath.md.

# --- Constants (mirror SamplePartitioner) -----------------------------------
$SampleOversampleFactor       = 10
$MinDocsPerSegment            = 10000
$MinDocsPerSegmentLarge       = 100000
$MinDocsPerChunk              = 100000
$MinDocsPerChunkLarge         = 1000000
$LargeCollectionThreshold     = 100000000
$DriverSubRangeMultiplier     = 10
$DefaultParallelThreads       = 40                # MaxSegments
function Get-MinDocsPerSegment([long]$docs) { if ($docs -gt $LargeCollectionThreshold) { $MinDocsPerSegmentLarge } else { $MinDocsPerSegment } }
function Get-MinDocsPerChunk  ([long]$docs) { if ($docs -gt $LargeCollectionThreshold) { $MinDocsPerChunkLarge  } else { $MinDocsPerChunk  } }
function Get-MaxSamples([bool]$hasMatch)     { if ($hasMatch) { 3000 } else { 300000 } }

# --- Planner (mirrors CalculatePartitioningStrategy) ------------------------
function Invoke-Planner {
    param(
        [long]$DocumentCount,
        [bool]$UseSampleCommand,
        [bool]$IsMongoDriver,
        [bool]$HasMatchBeforeSample,
        [int] $MaxSegments     = $DefaultParallelThreads
    )

    if ($DocumentCount -lt 1000000) {
        return [pscustomobject]@{
            Chunks       = 1
            Segments     = 1
            DocsPerChunk = $DocumentCount
            DocsPerSeg   = $DocumentCount
            RawSample    = 0
            FinalSample  = 0
        }
    }

    # Step 1: dump sub-ranges.
    if ($UseSampleCommand) {
        $dumpSubRanges = [long](Get-MaxSamples $HasMatchBeforeSample)
    } else {
        # Non-sample (analytical) seed = docCount / GetMinDocsPerChunk(docCount).
        # Same chunk-floor the sample path bottoms out at, so all four partitioners
        # produce the same chunk count for a given docCount.
        $dumpSubRanges = [long][Math]::Max(1, [long]($DocumentCount / (Get-MinDocsPerChunk $DocumentCount)))
    }

    # Step 2: driver multiplier (dropped when sample+$match).
    $capSampleAbsolute = $UseSampleCommand -and $HasMatchBeforeSample
    if ($IsMongoDriver -and -not $capSampleAbsolute) {
        $desiredSubRanges = $dumpSubRanges * $DriverSubRangeMultiplier
    } else {
        $desiredSubRanges = $dumpSubRanges
    }

    # Step 3: 5% cap on $sample.
    $subRangesActual = $desiredSubRanges
    if ($UseSampleCommand) {
        $sampleSizeCap = [Math]::Max(1, [long]($DocumentCount / 20))
        $subRangeCap   = [Math]::Max(1, [long]($sampleSizeCap / $SampleOversampleFactor))
        if ($subRangesActual -gt $subRangeCap) { $subRangesActual = $subRangeCap }
    }

    # Step 4: chunks = subRanges (dump) or subRanges / MaxSegments (driver).
    if ($IsMongoDriver) {
        $totalChunks = [int][Math]::Max(1, [Math]::Ceiling($subRangesActual / [double]$MaxSegments))
    } else {
        $totalChunks = [int][Math]::Max(1, $subRangesActual)
    }

    # Step 5: chunk floor.
    $perChunkFloor = [long](Get-MinDocsPerChunk $DocumentCount)
    if ($IsMongoDriver) {
        $perChunkFloorForFullSegments = [long]$MaxSegments * (Get-MinDocsPerSegment $DocumentCount)
        if ($perChunkFloorForFullSegments -gt $perChunkFloor) { $perChunkFloor = $perChunkFloorForFullSegments }
    }
    $maxChunksByMinDocs = [long][Math]::Max(1, [long]($DocumentCount / $perChunkFloor))
    if ($totalChunks -gt $maxChunksByMinDocs) { $totalChunks = [int]$maxChunksByMinDocs }

    $docsPerChunk = [long]($DocumentCount / [Math]::Max(1, $totalChunks))

    # Segment count (driver only); dump always = 1.
    if ($IsMongoDriver) {
        $segments = [int][Math]::Min($MaxSegments, [Math]::Max(1, [long]($docsPerChunk / (Get-MinDocsPerSegment $DocumentCount))))
    } else {
        $segments = 1
    }

    $docsPerSeg = [long]($docsPerChunk / [Math]::Max(1, $segments))

    # Raw + final $sample size (only meaningful when UseSampleCommand).
    if ($UseSampleCommand) {
        $rawSample = [long]$totalChunks * [long]$segments * [long]$SampleOversampleFactor
        if ($HasMatchBeforeSample -and $rawSample -gt 3000) {
            $finalSample = 3000
        } else {
            $finalSample = $rawSample
        }
    } else {
        $rawSample   = 0
        $finalSample = 0
    }

    return [pscustomobject]@{
        Chunks       = $totalChunks
        Segments     = $segments
        DocsPerChunk = $docsPerChunk
        DocsPerSeg   = $docsPerSeg
        RawSample    = $rawSample
        FinalSample  = $finalSample
    }
}

# --- Scenario matrix --------------------------------------------------------
# 5 doc counts x 2 job types x { sample-no-match, sample-with-match, analytical } = 30 cases.
$docCounts = @(
    @{ Label = '1M';   Docs = 1000000    },
    @{ Label = '10M';  Docs = 10000000   },
    @{ Label = '100M'; Docs = 100000000  },
    @{ Label = '1B';   Docs = 1000000000 },
    @{ Label = '2B';   Docs = 2000000000 }
)

$rows = New-Object System.Collections.Generic.List[object]
foreach ($dc in $docCounts) {
    foreach ($job in 'Dump','Driver') {
        $isDriver = ($job -eq 'Driver')

        # Sample no-match
        $r = Invoke-Planner -DocumentCount $dc.Docs `
                            -UseSampleCommand $true -IsMongoDriver $isDriver `
                            -HasMatchBeforeSample $false
        $rows.Add([pscustomobject]@{
            Docs = $dc.Label; Job = $job; Partitioner = 'Sample/noMatch'
            Chunks = $r.Chunks; Segments = $r.Segments
            DocsPerChunk = '{0:N0}' -f $r.DocsPerChunk
            DocsPerSeg   = '{0:N0}' -f $r.DocsPerSeg
            RawSample    = '{0:N0}' -f $r.RawSample
            FinalSample  = '{0:N0}' -f $r.FinalSample
        }) | Out-Null

        # Sample with-match
        $r = Invoke-Planner -DocumentCount $dc.Docs `
                            -UseSampleCommand $true -IsMongoDriver $isDriver `
                            -HasMatchBeforeSample $true
        $rows.Add([pscustomobject]@{
            Docs = $dc.Label; Job = $job; Partitioner = 'Sample/match'
            Chunks = $r.Chunks; Segments = $r.Segments
            DocsPerChunk = '{0:N0}' -f $r.DocsPerChunk
            DocsPerSeg   = '{0:N0}' -f $r.DocsPerSeg
            RawSample    = '{0:N0}' -f $r.RawSample
            FinalSample  = '{0:N0}' -f $r.FinalSample
        }) | Out-Null

        # Analytical (UseTimeBoundaries / UseAdjustedTimeBoundaries / UsePagination).
        # HasMatchBeforeSample is irrelevant for the non-sample path -- chunk math is
        # purely doc-count driven now.
        $r = Invoke-Planner -DocumentCount $dc.Docs `
                            -UseSampleCommand $false -IsMongoDriver $isDriver `
                            -HasMatchBeforeSample $false
        $rows.Add([pscustomobject]@{
            Docs = $dc.Label; Job = $job; Partitioner = 'Analytical'
            Chunks = $r.Chunks; Segments = $r.Segments
            DocsPerChunk = '{0:N0}' -f $r.DocsPerChunk
            DocsPerSeg   = '{0:N0}' -f $r.DocsPerSeg
            RawSample    = '-'
            FinalSample  = '-'
        }) | Out-Null
    }
}

$rows | Format-Table -AutoSize
