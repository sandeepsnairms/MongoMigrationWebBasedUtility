# MongoDumpRestoreCordinator - Quick Reference Guide

## Quick Start

### Enable Coordinator Mode
```csharp
var coordinator = new MongoDumpRestoreCordinator(log, sourceClient, config);
coordinator.EnableCoordinatorMode(true);
await coordinator.StartProcessAsync(muId, sourceConn, targetConn);
```

### Check Status
```csharp
var (downloads, restores, mus) = coordinator.GetCoordinatorStats();
```

### Pause/Resume
```csharp
coordinator.InitiateControlledPause();    // Pause
coordinator.ResumeCoordinatedProcessing(); // Resume
```

## Key Classes & Enums

### DumpRestoreProcessContext
Tracks individual chunk processing state.
```csharp
class DumpRestoreProcessContext
{
    string Id;              // "{MUId}_{ChunkIndex}"
    MigrationUnit MU;       // Parent migration unit
    int ChunkIndex;         // 0-based chunk index
    ProcessState State;     // Current state
    DateTime QueuedAt;      // When added to manifest
    DateTime? StartedAt;    // When processing started
    DateTime? CompletedAt;  // When completed
    int RetryCount;         // Number of retries
    Exception? LastError;   // Last error encountered
}
```

### ProcessState Enum
```csharp
enum ProcessState
{
    Pending,    // Waiting to be processed
    Processing, // Currently being processed
    Completed,  // Successfully completed
    Failed      // Failed after retries
}
```

### MigrationUnitTracker
Tracks overall progress for a migration unit.
```csharp
class MigrationUnitTracker
{
    MigrationUnit MU;
    int TotalChunks;
    int DownloadedChunks;
    int RestoredChunks;
    DateTime AddedAt;
    bool AllDownloadsCompleted;  // Property
    bool AllRestoresCompleted;   // Property
}
```

## Core Methods

### Public API

| Method | Purpose | Usage |
|--------|---------|-------|
| `EnableCoordinatorMode(bool)` | Enable/disable coordinator | `coordinator.EnableCoordinatorMode(true)` |
| `IsCoordinatorModeActive` | Check if coordinator active | `if (coordinator.IsCoordinatorModeActive)` |
| `GetCoordinatorStats()` | Get current statistics | `var (d, r, m) = GetCoordinatorStats()` |
| `ResumeCoordinatedProcessing()` | Resume after pause | `coordinator.ResumeCoordinatedProcessing()` |
| `StopCoordinatedProcessing()` | Stop and cleanup | Called automatically on dispose |

### Internal Coordination

| Method | Purpose | Trigger |
|--------|---------|---------|
| `OnTimerTick()` | Main coordination loop | Timer every 2s |
| `ProcessPendingDumps()` | Process download queue | Timer tick |
| `ProcessPendingRestores()` | Process restore queue | Timer tick |
| `ProcessChunkForDownload()` | Execute single dump | Worker task |
| `ProcessChunkForRestore()` | Execute single restore | Worker task |
| `CheckForCompletedMigrationUnits()` | Finalize completed MUs | Timer tick |

### Manifest Management

| Method | Purpose |
|--------|---------|
| `PrepareDownloadList(MU)` | Add chunks to download manifest |
| `PrepareRestoreList(MU)` | Add chunks to restore manifest |
| `UpdateMigrationUnitTracker()` | Update progress counters |
| `StartCoordinatedProcess(MU)` | Initialize MU for coordination |

## Data Flow

### Chunk Lifecycle
```
1. MU Added
   └─> StartCoordinatedProcess()
       ├─> PrepareDownloadList() → _downloadManifest
       └─> PrepareRestoreList() → _uploadManifest (if already dumped)

2. Timer Tick
   └─> ProcessPendingDumps()
       └─> For each pending download context:
           └─> ProcessChunkForDownload()
               ├─> Execute dump
               ├─> On Success:
               │   ├─> Remove from _downloadManifest
               │   └─> PrepareRestoreList() → Add to _uploadManifest
               └─> On Failure: Retry or mark failed

3. Timer Tick
   └─> ProcessPendingRestores()
       └─> For each pending restore context:
           └─> ProcessChunkForRestore()
               ├─> Execute restore
               └─> On Success: Remove from _uploadManifest

4. Timer Tick
   └─> CheckForCompletedMigrationUnits()
       └─> If MU complete: Finalize and remove from _activeMigrationUnits
```

## Thread Safety

### Concurrent Collections Used
- `ConcurrentDictionary<string, DumpRestoreProcessContext> _downloadManifest`
- `ConcurrentDictionary<string, DumpRestoreProcessContext> _uploadManifest`
- `ConcurrentDictionary<string, MigrationUnitTracker> _activeMigrationUnits`

### Locks Used
- `_timerLock`: Prevents re-entrant timer ticks
- `_pidLock`: Thread-safe PID tracking
- `_chunkUpdateLock`: Chunk status updates
- `_workerTaskLock`: Worker task list access

### Worker Pool Integration
- Acquires slots via `WorkerPoolManager.TryAcquire()`
- Releases via `WorkerPoolManager.Release()`
- Respects configured limits automatically

## Configuration Points

### Timer Settings
```csharp
private readonly int _timerIntervalMs = 2000;  // 2 seconds
```
Located in field declaration. Adjust for different polling frequencies.

### Retry Configuration
```csharp
const int MaxRetries = 3;
```
Located in `HandleDownloadFailure()` and `HandleRestoreFailure()`.

### Worker Pool Sizes
Configured via MigrationJobContext:
- `MaxParallelDumpProcesses`
- `MaxParallelRestoreProcesses`

## Common Patterns

### Check Coordinator Health
```csharp
if (coordinator.IsCoordinatorModeActive)
{
    var (downloads, restores, mus) = coordinator.GetCoordinatorStats();
    
    if (downloads == 0 && restores == 0 && mus == 0)
        Console.WriteLine("All work complete!");
    else
        Console.WriteLine($"{downloads + restores} chunks pending across {mus} MUs");
}
```

### Monitor Progress
```csharp
var timer = new System.Timers.Timer(5000);
timer.Elapsed += (s, e) =>
{
    var (d, r, m) = coordinator.GetCoordinatorStats();
    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {d} dumps, {r} restores, {m} MUs");
};
timer.Start();
```

### Graceful Shutdown
```csharp
// Pause to let ongoing work complete
coordinator.InitiateControlledPause();
await Task.Delay(5000);

// Stop coordinator
coordinator.StopProcessing();
```

## Error Handling

### Automatic Retry Flow
```
Attempt 1 fails → State = Pending (retry 1)
Attempt 2 fails → State = Pending (retry 2)
Attempt 3 fails → State = Pending (retry 3)
Attempt 4 fails → State = Failed, Trigger Controlled Pause
```

### Manual Intervention After Failure
```csharp
// After fixing underlying issue:
coordinator.ResumeCoordinatedProcessing();
// Failed chunks will retry from Pending state
```

## Performance Tips

1. **Tune Timer Interval**: Lower for faster polling, higher to reduce overhead
2. **Adjust Worker Pool Sizes**: Balance between parallelism and resource usage
3. **Monitor Queue Depths**: Large queues may indicate bottlenecks
4. **Use Appropriate Log Levels**: Debug logs add overhead

## Troubleshooting Checklist

- [ ] Is coordinator mode enabled? Check `IsCoordinatorModeActive`
- [ ] Is timer running? Look for "Started coordination timer" log
- [ ] Are manifests populated? Use `GetCoordinatorStats()`
- [ ] Is job paused? Check `MigrationJobContext.ControlledPauseRequested`
- [ ] Are workers available? Check pool capacity logs
- [ ] Are there errors? Review logs for exceptions
- [ ] Are chunks stuck? Look for "Processing" state chunks

## Integration Points

### With DumpRestoreProcessor
- Inherits all dump/restore logic
- Adds optional coordinator layer
- Fully backward compatible

### With WorkerPoolManager
- Respects configured limits
- Acquires/releases slots properly
- Integrates with WorkerPoolCoordinator

### With MigrationJobContext
- Reads pause state
- Updates progress percentages
- Saves migration unit state

## Testing Recommendations

1. **Single Chunk**: Test basic dump→restore flow
2. **Multiple Chunks**: Test parallel processing
3. **Pause During Dump**: Verify graceful pause
4. **Pause During Restore**: Verify state preservation
5. **Resume After Pause**: Verify continuation
6. **Failure Scenarios**: Test retry logic
7. **Multiple MUs**: Test concurrent coordination
8. **Resource Exhaustion**: Test with limited workers
9. **Long-Running Jobs**: Test timer stability
10. **Cancellation**: Test cleanup on cancel

## Version History

- **v0.9.4a**: Initial coordinator implementation
  - Timer-based centralized processing
  - Automatic retry logic
  - Seamless pause/resume
  - Thread-safe manifests
  - Progress tracking per MU
