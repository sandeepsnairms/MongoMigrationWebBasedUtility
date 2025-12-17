# MongoDumpRestoreCordinator - Centralized Dump/Restore Control

## Overview

The `MongoDumpRestoreCordinator` provides advanced control over MongoDB dump and restore operations with two modes:
1. **Coordinator Mode** (NEW): Centralized timer-based processing
2. **Legacy Mode** (DEFAULT): Direct parallel processing

## Coordinator Mode Architecture

### Key Components

1. **Download Manifest** (`_downloadManifest`)
   - Thread-safe dictionary tracking chunks needing dump
   - Key: `{MigrationUnitId}_{ChunkIndex}`
   - Tracks state: Pending → Processing → Completed/Failed

2. **Upload Manifest** (`_uploadManifest`)
   - Thread-safe dictionary tracking chunks needing restore
   - Same structure as download manifest
   - Automatically populated after chunks are downloaded

3. **Migration Unit Tracker** (`_activeMigrationUnits`)
   - Tracks overall progress for each migration unit
   - Monitors: Total chunks, Downloaded, Restored
   - Automatically removed when complete

4. **Process Timer**
   - Polls every 2 seconds (configurable)
   - Processes pending dumps and restores
   - Checks for completed migration units
   - Auto-stops when all work is complete

### State Machine

```
Chunk States:
┌─────────┐    Worker       ┌────────────┐    Success      ┌───────────┐
│ Pending │───Available────>│ Processing │───────────────>│ Completed │
└─────────┘                 └────────────┘                 └───────────┘
                                  │                               │
                                  │ Failure (< 3 retries)         │ Remove from
                                  └──────────────────>─────────────┘ manifest
                                  │
                                  │ Failure (≥ 3 retries)
                                  v
                             ┌────────┐
                             │ Failed │──> Trigger Controlled Pause
                             └────────┘
```

## Usage Guide

### Basic Usage - Coordinator Mode

```csharp
// Create coordinator
var coordinator = new MongoDumpRestoreCordinator(log, sourceClient, config);

// Enable coordinator mode for centralized control
coordinator.EnableCoordinatorMode(true);

// Start processing - coordinator handles everything
var result = await coordinator.StartProcessAsync(
    migrationUnitId, 
    sourceConnectionString, 
    targetConnectionString
);

// Monitor progress
var (pendingDownloads, pendingRestores, activeMUs) = coordinator.GetCoordinatorStats();
log.WriteLine($"Status: {pendingDownloads} downloads, {pendingRestores} restores pending");
```

### Pause and Resume

```csharp
// Pause gracefully - ongoing chunks complete, no new work starts
coordinator.InitiateControlledPause();

// Wait for ongoing work to complete
await Task.Delay(5000);

// Resume processing
coordinator.ResumeCoordinatedProcessing();
```

### Legacy Mode (Default)

```csharp
// Simply don't enable coordinator mode
var coordinator = new MongoDumpRestoreCordinator(log, sourceClient, config);

// Uses existing parallel infrastructure
await coordinator.StartProcessAsync(migrationUnitId, sourceConn, targetConn);
```

## Key Features

### 1. Automatic Retry Logic
- Failed chunks automatically retry up to 3 times
- Exponential backoff between retries
- Permanent failures trigger controlled pause

### 2. Resource Management
- Respects `WorkerPoolManager` capacity
- Never exceeds configured dump/restore worker limits
- Efficient worker utilization across multiple migration units

### 3. Pause/Resume Support
- Timer-based design makes pause/resume seamless
- Ongoing chunks complete before pause
- No chunks are lost during pause
- Resume picks up exactly where it left off

### 4. Progress Tracking
- Real-time tracking per migration unit
- Download and restore percentages
- Chunk-level state visibility
- Completion timestamps

### 5. Error Handling
- Thread-safe error tracking
- Detailed error logging with redacted PII
- Graceful degradation on failures
- Automatic pause on critical errors

## Implementation Details

### Timer Processing Flow

```
Timer Tick (every 2 seconds)
│
├─> Check for pause/cancellation
│   └─> If paused: Skip this tick
│
├─> ProcessPendingDumps()
│   ├─> Get available dump workers
│   ├─> Find pending download contexts (FIFO)
│   └─> Spawn worker tasks for each
│
├─> ProcessPendingRestores()
│   ├─> Get available restore workers
│   ├─> Find pending upload contexts (FIFO)
│   └─> Spawn worker tasks for each
│
├─> CheckForCompletedMigrationUnits()
│   ├─> Find MUs with all chunks done
│   ├─> Mark as complete
│   ├─> Save to database
│   └─> Remove from active tracking
│
└─> If all work complete: Stop timer
```

### Worker Task Flow

```
ProcessChunkForDownload()
│
├─> Acquire worker slot (blocks if pool full)
├─> Execute dump operation
├─> On Success:
│   ├─> Mark chunk as downloaded
│   ├─> Update tracker
│   ├─> Remove from download manifest
│   └─> Add to restore manifest
├─> On Failure:
│   ├─> Increment retry count
│   ├─> If < 3 retries: Reset to Pending
│   └─> If ≥ 3 retries: Mark Failed, trigger pause
└─> Always: Release worker slot
```

## Configuration

### Timer Interval
```csharp
private readonly int _timerIntervalMs = 2000; // Check every 2 seconds
```
Adjust this value to control how often the coordinator checks for work.

### Retry Limits
```csharp
const int MaxRetries = 3;
```
Located in `HandleDownloadFailure` and `HandleRestoreFailure` methods.

## Monitoring and Diagnostics

### Check Coordinator Status
```csharp
if (coordinator.IsCoordinatorModeActive)
{
    var (downloads, restores, mus) = coordinator.GetCoordinatorStats();
    Console.WriteLine($"Active: {mus} MUs, Pending: {downloads} dumps, {restores} restores");
}
```

### Log Levels
The coordinator uses these log types:
- `LogType.Debug`: Timer ticks, worker spawning, detailed state changes
- `LogType.Info`: Migration unit completion, mode changes
- `LogType.Warning`: Retry attempts, recoverable errors
- `LogType.Error`: Permanent failures, critical errors

## Comparison: Coordinator vs Legacy Mode

| Feature | Coordinator Mode | Legacy Mode |
|---------|-----------------|-------------|
| **Resource Management** | Centralized via timer | Direct worker spawning |
| **Retry Logic** | Built-in, automatic | Manual in calling code |
| **Pause/Resume** | Seamless | Requires careful coordination |
| **Multi-MU Support** | Unified processing | Per-MU processing |
| **Progress Tracking** | Centralized manifests | Per-MU tracking |
| **Worker Utilization** | Automatic optimization | Fixed per operation |
| **Error Handling** | Automatic retry & pause | Manual handling |

## Best Practices

1. **Enable Coordinator for Complex Jobs**
   - Multiple migration units
   - Need for pause/resume
   - Unreliable network conditions

2. **Use Legacy Mode for Simple Jobs**
   - Single migration unit
   - Stable environment
   - Maximum performance

3. **Monitor Progress**
   - Call `GetCoordinatorStats()` periodically
   - Log coordinator state changes
   - Track completion times

4. **Handle Failures Gracefully**
   - Monitor controlled pause triggers
   - Investigate failed chunks
   - Resume after fixing issues

5. **Tune Performance**
   - Adjust timer interval for workload
   - Configure worker pool sizes
   - Monitor resource utilization

## Troubleshooting

### Coordinator Not Processing
- Check if timer is enabled: Look for "Started coordination timer" log
- Verify manifests have pending items: Use `GetCoordinatorStats()`
- Check for pause state: `MigrationJobContext.ControlledPauseRequested`

### Chunks Stuck in Processing
- Check for worker task exceptions in logs
- Verify worker pool capacity: `_dumpPool.CurrentAvailable`
- Look for deadlocks or long-running operations

### Frequent Retries
- Check network connectivity
- Verify MongoDB server health
- Review error messages in logs
- Consider increasing retry limits

## Future Enhancements

Potential improvements for future versions:
1. Configurable retry strategies (exponential backoff, jitter)
2. Priority-based chunk processing
3. Adaptive timer intervals based on workload
4. Dead letter queue for permanently failed chunks
5. Metrics and telemetry integration
6. Distributed coordinator for multi-node processing

## Migration from Legacy to Coordinator

To migrate existing code to use coordinator mode:

### Before (Legacy)
```csharp
var processor = new MongoDumpRestoreCordinator(log, client, config);
await processor.StartProcessAsync(muId, source, target);
```

### After (Coordinator)
```csharp
var processor = new MongoDumpRestoreCordinator(log, client, config);
processor.EnableCoordinatorMode(true);  // Add this line
await processor.StartProcessAsync(muId, source, target);
```

That's it! The coordinator is fully backward compatible.

## Support and Feedback

For issues or questions about the coordinator:
1. Check logs for detailed state information
2. Use `GetCoordinatorStats()` for current state
3. Review this documentation for common scenarios
4. File issues in the repository with logs and context
