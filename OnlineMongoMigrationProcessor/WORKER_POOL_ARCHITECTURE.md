# Worker Pool Architecture

## Overview
The worker pool management has been refactored from instance-based to a shared static coordinator pattern. This allows multiple `DumpRestoreProcessor` instances (one per collection) to share the same dump and restore worker pools per job.

## Architecture Components

### 1. WorkerPoolCoordinator (Static)
**Location**: `OnlineMongoMigrationProcessor/Helpers/WorkerPoolCoordinator.cs`

A static class that manages shared worker pools across all `DumpRestoreProcessor` instances.

**Key Features**:
- **Job-Based Pool Management**: Maintains separate pools for each job ID
- **Shared Resources**: Dump and restore pools are shared across all collections in a job
- **Worker Tracking**: Tracks active workers per collection with metadata (worker ID, start time, completion status)
- **Thread-Safe**: All operations are protected by a static lock object
- **Automatic Cleanup**: Provides cleanup methods to dispose resources when a job completes

**Main Methods**:
```csharp
// Pool Management
GetOrCreateDumpPool(jobId, maxWorkers)
GetOrCreateRestorePool(jobId, maxWorkers)

// Worker Registration
RegisterDumpWorker(jobId, collectionKey, workerId)
RegisterRestoreWorker(jobId, collectionKey, workerId)

// Worker Completion
MarkDumpWorkerCompleted(jobId, workerId)
MarkRestoreWorkerCompleted(jobId, workerId)

// Active Worker Counts
GetActiveDumpWorkerCount(jobId)
GetActiveRestoreWorkerCount(jobId)

// Dynamic Worker Management
AdjustDumpWorkers(jobId, newCount, spawnAction)
AdjustRestoreWorkers(jobId, newCount, spawnAction)

// Cleanup
CleanupJob(jobId)
```

### 2. DumpRestoreProcessor (Instance)
**Location**: `OnlineMongoMigrationProcessor/Processors/DumpRestoreProcessor.cs`

Each instance handles a single collection but uses shared worker pools via the coordinator.

**Key Changes**:
- **Removed Instance Fields**:
  - `_activeDumpWorkers` (List<Task<TaskResult>>)
  - `_activeRestoreWorkers` (List<Task<TaskResult>>)
  - `_workerLock` (object)

- **Added Fields**:
  - `_jobId` (string) - Job identifier for coordinator lookups

- **Updated Constructor**:
  ```csharp
  _dumpPool = WorkerPoolCoordinator.GetOrCreateDumpPool(_jobId, dumpWorkers);
  _restorePool = WorkerPoolCoordinator.GetOrCreateRestorePool(_jobId, restoreWorkers);
  ```

- **Updated Worker Management**:
  - `AdjustDumpWorkers` and `AdjustRestoreWorkers` delegate to `WorkerPoolCoordinator`
  - `ParallelDumpChunksAsync` and `ParallelRestoreChunksAsync` use local worker task lists
  - Worker registration/completion is handled via coordinator callbacks

### 3. WorkerCountHelper (Static)
**Location**: `OnlineMongoMigrationProcessor/Helpers/WorkerCountHelper.cs`

Provides utility methods for worker count calculations and validations.

**Methods**:
- `ValidateWorkerCount(currentCount, newCount, maxWorkers)`
- `CalculateOptimalConcurrency(chunkCount, maxWorkers)`
- `AdjustDumpWorkers(pool, newCount, context, spawnAction, log, cts)`
- `AdjustRestoreWorkers(pool, newCount, context, spawnAction, log, cts)`

## Data Flow

### Initialization Flow
```
Job Start
  ↓
For Each Collection:
  Create DumpRestoreProcessor(jobId, ...)
    ↓
  WorkerPoolCoordinator.GetOrCreateDumpPool(jobId, maxWorkers)
    ↓ (First call creates pool, subsequent calls return existing)
  Shared WorkerPoolManager instance
```

### Worker Spawn Flow
```
ParallelDumpChunksAsync
  ↓
For each worker:
  WorkerPoolCoordinator.RegisterDumpWorker(jobId, collectionKey, workerId)
    ↓
  Task.Run(() => DumpWorkerAsync(...))
    ↓ (On completion)
  finally: WorkerPoolCoordinator.MarkDumpWorkerCompleted(jobId, workerId)
```

### Dynamic Worker Adjustment Flow
```
AdjustDumpWorkers(newCount)
  ↓
WorkerPoolCoordinator.AdjustDumpWorkers(jobId, newCount, spawnAction)
  ↓
WorkerCountHelper.AdjustDumpWorkers(pool, newCount, context, spawnAction, ...)
  ↓
If increase needed:
  For each new worker:
    WorkerPoolCoordinator.RegisterDumpWorker(jobId, collectionKey, workerId)
      ↓
    spawnAction(workerId) // Spawns new worker task
```

## Benefits

### 1. Resource Sharing
- Multiple collections share the same worker pools per job
- More efficient resource utilization
- Better control over total parallelism across all collections

### 2. Centralized Management
- Single source of truth for worker pool state
- Easier to implement global policies (e.g., max total workers across all collections)
- Simplified debugging and monitoring

### 3. Accurate Worker Tracking
- Workers are tracked with metadata (collection, worker ID, start time, completion status)
- Automatic cleanup of completed workers
- Prevents race conditions between worker spawning and worker completion

### 4. Better Isolation
- Each `DumpRestoreProcessor` focuses on collection-specific logic
- Worker pool management is centralized in the coordinator
- Clean separation of concerns

## Thread Safety

All shared state in `WorkerPoolCoordinator` is protected by a static `_lock` object:
- Pool creation and retrieval
- Worker registration and completion
- Active worker count queries
- Dynamic worker adjustments

The coordinator ensures thread-safe access across multiple concurrent `DumpRestoreProcessor` instances.

## Cleanup

When a job completes or is cancelled, call:
```csharp
WorkerPoolCoordinator.CleanupJob(jobId);
```

This will:
- Dispose all worker pools for the job
- Clear all worker tracking data
- Release allocated resources

## Migration Notes

### Before (Instance-Based)
```csharp
// Each processor had its own pools
private WorkerPoolManager _dumpPool;
private List<Task<TaskResult>> _activeDumpWorkers = new();
private object _workerLock = new();

// Workers were tracked locally
lock (_workerLock) {
    _activeDumpWorkers.Clear();
    _activeDumpWorkers.Add(workerTask);
}
```

### After (Shared Coordinator)
```csharp
// Processors use shared pools
private WorkerPoolManager? _dumpPool;
private readonly string _jobId;

// Pools obtained from coordinator
_dumpPool = WorkerPoolCoordinator.GetOrCreateDumpPool(_jobId, maxWorkers);

// Workers tracked via coordinator
List<Task<TaskResult>> workerTasks = new();
WorkerPoolCoordinator.RegisterDumpWorker(_jobId, collectionKey, workerId);
workerTasks.Add(Task.Run(async () => {
    try { return await DumpWorkerAsync(...); }
    finally { WorkerPoolCoordinator.MarkDumpWorkerCompleted(_jobId, workerId); }
}));
```

## Future Enhancements

Potential improvements:
1. **Global Worker Limits**: Enforce max total workers across all jobs
2. **Priority Queues**: Allow certain collections to have priority access to workers
3. **Metrics & Monitoring**: Track worker utilization, idle time, throughput per collection
4. **Adaptive Scaling**: Automatically adjust worker counts based on workload
5. **Worker Health Checks**: Detect and restart stuck workers
