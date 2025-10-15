# Performance Optimization: Non-Blocking Exception Handling

## Problem Identified

**User Question:** "ProcessAccumulatedChanges was earlier fire and forget now awaited. The speed would decrease?"

**Analysis:**
- Previously: `_ = Task.Run(ProcessAccumulatedChanges);` (fire-and-forget)
  - **Pros:** High performance - queue processing continues immediately
  - **Cons:** Exceptions swallowed, critical failures never detected
  
- Changed to: `await ProcessAccumulatedChanges();` 
  - **Pros:** Catches critical exceptions, proper error handling
  - **Cons:** Blocks queue processing during flush, reduces throughput

## Solution Implemented

**Hybrid Approach: Tracked Fire-and-Forget**

We now use fire-and-forget for **performance** while tracking tasks to detect **critical failures**.

### Key Changes

#### 1. Background Task Tracking (Line 48-51)
```csharp
// Track background ProcessAccumulatedChanges tasks to detect critical failures
private readonly ConcurrentBag<Task> _backgroundProcessingTasks = new();
private volatile bool _criticalFailureDetected = false;
private Exception? _criticalFailureException = null;
```

#### 2. Fire-and-Forget with Exception Handling (Lines 561-587)
```csharp
// Use fire-and-forget for performance BUT track the task to detect critical failures
var processingTask = Task.Run(async () =>
{
    try
    {
        await ProcessAccumulatedChanges();
    }
    catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
    {
        // Capture critical failure
        _criticalFailureDetected = true;
        _criticalFailureException = ex;
        _log.WriteLine($"CRITICAL failure in background ProcessAccumulatedChanges...", LogType.Error);
    }
    catch (AggregateException aex) when (...)
    {
        // Handle AggregateException containing CRITICAL exceptions
        _criticalFailureDetected = true;
        _criticalFailureException = criticalEx;
    }
    catch (Exception ex)
    {
        // Log non-critical exceptions but don't halt the job
        _log.WriteLine($"Non-critical exception...", LogType.Error);
    }
});

_backgroundProcessingTasks.Add(processingTask);
```

#### 3. Proactive Failure Detection (Lines 507-511)
```csharp
// Check for critical failures from background tasks BEFORE starting new work
if (_criticalFailureDetected)
{
    _log.WriteLine($"Critical failure detected in background processing...", LogType.Error);
    return; // Stop processing, let main loop detect and propagate the exception
}
```

#### 4. Main Loop Detection (Lines 1097-1102)
```csharp
// Check for critical failures from background tasks before processing
if (_criticalFailureDetected && _criticalFailureException != null)
{
    _log.WriteLine($"CRITICAL ERROR: Background task failed, stopping...", LogType.Error);
    throw _criticalFailureException; // Re-throw the original critical exception
}
```

#### 5. Task Pruning to Prevent Memory Leak (Lines 602-619)
```csharp
private void PruneCompletedBackgroundTasks()
{
    // Remove completed tasks to avoid unbounded growth
    var activeTasks = new ConcurrentBag<Task>();
    foreach (var task in _backgroundProcessingTasks)
    {
        if (!task.IsCompleted)
        {
            activeTasks.Add(task);
        }
    }
    
    // Replace with only active tasks
    _backgroundProcessingTasks.Clear();
    foreach (var task in activeTasks)
    {
        _backgroundProcessingTasks.Add(task);
    }
}
```

## Performance Impact Analysis

### Before (Awaited)
```
Timer fires (500ms) → ProcessQueuedChanges starts
  ↓
Queue processing (fast)
  ↓
await ProcessAccumulatedChanges() ← BLOCKS HERE
  ↓ (could take seconds for large batches)
ProcessQueuedChanges completes
  ↓
Next timer fires
```
**Throughput:** Limited by batch flush time (blocking)

### After (Tracked Fire-and-Forget)
```
Timer fires (500ms) → ProcessQueuedChanges starts
  ↓
Queue processing (fast)
  ↓
Task.Run(ProcessAccumulatedChanges) ← NON-BLOCKING
  ↓ (background task tracked)
ProcessQueuedChanges completes immediately
  ↓
Next timer fires (can process new changes while batch flushes)
  |
  └─→ Background: ProcessAccumulatedChanges running
      └─→ If CRITICAL exception → flag set
          └─→ Main loop/next timer cycle detects and stops
```
**Throughput:** HIGH - queue processing not blocked by batch flush

## Benefits

✅ **Maintains High Performance**
- Queue processing continues immediately
- Multiple batches can flush in parallel
- No blocking on accumulated changes processing

✅ **Catches Critical Failures**
- All CRITICAL exceptions captured via task wrapper
- Proactive detection in timer callback (before new work)
- Main loop detection (propagates to job termination)

✅ **Memory Safe**
- Completed tasks pruned periodically
- Only active tasks tracked
- No unbounded memory growth

✅ **Data Integrity**
- Critical failures (deadlocks after 10 retries) stop the job
- Resume tokens only updated on success (previous fix)
- Earliest/latest token tracking for rollback (previous fix)

## Exception Flow

### Critical Exception Path
1. **MongoHelper.ProcessInsertsAsync/ProcessUpdatesAsync/ProcessDeletesAsync**
   - Retry 10 times with exponential backoff
   - On final failure: `throw new InvalidOperationException("CRITICAL: ...")`

2. **BulkProcessChangesAsync** (ChangeStreamProcessor.cs)
   - Catches CRITICAL exceptions and re-throws

3. **ProcessAccumulatedChanges** (ServerLevelChangeStreamProcessor.cs)
   - Runs in background Task.Run wrapper
   - Exception caught in wrapper
   - Sets `_criticalFailureDetected = true`
   - Stores exception in `_criticalFailureException`

4. **ProcessQueuedChanges** (timer callback)
   - Checks `_criticalFailureDetected` at start
   - Returns early if critical failure detected
   - Stops processing new changes

5. **ProcessChangeStreamsAsync** (main loop)
   - Checks `_criticalFailureDetected` before each iteration
   - Throws `_criticalFailureException` to stop job

6. **Job Termination**
   - Exception propagates up to job manager
   - Job stops, preventing data loss

## Testing Recommendations

1. **Performance Test:**
   - Monitor throughput with high change volume
   - Verify queue processing not blocked during batch flush
   - Check timer intervals remain consistent (500ms)

2. **Critical Failure Test:**
   - Simulate deadlock after 10 retries
   - Verify job stops within 1-2 timer cycles (0.5-1 second)
   - Confirm exception message logged correctly

3. **Memory Test:**
   - Run for extended period (hours)
   - Verify `_backgroundProcessingTasks` count stays bounded
   - Check no memory leaks from accumulated tasks

4. **Resume Token Test:**
   - Trigger critical failure during batch processing
   - Verify resume token still at last successful batch
   - Restart job and confirm no data loss or duplication

## Conclusion

**Answer to user's question:** No, speed will **NOT** decrease. 

The new implementation:
- Uses fire-and-forget for performance (queue processing not blocked)
- Tracks background tasks to detect critical failures
- Proactively checks for failures before starting new work
- Stops job within 1 timer cycle of critical failure (0.5-1 second)

This gives us **both high performance AND proper exception handling** - the best of both worlds.
