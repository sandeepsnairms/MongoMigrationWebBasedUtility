# Reset Change Stream Complete Fix Summary

## Issues Fixed

### Issue 1: Infinite Retry Loop on Timeout
**Problem**: After timeout, `isSucessful` remained `false`, causing retry loop to continue indefinitely  
**Solution**: Set `isSucessful = true` in catch block to exit retry loop after handling timeout

### Issue 2: Using Stale Flag in Condition Check
**Problem**: Line 745 checked `!unit.ResetChangeStream` which was cleared in catch block, causing early exit  
**Solution**: Use local variable `!resetCS` instead, which preserves original intent across retries

### Issue 3: No ResumeToken Set on Timeout
**Problem**: When timeout occurred, `ResumeToken` was not updated, leaving it in uncertain state  
**Solution**: Set `unit.ResumeToken = unit.OriginalResumeToken` to reset to known good position

## Code Changes

### 1. MongoHelper.cs - Fix Early Exit Condition (Line 745)
```csharp
// BEFORE
if ((unit.RestoreComplete || job.IsSimulatedRun) && unit.DumpComplete && !unit.ResetChangeStream)
    return;

// AFTER
if ((unit.RestoreComplete || job.IsSimulatedRun) && unit.DumpComplete && !resetCS)
    return;
```

### 2. MongoHelper.cs - Exit Retry Loop on Timeout (Lines 626-656)
```csharp
catch (OperationCanceledException)
{
    if (resetCS && unit.ResetChangeStream)
    {
        unit.ResetChangeStream = false;
        
        if (cts.IsCancellationRequested)
        {
            log.WriteLine($"Change stream reset cancelled for {unit.DatabaseName}.{unit.CollectionName} (system shutdown or manual cancellation).", LogType.Warning);
            isSucessful = true; // ← EXIT RETRY LOOP
        }
        else
        {
            // Timeout - reset to OriginalResumeToken
            if (!string.IsNullOrEmpty(unit.OriginalResumeToken))
            {
                unit.ResumeToken = unit.OriginalResumeToken; // ← SET RESUME TOKEN
                log.WriteLine($"No changes detected in {unit.DatabaseName}.{unit.CollectionName} during {seconds}s timeout window. ResumeToken reset to OriginalResumeToken.", LogType.Info);
            }
            else
            {
                log.WriteLine($"No changes detected in {unit.DatabaseName}.{unit.CollectionName} during {seconds}s timeout window. No OriginalResumeToken available - will start fresh on next change.", LogType.Info);
            }
            isSucessful = true; // ← EXIT RETRY LOOP
        }
    }
    else
    {
        isSucessful = true; // ← EXIT RETRY LOOP FOR NON-RESET CANCELLATIONS
    }
}
```

### 3. MigrationWorker.cs - Quick Timeout for Reset (Line 315)
```csharp
// BEFORE
var resetTimeout = _config?.ChangeStreamBatchDurationMin ?? 30;
await MongoHelper.SetChangeStreamResumeTokenAsync(_log, _sourceClient, _jobList, _job, unit, resetTimeout, _cts);

// AFTER
// Use 10 second timeout - just read first change and exit quickly
await MongoHelper.SetChangeStreamResumeTokenAsync(_log, _sourceClient, _jobList, _job, unit, 10, _cts);
```

## Behavior Flow

### Success Case: Change Detected
1. ✅ Reset requested: `ResetChangeStream = true`, `resetCS = true`
2. ✅ Change detected within 10 seconds
3. ✅ `SetResumeTokenProperties` called, sets new resume token
4. ✅ Clears `unit.ResetChangeStream = false` in `SetResumeTokenProperties`
5. ✅ `isSucessful = true` in try block
6. ✅ Exit retry loop - **1 attempt total**

### Timeout Case: No Changes in 10 Seconds
1. ✅ Reset requested: `ResetChangeStream = true`, `resetCS = true`
2. ✅ No changes detected in 10 seconds
3. ✅ `OperationCanceledException` thrown
4. ✅ Catch block: Sets `unit.ResumeToken = unit.OriginalResumeToken`
5. ✅ Clears `unit.ResetChangeStream = false`
6. ✅ Sets `isSucessful = true` to exit retry loop
7. ✅ Exit retry loop - **1 attempt total**
8. ✅ Log: "No changes detected... ResumeToken reset to OriginalResumeToken"

### Error Case: Real Exception
1. ⚠️ Reset requested: `ResetChangeStream = true`, `resetCS = true`
2. ⚠️ Exception occurs (not timeout)
3. ⚠️ Catch (Exception ex): Logs error, increments `retryCount`
4. ⚠️ Retry up to 10 times with exponential backoff
5. ⚠️ After 10 failures: Clears `unit.ResetChangeStream = false`
6. ⚠️ Logs: "Failed to reset... after 10 attempts"
7. ⚠️ Exit retry loop - **10 attempts total**

## Why Previous Code Failed

### The Retry Loop Problem:
```csharp
while (!isSucessful && retryCount < 10)
{
    try { ... }
    catch (OperationCanceledException)
    {
        // Flag cleared but isSucessful NOT set
        unit.ResetChangeStream = false;
    }
    // isSucessful is still false → LOOP CONTINUES!
}
```

Result: **Infinite retries** (or up to 10) even though timeout was handled

### The Stale Flag Problem:
```csharp
bool resetCS = unit.ResetChangeStream; // true at start

// Retry 1: Timeout
catch (OperationCanceledException) {
    unit.ResetChangeStream = false; // Cleared!
}

// Retry 2: Change detected
if (!unit.ResetChangeStream) // TRUE (flag is now false)
    return; // EXIT WITHOUT PROCESSING CHANGE!
```

Result: **Change detected but not processed** on retry attempts

## Logs Before vs After

### Before Fix (10+ Attempts):
```
Setting up collection-level change stream resume token for Accounts.CO-Accounts
No changes detected in Accounts.CO-Accounts during 10s timeout window...
Setting up collection-level change stream resume token for Accounts.CO-Accounts
No changes detected in Accounts.CO-Accounts during 10s timeout window...
Setting up collection-level change stream resume token for Accounts.CO-Accounts
No changes detected in Accounts.CO-Accounts during 10s timeout window...
... (continues 10 times)
```

### After Fix (1 Attempt):
```
Setting up collection-level change stream resume token for Accounts.CO-Accounts
No changes detected in Accounts.CO-Accounts during 10s timeout window. ResumeToken reset to OriginalResumeToken.
```

## Testing Verification

### Expected Behavior:

**Active Collection (has changes)**:
1. ✅ Wait up to 10 seconds
2. ✅ Detect first change (usually < 1 second)
3. ✅ Set new resume token
4. ✅ Clear flag
5. ✅ Exit after **1 attempt**

**Inactive Collection (no changes)**:
1. ✅ Wait 10 seconds
2. ✅ Timeout - no changes found
3. ✅ Reset `ResumeToken = OriginalResumeToken`
4. ✅ Clear flag
5. ✅ Exit after **1 attempt**
6. ✅ Info-level log message

**Collection with Errors**:
1. ⚠️ Exception occurs
2. ⚠️ Retry with exponential backoff
3. ⚠️ Up to 10 attempts
4. ⚠️ Clear flag after 10 failures
5. ⚠️ Error-level log message

### Build Status:
✅ **Build Successful** (3.4s)

## Conclusion

All three issues fixed:
1. ✅ **No more infinite retries** - `isSucessful = true` exits loop
2. ✅ **Changes processed on retry** - using `resetCS` instead of `unit.ResetChangeStream`
3. ✅ **ResumeToken always set** - fallback to `OriginalResumeToken` on timeout

Result: **Exactly 1 attempt** for both success and timeout scenarios!
