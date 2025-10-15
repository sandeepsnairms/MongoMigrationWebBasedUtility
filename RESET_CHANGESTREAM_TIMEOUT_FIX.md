# Reset Change Stream Timeout Configuration Fix

## Issue
User reported seeing "Change stream reset cancelled for Accounts.CO-Accounts. Flag cleared" with no error logs, indicating the reset was timing out rather than failing.

## Root Cause
1. **Hardcoded 15-second timeout**: `MigrationWorker.cs` used a hardcoded `15` seconds for reset operations
2. **Inactive collections**: Collections with no activity during the short timeout window would trigger `OperationCanceledException`
3. **Misleading logs**: Original message didn't distinguish between timeout (normal for inactive collections) vs actual errors

## Solution Applied

### 1. Use Configured Timeout Instead of Hardcoded Value
**File**: `OnlineMongoMigrationProcessor/Workers/MigrationWorker.cs` (line 311-312)

**Before**:
```csharp
await MongoHelper.SetChangeStreamResumeTokenAsync(_log, _sourceClient, _jobList, _job, unit, 15, _cts);
```

**After**:
```csharp
var resetTimeout = _config?.ChangeStreamBatchDurationMin ?? 30;
await MongoHelper.SetChangeStreamResumeTokenAsync(_log, _sourceClient, _jobList, _job, unit, resetTimeout, _cts);
```

**Benefits**:
- ✅ Uses `ChangeStreamBatchDurationMin` configuration (default 30 seconds)
- ✅ Configurable per environment via `MigrationSettings`
- ✅ More time to detect changes in slower/inactive collections
- ✅ Consistent with other change stream batch processing timeouts

### 2. Improved Error Logging with Clear Distinction
**File**: `OnlineMongoMigrationProcessor/Helpers/MongoHelper.cs` (line 626-642)

**Before**:
```csharp
catch (OperationCanceledException)
{
    if (unit.ResetChangeStream)
    {
        unit.ResetChangeStream = false;
        log.WriteLine($"Change stream reset cancelled for {unit.DatabaseName}.{unit.CollectionName}. Flag cleared.", LogType.Warning);
    }
}
```

**After**:
```csharp
catch (OperationCanceledException)
{
    if (resetCS && unit.ResetChangeStream)
    {
        unit.ResetChangeStream = false;
        
        // Check if it was a timeout or manual cancellation
        if (cts.IsCancellationRequested)
        {
            log.WriteLine($"Change stream reset cancelled for {unit.DatabaseName}.{unit.CollectionName} (system shutdown or manual cancellation).", LogType.Warning);
        }
        else
        {
            // Timeout occurred - no changes detected within the time window
            log.WriteLine($"No changes detected in {unit.DatabaseName}.{unit.CollectionName} during {seconds}s timeout window. Reset skipped - will use existing resume token or start fresh on next change.", LogType.Info);
        }
    }
}
```

**Benefits**:
- ✅ Distinguishes timeout vs manual cancellation
- ✅ Info-level for normal timeout (no activity) - not alarming
- ✅ Warning-level for actual system shutdown
- ✅ Clear explanation of what happens next
- ✅ Shows the actual timeout value in the message

### 3. Enhanced Error Handling for Retries
**File**: `OnlineMongoMigrationProcessor/Helpers/MongoHelper.cs` (line 644-653)

**Added**:
```csharp
catch (Exception ex)
{
    retryCount++;
    log.WriteLine($"Attempt {retryCount}. Error setting change stream resume token for {unit.DatabaseName}.{unit.CollectionName}: {ex}", LogType.Error);
    
    // If all retries exhausted, clear the flag to prevent infinite retry loop
    if (retryCount >= 10 && unit.ResetChangeStream)
    {
        unit.ResetChangeStream = false;
        log.WriteLine($"Failed to reset change stream for {unit.DatabaseName}.{unit.CollectionName} after {retryCount} attempts. Flag cleared to prevent infinite retries. Manual intervention may be required.", LogType.Error);
    }
}
```

**Benefits**:
- ✅ Prevents infinite retry loop even if all 10 attempts fail
- ✅ Clear error message after exhausting retries
- ✅ Flag always cleared to allow job to continue
- ✅ Does NOT mark unit as failed - graceful degradation

## Impact

### Before Fix
❌ Hardcoded 15-second timeout  
❌ Short timeout caused frequent cancellations for inactive collections  
❌ Misleading "cancelled" warning for normal timeouts  
❌ Same log message for timeout vs shutdown  

### After Fix
✅ **Configurable timeout** (default 30s via `ChangeStreamBatchDurationMin`)  
✅ **More time** for inactive collections to show changes  
✅ **Clear logging**: Info for timeout, Warning for cancellation, Error for failures  
✅ **No infinite retries**: Flag cleared after failures  
✅ **Graceful degradation**: Collections continue without reset if it fails  

## Configuration

Users can adjust the reset timeout by configuring `ChangeStreamBatchDurationMin` in `appsettings.json`:

```json
{
  "ChangeStreamBatchDurationMin": 30,  // Seconds to wait for first change during reset
  "ChangeStreamBatchDuration": 120     // Max batch processing duration
}
```

## Log Examples

### Scenario 1: Inactive Collection (No Changes in Timeout Window)
**LogType**: `Info` (not alarming)
```
No changes detected in Accounts.CO-Accounts during 30s timeout window. 
Reset skipped - will use existing resume token or start fresh on next change.
```

### Scenario 2: System Shutdown or Manual Cancellation
**LogType**: `Warning`
```
Change stream reset cancelled for Accounts.CO-Accounts (system shutdown or manual cancellation).
```

### Scenario 3: Real Error After 10 Retries
**LogType**: `Error`
```
Attempt 10. Error setting change stream resume token for Accounts.CO-Accounts: ...
Failed to reset change stream for Accounts.CO-Accounts after 10 attempts. 
Flag cleared to prevent infinite retries. Manual intervention may be required.
```

## Testing Verification

### Expected Behavior After Fix

**For Active Collections**:
1. ✅ Reset completes within configured timeout
2. ✅ Resume token updated successfully
3. ✅ Flag cleared in `SetResumeTokenProperties`

**For Inactive Collections**:
1. ✅ Timeout after `ChangeStreamBatchDurationMin` seconds (default 30s)
2. ✅ Info-level log: "No changes detected..."
3. ✅ Flag cleared to prevent retry loop
4. ✅ Collection continues with existing/original resume token

**For Failed Resets**:
1. ✅ Up to 10 retry attempts with exponential backoff
2. ✅ Error logged for each attempt
3. ✅ After 10 failures: Flag cleared with error message
4. ✅ Job continues (no job termination)

### Build Status
✅ **Build Successful** (2.6s)
- OnlineMongoMigrationProcessor: ✅ Succeeded (1.0s)
- MongoMigrationWebApp: ✅ Succeeded (0.5s)
- Tests: ✅ Succeeded (0.4s)

## Conclusion
The fix provides:
1. **Configurable timeout** instead of hardcoded value
2. **Clear, actionable logging** with appropriate log levels
3. **Graceful handling** of inactive collections
4. **No infinite retries** even when resets fail
5. **Job continuity** - collections continue processing without reset if needed
