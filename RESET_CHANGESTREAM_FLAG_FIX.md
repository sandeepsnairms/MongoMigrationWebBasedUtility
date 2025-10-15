# Reset Change Stream Flag Fix

## Issue
User reported seeing "Resetting change stream resume token" log message **multiple times for the same collection**.

## Root Cause Analysis

### Problem Flow
1. **MigrationWorker.cs** (line 307-316): When `unit.ResetChangeStream = true`, calls `SetChangeStreamResumeTokenAsync` synchronously
2. **MongoHelper.cs** (line 565): Logs "Resetting change stream resume token..." each time
3. **SetResumeTokenProperties** (line 1253-1303): Sets new resume token but **never cleared the flag**
4. **CollectionLevelChangeStreamProcessor.cs** (line 700): Only cleared flag during change stream processing, not immediately after reset

### Why Multiple Log Messages?
The MigrationWorker processes collections in a loop. Between the reset operation and the actual change stream processing:
- Worker loop runs multiple times
- Each iteration sees `ResetChangeStream = true`
- Each iteration calls `SetChangeStreamResumeTokenAsync`
- Each call logs "Resetting change stream resume token..."
- Flag only cleared **later** when `ProcessCollectionChangeStream` runs

### Timing Gap
```
Time 0: ResetChangeStream = true
Time 1: Worker loop iteration 1 → SetChangeStreamResumeTokenAsync → Log message #1
Time 2: Worker loop iteration 2 → SetChangeStreamResumeTokenAsync → Log message #2
Time 3: Worker loop iteration 3 → SetChangeStreamResumeTokenAsync → Log message #3
...
Time N: ProcessCollectionChangeStream runs → ResetChangeStream = false
```

## Solution

### Fix Location
**File**: `OnlineMongoMigrationProcessor/Helpers/MongoHelper.cs`  
**Method**: `SetResumeTokenProperties` (line 1253-1310)

### Change Made
Added flag clearing logic immediately after successfully resetting the change stream resume token:

```csharp
else if (target is MigrationUnit unit)
{
    // Collection-level resume token setting
    unit.ResumeToken = resumeTokenJson;
    
    if (!resetCS && string.IsNullOrEmpty(unit.OriginalResumeToken))
        unit.OriginalResumeToken = resumeTokenJson;

    unit.CursorUtcTimestamp = timestamp;
    unit.ResumeTokenOperation = operationType;
    unit.ResumeDocumentId = documentKeyJson;

    // Clear the reset flag after successfully resetting the change stream
    // This prevents duplicate "Resetting change stream resume token" log messages
    if (resetCS)
    {
        unit.ResetChangeStream = false;  // ← NEW: Clear flag immediately
    }
}
```

### Why This Works
1. **Immediate clearing**: Flag cleared as soon as reset token is set, not waiting for change stream processing
2. **Prevents duplicate logs**: Subsequent worker loop iterations see `ResetChangeStream = false`
3. **Safe**: Only clears flag after successful resume token setting
4. **Idempotent**: Line 700 in CollectionLevelChangeStreamProcessor still sets to false (harmless redundancy)

## Impact

### Before Fix
- Multiple "Resetting change stream resume token" log messages for same collection
- Confusing logs suggesting repeated reset attempts
- No functional issue, just log pollution

### After Fix
- Single "Resetting change stream resume token" log message per collection reset
- Clear indication that reset completed successfully
- Flag cleared immediately after reset operation

## Related Code Locations

1. **MigrationWorker.cs** (line 307-316): Checks `ResetChangeStream` flag and calls reset
2. **MongoHelper.cs** (line 540-640): `SetChangeStreamResumeTokenAsync` - performs reset
3. **MongoHelper.cs** (line 565): Logs the reset message
4. **MongoHelper.cs** (line 1253-1310): `SetResumeTokenProperties` - **FIX APPLIED HERE**
5. **CollectionLevelChangeStreamProcessor.cs** (line 700): Redundant flag clear (kept for safety)

## Testing Verification

### Expected Behavior After Fix
When resetting a change stream for a collection:
1. ✅ Single log message: "Resetting change stream resume token for {database}.{collection}..."
2. ✅ Flag cleared immediately after successful reset
3. ✅ No duplicate reset attempts in subsequent worker iterations
4. ✅ Normal change stream processing continues

### Build Status
✅ **Build Successful** (17.8s)
- OnlineMongoMigrationProcessor: ✅ Succeeded (5.9s)
- MongoMigrationWebApp: ✅ Succeeded (9.9s)
- Tests: ✅ Succeeded (2.8s)

## Conclusion
The fix ensures the `ResetChangeStream` flag is cleared immediately after successfully setting the reset resume token, preventing duplicate log messages and providing clearer operational visibility.
