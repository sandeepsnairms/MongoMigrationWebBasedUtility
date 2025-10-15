# Resume Token & Timestamp Checkpoint Fix

## Critical Issue Identified

**Problem**: Resume tokens and cursor timestamps were being updated **immediately** when changes were received from the MongoDB change stream, **before** the corresponding insert/update/delete operations were successfully processed to the target database.

**Impact**: If the job terminated due to a processing failure (deadlock, timeout, etc.), the resume token would already be advanced, causing **permanent data loss** on restart because MongoDB would resume **after** the failed batch.

## Root Cause Analysis

### Previous (Incorrect) Flow:
1. Change arrives from MongoDB change stream
2. **Resume token & timestamp updated immediately** ❌
3. Change added to buffer for later processing
4. Batch processing occurs (may fail)
5. **If failure occurs → resume token already moved forward → data skipped on restart**

### Example Scenario of Data Loss:
```
1. Changes 1-100 arrive, resume token updated to change #100
2. Batch processing starts for changes 1-100
3. Change #50 encounters persistent deadlock after 10 retries
4. Job terminates (as designed to prevent data loss)
5. Resume token already at #100
6. On restart → MongoDB resumes AFTER change #100
7. Changes 1-100 are LOST FOREVER ❌
```

## Solution Implemented

### New (Correct) Flow:
1. Change arrives from MongoDB change stream
2. Change added to buffer **without** updating resume token/timestamp
3. Buffer tracks latest metadata (token, timestamp) internally
4. Batch processing occurs via `BulkProcessChangesAsync()`
5. **Only AFTER successful processing** → resume token & timestamp updated ✅
6. **If failure occurs** → resume token still at last successful batch → job can safely restart ✅

### Code Changes

#### 1. Enhanced `ChangeStreamDocuments.cs`
Added tracking fields to store metadata **before** processing:

```csharp
public class ChangeStreamDocuments
{
    // Existing lists
    public List<ChangeStreamDocument<BsonDocument>> DocsToBeInserted { get; }
    public List<ChangeStreamDocument<BsonDocument>> DocsToBeUpdated { get; }
    public List<ChangeStreamDocument<BsonDocument>> DocsToBeDeleted { get; }

    // NEW: Track latest metadata for safe checkpoint updates
    public string LatestResumeToken { get; private set; }
    public DateTime LatestTimestamp { get; private set; }
    public ChangeStreamOperationType LatestOperationType { get; private set; }
    public string LatestDocumentKey { get; private set; }

    // Called by AddInsert/AddUpdate/AddDelete
    private void UpdateLatestMetadata(ChangeStreamDocument<BsonDocument> change)
    {
        if (change.ResumeToken != null && change.ResumeToken != BsonNull.Value)
        {
            LatestResumeToken = change.ResumeToken.ToJson();
            LatestOperationType = change.OperationType;
            LatestDocumentKey = change.DocumentKey.ToJson();
        }

        if (change.ClusterTime != null)
        {
            LatestTimestamp = MongoHelper.BsonTimestampToUtcDateTime(change.ClusterTime);
        }
        else if (change.WallTime != null)
        {
            LatestTimestamp = change.WallTime.Value;
        }
    }

    public void ClearLatestMetadata() { ... }
}
```

#### 2. Removed Immediate Updates in `ServerLevelChangeStreamProcessor.cs`

**Before (lines 708-748)**:
```csharp
// REMOVED: Immediate timestamp update
cached.Unit.CursorUtcTimestamp = timeStamp;
_job.CursorUtcTimestamp = timeStamp;

// REMOVED: Immediate resume token update
var resumeTokenJson = change.ResumeToken.ToJson();
UpdateResumeToken(resumeTokenJson, change.OperationType, ...);
```

**After**:
```csharp
// REMOVED: Do NOT update timestamps/resume tokens immediately
// They will be updated AFTER successful batch processing to prevent data loss on failure
// See ProcessAccumulatedChanges() where tokens are updated post-processing
```

#### 3. Updated `ProcessAccumulatedChanges()` to Update After Success

**New logic in `ProcessAccumulatedChanges()`**:
```csharp
lock (buffer)
{
    // Extract changes
    inserts = new List<ChangeStreamDocument<BsonDocument>>(buffer.DocsToBeInserted);
    updates = new List<ChangeStreamDocument<BsonDocument>>(buffer.DocsToBeUpdated);
    deletes = new List<ChangeStreamDocument<BsonDocument>>(buffer.DocsToBeDeleted);
    
    // Capture latest metadata for checkpoint update AFTER processing
    latestResumeToken = buffer.LatestResumeToken;
    latestTimestamp = buffer.LatestTimestamp;
    latestOpType = buffer.LatestOperationType;
    latestDocKey = buffer.LatestDocumentKey;
    
    buffer.DocsToBeInserted.Clear();
    buffer.DocsToBeUpdated.Clear();
    buffer.DocsToBeDeleted.Clear();
    buffer.ClearLatestMetadata();
}

// Process the batch - this will throw if critical failure occurs
await BulkProcessChangesAsync(
    cached.Unit,
    cached.TargetCollection,
    inserts,
    updates,
    deletes,
    batchSize);

// ✅ ONLY update resume token and timestamp AFTER successful processing
// This prevents data loss if job terminates during processing
if (!string.IsNullOrEmpty(latestResumeToken))
{
    UpdateResumeToken(latestResumeToken, latestOpType, latestDocKey, collectionKey);
}

if (latestTimestamp > DateTime.MinValue)
{
    if (!_syncBack)
    {
        cached.Unit.CursorUtcTimestamp = latestTimestamp;
        _job.CursorUtcTimestamp = latestTimestamp;
    }
    else
    {
        cached.Unit.SyncBackCursorUtcTimestamp = latestTimestamp;
        _job.SyncBackCursorUtcTimestamp = latestTimestamp;
    }
}
```

## How It Works Together with Retry Logic

This fix works in conjunction with the **10-retry deadlock/timeout handling** implemented in `MongoHelper.cs`:

1. Changes buffered in `ChangeStreamDocuments` (token/timestamp tracked but not saved)
2. `BulkProcessChangesAsync()` calls `ProcessInsertsAsync/ProcessUpdatesAsync/ProcessDeletesAsync`
3. Each Process method retries up to 10 times with exponential backoff
4. **If all 10 retries fail** → `InvalidOperationException` with "CRITICAL" thrown
5. Exception propagates through `BulkProcessChangesAsync` → `ProcessAccumulatedChanges` → job stops
6. **Resume token NEVER updated** because processing never completed successfully
7. On restart → MongoDB resumes from last **successfully processed** batch
8. **No data loss** ✅

## Testing Scenarios

### Scenario 1: Successful Processing
```
1. Changes 1-100 received, buffered (token tracked internally)
2. BulkProcessChangesAsync() processes successfully
3. Resume token updated to change #100
4. On restart → resumes from change #101 ✅
```

### Scenario 2: Failure After 10 Retries
```
1. Changes 1-100 received, buffered (token tracked internally)
2. BulkProcessChangesAsync() starts processing
3. Change #50 fails after 10 retries
4. InvalidOperationException thrown, job stops
5. Resume token STILL at change #0 (last successful batch)
6. On restart → resumes from change #1
7. Retries processing changes 1-100 again ✅
```

### Scenario 3: Partial Batch Success
```
1. Changes 1-100 buffered
2. Inserts (changes 1-30) succeed
3. Updates (changes 31-60) succeed
4. Deletes (changes 61-100) fail after 10 retries
5. Task.WhenAll() in BulkProcessChangesAsync catches exception
6. Resume token NEVER updated (no token update on exception path)
7. On restart → all 100 changes reprocessed
8. Inserts deduplicated, updates reapplied, deletes retried ✅
```

## Files Modified

1. **`ChangeStreamDocuments.cs`**
   - Added: `LatestResumeToken`, `LatestTimestamp`, `LatestOperationType`, `LatestDocumentKey` properties
   - Added: `UpdateLatestMetadata()` private method
   - Added: `ClearLatestMetadata()` public method
   - Modified: `AddInsert()`, `AddUpdate()`, `AddDelete()` to call `UpdateLatestMetadata()`

2. **`ServerLevelChangeStreamProcessor.cs`**
   - Removed: Immediate timestamp updates (lines ~708-714)
   - Removed: Immediate resume token updates (lines ~745-748)
   - Modified: `ProcessAccumulatedChanges()` to capture metadata and update AFTER successful processing

## Benefits

✅ **Zero Data Loss**: Resume tokens only updated after confirmed successful processing  
✅ **Safe Restart**: Job can always resume from last successful checkpoint  
✅ **Idempotent Retry**: Failed batches can be safely reprocessed  
✅ **Works with Deduplication**: `ChangeStreamDocuments` deduplication handles reprocessed changes  
✅ **Exception-Safe**: Critical exceptions prevent token updates, preserving restart point  

## Backward Compatibility

This change is **fully backward compatible**:
- No changes to database schema
- No changes to resume token format
- No changes to migration job configuration
- Existing jobs will simply start using safer checkpoint logic

## Performance Impact

**Negligible**: 
- Token updates moved from inline (per-change) to batch (per-flush)
- Reduces write frequency to job metadata
- May actually **improve** performance by reducing lock contention

---

**Date**: 2025-10-15  
**Related**: DEADLOCK_RETRY_ENHANCEMENT.md, DEADLOCK_FIX_VERIFICATION.md
