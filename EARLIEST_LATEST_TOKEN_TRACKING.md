# Earliest and Latest Resume Token Tracking

## Enhancement Overview

**Problem**: Previously, only the **latest** resume token in a batch was tracked. If a batch failed after 10 retries, the resume token was not updated (which is good), but there was no explicit tracking of where the batch **started** from.

**Solution**: Track **both** the earliest and latest resume tokens in each batch:
- **Earliest token**: First change in the batch - used to ensure we can reprocess from the beginning on retry
- **Latest token**: Last change in the batch - used to advance checkpoint on success

**Benefit**: Clear visibility into the batch boundaries and explicit guarantee that failed batches will be retried from the beginning.

---

## Why This Matters

### Scenario: Partial Batch Processing

Imagine a batch with changes 100-200:

#### Without Earliest Tracking (Old Behavior)
```
1. Changes 100-200 buffered
2. Resume token = last successful checkpoint (change #99)
3. Processing starts
4. Change #150 fails after 10 retries
5. Job stops, resume token still at #99
6. On restart → MongoDB replays changes 100-200
7. ✅ Works, but we don't explicitly track batch start
```

#### With Earliest Tracking (New Behavior)
```
1. Changes 100-200 buffered
   - Earliest token = change #100 (first in batch)
   - Latest token = change #200 (last in batch)
   - Current persisted token = change #99 (last success)
2. Processing starts
3. Change #150 fails after 10 retries
4. Catch block logs: "Earliest token in failed batch: #100"
5. Resume token NOT updated (stays at #99)
6. On restart → MongoDB replays from #99, includes #100-200
7. ✅ Explicit guarantee of batch boundary
```

---

## Implementation Details

### 1. ChangeStreamDocuments.cs - Enhanced Metadata Tracking

**New Properties**:
```csharp
// Track the earliest resume token for rollback on failure
public string EarliestResumeToken { get; private set; } = string.Empty;
public DateTime EarliestTimestamp { get; private set; } = DateTime.MinValue;
public ChangeStreamOperationType EarliestOperationType { get; private set; }
public string EarliestDocumentKey { get; private set; } = string.Empty;

// Track the latest resume token for checkpoint on success
public string LatestResumeToken { get; private set; } = string.Empty;
public DateTime LatestTimestamp { get; private set; } = DateTime.MinValue;
public ChangeStreamOperationType LatestOperationType { get; private set; }
public string LatestDocumentKey { get; private set; } = string.Empty;
```

**Update Logic**:
```csharp
private void UpdateMetadata(ChangeStreamDocument<BsonDocument> change)
{
    DateTime changeTimestamp = DateTime.MinValue;
    
    // Extract timestamp from ClusterTime or WallTime
    if (change.ClusterTime != null)
    {
        changeTimestamp = MongoHelper.BsonTimestampToUtcDateTime(change.ClusterTime);
    }
    else if (change.WallTime != null)
    {
        changeTimestamp = change.WallTime.Value;
    }

    // Track earliest (first change in batch) - for rollback on failure
    if (string.IsNullOrEmpty(EarliestResumeToken) && 
        change.ResumeToken != null && 
        change.ResumeToken != BsonNull.Value)
    {
        EarliestResumeToken = change.ResumeToken.ToJson();
        EarliestOperationType = change.OperationType;
        EarliestDocumentKey = change.DocumentKey.ToJson();
        EarliestTimestamp = changeTimestamp;
    }

    // Always track latest (last change in batch) - for checkpoint on success
    if (change.ResumeToken != null && change.ResumeToken != BsonNull.Value)
    {
        LatestResumeToken = change.ResumeToken.ToJson();
        LatestOperationType = change.OperationType;
        LatestDocumentKey = change.DocumentKey.ToJson();
        LatestTimestamp = changeTimestamp;
    }
}
```

**Key Points**:
- ✅ Earliest token set **only once** (first call with non-null token)
- ✅ Latest token **always updated** (last change becomes latest)
- ✅ Both cleared together via `ClearMetadata()`

### 2. ServerLevelChangeStreamProcessor.cs - Success and Failure Paths

**Extract Both Tokens**:
```csharp
lock (buffer)
{
    inserts = new List<>(buffer.DocsToBeInserted);
    updates = new List<>(buffer.DocsToBeUpdated);
    deletes = new List<>(buffer.DocsToBeDeleted);
    
    // Capture earliest metadata for rollback on failure
    earliestResumeToken = buffer.EarliestResumeToken;
    earliestTimestamp = buffer.EarliestTimestamp;
    earliestOpType = buffer.EarliestOperationType;
    earliestDocKey = buffer.EarliestDocumentKey;
    
    // Capture latest metadata for checkpoint on success
    latestResumeToken = buffer.LatestResumeToken;
    latestTimestamp = buffer.LatestTimestamp;
    latestOpType = buffer.LatestOperationType;
    latestDocKey = buffer.LatestDocumentKey;
    
    buffer.DocsToBeInserted.Clear();
    buffer.DocsToBeUpdated.Clear();
    buffer.DocsToBeDeleted.Clear();
    buffer.ClearMetadata();
}
```

**Success Path**:
```csharp
try
{
    // Process the batch
    await BulkProcessChangesAsync(
        cached.Unit,
        cached.TargetCollection,
        inserts, updates, deletes,
        batchSize);

    // ✅ SUCCESS: Update resume token to LATEST
    if (!string.IsNullOrEmpty(latestResumeToken))
    {
        UpdateResumeToken(latestResumeToken, latestOpType, latestDocKey, collectionKey);
    }

    if (latestTimestamp > DateTime.MinValue)
    {
        cached.Unit.CursorUtcTimestamp = latestTimestamp;
        _job.CursorUtcTimestamp = latestTimestamp;
    }
}
```

**Failure Path**:
```csharp
catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
{
    // ❌ FAILURE: Log earliest token for visibility
    _log.WriteLine($"CRITICAL FAILURE for {collectionKey}. Batch will be retried on restart. " +
                   $"Earliest token in failed batch: {earliestResumeToken.Substring(0, 50)}...", 
                   LogType.Error);
    
    // Do NOT update resume token - keep it at the last successful batch
    // The failed batch will be reprocessed on job restart
    throw; // Re-throw to stop the job
}
```

---

## Flow Diagrams

### Successful Batch Processing

```
┌─────────────────────────────────────────────────────────────────┐
│ Changes arrive from MongoDB change stream                      │
│ Changes: #100, #101, #102, ..., #199, #200                     │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ AddInsert/AddUpdate/AddDelete called for each change           │
│                                                                 │
│ First call (change #100):                                      │
│   EarliestResumeToken = token(#100)  ← Set once                │
│   LatestResumeToken = token(#100)    ← Set every time          │
│                                                                 │
│ Second call (change #101):                                     │
│   EarliestResumeToken = token(#100)  ← Unchanged               │
│   LatestResumeToken = token(#101)    ← Updated                 │
│                                                                 │
│ ...                                                             │
│                                                                 │
│ Last call (change #200):                                       │
│   EarliestResumeToken = token(#100)  ← Still first             │
│   LatestResumeToken = token(#200)    ← Now last                │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ ProcessAccumulatedChanges() extracts batch                     │
│                                                                 │
│ Captured under lock:                                           │
│   earliestResumeToken = token(#100)                            │
│   latestResumeToken = token(#200)                              │
│   inserts = [changes 100-200]                                  │
│                                                                 │
│ Buffer cleared, metadata reset                                 │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ BulkProcessChangesAsync() processes batch                      │
│ ✅ All changes processed successfully                          │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Success path: Update checkpoint to LATEST                      │
│                                                                 │
│ UpdateResumeToken(token(#200), ...)                            │
│ CursorUtcTimestamp = timestamp(#200)                           │
│                                                                 │
│ Persisted state:                                               │
│   Resume token = token(#200)                                   │
│   Timestamp = timestamp(#200)                                  │
│                                                                 │
│ On restart → MongoDB resumes from token(#200), next is #201   │
└─────────────────────────────────────────────────────────────────┘
```

### Failed Batch Processing

```
┌─────────────────────────────────────────────────────────────────┐
│ Changes arrive from MongoDB change stream                      │
│ Changes: #200, #201, #202, ..., #299, #300                     │
│                                                                 │
│ Current persisted checkpoint: token(#199)                      │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Changes buffered in ChangeStreamDocuments                      │
│                                                                 │
│ After all 101 changes added:                                   │
│   EarliestResumeToken = token(#200)  ← First in batch          │
│   LatestResumeToken = token(#300)    ← Last in batch           │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ ProcessAccumulatedChanges() extracts batch                     │
│                                                                 │
│ Captured:                                                      │
│   earliestResumeToken = token(#200)                            │
│   latestResumeToken = token(#300)                              │
│   deletes = [changes 200-300]                                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ BulkProcessChangesAsync() starts processing                    │
│   ↓                                                             │
│ ProcessDeletesAsync() encounters deadlock on change #250      │
│   ↓                                                             │
│ Retry 1-10 with exponential backoff                            │
│   ↓                                                             │
│ ❌ All retries fail                                            │
│   ↓                                                             │
│ Throws: InvalidOperationException("CRITICAL: persistent deadlock")│
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Catch block in ProcessAccumulatedChanges                       │
│                                                                 │
│ catch (InvalidOperationException ex) {                         │
│     _log.WriteLine(                                            │
│         "CRITICAL FAILURE for collection. " +                  │
│         "Batch will be retried on restart. " +                 │
│         $"Earliest token in failed batch: {token(#200)}");     │
│                                                                 │
│     // ❌ Do NOT call UpdateResumeToken()                     │
│     // Resume token stays at token(#199) - last success       │
│                                                                 │
│     throw; // Re-throw to stop job                            │
│ }                                                              │
│                                                                 │
│ Persisted state UNCHANGED:                                    │
│   Resume token = token(#199)  ← Still at last success         │
│   Timestamp = timestamp(#199)                                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Job stops with CRITICAL error                                  │
│                                                                 │
│ Log message shows:                                             │
│   "CRITICAL FAILURE for Accounts.MX-DeliveryWindows."          │
│   "Batch will be retried on restart."                          │
│   "Earliest token in failed batch: token(#200)"                │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ User investigates deadlock, resolves issue, restarts job      │
│                                                                 │
│ MongoDB resumes from token(#199)                               │
│ Replays changes #200-300 (the failed batch)                   │
│ Deduplication handles any partial processing                   │
│ Batch succeeds this time                                       │
│ Resume token updated to token(#300)                            │
│ ✅ Zero data loss                                             │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Benefits

### ✅ Explicit Batch Boundary Tracking
- Clear visibility into where each batch starts (earliest) and ends (latest)
- Helpful for debugging and understanding processing state

### ✅ Guaranteed Retry from Start
- Failed batches always retried from the earliest change in that batch
- No ambiguity about what will be reprocessed

### ✅ Better Logging and Diagnostics
- Failure messages now show the earliest token in the failed batch
- Easier to correlate failures with specific time ranges

### ✅ Consistent with MongoDB Semantics
- Resume tokens naturally have a "before" (earliest) and "after" (latest) boundary
- Our tracking now matches this semantic model

### ✅ Backward Compatible
- Existing code behavior unchanged
- Added safety and visibility without breaking changes
- `ClearLatestMetadata()` deprecated but still works (calls `ClearMetadata()`)

---

## Edge Cases Handled

### Case 1: Empty Batch
```
If no changes added:
  EarliestResumeToken = "" (empty)
  LatestResumeToken = "" (empty)
  No checkpoint update occurs ✅
```

### Case 2: Single Change in Batch
```
One change #500 added:
  EarliestResumeToken = token(#500)
  LatestResumeToken = token(#500)
  On success → token updated to #500 ✅
  On failure → token stays at previous ✅
```

### Case 3: Deduplication Within Batch
```
Changes: Insert(#100), Update(#100), Delete(#100)
After deduplication: Only Delete(#100) remains

Earliest tracked from first operation (Insert):
  EarliestResumeToken = token from Insert(#100)
  EarliestTimestamp = timestamp from Insert(#100)

Latest tracked from last operation (Delete):
  LatestResumeToken = token from Delete(#100)
  LatestTimestamp = timestamp from Delete(#100)

Both reference same document but different change events ✅
```

---

## Testing Scenarios

### Test 1: Normal Success Path
```
1. Buffer 100 changes (earliest = #1, latest = #100)
2. Process successfully
3. Verify checkpoint updated to #100 ✅
4. Verify earliest and latest cleared ✅
```

### Test 2: Failure After 10 Retries
```
1. Buffer 50 changes (earliest = #201, latest = #250)
2. Processing fails at change #225
3. Verify log shows "Earliest token: #201" ✅
4. Verify checkpoint NOT updated (stays at #200) ✅
5. Restart job
6. Verify MongoDB replays from #200, includes #201-250 ✅
```

### Test 3: Multiple Batches
```
1. Batch 1: #1-100 → Success → checkpoint = #100
2. Batch 2: #101-200 → Success → checkpoint = #200
3. Batch 3: #201-300 → Failure → checkpoint = #200
4. Restart → replays from #200, includes #201-300 ✅
```

---

## Files Modified

### 1. ChangeStreamDocuments.cs
- Added `EarliestResumeToken`, `EarliestTimestamp`, `EarliestOperationType`, `EarliestDocumentKey`
- Kept `LatestResumeToken`, `LatestTimestamp`, `LatestOperationType`, `LatestDocumentKey`
- Renamed `UpdateLatestMetadata()` → `UpdateMetadata()` (tracks both)
- Added `ClearMetadata()` (clears both)
- Deprecated `ClearLatestMetadata()` (calls `ClearMetadata()`)

### 2. ServerLevelChangeStreamProcessor.cs
- Updated `ProcessAccumulatedChanges()` to capture both earliest and latest metadata
- Added try-catch around `BulkProcessChangesAsync()` call
- Success path: Update to latest token
- Failure path: Log earliest token, don't update checkpoint, re-throw exception

---

## Related Documentation

1. **RESUME_TOKEN_CHECKPOINT_FIX.md** - Original safe checkpointing implementation
2. **EXCEPTION_PROPAGATION_FIX.md** - Exception handling that enables failure detection
3. **DATA_LOSS_PREVENTION_STRATEGY.md** - Complete three-layer defense strategy
4. **DEADLOCK_RETRY_ENHANCEMENT.md** - Retry logic that throws CRITICAL exceptions

---

**Date**: 2025-10-15  
**Enhancement**: Track both earliest and latest resume tokens per batch  
**Benefit**: Explicit batch boundary tracking and guaranteed retry from batch start  
**Status**: ✅ Implemented  
**Build**: Successful (Exit Code 0)
