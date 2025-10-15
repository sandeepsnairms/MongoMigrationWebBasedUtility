# Deadlock Fix Verification Guide

## Overview
This document explains how to verify that the deadlock retry enhancement properly stops the migration job when persistent deadlocks occur.

## What Changed

### Exception Propagation Flow

**Before the Fix:**
```
MongoHelper.ProcessUpdatesAsync
  → Catches deadlock after 3 retries
  → Logs "Skipping batch" 
  → Throws exception
  → BulkProcessChangesAsync catches and logs
  → ProcessQueuedChangesAsync catches and returns 0
  → ProcessCollectionChangeStream catches and logs
  → Job CONTINUES (DATA LOSS!)
```

**After the Fix:**
```
MongoHelper.ProcessUpdatesAsync
  → Retries 10 times with exponential backoff
  → Throws InvalidOperationException("CRITICAL: ... persistent deadlock ...")
  → BulkProcessChangesAsync catches CRITICAL exception and RE-THROWS
  → ProcessQueuedChangesAsync catches CRITICAL exception and RE-THROWS  
  → ProcessCollectionChangeStream catches CRITICAL exception and RE-THROWS
  → Task.WhenAll propagates exception
  → Job TERMINATES (NO DATA LOSS!)
```

## Files Modified

| File | Purpose |
|------|---------|
| `MongoHelper.cs` | Implements 10 retries with exponential backoff; throws critical exception if all fail |
| `ChangeStreamProcessor.cs` | Re-throws critical exceptions from BulkProcessChangesAsync |
| `CollectionLevelChangeStreamProcessor.cs` | Re-throws critical exceptions from ProcessQueuedChangesAsync and ProcessCollectionChangeStream |

## Expected Behavior

### Scenario 1: Transient Deadlock (Resolves Within 10 Retries)
**What happens:**
1. Deadlock detected on first attempt
2. Waits 500ms, retries
3. Deadlock detected on second attempt
4. Waits 1000ms, retries
5. Third attempt succeeds ✅

**Log output:**
```
[Warning] Deadlock detected for Accounts.BR-BeesAccounts. Retry 1/10 after 500ms...
[Warning] Deadlock detected for Accounts.BR-BeesAccounts. Retry 2/10 after 1000ms...
```

**Result:** Migration continues successfully. No data loss.

---

### Scenario 2: Persistent Deadlock (Fails After 10 Retries)
**What happens:**
1. Deadlock detected and retried 10 times
2. All retries fail with exponential backoff:
   - 500ms, 1s, 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s
3. After 10th retry fails, throws critical exception
4. Exception propagates through all layers
5. **Migration job STOPS** 🛑

**Log output:**
```
[Warning] Deadlock detected for Accounts.BR-BeesAccounts. Retry 1/10 after 500ms...
[Warning] Deadlock detected for Accounts.BR-BeesAccounts. Retry 2/10 after 1000ms...
[Warning] Deadlock detected for Accounts.BR-BeesAccounts. Retry 3/10 after 2000ms...
...
[Warning] Deadlock detected for Accounts.BR-BeesAccounts. Retry 10/10 after 256000ms...
[ERROR] CRITICAL ERROR: Deadlock persisted after 10 retries for Accounts.BR-BeesAccounts. Cannot proceed as this would result in DATA LOSS. Batch size: 50 documents. JOB MUST BE STOPPED AND INVESTIGATED!
[ERROR] CRITICAL FAILURE: Stopping job due to persistent deadlock that would cause data loss. Details: CRITICAL: Unable to process update batch for Accounts.BR-BeesAccounts after 10 retry attempts due to persistent deadlock...
[ERROR] CRITICAL FAILURE processing change stream for Accounts.BR-BeesAccounts. Stopping job to prevent data loss...
```

**Exception thrown:**
```
InvalidOperationException: CRITICAL: Unable to process update batch for Accounts.BR-BeesAccounts after 10 retry attempts due to persistent deadlock. Data loss prevention requires job termination. Please investigate database locks and retry the migration.
```

**Result:** Migration job terminates. Administrator must investigate and resolve deadlock issue.

---

## Verification Steps

### Manual Testing

1. **Monitor logs during migration:**
   ```powershell
   # Watch for Warning level deadlock retries
   Select-String -Path ".\logs\*.log" -Pattern "Deadlock detected"
   
   # Watch for CRITICAL errors
   Select-String -Path ".\logs\*.log" -Pattern "CRITICAL ERROR"
   ```

2. **Check job status after CRITICAL error:**
   - Job should be in **Failed** or **Stopped** state
   - Resume token should be preserved for safe restart
   - No batches should be silently skipped

3. **Verify no data loss:**
   - After fixing deadlock issue, restart migration
   - It should resume from last successful resume token
   - All documents should eventually be migrated

### Unit Test Scenarios

Create tests to simulate:
1. ✅ Transient deadlock that resolves on retry #3
2. ✅ Persistent deadlock that fails after 10 retries
3. ✅ Exception propagation through all layers
4. ✅ Job termination on critical failure

### Integration Test

1. Create a test environment with intentional deadlock conditions
2. Start migration
3. Verify:
   - Retries occur with exponential backoff
   - Job stops after 10 failures
   - CRITICAL error message is logged
   - Job status reflects failure

---

## Troubleshooting

### If Job Doesn't Stop After 10 Retries

**Check:**
1. Verify all files are updated with latest code
2. Check if exception is being caught elsewhere
3. Review logs for exception handling

**Expected Exception Type:**
```csharp
InvalidOperationException
```

**Expected Message Pattern:**
```
CRITICAL: Unable to process [insert|update|delete] batch for {collection} after 10 retry attempts due to persistent deadlock
```

### If Retries Don't Use Exponential Backoff

**Verify in logs:**
- Retry 1: ~500ms delay
- Retry 2: ~1000ms delay
- Retry 3: ~2000ms delay
- Retry 4: ~4000ms delay
- ...
- Retry 10: ~256000ms delay

---

## Resolving Persistent Deadlocks

If the job stops due to persistent deadlocks:

### 1. Investigate Database Locks
```javascript
// MongoDB - Check current locks
db.currentOp({
  $or: [
    { "locks.^Database": { $exists: true } },
    { waitingForLock: true }
  ]
})

// Find long-running operations
db.currentOp({
  "active": true,
  "secs_running": { "$gt": 60 }
})
```

### 2. Reduce Concurrency
In `appsettings.json`:
```json
{
  "MigrationSettings": {
    "ConcurrentProcessors": 2,  // Reduce from 5 to 2
    "ChangeStreamMaxDocsInBatch": 25  // Reduce from 50 to 25
  }
}
```

### 3. Check Target Database Resources
- CPU utilization
- Memory usage
- IOPS/throughput
- Connection pool size

### 4. Review Indexes
Ensure target database indexes match source to prevent lock escalation during writes.

### 5. Retry Migration
Once deadlock issue is resolved:
1. Verify job preserved resume token
2. Restart migration from UI
3. Job will resume from last successful position
4. Monitor for successful completion

---

## Success Criteria

✅ **Transient deadlocks:** Automatically recovered with retries  
✅ **Persistent deadlocks:** Job stops with clear error message  
✅ **No silent data loss:** All batches either succeed or job fails visibly  
✅ **Resume capability:** Job can be restarted from last resume token  
✅ **Clear diagnostics:** Logs show retry attempts and failure reasons  

---

## Summary

The deadlock retry enhancement ensures **ZERO DATA LOSS** by:
1. Retrying deadlocks 10 times with exponential backoff
2. Stopping the entire job if retries are exhausted
3. Preserving resume tokens for safe restart
4. Providing clear error messages for troubleshooting

**The migration will NEVER silently skip batches due to deadlocks!** 🎉
