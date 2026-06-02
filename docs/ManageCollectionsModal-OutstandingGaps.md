# Outstanding Implementation Gaps

**Document Status:** Tracking remaining work for ManageCollectionsModal  
**Last Updated:** June 2, 2026

---

## High-Priority Gaps

### 1. ShardingStrategy & MoveToShard Worker Consumption

**Status:** 🔴 Not Implemented  
**Section:** 10.3  
**Severity:** HIGH

**Description:**
The four new per-unit fields are persisted on `MigrationUnit`, but only `Overwrite` and `IndexingStrategy` are consumed by the worker. `ShardingStrategy` and `MoveToShard` are saved but not read during target collection creation.

**Impact:**
- Users can set these options in the UI
- Values are saved to the job definition
- But the worker ignores them when creating target collections

**Implementation Required:**

1. **Target Collection Creation Logic**
   - Location: Worker code that creates target collections
   - Check: `mu.ShardingStrategy`
     - If `DontShard`: Create unsharded collection
     - If `SameAsSource` or null: Preserve source sharding configuration
   - Check: `mu.MoveToShard`
     - If set and `ShardingStrategy == DontShard`: Route collection to specified shard/node
     - If null: Use automatic placement

2. **vCore Sharding Commands**
   ```csharp
   // For vCore with MoveToShard specified:
   var targetDb = client.GetDatabase(mu.GetEffectiveTargetDatabaseName());
   var command = new BsonDocument
   {
       { "create", mu.GetEffectiveTargetCollectionName() },
       { "shardCollection", false },
       { "targetShard", mu.MoveToShard }
   };
   await targetDb.RunCommandAsync<BsonDocument>(command);
   ```

3. **Native Sharded Clusters**
   ```csharp
   // For native sharded with MoveToShard:
   // Use movePrimary command or zone sharding configuration
   ```

**Files to Modify:**
- `OnlineMongoMigrationProcessor/Workers/MigrationWorker.cs`
- Possibly `OnlineMongoMigrationProcessor/Helpers/Mongo/MongoHelper.cs`

**Testing:**
- vCore: Set collection to DontShard + specific shard → verify created on correct shard
- Native sharded: Set to DontShard → verify unsharded collection created
- Simulated run: Verify sharding options ignored (double-check)

---

## Medium-Priority Gaps

### 2. Simulated-Run Double-Check for Sharding

**Status:** 🟡 Partial  
**Section:** 10.3  
**Severity:** MEDIUM

**Description:**
The UI disables Indexing/Sharding/Move-to controls in simulated mode, and the worker helper methods skip them. However, there's no explicit server-side double-check that rejects these values if they somehow get set (e.g., via API manipulation).

**Implementation Required:**

Add to `ApplyChanges()` in `ManageCollectionsModal.razor.cs`:

```csharp
private async Task ApplyChanges()
{
    _isApplying = true;
    _error = null;

    try
    {
        // Simulated-run double-check
        if (MigrationJob.IsSimulatedRun)
        {
            foreach (var draft in _drafts)
            {
                if (draft.IndexingStrategy.HasValue || 
                    draft.ShardingStrategy.HasValue || 
                    !string.IsNullOrWhiteSpace(draft.MoveToShard))
                {
                    _error = "Indexing, sharding, and move-to options are not allowed in simulated run mode.";
                    return;
                }
            }
        }

        // ... rest of apply logic
```

**Testing:**
- Simulated job: Try to force-set these values → should be rejected

---

### 3. Sync-Back Resume Token Cleanup

**Status:** 🟡 Not Implemented  
**Section:** 10.3  
**Severity:** MEDIUM

**Description:**
When removing a collection from a running migration, the change-stream resume token should be cleaned up. Currently, the existing warning message is shown, but no explicit token cleanup is triggered.

**Implementation Required:**

In `ApplyChanges()` method, after removing collections:

```csharp
// Step 1: Remove collections
foreach (var id in _toRemoveIds)
{
    var unit = _liveUnits.FirstOrDefault(u => u.Id == id);
    if (unit != null)
    {
        // Clear change-stream state before removal
        unit.ResumeToken = null;
        unit.OriginalResumeToken = null;
        unit.CSLastResumeTokenWithChange = null;
        unit.CSLastChangeUTCTime = null;
        
        if (MigrationJob.SyncBackEnabled)
        {
            unit.SyncBackResumeToken = null;
            unit.SyncBackOriginalResumeToken = null;
            unit.SyncBackCSLastResumeTokenWithChange = null;
            unit.SyncBackCSLastChangeUTCTime = null;
        }
        
        unit.Persist();
        unit.Remove();
    }
}
```

**Testing:**
- Online job with change stream: Remove a collection → verify resume token cleared

---

## Deferred (Low-Priority)

### 4. Upload File Button
**Status:** ⚪ Deferred  
**Reason:** Manual paste works fine; nice-to-have for bulk imports

### 5. Filter Hover-to-Expand
**Status:** ⚪ Deferred  
**Reason:** Filters are visible when short; truncation acceptable for long ones

### 6. Simulated-Run Helper Text
**Status:** ⚪ Deferred  
**Reason:** Disabled state is clear; explanatory text is nice-to-have

### 7. GetClusterNodesAsync Automated Tests
**Status:** ⚪ Deferred  
**Reason:** Manual testing sufficient for v1; unit tests can be added later

---

## Completed Gaps

### ✅ Cancel Confirmation Dialog
**Status:** DONE  
Integrated `YesNoDialog` for Cancel with pending changes.

### ✅ Conflict Row Highlighting
**Status:** DONE (already implemented)  
Lines 206 and 227 apply `table-warning` class to conflicting rows.

### ✅ Filter JSON Validation
**Status:** DONE  
Added client-side JSON parsing validation in `AddCollections()` and `SaveEdit()`.

### ✅ Duplicate Collections Warning
**Status:** DONE  
Shows inline error message when duplicate namespace is detected.

---

## Priority Recommendation

**For MVP / v1:**
1. ✅ Cancel confirmation (done)
2. ✅ Filter validation (done)
3. ✅ Duplicate warning (done)
4. 🔴 ShardingStrategy consumption (HIGH - users expect this to work)
5. 🟡 Simulated-run double-check (MEDIUM - security/consistency)

**For v1.1:**
6. 🟡 Sync-back token cleanup (MEDIUM - correctness for online jobs)
7. ⚪ Upload file (LOW - nice-to-have)
8. ⚪ Helper text polish (LOW - UX improvement)

---

## Notes

- All backend models, helpers, and UI components are complete
- Worker wiring for Overwrite and IndexingStrategy is done
- Sharding integration requires deeper understanding of target cluster setup logic
- Consider consulting MongoDB documentation for vCore shard placement commands
