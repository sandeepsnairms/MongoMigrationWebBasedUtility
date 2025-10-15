# Log Level Update Progress Report

## Status: IN PROGRESS

### Completed Changes

#### 1. LogType Enum Updated ✅
**File**: `LogType.cs`
- Added `Warning = 1` level
- Updated hierarchy:
  - Error = 0
  - **Warning = 1 (NEW)**
  - Info = 2 (was 1)
  - Debug = 3 (was 2)
  - Verbose = 4 (was 3)

#### 2. UI Dropdown Updated ✅
**File**: `MigrationJobViewer.razor`
- Added "Warning + Error" option
- Updated all option labels to reflect new hierarchy

#### 3. MigrationWorker.cs - PARTIALLY COMPLETE ✅
**Lines Updated**: ~20 WriteLine calls updated
- Job lifecycle messages: **Info**
- Configuration warnings (simulated run, skip indexes): **Warning**
- Retry attempts: **Warning** (was Error)
- Pause operations: **Info**
- Collection counts/chunks: **Info**
- Debug progress: **Debug**
- Job start messages: **Info**
- Resume operations: **Info**

### Categorization Applied in MigrationWorker.cs

| Message Type | LogType | Example |
|--------------|---------|---------|
| Job started/completed | Info | "Job {id} started on {date}" |
| Configuration warnings | Warning | "Simulated Run. No changes will be made" |
| Retry attempts | Warning | "attempt {n} failed. Retrying..." |
| Collection processing | Info | "has {n} chunk(s)" |
| Change stream setup | Info | "Setting up server-level change stream" |
| Pause operations | Info | "operation was paused" |
| Debug info | Debug | "already exists on target and is ready" |

### Files Remaining to Update

#### High Priority (Core Processing):
1. **ServerLevelChangeStreamProcessor.cs** - ~40 WriteLine calls
   - Memory warnings, OOM handling, performance metrics
2. **CollectionLevelChangeStreamProcessor.cs** - ~30 WriteLine calls
   - Collection-level stream processing
3. **ChangeStreamProcessor.cs** - ~15 WriteLine calls
   - Base change stream operations
4. **DocumentCopyWorker.cs** - ~25 WriteLine calls
   - Document copying progress
5. **MongoHelper.cs** - ~35 WriteLine calls
   - Helper operations, index copying, change streams

#### Medium Priority:
6. **CopyProcessor.cs** - ~10 WriteLine calls
7. **RUCopyProcessor.cs** - ~15 WriteLine calls
8. **DumpRestoreProcessor.cs** - ~25 WriteLine calls
9. **MigrationProcessor.cs** - ~5 WriteLine calls
10. **SyncBackProcessor.cs** - ~5 WriteLine calls

#### Low Priority (Helpers/Utils):
11. **Helper.cs** - ~5 WriteLine calls
12. **ComparisonProcessor.cs** - ~8 WriteLine calls
13. **IndexCopier.cs** - ~2 WriteLine calls
14. **RetryHelper.cs** - ~1 WriteLine call
15. **MongoClientFactory.cs** - ~1 WriteLine call
16. **ProcessExecutor.cs** - ~5 WriteLine calls
17. **SamplePartitioner.cs** - ~3 WriteLine calls
18. **MongoChangeStreamProcessor.cs** - ~3 WriteLine calls
19. **ChangeStreamQueueManager.cs** - ~2 WriteLine calls

### Estimated Remaining Work
- **Total WriteLine calls to review**: ~230 calls
- **Completed**: ~20 calls (9%)
- **Remaining**: ~210 calls (91%)

### Next Steps
1. Update ServerLevelChangeStreamProcessor.cs (highest volume)
2. Update CollectionLevelChangeStreamProcessor.cs
3. Update ChangeStreamProcessor.cs
4. Continue with remaining files in priority order

### Build Status
✅ **All builds passing** - No compilation errors

### Log Level Assignment Strategy

**Error (0)**: 
- Exceptions with stack traces
- Fatal failures preventing execution
- Data integrity issues

**Warning (1)**:
- Retry attempts (non-critical failures)
- Resource constraints detected
- Fallback behaviors triggered
- Configuration issues
- Skipped operations

**Info (2)**:
- Job/migration start/stop/complete
- Major phase transitions
- Collection-level summaries
- Resume/pause events
- Important milestones

**Debug (3)**:
- Batch processing details
- Performance metrics (throughput, lag, memory)
- Chunk/segment progress
- Queue operations
- Counts and statistics

**Verbose (4)**:
- Individual document operations
- Auto-replay details
- Cache operations
- Internal state changes
- Detailed error context
