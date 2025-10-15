# Log Level Categorization Plan

## LogType Hierarchy (Severity: Low to High)
- **Error (0)**: Critical failures, exceptions that prevent operation
- **Warning (1)**: Important issues that don't stop execution but need attention
- **Info (2)**: Key milestone messages (job started/completed, major phase transitions)
- **Debug (3)**: Detailed operational information (batch processing, counts, performance metrics)
- **Verbose (4)**: Granular detail (individual operations, auto-replay, cache operations)

## Categorization Guidelines

### **Error** - Use for:
- Exceptions and failures
- Operations that cannot proceed
- Data integrity issues
- Critical resource problems (OOM, disk space)

### **Warning** - Use for:
- Potential issues that don't stop execution
- Fallback behavior triggered
- Resource constraints detected (not critical)
- Configuration issues
- Skipped operations that might matter

### **Info** - Use for:
- Job/migration start/stop/completion
- Major phase transitions (starting change streams, completing chunks)
- Collection-level status updates
- Resume/pause operations
- Important counts and summaries

### **Debug** - Use for:
- Batch processing details
- Performance metrics (throughput, lag)
- Memory usage reports
- Chunk/segment progress
- Queue operations
- Retry attempts

### **Verbose** - Use for:
- Individual document operations
- Auto-replay details
- Cache operations
- Detailed error context
- Internal state changes
- Individual index operations

## Files to Update (in priority order)

1. **MigrationWorker.cs** - Core job lifecycle
2. **ServerLevelChangeStreamProcessor.cs** - High-volume change stream
3. **CollectionLevelChangeStreamProcessor.cs** - Collection-level streams
4. **ChangeStreamProcessor.cs** - Base change stream logic
5. **DocumentCopyWorker.cs** - Document copying
6. **MongoHelper.cs** - Helper operations
7. **CopyProcessor.cs** - Copy operations
8. **RUCopyProcessor.cs** - RU-specific copying
9. **DumpRestoreProcessor.cs** - Dump/restore operations
10. **Other files** - Supporting helpers

## Default LogType Assignment Strategy

When a WriteLine call has NO LogType specified:
- If it contains "Error", "Exception", "Failed", "Cannot" → **Error**
- If it contains "Warning", "High memory", "Reducing", "Fallback" → **Warning** or **Debug**
- If it's a start/completion/milestone → **Info**
- If it's progress/metrics/counts → **Debug**
- If it's detailed operations → **Verbose**

## Important Info Messages (Keep as Info)

- Job started/completed
- Migration phase started/completed
- Change stream started/paused/completed
- Collection processing started/completed
- Resume operations
- Major configuration messages
- Index copy completion
- Chunk completion summaries
