# LogType.Message Deprecation Plan

## Overview
The `LogType.Message` enum value has been deprecated in favor of `LogType.Info`. This document outlines the deprecation timeline and removal plan.

## Current Status (v0.9.3)
- ✅ `LogType.Message` marked with `[Obsolete]` attribute
- ✅ Compiler warnings guide developers to use `LogType.Info` instead
- ✅ Full backward compatibility maintained for old log files
- ✅ New logs automatically write as "Info" instead of "Message"

## Deprecation Timeline

### Phase 1: Deprecation (Current - v0.9.x)
**Status**: ACTIVE

**What's happening:**
- `Message` enum value exists at position 1 with `[Obsolete]` attribute
- Old log files with "Message" continue to work perfectly
- New code gets compiler warnings when using `Message`
- Documentation updated to recommend `Info` over `Message`

**Action Required:**
- None for existing deployments
- New code should use `Info` instead of `Message`

### Phase 2: Extended Deprecation (v0.10.x - v1.x)
**Target**: Next 2-3 major versions

**What will happen:**
- `Message` enum value remains in place
- Stronger deprecation warnings in documentation
- Migration tooling to convert old log files (optional)

**Recommended Actions:**
- Update any custom code still using `LogType.Message`
- Consider converting old archived log files to use "Info"
- Test applications with `Message` support disabled (future readiness)

### Phase 3: Removal (v2.0+)
**Target**: Future major version (breaking change)

**What will happen:**
- `Message` enum value completely removed from LogType enum
- New enum values:
  ```csharp
  public enum LogType
  {
      Error = 0,
      Warning = 1,  // Moves from 2 to 1
      Info = 2,     // Moves from 3 to 2
      Debug = 3,    // Moves from 4 to 3
      Verbose = 4   // Moves from 5 to 4
  }
  ```
- Old log files with "Message" will need migration tool
- Breaking change announcement in release notes

**Required Actions:**
- Run migration tool on old log files
- Update any legacy integrations
- Verify no code references `LogType.Message`

## Migration Tools (Future)

When approaching Phase 3 removal, we'll provide:

### 1. Log File Converter
```bash
# Convert old log files to new format
dotnet run --project LogMigrator -- convert --input old_logs/ --output new_logs/
```

This tool will:
- Replace `"Type":"Message"` with `"Type":"Info"` in JSON
- Replace `"Type":1` with `"Type":2` in JSON
- Preserve all other log data
- Create backup before conversion

### 2. Validation Tool
```bash
# Check if any log files still use deprecated Message
dotnet run --project LogMigrator -- validate --path logs/
```

This tool will:
- Scan log files for "Message" references
- Report files that need migration
- Provide conversion estimates

## Developer Guidance

### For New Code (NOW)
```csharp
// ❌ DON'T - Will trigger compiler warning
Log.WriteLine(LogType.Message, "User logged in");

// ✅ DO - Use Info instead
Log.WriteLine(LogType.Info, "User logged in");
```

### Handling Compiler Warnings
If you see warnings like:
```
warning CS0618: 'LogType.Message' is obsolete: 'Use Info instead. 
This value is kept only for backward compatibility with old log files.'
```

**Solution**: Replace `LogType.Message` with `LogType.Info`

### Suppressing Warnings (If Needed)
Only for maintenance of legacy code:
```csharp
#pragma warning disable CS0618 // Type or member is obsolete
var logType = LogType.Message; // Legacy code support
#pragma warning restore CS0618
```

**Note**: This should only be used temporarily during migration.

## FAQ

### Q: Will my old log files stop working?
**A:** No, not in the current version (v0.9.x) or near-term future versions. Old log files with "Message" will continue to work.

### Q: When will Message be removed?
**A:** Not before version 2.0, which is several releases away. We'll provide ample notice and migration tools.

### Q: What happens if I keep using LogType.Message in my code?
**A:** You'll get compiler warnings, but it will continue to work. However, new logs will be written as "Info" instead of "Message".

### Q: Do I need to update my code now?
**A:** It's recommended but not required. Update when convenient to avoid warnings.

### Q: Can I continue to read old archived logs?
**A:** Yes, old logs will remain readable through the converter even after Message is removed.

## Communication Plan

### Release Notes Template (for each version)

**v0.9.3:**
> ⚠️ **Deprecation Notice**: `LogType.Message` has been marked as obsolete. Please use `LogType.Info` instead. The Message enum value is kept for backward compatibility with old log files and will be removed in a future major version.

**v1.0.0:**
> ⚠️ **Reminder**: `LogType.Message` remains deprecated. Plan to migrate before version 2.0. New logs automatically use "Info" instead of "Message".

**v2.0.0 (Future):**
> 🔴 **Breaking Change**: `LogType.Message` has been removed. Use the provided migration tool to convert old log files. See LOGTYPE_MESSAGE_DEPRECATION_PLAN.md for details.

## Tracking

- **Deprecated**: v0.9.3 (October 2025)
- **Planned Removal**: v2.0.0 (TBD - estimated 12-18 months minimum)
- **Migration Tool**: To be developed before v2.0.0

## References

- [LOGTYPE_BACKWARD_COMPATIBILITY.md](./LOGTYPE_BACKWARD_COMPATIBILITY.md) - Current implementation details
- [LogType.cs](./OnlineMongoMigrationProcessor/Models/LogType.cs) - Enum definition
- [LogTypeConverter.cs](./OnlineMongoMigrationProcessor/Models/LogTypeConverter.cs) - Conversion logic
