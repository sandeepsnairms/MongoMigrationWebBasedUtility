# LogType Backward Compatibility

## Problem
Old log files contain `LogType=Message` (the previous name for informational messages). With the addition of a new `LogType=Warning` level, we needed a solution to maintain compatibility with existing log files.

## Solution
Re-added the deprecated `Message` enum value at position 1 in the LogType enum. This provides the cleanest backward compatibility:
- Old log files can be read without any conversion
- Both string ("Message") and numeric (1) representations work automatically
- The `Message` value is marked as `[Obsolete]` and will be removed in a future version
- New logs automatically write as "Info" instead of "Message"

### Changes Made

1. **Re-added LogType.Message at position 1**
   - Marked with `[Obsolete]` attribute to discourage new usage
   - Keeps numeric value 1 for perfect backward compatibility
   - Documented as deprecated in XML comments

2. **Updated LogObject.cs**
   - Applied `[JsonConverter(typeof(LogTypeConverter))]` attribute to the Type property
   - Ensures automatic conversion when reading/writing JSON

3. **Created LogTypeConverter.cs**
   - Automatically converts `Message` to `Info` when reading old logs
   - Writes `Info` instead of `Message` for new logs
   - Handles both string and numeric JSON representations

### How It Works

**Reading old log files** (both formats work automatically):
```json
// Old format with string
{"Message":"Test","Type":"Message","Datetime":"2025-10-15T10:00:00Z"}

// Old format with numeric value
{"Message":"Test","Type":1,"Datetime":"2025-10-15T10:00:00Z"}

// Both automatically converted to:
LogType.Info  // For filtering and display purposes
```

**Reading new log files**:
```json
// New format
{"Message":"Test","Type":"Info","Datetime":"2025-10-15T10:00:00Z"}

// Correctly parsed as:
LogType.Info
```

**Writing new log files**:
```csharp
// Even if code uses deprecated Message enum value
new LogObject(LogType.Message, "Test");

// Always writes as "Info" in JSON:
{"Message":"Test","Type":"Info","Datetime":"..."}
```

### Enum Mapping

**Current LogType values:**
```csharp
public enum LogType
{
    Error = 0,
    Message = 1,    // [DEPRECATED] Kept for backward compatibility
    Warning = 2,    // NEW - added between Message and Info
    Info = 3,       // Replaces Message for new logs
    Debug = 4,
    Verbose = 5
}
```

**Key Points:**
- `Message = 1` is kept in the enum but marked as `[Obsolete]`
- Old log files with `Message` (string or numeric 1) work seamlessly
- New code should use `Info` instead of `Message`
- Compiler warnings guide developers away from using `Message`
- `Message` will be removed in a future major version

**Why this approach is better:**
1. **No complex conversion logic** - enum naturally handles both string and numeric
2. **Perfect backward compatibility** - old files work without modification
3. **Clean migration path** - `[Obsolete]` attribute warns about deprecated usage
4. **Simple to remove later** - just delete the enum value when ready

### Testing

Created comprehensive unit tests in `LogTypeConverterTests.cs`:
- ✅ Old "Message" string value reads and converts to Info
- ✅ Old numeric value `1` (Message) reads and converts to Info
- ✅ New "Info" string value parses correctly
- ✅ All LogType values (Error, Warning, Debug, Verbose) work correctly
- ✅ Serialization writes "Info" even if deprecated Message enum used
- ✅ Deprecated Message always treated as Info internally

All 9 tests pass successfully.

### Benefits

1. **Simplicity**: No complex numeric mapping logic needed
2. **Backward Compatibility**: Old files work perfectly without modification
3. **Forward Compatibility**: New logs always use "Info"
4. **Developer Guidance**: `[Obsolete]` attribute warns about deprecated usage
5. **Clean Migration**: Easy to remove `Message` enum value in future version
6. **Type Safety**: Enum handling is automatic via standard JSON serialization

### Usage

No changes required in existing code - the converter is automatically applied when deserializing `LogObject` instances:

```csharp
// This automatically handles both old "Message" and new "Info" values
var logObject = JsonSerializer.Deserialize<LogObject>(json);
```
