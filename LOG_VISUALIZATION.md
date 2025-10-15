# Log Visualization in MigrationJobViewer

## Overview
The MigrationJobViewer now displays logs with distinct colors and icons for each LogType, making it easier to quickly identify different severity levels.

## Visual Indicators

### Icons (Bootstrap Icons)
Each log type has a unique icon displayed before the message:

| LogType  | Icon                          | Bootstrap Class              |
|----------|-------------------------------|------------------------------|
| Error    | ❌ Circle X (filled)          | `bi-x-circle-fill`          |
| Warning  | ⚠️ Triangle Exclamation       | `bi-exclamation-triangle-fill` |
| Info     | ℹ️ Info Circle (filled)      | `bi-info-circle-fill`       |
| Debug    | 🐛 Bug (filled)               | `bi-bug-fill`               |
| Verbose  | 💬 Chat Dots (filled)         | `bi-chat-dots-fill`         |

### Colors
Each log type has a distinct color for quick visual scanning:

| LogType  | Color                    | Hex Code  | Visual Appearance          |
|----------|--------------------------|-----------|----------------------------|
| Error    | Bright Red               | `#ff4444` | **Bold** and attention-grabbing |
| Warning  | Orange/Amber             | `#ffaa00` | **Bold** and cautionary    |
| Info     | Light Blue               | `#44aaff` | Clear and informative      |
| Debug    | Green                    | `#44ff44` | Technical and diagnostic   |
| Verbose  | Light Gray               | `#cccccc` | Subtle and detailed        |

## UI Implementation

### Log Display Format

**Main Log Table:**
```
[Icon Column] | [Datetime] | [Message]
    ❌        | 2025-10-15 14:30:22 | Database connection failed
    ⚠️        | 2025-10-15 14:30:25 | Retry attempt 3/5
    ℹ️        | 2025-10-15 14:30:28 | Migration started for collection users
    🐛        | 2025-10-15 14:30:30 | Processing batch 1000/5000 documents
    💬        | 2025-10-15 14:30:31 | Cache hit for document xyz123
```

**Table Structure:**
- **Icon Column**: Leftmost column (30px width), center-aligned, displays colored icon
- **DateTime Column**: Shows UTC timestamp (150px width)
- **Message Column**: Displays the log message (auto-width)

### Where Applied
The visual indicators appear in:
1. **Main Log Table** - Shows last 200 log entries with full formatting
2. **Monitor Section** - Verbose messages display with icons and colors
3. **Archived Logs** - First 30 entries when log count exceeds 250

## Technical Details

### CSS Classes
- `.error-row` - Red color (#ff4444), font-weight 500
- `.warning-row` - Orange color (#ffaa00), font-weight 500
- `.info-row` - Light blue color (#44aaff)
- `.debug-row` - Green color (#44ff44)
- `.verbose-row` - Light gray color (#cccccc)
- `.log-icon-cell` - Icon column styling (30px width, center-aligned, vertical-middle)
- `.log-icon-header` - Icon column header (30px width, minimal padding)

### Methods
- `GetRowClass(string type, string message)` - Returns appropriate CSS class based on LogType
- `GetLogIcon(LogType logType)` - Returns MarkupString with Bootstrap icon HTML

## Benefits

1. **Quick Scanning**: Instantly identify critical errors (red) vs informational messages (blue)
2. **Visual Hierarchy**: Bold colors for important messages (Error, Warning)
3. **Icon Recognition**: Icons in dedicated column provide clear visual cue at a glance
4. **Accessibility**: Multiple visual indicators (color + icon) help users distinguish log types
5. **Professional Appearance**: Modern, polished look with consistent design system
6. **Clean Layout**: Separate icon column keeps the log message clean and uncluttered
7. **Easy Alignment**: Icons are center-aligned in their own column for perfect visual alignment

## Log Level Filtering
Combined with the log level dropdown, users can:
- View only errors (red entries)
- View warnings and errors (orange + red)
- View info, warnings, and errors (blue + orange + red)
- View debug messages (green + previous levels)
- View all verbose logs (gray + all levels)

The visual distinction makes it easy to see which messages pass through each filter level.
