# Manage Collections Wizard — Design & Implementation Plan

Status: Draft / proposal
Owner: TBD
Target branch: `0.9.7`
Related files referenced throughout:
[MongoMigrationWebApp/Components/ManageCollections.razor](../MongoMigrationWebApp/Components/ManageCollections.razor),
[MongoMigrationWebApp/Pages/MigrationJobViewer.razor](../MongoMigrationWebApp/Pages/MigrationJobViewer.razor),
[OnlineMongoMigrationProcessor/Helpers/Mongo/MongoHelper.cs](../OnlineMongoMigrationProcessor/Helpers/Mongo/MongoHelper.cs),
[OnlineMongoMigrationProcessor/Models/CollectionInfo.cs](../OnlineMongoMigrationProcessor/Models/CollectionInfo.cs),
[OnlineMongoMigrationProcessor/Models/MigrationUnit.cs](../OnlineMongoMigrationProcessor/Models/MigrationUnit.cs),
[OnlineMongoMigrationProcessor/Helpers/Helper.cs](../OnlineMongoMigrationProcessor/Helpers/Helper.cs).

---

## 1. Goal

Introduce a new **3‑step Manage Collections Wizard** that replaces the current single‑modal flow for editing the collection list of a running/paused migration job. The wizard must:

1. Make adding collections richer (per‑collection overwrite / indexing / sharding / placement / filter controls) without overwhelming the user with extra columns.
2. Keep the remove flow intact (with search, pagination, multi‑select).
3. Show a clear summary + confirmation before any change is committed.
4. Respect simulated‑run mode (some controls must be disabled).
5. Re‑use the existing visual style, pagination helper, and validation paths.

The current modal lives in [ManageCollections.razor](../MongoMigrationWebApp/Components/ManageCollections.razor). It is invoked from the `Update Collections` button in [MigrationJobViewer.razor](../MongoMigrationWebApp/Pages/MigrationJobViewer.razor) (line ~419).

---

## 2. Entry point

Add a sibling button next to `Update Collections` in [MigrationJobViewer.razor](../MongoMigrationWebApp/Pages/MigrationJobViewer.razor#L419):

```razor
<button class="btn btn-primary mx-2"
        title="Add / remove collections with per‑collection options (indexing, sharding, placement, filter)."
        @onclick="OpenManageCollectionsWizard"
        disabled="@_isLoadingCollections">
    Temp
</button>
```

- Label `Temp` is per the spec sheet — to be renamed once the feature is finalized (suggestion: `Manage Collections (Advanced)`).
- Same enable/disable rules as `Update Collections`: only visible when `IsPauseEnabled() || IsResumeEnabled()` is true, and disabled while loading.
- `OpenManageCollectionsWizard()` sets a `_showManageCollectionsWizard` flag and `StateHasChanged()`.

A new component `ManageCollectionsWizard.razor` is rendered conditionally below the existing `ManageCollections` modal block:

```razor
@if (_showManageCollectionsWizard && CurrentlyLoadedMigrationJob != null)
{
    <ManageCollectionsWizard MigrationJob="CurrentlyLoadedMigrationJob"
                             MigrationUnits="CurrentlyLoadedMigrationJob.MigrationUnits"
                             SourceConnectionString="@_sourceConnectionString"
                             TargetConnectionString="@_targetConnectionString"
                             OnCollectionsCommitted="HandleWizardCommitted"
                             OnCancelled="() => _showManageCollectionsWizard = false" />
}
```

`HandleWizardCommitted` runs the same persistence path as the existing flow (save job, reload migration units, re-render).

---

## 3. New component: `ManageCollectionsWizard.razor`

Location: [MongoMigrationWebApp/Components/ManageCollectionsWizard.razor](../MongoMigrationWebApp/Components/ManageCollectionsWizard.razor) (new file).

### 3.1 Parameters

| Parameter | Type | Notes |
| --- | --- | --- |
| `MigrationJob` | `MigrationJob` | Required. Same usage as existing modal. |
| `MigrationUnits` | `List<MigrationUnit>` | Current set of units in the job. |
| `SourceConnectionString` | `string` | For wildcard expansion & validation. |
| `TargetConnectionString` | `string` | Required for **Step 1** `Move to` dynamic list (cluster nodes come from target). |
| `OnCollectionsCommitted` | `EventCallback<WizardResult>` | Fires once user confirms. Result carries the new `MigrationUnit` list and the list of removed unit IDs. |
| `OnCancelled` | `EventCallback` | Fires on Cancel / close. |

### 3.2 Shell layout

Re‑use the same `modal modal-lg` shell + styling as [ManageCollections.razor](../MongoMigrationWebApp/Components/ManageCollections.razor) so the look stays consistent. Header shows the wizard title and step indicator:

```
[ 1. Add Collections ] ── [ 2. Remove Collections ] ── [ 3. Summary ]
```

Footer:
- Left: `Cancel`
- Right: `Back` (hidden on step 1), `Next` (steps 1 & 2), `Confirm` (step 3 only, primary).

State held in code-behind:
```csharp
private int _step = 1;
private readonly List<PendingAddition> _toAdd = new();
private readonly HashSet<string> _toRemoveIds = new();
private List<string> _clusterNodes = new();
private bool _loadingNodes;
private string? _error;
```

---

## 4. Step 1 — Add Collections

### 4.0 Pre-step — Connection strings (only if missing)

Before the user can add anything, both the source and target connection strings must be available. In most cases they are already on the loaded `MigrationJob` (passed in via parameters) and this pre-step is silently skipped. If either is missing — e.g. the user opened a job from disk whose connection strings were not persisted, or the wizard was launched from a context that doesn't have them — the wizard renders a small **Connections** panel at the top of Step 1 before the card stack:

```
┌─────────────────────────────────────────────────────────────┐
│  Connections                                                │
│  Source connection string  [ password-masked input ]        │
│  Target connection string  [ password-masked input ]        │
│  [ Test & Continue ]                                        │
└─────────────────────────────────────────────────────────────┘
```

Behaviour:
- Fields are pre-filled with whatever the wizard already has (may be empty).
- Only the missing string(s) are shown as editable; if only the target is missing, only the target field renders.
- Inputs are `type="password"` so the strings are masked, matching the create-job page convention.
- `Test & Continue` runs the same lightweight ping each modal uses today (open client → `ListDatabaseNames` with a short timeout). On success, the wizard caches the strings into `SourceConnectionString` / `TargetConnectionString`, hides the panel, and unlocks the rest of Step 1. On failure, an inline error appears under the field.
- Until both strings are present and verified, the **Namespace / Add** form and the **Move to** dropdown stay disabled (the latter because it needs the target client to call `GetClusterNodesAsync`).
- The strings are held in component-local state only; they are not persisted to disk by the wizard. Persistence remains the responsibility of the existing job-save path when the user clicks Confirm.

### 4.1 List of pending additions (card stack)

Replace the multi‑column table with a vertical stack of **sleek cards**, one per pending entry. Each card shows:

```
┌─────────────────────────────────────────────────────────────┐
│  SalesDB.Customers                                    [✕]   │
│  [Overwrite] [Indexing: Same as source] [Sharding: Don't]   │
│  [Move to: Auto] [Filter: {status:"active"}]                │
└─────────────────────────────────────────────────────────────┘
```

- `[✕]` removes the entry from the pending list.
- Tags use Bootstrap badges (`badge rounded-pill bg-secondary` etc.) so they wrap nicely on narrow widths.
- **One card per resolved namespace.** When the user pastes a `CollectionInfo` JSON array, the wizard renders **one card per array element** — each `{ DatabaseName, CollectionName, Filter, ... }` becomes its own card, with the per-group form options (Overwrite / Indexing / Sharding / Move-to) applied uniformly to every card produced by that single `Add` click. Wildcard expansion (`db.*`, `*.col`, `*.*`) follows the same rule: one card per resolved `db.col` pair.
- If no entries yet: show empty state `No collections queued. Use the form below to add one.`

### 4.2 Form below the card stack

Fields (rendered as a simple two‑column form‑grid):

| Field | Control | Values / behaviour |
| --- | --- | --- |
| **Namespace / CollectionInfo JSON** | `<textarea>` + “Upload” button | Same widget as [ManageCollections.razor](../MongoMigrationWebApp/Components/ManageCollections.razor) (`newCollection`, file upload). Accepts comma‑separated `db.col` pairs, wildcards (`*.col`, `db.*`, `*.*`), or [CollectionInfo JSON](../CollectionInfoFormat.JSON). |
| **Overwrite** | Dropdown | `TRUE` / `FALSE`. Default `FALSE`. When `TRUE`, target collection (if it exists) will be dropped and re‑created. Drives the defaults of the next two fields. |
| **Indexing Strategy** | Dropdown | `Same as source` / `Don't Index`. **Enabled only when Overwrite = TRUE.** When Overwrite = FALSE, this is locked to `Same as source` (no drop ⇒ existing indexes kept). |
| **Sharding Strategy** | Dropdown | `Same as source` / `Don't Shard`. Default = `Same as source` when Overwrite = TRUE; locked to `Same as source` when Overwrite = FALSE. |
| **Move to** | Dropdown | `Auto` / **Dynamic list** of replica node IDs from `GetClusterNodes(targetClient)`. **Enabled only when Sharding Strategy = `Don't Shard`.** When `Don't Shard` is selected, the user can pin the collection to a single physical/logical shard. |
| **Filter** | TextBox | Optional. Must be valid BSON if provided. Blank = no filter. |

#### Conditional matrix

| Overwrite | Indexing enabled? | Sharding enabled? | Move to enabled? |
| --- | --- | --- | --- |
| FALSE | No (locked to `Same as source`) | No (locked to `Same as source`) | No (locked to `Auto`) |
| TRUE  | Yes | Yes | Only when Sharding = `Don't Shard` |

#### Simulated‑run mode

When `MigrationJob.IsSimulatedRun == true`, **disable**:
- Indexing Strategy
- Sharding Strategy
- Move to

These dropdowns render disabled with helper text `Disabled in simulated run`. The Filter, Overwrite, and Namespace fields remain enabled (filter still drives the simulation; Overwrite still affects whether index/shard work is queued, even if simulated).

The same gating is checked in `MigrationJob.IsSimulatedRun` server‑side before persisting, so a tampered UI can never push these values for a simulated job.

### 4.3 Wildcard / JSON expansion

`Add` button behaviour:
1. Run `Helper.ValidateNamespaceFormat(...)` (same as today).
2. If JSON, deserialize into `List<CollectionInfo>` via `Helper` JSON path. Each entry becomes one card.
3. If wildcard / comma list, expand against the source via the same path the existing modal uses (`AddCollectionAsync` in [ManageCollections.razor](../MongoMigrationWebApp/Components/ManageCollections.razor#L260)).
4. For every resolved `db.col`:
   - Build a `PendingAddition` record (see section 6).
   - Skip if already in `_toAdd` (compare by `Database.Collection`); show a warning toast for duplicates.
5. After expansion, clear the textarea and append the new cards.

`Next` is enabled when `_toAdd.Count > 0` **or** the user explicitly chose to skip additions (Step 1 is optional — `Next` is always enabled, but with a hint when the list is empty).

---

## 5. Step 2 — Remove Collections

Lifted almost verbatim from the current [ManageCollections.razor](../MongoMigrationWebApp/Components/ManageCollections.razor) list, with two changes:

1. Add a **checkbox column** in each row. Selected IDs accumulate into `_toRemoveIds`.
2. Header shows `N selected` counter and a `Clear selection` link.
3. Keep filtering, pagination (`PaginationHelper<MigrationUnit>`), and the `25+ collections triggers filter` rule unchanged.

No collection is actually removed at this step — selection only updates `_toRemoveIds`. Removal happens on Confirm in step 3.

`Next` is always enabled; both lists are allowed to be empty (the user can use the wizard purely to review, although Confirm will then be a no‑op and we should disable Confirm in that case).

---

## 6. Data model additions

### 6.1 New record in the web project

```csharp
// MongoMigrationWebApp/Components/Wizard/PendingAddition.cs (new)
public sealed record PendingAddition
{
    public required string DatabaseName { get; init; }
    public required string CollectionName { get; init; }
    public string? Filter { get; init; }
    public bool Overwrite { get; init; }
    public IndexingStrategy IndexingStrategy { get; init; } = IndexingStrategy.SameAsSource;
    public ShardingStrategy ShardingStrategy { get; init; } = ShardingStrategy.SameAsSource;
    public string? MoveToShard { get; init; }   // null = Auto
}

public enum IndexingStrategy { SameAsSource, DontIndex }
public enum ShardingStrategy { SameAsSource, DontShard }
```

### 6.2 Backend model extensions

**Do not modify [CollectionInfo](../OnlineMongoMigrationProcessor/Models/CollectionInfo.cs).** That class is part of the user‑facing JSON contract (see [CollectionInfoFormat.JSON](../CollectionInfoFormat.JSON)) and any change risks breaking existing input files that customers maintain by hand.

Instead, introduce a new parent class that wraps a `List<CollectionInfo>` and carries the wizard's per‑group options. The wizard emits one of these per "Add" action; the JSON import path that already accepts a raw `List<CollectionInfo>` continues to work unchanged.

```csharp
// OnlineMongoMigrationProcessor/Models/CollectionInfoGroup.cs (new)
public class CollectionInfoGroup
{
    // Existing user-facing payload — untouched.
    public required List<CollectionInfo> Collections { get; set; } = new();

    // NEW per-group options (all optional, backward compatible).
    // Apply to every CollectionInfo in the Collections list above.
    public bool?   Overwrite        { get; set; }
    public string? IndexingStrategy { get; set; }  // "SameAsSource" | "DontIndex"
    public string? ShardingStrategy { get; set; }  // "SameAsSource" | "DontShard"
    public string? MoveToShard      { get; set; }  // null => Auto
}
```

Notes:
- `CollectionInfo` remains exactly as it is today.
- The JSON parser path in [Helper.cs](../OnlineMongoMigrationProcessor/Helpers/Helper.cs) (`JsonConvert.DeserializeObject<List<CollectionInfo>>`) is kept for backward compatibility with files customers already have. A second parser tries `CollectionInfoGroup` first and falls back to the legacy `List<CollectionInfo>` shape if the root token is a JSON array.
- The wizard always produces `CollectionInfoGroup` internally — one per "Add" click — so users can stack multiple groups with different options before going to Step 3.

Mirror the same four wizard options on `MigrationUnit` (the persisted unit) so the migration worker can consume them per chunk. Defaults match the current behaviour exactly: `Overwrite = job.AppendMode == false` for legacy units, `IndexingStrategy = SameAsSource`, `ShardingStrategy = SameAsSource`, `MoveToShard = null`. Existing on‑disk jobs deserialize cleanly because every new field is nullable.

### 6.3 `CollectionInfoFormat.JSON` is unchanged

The existing user‑provided JSON contract (`[ { CollectionName, DatabaseName, Filter, ... } ]`) keeps working as‑is. The wizard's richer per‑group options live only inside the app — they are surfaced through the UI form (Section 4.2) and persisted on `MigrationUnit`, not through the legacy JSON file format. A separate document can be added later if we ever want to let users author `CollectionInfoGroup` JSON directly.

---

## 7. New helper: `MongoHelper.GetClusterNodesAsync`

Add to [MongoHelper.cs](../OnlineMongoMigrationProcessor/Helpers/Mongo/MongoHelper.cs).

> **Constraint:** Azure Cosmos DB for MongoDB RU has no user‑visible shards and rejects most cluster‑introspection commands. vCore exposes a limited set of admin commands — notably `listShards` works against the `admin` database — but other commands such as `hello`, `isMaster`, and `replSetGetStatus` are still rejected. The helper below uses a layered probe so it works equally on Cosmos vCore, Cosmos RU, vanilla replica sets, and sharded MongoDB.

### 7.1 Discovery strategy (in order)

1. **Cosmos DB for MongoDB vCore** — run `db.adminCommand({ listShards: 1 })` against the `admin` database. vCore supports this admin command and returns `{ shards: [ { _id, host, ... } ], ok: 1 }`, one document per physical shard.
2. **Cosmos DB for MongoDB RU** — there are no user‑visible shards; the entire account is one logical endpoint. Return an empty list so the UI collapses to `Auto` only.
3. **Native sharded MongoDB** — read `config.shards` directly (the `config` DB is accessible to any authenticated user with `clusterMonitor` or equivalent), which avoids `admin.listShards`.
4. **Native replica set** — read `IMongoClient.Cluster.Description.Servers` from the driver's already‑established topology. The driver populates this from SDAM heartbeats without us needing to issue `hello`.
5. **Standalone / unknown** — return empty list.

Detection of "is this a Cosmos endpoint" can re-use the existing helpers in the codebase (see [Helper.cs](../OnlineMongoMigrationProcessor/Helpers/Helper.cs) — `IsRUEndpoint` / `IsVCoreEndpoint` style checks if present, otherwise sniff the connection string host for `mongo.cosmos.azure.com` / `mongocluster.cosmos.azure.com`).

### 7.2 Sketch

```csharp
/// <summary>
/// Returns the shard / node identifiers of the target cluster used by the
/// Manage Collections Wizard "Move to" dropdown. Works on Cosmos DB for
/// MongoDB (vCore + RU), native sharded clusters, and replica sets without
/// touching the admin database.
/// </summary>
public static async Task<List<string>> GetClusterNodesAsync(
    IMongoClient targetClient,
    string targetConnectionString,
    string anyUserDatabaseName,        // e.g. one of the migration unit DBs
    CancellationToken cancellationToken = default)
{
    // 1) Cosmos vCore — listShards admin command
    if (Helper.IsCosmosVCore(targetConnectionString))
    {
        try
        {
            var adminDb = targetClient.GetDatabase("admin");
            // db.adminCommand({ listShards: 1 })
            var cmd = new BsonDocument { { "listShards", 1 } };
            var resp = await adminDb.RunCommandAsync<BsonDocument>(cmd, cancellationToken: cancellationToken);

            // Reply shape: { shards: [ { _id, host, ... } ], ok: 1 }
            if (resp.TryGetValue("shards", out var shardsVal) && shardsVal.IsBsonArray)
            {
                return shardsVal.AsBsonArray
                    .OfType<BsonDocument>()
                    .Select(d => d.GetValue("_id", BsonNull.Value)?.ToString())
                    .Where(s => !string.IsNullOrEmpty(s))
                    .Cast<string>()
                    .ToList();
            }
        }
        catch { /* fall through */ }

        return new List<string>(); // vCore with no exposed shards => Auto only
    }

    // 2) Cosmos RU — no shard concept exposed to the user
    if (Helper.IsCosmosRU(targetConnectionString))
        return new List<string>();

    // 3) Native sharded MongoDB — read from the config DB (no admin needed)
    try
    {
        var config = targetClient.GetDatabase("config");
        var shards = config.GetCollection<BsonDocument>("shards");
        var list = await shards.Find(FilterDefinition<BsonDocument>.Empty)
                               .Project(Builders<BsonDocument>.Projection.Include("_id"))
                               .ToListAsync(cancellationToken);
        if (list.Count > 0)
            return list.Select(d => d["_id"].AsString).ToList();
    }
    catch { /* not a mongos or no permission — fall through */ }

    // 4) Native replica set — read from the driver's SDAM topology
    var servers = targetClient.Cluster?.Description?.Servers;
    if (servers != null && servers.Count > 0)
    {
        return servers.Select(s => s.EndPoint.ToString())
                      .Where(s => !string.IsNullOrEmpty(s))
                      .ToList()!;
    }

    // 5) Standalone / unknown
    return new List<string>();
}
```

### 7.3 Notes & fallbacks

- **No `admin` access required** in any branch. The vCore branch queries a user DB's `_shards` pseudo-collection; the native sharded branch reads `config.shards`; the replica-set branch uses in-process driver state.
- **Field name in `_shards`** — vCore documents in `_shards` carry the shard identifier under `name` (preferred) with `_id` as a fallback; the helper tries both.
- **`anyUserDatabaseName`** parameter: the wizard passes the first DB name from the existing migration units. If the job has none yet, the wizard skips the cluster-node lookup entirely and presents only `Auto`.
- The wizard wraps the call with a 5–10 s `CancellationTokenSource`; on failure it logs and shows the inline warning `Could not load cluster nodes; only 'Auto' is available`.
- The helper is called once when **Sharding Strategy** first switches to `Don't Shard` and cached for the lifetime of the modal.

---

## 8. Step 3 — Summary & confirmation

Two side‑by‑side panels (`row > col-md-6` each):

### 8.1 Collections to be added

For each `PendingAddition` show a compact summary row:
```
+ SalesDB.Customers   Overwrite • Same as source • Don't Shard → shard0001 • Filter set
```
Hover tooltip on the filter chip shows the raw BSON.

### 8.2 Collections to be removed

For each ID in `_toRemoveIds` look up the matching `MigrationUnit` and render:
```
− InventoryDB.Products
```

### 8.3 Conflict resolution

If the same `db.col` appears in both lists (e.g. user wants to drop & re‑add with different options), show a banner:

> One or more collections appear in both lists. They will be **removed first and then added** as new entries. Migration and change‑stream state for these collections will be lost.

Conflict detection: simple `HashSet<string>` intersection by `db.col` key. No special handling beyond the warning; on Confirm the commit applies remove first, then add.

### 8.4 Confirm button

Disabled when both lists are empty. On click:

1. Validate filters / strategies one last time server‑side (re-use [Helper](../OnlineMongoMigrationProcessor/Helpers/Helper.cs) validation).
2. Build the change set:
   - **Removals**: remove `MigrationUnit`s by ID (re‑use the existing remove path from [ManageCollections.razor](../MongoMigrationWebApp/Components/ManageCollections.razor) — same warning copy about losing change‑stream state).
   - **Additions**: convert each `PendingAddition` to a `MigrationUnit` via the existing add path; copy the new four fields onto each unit.
3. Persist via the same job‑save call the current modal uses (`JobManager.SaveJob` / equivalent).
4. Raise `OnCollectionsCommitted(new WizardResult(...))` and close the modal.

---

## 9. Backend wiring (worker side)

These changes are minimal because most of the new metadata simply lands on `MigrationUnit` and is consumed in the existing dump/restore flow:

| Field on `MigrationUnit` | Consumer | Behaviour |
| --- | --- | --- |
| `Overwrite` | [MigrationWorker.cs](../OnlineMongoMigrationProcessor/Workers/MigrationWorker.cs) — collection prepare path (existing `AppendMode` / target‑created checks). | When `Overwrite == true`, force drop+create on the target before any chunk dispatch, even if `AppendMode` is true at the job level. Default `false` keeps current behaviour. |
| `IndexingStrategy == DontIndex` | Index‑creation path inside `MigrationWorker`. | Skip the per‑collection index build step. Equivalent to per‑collection `SkipIndexes`. |
| `ShardingStrategy == DontShard` | Target collection creation. | Create the target as **unsharded** (skip shardCollection call) — for Cosmos DB for MongoDB this maps to a single fixed‑throughput container. |
| `MoveToShard` | Target collection creation, only when `ShardingStrategy == DontShard`. | After creation, run `moveChunk` (or `moveCollection` on 8.0+) to pin the collection to the named shard. No‑op when null/Auto. |

All four behaviours short‑circuit in simulated‑run mode (the UI already disables the inputs; the worker double‑checks via `MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun`).

No change is required in the change‑stream processor, partitioner, or restore coordinator — they remain shard‑aware via the existing target‑collection metadata.

---

## 10. Validation & error handling

- **Namespace format**: re‑use `Helper.ValidateNamespaceFormat`. Errors render inline under the textarea, same pattern as today.
- **Filter BSON**: re‑use the existing BSON validator (same one [ManageCollections.razor](../MongoMigrationWebApp/Components/ManageCollections.razor) uses for the per‑unit filter edit). Invalid filter blocks `Add`.
- **Move to shard**: must be one of the values returned by `GetClusterNodesAsync`. A stale value (e.g. user opened wizard, shard was decommissioned) is caught server‑side on Confirm; UI shows `Shard 'X' no longer exists, choose another`.
- **Sync‑back**: same guard as today (`You cannot update collections after sync back has started.` — [MigrationJobViewer.razor#L1505](../MongoMigrationWebApp/Pages/MigrationJobViewer.razor#L1505)). Wizard refuses to open if sync‑back has started; the new `Temp` button is also disabled in that state.

---

## 11. Testing plan

| Area | Test |
| --- | --- |
| Step navigation | Forward/back preserves both lists; Cancel discards everything. |
| Add card stack | Wildcard expansion, JSON upload, duplicate detection, remove‑from‑pending. |
| Conditional matrix | Overwrite=FALSE locks Indexing/Sharding/MoveTo; flipping back to TRUE restores prior selections. |
| Simulated run | All three advanced dropdowns disabled; Overwrite + Filter still editable; Confirm still works and persists the (defaulted) values. |
| GetClusterNodes | Sharded cluster → list of shard `_id`s. Replica set → list of hosts. Standalone → empty list, only `Auto` available. Network failure → inline warning. |
| Remove step | Selection counter accurate across paginated pages; `Clear selection` resets. |
| Summary conflict | Same `db.col` in both lists triggers banner; Confirm applies remove then add. |
| Persistence | Reopen job after Confirm — new fields round‑trip on disk; legacy jobs (no new fields) deserialize with defaults. |
| Backend behaviour | Overwrite=TRUE drops & recreates target; DontIndex skips index builds; DontShard creates unsharded target; MoveToShard pins to chosen shard. |

---

## 12. Out of scope (explicit)

- No changes to the existing `Update Collections` button or modal. Both flows live side‑by‑side until product decides to retire the old one.
- No new REST endpoints on `MongoMigrationWebApp/Controller/` — the wizard talks to the in‑process `JobManager` like the existing modal does.
- No migration of historical jobs to populate the new optional fields; defaults preserve current behaviour.

---

## 13. Open questions

1. Final name for the `Temp` button (suggest `Manage Collections (Advanced)`).
2. Should the wizard fully replace the current modal in a future release, or coexist permanently?
3. For Cosmos DB for MongoDB targets, do we expose `Move to` at all? (Cosmos surfaces logical partitions, not shards — may need a different label or to be hidden entirely.)
4. Should `Overwrite=TRUE` on an in‑flight collection require an explicit confirmation step (it discards target data)?

Resolve these before implementation begins.
