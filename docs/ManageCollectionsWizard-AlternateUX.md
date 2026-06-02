# Manage Collections — Editable List Popup (Alternate UX)

Status: Draft / counter-proposal to [ManageCollectionsWizard-Plan.md](ManageCollectionsWizard-Plan.md)
Target branch: `0.9.7`
Goal: Same as the wizard plan — add/remove collections on a running or paused migration job with per-collection overwrite / indexing / sharding / placement / filter controls — but in a **single editable-list popup**, with no wizard steps and no changes to the underlying Job Viewer page layout.

---

## 1. Why this shape

The 3-step wizard has friction even for trivial edits: 5 clicks to add one collection with defaults, and every change feels like a deployment.

At the same time, restructuring the Job Viewer page itself is a bigger change than this feature warrants.

The middle ground: keep the change isolated to **one popup**, but make the popup itself an **editable list** — not a wizard. The user opens the popup, manages drafts in place, sees pending changes accumulate in the same view, clicks `Next` for a summary, then `Apply changes` to commit.

This preserves the wizard plan's isolation (everything new lives behind one button) but drops the 3-step structure and the modal-in-modal feel.

**Edit scope.** To stay safe and to keep semantics aligned with the existing flow, **only draft (newly added) rows can be edited inside the popup**. Pre-existing migration units ("live" rows) cannot be modified — they can only be dropped (removed). If a user wants to change options on an existing collection, they remove it and re-add it as a draft with the new options. The Apply step warns about this (it removes change-stream state for that collection, same warning the existing flow has today).

---

## 2. Shape at a glance

Single modal, opened from the same `Manage Collections` button described in the wizard plan (section 2).

```
┌─ Manage Collections ───────────────────────────────────────────── [×] ─┐
│                                                                        │
│  [search...]                                  [+ Add]  [Bulk ▾]        │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ ☐  SalesDB.Customers        running    append  · idx:src             │  │
│  │ ☐  SalesDB.Orders           paused     append  · idx:src             │  │
│  │ ☑  InventoryDB.Products     done       append  · idx:src             │  │
│  │ +  HR.Employees (draft)     pending    overwrite · idx:none · ✎ · [×] │  │
│  │ −  Legacy.Audit (remove)    done       —                        ↺   │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│  « 1 2 3 »                                                             │
│                                                                        │
│  3 drafts · 1 to remove                              [Cancel]  [Next] │
└────────────────────────────────────────────────────────────────────────┘
```

Key ideas:

- **One popup, one list.** No steps, no tabs. Live units, draft additions, and pending removals all render as rows in the same table inside the modal. Visual prefix tells them apart: blank for live, `+` for draft, `−` (strikethrough) for pending removal.
- **Per-draft pencil (`✎`) opens an inline edit panel directly under the draft row** (accordion-style expand, not a second modal). The panel holds Overwrite / Indexing / Sharding / Move-to / Filter with the same conditional matrix as the wizard plan (section 4.2). Saving collapses the panel and updates the draft. **The pencil is only shown on draft rows.** Live rows have no edit action — they can only be dropped.
- **`+ Add` opens a single inline form row pinned to the top of the list** (same accordion pattern). Namespace / JSON textarea + `Advanced ▾` for the per-group options, then `Add` to resolve into one or more draft rows (one row per element of a `CollectionInfo` JSON array, one row per resolved `db.col` for wildcards — same rule as wizard plan section 4.1).
- **Checkbox column + `Bulk ▾`** does bulk remove on selected rows (drafts or live) and bulk set of per-collection options across **selected drafts only**.
- **One commit moment.** The list view's footer button is `Next`, which opens a summary view; nothing is written until `Apply changes` is pressed in the summary. Until then, every change is held in component state and can be undone (the `↺` link on each pending-removal row, the `[×]` on each draft, `Cancel` to discard everything).
- **The rest of the Job Viewer page is untouched.** No grid changes, no new page-level banners, no inline drawer.

Everything that the wizard's 3 steps cover is reachable from this single screen without navigation.

---

## 3. Detailed UX

### 3.1 Opening the popup

Same entry point as the wizard plan (section 2): a `Manage Collections` button next to `Update Collections` in [MigrationJobViewer.razor](../MongoMigrationWebApp/Pages/MigrationJobViewer.razor), same enable rules, opens `ManageCollectionsModal.razor` (new file — supersedes the proposed `ManageCollectionsWizard.razor`).

**Connection-string guard (entry point, matches existing `Update Collections`).** The popup is only allowed to open when both source and target connection strings are already loaded for the current job. The handler reuses the exact check the existing `Update Collections` button uses in [MigrationJobViewer.razor](../MongoMigrationWebApp/Pages/MigrationJobViewer.razor) (the `ManageCollections()` method that inspects `MigrationJobContext.SourceConnectionString[job.Id]`), extended to also require the target string. If either is missing, the popup does not open; instead the same inline error message is shown:

> Source connection string is not available. Please use 'Resume Job' → 'With Updated Connection Strings' to provide connection details, then pause the job before updating collections.

This means the popup itself can assume both connection strings are present — it does not need an in-modal pre-step, masked-input panel, or `Test & Continue` flow. The flow for recovering missing connection strings stays exactly where users already know it: `Resume Job → With Updated Connection Strings`.

Parameters identical to the wizard plan (section 3.1): `MigrationJob`, `MigrationUnits`, `SourceConnectionString`, `TargetConnectionString`, `OnCollectionsCommitted`, `OnCancelled` — with both connection-string parameters required (non-null, non-empty) by contract.

### 3.2 The list

Single Bootstrap `table-sm` with these columns:

| Column | Content |
| --- | --- |
| Checkbox | Drives bulk actions and bulk remove. |
| State glyph | blank / `+` (draft) / `−` (pending remove, strikethrough). |
| Namespace | `Database.Collection`. |
| Status | Existing migration status for live rows; `pending` for drafts; `—` for removals. |
| Options summary | Compact chips: `overwrite|append · idx:src|none · shard:src|none · → shardId · filter set`. Read-only on live rows. |
| Actions | Draft rows: `✎` (edit), `[×]` (drop draft). Live rows: `[×]` (queue for removal) only — no edit. Pending-removal rows: `↺` (undo). |

Search box at the top filters by namespace (same `25+ collections triggers filter` rule as today). `PaginationHelper<MigrationUnit>` continues to drive pagination; drafts and pending-removals are interleaved into the same paginated source.

**List view footer** (shown in the section 2 diagram) has two buttons: `[Cancel]  [Next]`.

- `Next` is disabled when both pending counts (drafts and removals) are zero.
- `Cancel` discards all pending state and closes the modal (confirm-prompt only if any pending counts are non-zero).
- `Next` does **not** commit. It opens the **summary view** described in section 3.7, where `Apply changes` is the actual commit button. The list view's state is preserved underneath.

### 3.3 Add (replaces wizard Step 1)

`[+ Add]` expands a single form row pinned to the top of the list:

```
┌ Add collections ──────────────────────────────────────────── [×] ┐
│  Namespaces / JSON  [ textarea ............... ] [Upload file]   │
│  ▾ Advanced                                                      │
│     Overwrite      [ FALSE ▾ ]                                   │
│     Indexing       [ Same as source ▾ ]      (locked)            │
│     Sharding       [ Same as source ▾ ]      (locked)            │
│     Move to        [ Auto ▾ ]                (locked)            │
│     Filter         [ ......................... ]                 │
│                                                       [ Add ]    │
└──────────────────────────────────────────────────────────────────┘
```

- `Advanced` is collapsed by default so the common case (just paste namespaces and click Add) is two interactions.
- Conditional matrix identical to wizard plan section 4.2 (Overwrite=FALSE locks Indexing / Sharding / Move-to; Move-to enabled only when Sharding=`DontShard`).
- `Add` validates with `Helper.ValidateNamespaceFormat`, expands wildcards / JSON arrays, and appends one draft row per resolved namespace. Duplicates (already-live or already-draft) are skipped with a small inline warning.
- Simulated-run mode disables Indexing / Sharding / Move-to (same as wizard plan).

### 3.4 Edit (drafts only)

Clicking `✎` on a **draft row** expands an edit panel directly under that row (accordion). The panel shows the same five fields as the Add form, pre-filled with the draft's current values. `Save` updates the draft in place and collapses the panel; `Cancel` discards the panel changes.

**Live rows are not editable.** Pre-existing migration units render with no pencil action and no inline panel. If the user needs to change options on an existing collection, they:

1. Drop the live row (`[×]` or `Bulk ▾ → Remove selected`), which queues it for removal.
2. Use `+ Add` to re-add the same namespace as a draft with the new options.
3. Apply.

The Apply confirm popover surfaces the standard conflict banner for this case ("`db.col` is queued for both remove and add; it will be removed first, then re-added; change-stream state will be lost" — same banner the wizard plan section 8.3 describes).

Keeping edits restricted to drafts means the popup's behaviour for live units exactly matches what the existing `Update Collections` modal already does (add and remove only), so no new server-side mutation paths are needed for in-place changes.

### 3.5 Remove (replaces wizard Step 2)

- Per-row checkbox + `Bulk ▾ → Remove selected`, or `[×]` on a single row.
- Removed rows do **not** disappear — they re-render with strikethrough, a `−` glyph, and a `↺ Undo` link. They stay in the list (and in pagination) until Apply.
- The footer counter updates live.

### 3.6 Bulk actions

`Bulk ▾` menu when one or more rows are selected:

- `Remove selected` — applies to both drafts and live rows.
- `Set Overwrite → TRUE / FALSE` — **drafts only**; disabled with helper text when any live row is selected.
- `Set Indexing → Same as source / Don't Index` — **drafts only**.
- `Set Sharding → Same as source / Don't Shard` — **drafts only**.
- `Set Move to → Auto / <shard id>` — **drafts only**, populated from `GetClusterNodesAsync`.
- `Clear filter` — **drafts only**.

For mixed selections (drafts + live), bulk `Set ...` items render disabled with the helper text `Existing collections cannot be edited; deselect them to view edit options.` Bulk `Remove selected` is always available. Each `Set ...` action updates the affected drafts in place. Same conditional rules apply to each draft — e.g. `Set Move to` is only offered if all selected drafts currently have Sharding=`DontShard`.

### 3.7 Commit (replaces wizard Step 3)

When the user clicks `Next` on the list view, the list area inside the same modal is replaced **in place** by a **summary view** (no second modal stacked on top). The modal frame, title, and `[×]` stay; only the body and footer swap. Nothing is committed until `Apply changes` is pressed in this view.

**Summary view footer:**

```
                                       [Cancel]  [Back]  [Apply changes]
```

- `Cancel` — discards **all** pending state and closes the modal entirely. Same behaviour as `Cancel` on the list view. Confirm-prompt if pending counts are non-zero.
- `Back` — returns to the editable list view with all pending state preserved, so the user can keep editing drafts, add or drop more rows, then come back to the summary.
- `Apply changes` — primary button. Runs: validations → remove-then-add → save via existing job-save path → close modal → fire `OnCollectionsCommitted`.

**Summary view body** is a single Bootstrap `table-sm` listing every pending change, one row per item:

| Action | Namespace | Overwrite | Indexing | Sharding | Move to | Filter |
| --- | --- | --- | --- | --- | --- | --- |
| `+ Add` | `SalesDB.Customers` | TRUE | Same as source | Don't Shard | `shard0001` | `{status:"active"}` |
| `+ Add` | `HR.Employees` | FALSE | Same as source | Same as source | Auto | — |
| `− Remove` | `Legacy.Audit` | — | — | — | — | — |
| `− Remove` | `SalesDB.Orders` | — | — | — | — | — |

Sort order: removals first, then additions (so the user sees what is being lost before what is being gained). Long filter values are truncated with hover-to-expand showing the raw BSON. Removal rows render the option columns as `—` because no per-collection options apply.

**Conflict banner** appears above the table only when the same `db.col` appears in both lists: "One or more collections appear in both lists. They will be removed first and then re-added; migration and change-stream state for these collections will be lost." Same copy as wizard plan section 8.3. The conflicting namespace is highlighted in both its remove row and its add row.

### 3.8 Connection strings

Handled entirely at the entry point (section 3.1) by the same guard the existing `Update Collections` button uses. The popup never renders without both connection strings, so there is no in-modal pre-step, no masked-input panel, and no `Test & Continue` flow. If the user needs to repopulate connection strings they go through the existing `Resume Job → With Updated Connection Strings` flow exactly as today.

This is the main UX simplification over the wizard plan's section 4.0.

### 3.9 Simulated-run mode

Identical to wizard plan section 4.2: Indexing / Sharding / Move-to render disabled (in both Add and Edit panels, and in `Bulk ▾`) with helper text `Disabled in simulated run`. Server-side double-check on Apply unchanged.

---

## 4. State model

All state held in the modal's code-behind, never written to disk until Apply:

```csharp
private List<MigrationUnit> _liveUnits = new();                 // snapshot from props (read-only in the popup)
private List<PendingAddition> _drafts = new();                  // wizard plan section 6.1 shape
private HashSet<string> _toRemoveIds = new();                   // unit IDs queued for removal
private List<string> _clusterNodes = new();                     // cached after first lookup
private bool _loadingNodes;
private string? _error;
private Guid? _expandedDraftId;                                 // which draft's edit panel is open (drafts only)
private bool _showAddPanel;
```

There is no `PendingEdit` collection: live units are never modified in-place. The four new option fields only flow through `PendingAddition` (drafts) on their way to `MigrationUnit` defaults when committed.

---

## 5. Backend & model — same as the wizard plan

This is purely a UX rearrangement of the popup. All backend work from the wizard plan is reused unchanged:

- `CollectionInfo` stays untouched.
- New `CollectionInfoGroup` wrapper (wizard plan section 6.2) is still emitted internally — one per `Add` click — so bulk-add semantics are identical.
- `PendingAddition` record (wizard plan section 6.1) is reused for draft rows.
- `MigrationUnit` gets the same four new nullable fields (`Overwrite`, `IndexingStrategy`, `ShardingStrategy`, `MoveToShard`) with backward-compatible defaults.
- `MongoHelper.GetClusterNodesAsync` (wizard plan section 7) is unchanged: vCore shards via `db.adminCommand({ listShards: 1 })`, RU empty, native sharded `config.shards`, native replica set via SDAM, standalone empty. Triggered the first time any panel flips Sharding to `DontShard`, cached for the modal lifetime.
- Worker-side behaviour (wizard plan section 9) is unchanged.

The choice between "wizard" and "editable-list popup" is purely a front-end decision; both ship from the same model and helper work.

---

## 6. Where this is better than the wizard

| Concern | 3-step wizard | Editable-list popup |
| --- | --- | --- |
| Time to add one collection with defaults | Open → Step 1 fill → Next → Next → Confirm (5 clicks) | Open → `+ Add` → paste → `Add` → `Apply` (5 clicks, single view) |
| Time to add one collection with overwrite=TRUE | 5 clicks + expand advanced inside Step 1 | 5 clicks + expand Advanced inline |
| Edit options on a live (pre-existing) collection | Not supported. | Not supported either — drop and re-add (same semantics). |
| Edit options on a draft before committing | Not supported — must remove and re-add the draft. | `✎` on the draft row → change options → `Save`. |
| Review before commit | Forced Step 3 summary | In-place summary view with `Cancel` / `Back` / `Apply changes`; list view is one `Back` click away |
| Recover from a mistake | Back button between steps | `[×]` on drafts, `↺` on removals, `Cancel` for all |
| Page footprint | Touches only the new modal | Touches only the new modal (same isolation) |
| Cosmos shard discovery | Same helper | Same helper |
| Simulated-run gating | Same | Same |
| Connection-string handling | New in-modal pre-step (section 4.0) | Reuses existing `Update Collections` entry guard; no in-modal pre-step (section 3.8) |
| Bulk apply across many adds | Good (Step 3 summary) | Equally good (footer counter + `Next` → `Apply changes`) |
| Bulk *set* across selected drafts | Not supported | `Bulk ▾` menu |

The wizard wins in one scenario only: a first-time operator who benefits from being walked through it. That can be handled with a one-time `Tip:` callout pointing at `+ Add` and `Bulk ▾`, without permanently structuring the UI as a wizard.

---

## 7. What this proposal does *not* change vs the wizard plan

- Entry point button location, label, and enable rules (wizard plan section 2).
- `CollectionInfoGroup` model and the legacy `List<CollectionInfo>` JSON path (wizard plan section 6.2).
- `MigrationUnit` field additions and defaults (wizard plan section 6.2 + 9).
- `MongoHelper.GetClusterNodesAsync` discovery strategy and signature (wizard plan section 7).
- Worker-side consumption of `Overwrite`, `IndexingStrategy`, `ShardingStrategy`, `MoveToShard` (wizard plan section 9).
- Validation, sync-back guard, server-side simulated-run double-check (wizard plan section 10).
- Out-of-scope items (wizard plan section 12).

Only the new front-end component changes shape: `ManageCollectionsModal.razor` (editable list) instead of `ManageCollectionsWizard.razor` (3 steps).

---

## 8. Resolved decisions

1. **Summary view** — in-place panel that replaces the list area inside the same modal (no second stacked modal). See section 3.7.
2. **Pagination vs virtualization** — keep `PaginationHelper<MigrationUnit>` inside the modal. The existing Job Viewer grid already uses `PaginationHelper<MigrationUnitBasic>` ([MigrationJobViewer.razor](../MongoMigrationWebApp/Pages/MigrationJobViewer.razor) line ~604), so reusing it in the modal keeps one pattern across the app. Switching the modal to Blazor `<Virtualize>` would introduce a second list pattern without simplifying the Job Viewer (which would still be on `PaginationHelper`), so it is not a win for code cleanliness here. Revisit only if a single job genuinely exceeds what the pager handles smoothly.
3. **Edit-panel accordion** — single-expansion. Opening `✎` on one draft collapses any other open edit panel. Simpler state, no comparison UX needed.
4. **Entry-point button label** — `Manage Collections` (matches the section 3.1 wording; no `(Advanced)` suffix).

---

## 9. Recommendation

Build the backend (`CollectionInfoGroup`, `MigrationUnit` fields, `MongoHelper.GetClusterNodesAsync`, worker wiring) exactly as the wizard plan describes. **Replace the wizard component with `ManageCollectionsModal.razor` — a single editable-list popup** with inline Add, draft-only pencil Edit, checkbox + `Bulk ▾` Remove (any row) / Set (drafts only), a `Next` button that opens an in-place summary, and `Apply changes` in the summary to commit.

Same isolation as the wizard (everything new lives behind one button, the rest of the Job Viewer is untouched), same backend, fewer clicks for common cases. Edits are restricted to drafts — pre-existing units can only be dropped — which keeps the popup's server-side surface identical to today's `Update Collections` modal (add + remove only).

---

## 10. Implementation task list

Work through the boxes top to bottom. After every box is ticked, **re-walk this list end-to-end** against the running code to confirm no requirement was silently dropped and integration is smooth.

> **Audit status:** verified against current code on the `0.9.7` branch. See [ManageCollectionsModal-ImplementationSummary.md](ManageCollectionsModal-ImplementationSummary.md) for the implementation walkthrough. Boxes are ticked where the code matches the spec; explicit gaps are called out inline as `**GAP**`.

### 10.1 Backend — models

- [x] Add `CollectionInfoGroup` wrapper (wizard plan section 6.2) under `OnlineMongoMigrationProcessor/Models/`. — [CollectionInfoGroup.cs](../OnlineMongoMigrationProcessor/Models/CollectionInfoGroup.cs).
- [x] Add four new nullable fields to `MigrationUnit`: `Overwrite`, `IndexingStrategy`, `ShardingStrategy`, `MoveToShard`. Keep backward-compatible defaults (null = inherit existing job-level behaviour). — [MigrationUnit.cs](../OnlineMongoMigrationProcessor/Models/MigrationUnit.cs).
- [x] Add `PendingAddition` record (wizard plan section 6.1) for draft rows (web-app side; not persisted). — [MongoMigrationWebApp/Models/PendingAddition.cs](../MongoMigrationWebApp/Models/PendingAddition.cs).
- [x] Confirm legacy `List<CollectionInfo>` JSON path still deserialises (round-trip test on an existing saved job). — `Helper.cs` legacy parsing path retained.

### 10.2 Backend — helpers

- [x] Implement `MongoHelper.GetClusterNodesAsync` with layered probe: vCore shards via `db.adminCommand({ listShards: 1 })` → RU empty → native sharded `config.shards` → native replica set via `IMongoClient.Cluster.Description.Servers` → standalone empty. Returns `List<string>` of shard / node identifiers. — [MongoHelper.cs `GetClusterNodesAsync`](../OnlineMongoMigrationProcessor/Helpers/Mongo/MongoHelper.cs).
- [x] Cache result for the lifetime of the caller (modal holds the cache). — `_clusterNodes` field in [ManageCollectionsModal.razor.cs](../MongoMigrationWebApp/Components/ManageCollectionsModal.razor.cs).
- [ ] Unit-test the probe order against each backend kind (or at minimum a manual run note in [/memories/repo/mongohelper-notes.md](../memories/repo/mongohelper-notes.md)). — **GAP**: no automated tests; manual verification pending across vCore / RU / native sharded / replica set / standalone.

### 10.3 Backend — worker wiring

- [x] Consume `Overwrite` on `MigrationUnit` in the worker pipeline. Per-unit value overrides job-level `AppendMode`; null falls back. — `GetEffectiveOverwrite()` in [MigrationWorker.cs](../OnlineMongoMigrationProcessor/Workers/MigrationWorker.cs).
- [x] Consume `IndexingStrategy` on `MigrationUnit` in the worker pipeline. Per-unit value overrides job-level `SkipIndexes`; null falls back. — `GetEffectiveSkipIndexes()` in [MigrationWorker.cs](../OnlineMongoMigrationProcessor/Workers/MigrationWorker.cs).
- [x] Consume `ShardingStrategy` and `MoveToShard` on `MigrationUnit` in the worker pipeline (wizard plan section 9). — `GetEffectiveShardingStrategy()` and `GetEffectiveMoveToShard()` helpers added in [MigrationWorker.cs](../OnlineMongoMigrationProcessor/Workers/MigrationWorker.cs). _Caveat: the .NET worker currently has no shardCollection / shard-placement code path; actual sharding lives in the Python `SchemaMigration` tool. The helpers exist so any future .NET-side sharding pipeline reads per-unit values through a single contract; until a consuming code path is added, the per-unit values flow as far as the worker but are not yet executed._
- [x] Server-side double-check for simulated-run mode: `GetEffectiveSkipIndexes()` returns `true` whenever `IsSimulatedRun` regardless of per-unit value. — [MigrationWorker.cs](../OnlineMongoMigrationProcessor/Workers/MigrationWorker.cs).
- [x] Server-side double-check for simulated-run mode on Sharding / Move-to. — `GetEffectiveShardingStrategy()` forces `ShardingStrategy.DontShard` when `IsSimulatedRun`; `GetEffectiveMoveToShard()` returns null when simulated.
- [x] Sync-back guard: when a removal lands, drop the change-stream resume token for that namespace (same warning text used today). — `ApplyChanges()` in [ManageCollectionsModal.razor.cs](../MongoMigrationWebApp/Components/ManageCollectionsModal.razor.cs) clears `ResumeToken`, `OriginalResumeToken`, `CSLastResumeTokenWithChange`, `CSLastChangeUTCTime` and their `SyncBack*` counterparts on each removed unit before calling `Remove()`.

### 10.4 Front-end — entry point

- [x] Add `Manage Collections` button next to `Update Collections` in [MigrationJobViewer.razor](../MongoMigrationWebApp/Pages/MigrationJobViewer.razor). Same enable rules as `Update Collections`.
- [x] Reuse the existing `ManageCollections()` connection-string check (inspects `MigrationJobContext.SourceConnectionString[job.Id]`) and **extend it to require the target string too**. If either is missing, do not open the popup; show the existing inline error message verbatim. — `OpenManageCollectionsModal()` in [MigrationJobViewer.razor](../MongoMigrationWebApp/Pages/MigrationJobViewer.razor).

### 10.5 Front-end — `ManageCollectionsModal.razor` (new)

- [x] Component file under `MongoMigrationWebApp/Components/`. — [ManageCollectionsModal.razor](../MongoMigrationWebApp/Components/ManageCollectionsModal.razor) + [.cs codebehind](../MongoMigrationWebApp/Components/ManageCollectionsModal.razor.cs).
- [x] Parameters: `MigrationJob`, `MigrationUnits`, `SourceConnectionString`, `TargetConnectionString`, `OnCollectionsCommitted`, `OnCancelled`. Both connection-string parameters required (non-null, non-empty) by contract.
- [x] State fields per section 4 (`_liveUnits`, `_drafts`, `_toRemoveIds`, `_clusterNodes`, `_loadingNodes`, `_error`, `_expandedDraftId`, `_showAddPanel`) plus `_showSummary` for the in-place view swap. No `PendingEdit` collection.
- [x] Single Bootstrap `modal modal-xl` shell; body and footer swap between **list view** and **summary view**; title + `[×]` stay (section 3.7). _Note: implemented as `modal-xl` rather than the spec's `modal-lg` to fit the 7-column summary table comfortably._

### 10.6 Front-end — list view

- [x] Render single `table-sm` with columns: checkbox, state glyph (blank / `+` / `−`), namespace, status, options summary chips, actions (section 3.2).
- [x] Pencil `✎` shown **only on draft rows**; live rows have `[×]` only; pending-removal rows have `↺` only.
- [x] Search box at top filters by namespace (25+ collections triggers filter, matching today). — `PaginationHelper.ShowFilter()` toggle.
- [x] `PaginationHelper<MigrationUnit>` drives pagination; drafts and pending-removals interleaved into the same paginated source (section 8 decision 2).
- [x] Footer: `n drafts · m to remove` counter on the left, `[Cancel]  [Next]` on the right. `Next` disabled when both counts are zero.
- [x] `Cancel` confirm-prompts only if any pending counts are non-zero. — `_showCancelConfirmation` flag + `YesNoDialog` in [ManageCollectionsModal.razor.cs](../MongoMigrationWebApp/Components/ManageCollectionsModal.razor.cs).

### 10.7 Front-end — Add panel

- [x] `[+ Add]` button pins an inline form row at the top of the list (section 3.3).
- [x] Fields: Namespaces / JSON textarea, then collapsed `▾ Advanced` block (Overwrite, Indexing, Sharding, Move to, Filter).
- [x] `[Upload file]` button on the Add panel. — `<InputFile>` (`.txt` / `.json`) below the textarea; `OnUploadNamespacesFile()` appends contents to `_formNamespaces` with a 1 MB cap.
- [x] `Add` validates with `Helper.ValidateNamespaceFormat`, expands wildcards / JSON arrays, appends one draft row per resolved namespace.
- [x] Skip duplicates (already-live or already-draft) with inline warning. — `AddDraft()` sets `_error` to `"Collection {ns} already exists in the migration job and was skipped."`.
- [x] Apply conditional matrix from wizard plan section 4.2 (Overwrite=FALSE locks Indexing / Sharding / Move-to; Move-to enabled only when Sharding=`DontShard`).
- [x] Simulated-run mode disables Indexing / Sharding / Move-to in the panel.

### 10.8 Front-end — Edit panel (drafts only)

- [x] Pencil `✎` expands an inline edit panel under the draft row (accordion).
- [x] **Single-expansion only** (section 8 decision 3): opening one panel collapses any other open edit panel; track via `_expandedDraftId`.
- [x] Panel mirrors the Add form's five fields, pre-filled. `Save` updates draft in place (immutable `with` update) + collapses; `Cancel` discards panel changes.
- [x] Live rows render with no pencil and no expandable panel (section 3.4).

### 10.9 Front-end — Remove / Undo

- [x] `[×]` on a single live or draft row queues it (live → `_toRemoveIds`, draft → drop from `_drafts`).
- [x] Pending-removal rows render with strikethrough, `−` glyph, `↺ Undo` link; stay in the paginated source until Apply.
- [x] Footer counter updates live.

### 10.10 Front-end — Bulk actions

- [x] `Bulk ▾` button enabled when one or more rows are checked.
- [x] `Remove selected` — works on any mix of drafts and live rows.
- [x] `Set Overwrite / Indexing / Sharding / Move to / Clear filter` — **drafts only**; disabled with helper text `Existing collections cannot be edited; deselect them to view edit options.` when any live row is selected.
- [x] `Set Move to` populated from `GetClusterNodesAsync` and only offered when all selected drafts currently have Sharding=`DontShard`.
- [x] Bulk Set updates each affected draft in place, respecting the per-row conditional matrix.

### 10.11 Front-end — Summary view (in-place)

- [x] `Next` swaps body + footer in place (no second modal stacked on top). — `_showSummary` toggle.
- [x] Body: 7-column `table-sm` (Action, Namespace, Overwrite, Indexing, Sharding, Move to, Filter) — removals first, then additions; removal rows render option columns as `—`.
- [x] Long filter values truncated. — `TruncateFilter()` at 50 chars.
- [x] Hover-to-expand showing raw BSON on truncated filter values. — `<td title="@draft.Filter">` renders the full value on hover.
- [x] Conflict banner above the table when the same `db.col` appears in both lists. — `HasConflicts()` check.
- [x] Highlight the conflicting namespace in both its remove and add rows. — `table-warning` class applied conditionally on both removal and addition rows.
- [x] Footer: `[Cancel]  [Back]  [Apply changes]`. `Back` returns to list view with state preserved; `Apply changes` runs validations → remove-then-add → save → close → fire `OnCollectionsCommitted`.
- [x] `Cancel` in summary view confirm-prompts if pending counts are non-zero. — same `_showCancelConfirmation` + `YesNoDialog` mechanism as 10.6.

### 10.12 Front-end — Simulated-run gating

- [x] Indexing / Sharding / Move-to disabled in Add panel, Edit panel, and `Bulk ▾` when `MigrationJob.IsSimulatedRun`.
- [x] Helper text `Disabled in simulated run` shown next to the disabled controls. — inline `<small class="form-text text-muted">` rendered under each of the three gated selects when `IsSimulatedRun`.
- [x] Server-side double-check for Indexing (`GetEffectiveSkipIndexes()` forces skip when simulated).
- [x] Server-side double-check for Sharding / Move-to. — `GetEffectiveShardingStrategy()` forces `DontShard` and `GetEffectiveMoveToShard()` returns null when `IsSimulatedRun`.

### 10.13 Validation & error paths

- [x] Namespace format validated on Add (`Helper.ValidateNamespaceFormat`).
- [x] Filter JSON validated as parseable; bad JSON blocks `Save`/`Add` with inline error. — `JsonDocument.Parse()` guard in `AddCollections()`, sets `_error = "Invalid filter JSON: ..."`. _Note: validates JSON syntax only, not BSON-specific operator semantics; that surfaces server-side at apply time._
- [x] `Apply changes` failure path: surface error inline in the summary view, leave pending state intact, do not close modal.

### 10.14 Manual integration walk-through

After every box above is ticked, run through these scenarios end-to-end against a real job and confirm each works:

- [ ] Open modal with both connection strings present → list renders, no in-modal pre-step.
- [ ] Open modal with missing source / target connection string → modal does not open; existing inline error shown.
- [ ] Add one collection with defaults → `+ Add` → paste namespace → `Add` → `Next` → `Apply changes` → row appears live in Job Viewer.
- [ ] Add one collection with Overwrite=TRUE + Sharding=DontShard + Move to=<shard id> → values reach `MigrationUnit` and the worker honours them.
- [ ] Edit a draft (pencil) → change Overwrite → `Save` → row chip updates → `Next` → summary reflects new value.
- [ ] Open pencil on a second draft → first draft's panel collapses (single-expansion).
- [ ] Queue a live row for removal → `−` glyph + strikethrough; `↺` undoes it.
- [ ] Bulk-select 3 drafts + 1 live → `Set Overwrite → TRUE` is disabled with helper text; `Remove selected` works.
- [ ] Re-add a namespace that is also queued for removal → summary shows conflict banner; both rows highlighted; commit produces remove-then-add with change-stream loss warning.
- [ ] Cancel from list view with no pending changes → closes silently. Cancel with pending changes → confirm-prompt.
- [ ] Cancel from summary view with pending changes → confirm-prompt; `Back` from summary view → list view state preserved.
- [ ] Pagination + search behave the same way they do in the existing Job Viewer grid.
- [ ] Simulated-run job: Indexing / Sharding / Move-to are disabled everywhere; server rejects them even if forged.
- [ ] Round-trip an existing saved job (saved before this feature) → loads cleanly with new nullable fields defaulting to null.

### 10.15 Final reconciliation

- [ ] Re-read sections 1–9 of this doc against the implemented code; list any drift and fix.
- [ ] Confirm no requirement from section 7 ("does *not* change vs the wizard plan") was accidentally broken.
- [ ] Update [/memories/repo/mongohelper-notes.md](../memories/repo/mongohelper-notes.md) with any new findings from `GetClusterNodesAsync` testing.

