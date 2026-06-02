using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Forms;
using Microsoft.JSInterop;
using OnlineMongoMigrationProcessor;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Context;
using MongoMigrationWebApp.Models;
using MongoMigrationWebApp.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Text.Json;

namespace MongoMigrationWebApp.Components
{
    public partial class ManageCollectionsModal : ComponentBase
    {
        [Parameter, EditorRequired]
        public MigrationJob MigrationJob { get; set; } = null!;

        [Parameter, EditorRequired]
        public List<MigrationUnit> MigrationUnits { get; set; } = null!;

        [Parameter, EditorRequired]
        public string SourceConnectionString { get; set; } = string.Empty;

        [Parameter, EditorRequired]
        public string TargetConnectionString { get; set; } = string.Empty;

        [Parameter]
        public EventCallback OnCollectionsCommitted { get; set; }

        [Parameter]
        public EventCallback OnCancelled { get; set; }

        [Inject]
        private IJSRuntime JS { get; set; } = null!;

        // State fields per spec section 4
        private List<MigrationUnit> _liveUnits = new();
        private List<PendingAddition> _drafts = new();
        private HashSet<string> _toRemoveIds = new();
        private List<string> _clusterNodes = new();
        private bool _loadingNodes;
        private string? _error;
        private Guid? _expandedDraftId;
        private bool _showAddPanel;
        private bool _showSummary;
        private bool _isApplying;
        private bool _showCancelConfirmation;

        // For Add/Edit form state
        private string _formNamespaces = string.Empty;
        private bool? _formOverwrite = false;
        private IndexingStrategy? _formIndexing = IndexingStrategy.SameAsSource;
        private ShardingStrategy? _formSharding = ShardingStrategy.SameAsSource;
        private string? _formMoveToShard = null;
        private string? _formFilter = null;
        // Set when an edit panel just opened, so OnAfterRender can scroll it into view.
        private Guid? _pendingScrollDraftId;

        // Selection state
        private HashSet<string> _selectedLiveIds = new();
        private HashSet<Guid> _selectedDraftIds = new();

        // Pagination
        private PaginationHelper<object> _paginationHelper = null!;

        protected override void OnInitialized()
        {
            _liveUnits = MigrationUnits.ToList();

            // Initialize pagination with merged list (live units + drafts + pending removals)
            _paginationHelper = new PaginationHelper<object>(
                GetAllRowItems(),
                pageSize: 25,
                filterPredicate: (item, filter) =>
                {
                    var ns = GetNamespace(item);
                    return string.IsNullOrWhiteSpace(filter) ||
                           ns.Contains(filter, StringComparison.OrdinalIgnoreCase);
                }
            );
        }

        private List<object> GetAllRowItems()
        {
            var items = new List<object>();
            items.AddRange(_liveUnits.Cast<object>());
            items.AddRange(_drafts.Cast<object>());
            return items;
        }

        private string GetNamespace(object item)
        {
            return item switch
            {
                MigrationUnit u => $"{u.DatabaseName}.{u.CollectionName}",
                PendingAddition d => $"{d.DatabaseName}.{d.CollectionName}",
                _ => string.Empty
            };
        }

        private void RefreshPagination()
        {
            _paginationHelper.UpdateSource(GetAllRowItems());
            StateHasChanged();
        }

        // === LIST VIEW METHODS ===

        private void ToggleAddPanel()
        {
            _showAddPanel = !_showAddPanel;
            if (_showAddPanel)
            {
                ResetAddForm();
                _expandedDraftId = null; // Close any open edit panel
            }
        }

        private void ResetAddForm()
        {
            _formNamespaces = string.Empty;
            _formOverwrite = false;
            _formIndexing = IndexingStrategy.SameAsSource;
            _formSharding = ShardingStrategy.SameAsSource;
            _formMoveToShard = null;
            _formFilter = null;
        }

        private async Task AddCollections()
        {
            _error = null;

            if (string.IsNullOrWhiteSpace(_formNamespaces))
            {
                _error = "Please enter at least one namespace.";
                return;
            }

            // Validate filter JSON if provided
            if (!string.IsNullOrWhiteSpace(_formFilter))
            {
                try
                {
                    JsonDocument.Parse(_formFilter);
                }
                catch (JsonException ex)
                {
                    _error = $"Invalid filter JSON: {ex.Message}";
                    return;
                }
            }

            // Validate the user input using the shared helper so CSV (comma-separated),
            // wildcard (db.*, *.col, *.*), and JSON-array forms all parse the same way
            // they do in the existing Update Collections modal.
            var validation = Helper.ValidateNamespaceFormat(_formNamespaces, MigrationJob.JobType);
            if (!validation.Item1)
            {
                _error = validation.Item3;
                return;
            }

            List<MigrationUnit> resolvedUnits;
            try
            {
                resolvedUnits = await Helper.PopulateJobCollectionsAsync(MigrationJob, _formNamespaces, SourceConnectionString ?? string.Empty);
            }
            catch (Exception ex)
            {
                _error = $"Failed to resolve namespaces: {ex.Message}";
                return;
            }

            if (resolvedUnits == null || resolvedUnits.Count == 0)
            {
                _error = "No collections matched the supplied namespaces.";
                return;
            }

            int skipped = 0;
            foreach (var mu in resolvedUnits)
            {
                // For a wildcard / CSV expansion any per-row filter typed in the form
                // applies to every resolved row unless the JSON itself carries a filter.
                var rowFilter = string.IsNullOrWhiteSpace(mu.UserFilter) ? _formFilter : mu.UserFilter;
                if (!AddDraft(mu.DatabaseName, mu.CollectionName, mu.TargetDatabaseName, mu.TargetCollectionName, rowFilter))
                {
                    skipped++;
                }
            }

            if (skipped > 0 && _error == null)
            {
                _error = $"{skipped} namespace(s) were skipped because they already exist in the job or in the draft list.";
            }

            RefreshPagination();
            _showAddPanel = false;
            ResetAddForm();
        }

        private bool AddDraft(string db, string col, string? targetDb, string? targetCol, string? filter)
        {
            var ns = $"{db}.{col}";

            if (_liveUnits.Any(u => $"{u.DatabaseName}.{u.CollectionName}" == ns))
            {
                return false;
            }
            if (_drafts.Any(d => $"{d.DatabaseName}.{d.CollectionName}" == ns))
            {
                return false;
            }

            var normalized = DraftOptionRules.Normalize(
                _formOverwrite, _formIndexing, _formSharding, _formMoveToShard,
                MigrationJob.IsSimulatedRun);

            _drafts.Add(new PendingAddition
            {
                DatabaseName = db,
                CollectionName = col,
                TargetDatabaseName = targetDb,
                TargetCollectionName = targetCol,
                Filter = filter,
                Overwrite = normalized.Overwrite,
                IndexingStrategy = normalized.Indexing,
                ShardingStrategy = normalized.Sharding,
                MoveToShard = normalized.MoveToShard
            });
            return true;
        }

        private void RemoveDraft(Guid id)
        {
            _drafts.RemoveAll(d => d.Id == id);
            _selectedDraftIds.Remove(id);
            RefreshPagination();
        }

        private void QueueForRemoval(string unitId)
        {
            _toRemoveIds.Add(unitId);
            StateHasChanged();
        }

        private void UndoRemoval(string unitId)
        {
            _toRemoveIds.Remove(unitId);
            StateHasChanged();
        }

        private void ToggleEditPanel(Guid draftId)
        {
            if (_expandedDraftId == draftId)
            {
                _expandedDraftId = null;
            }
            else
            {
                _expandedDraftId = draftId;
                _showAddPanel = false; // Close add panel if open
                _pendingScrollDraftId = draftId;

                // Populate form with draft values
                var draft = _drafts.FirstOrDefault(d => d.Id == draftId);
                if (draft != null)
                {
                    _formOverwrite = draft.Overwrite;
                    _formIndexing = draft.IndexingStrategy;
                    _formSharding = draft.ShardingStrategy;
                    _formMoveToShard = draft.MoveToShard;
                    _formFilter = draft.Filter;
                }
            }
        }

        protected override async Task OnAfterRenderAsync(bool firstRender)
        {
            if (_pendingScrollDraftId.HasValue)
            {
                var id = $"draft-edit-{_pendingScrollDraftId.Value}";
                _pendingScrollDraftId = null;
                try
                {
                    await JS.InvokeVoidAsync("scrollToElement", id);
                }
                catch { /* JS may be unavailable during prerender */ }
            }
        }

        private void SaveEdit(Guid draftId)
        {
            var draft = _drafts.FirstOrDefault(d => d.Id == draftId);
            if (draft == null) return;

            // Validate filter JSON if provided
            if (!string.IsNullOrWhiteSpace(_formFilter))
            {
                try
                {
                    JsonDocument.Parse(_formFilter);
                }
                catch (JsonException ex)
                {
                    _error = $"Invalid filter JSON: {ex.Message}";
                    return;
                }
            }

            // Update draft with new values (create new record since it's immutable).
            // Run through the central rules helper so disabled-form fields don't leak stale values.
            var normalized = DraftOptionRules.Normalize(
                _formOverwrite, _formIndexing, _formSharding, _formMoveToShard,
                MigrationJob.IsSimulatedRun);
            var index = _drafts.IndexOf(draft);
            _drafts[index] = draft with
            {
                Overwrite = normalized.Overwrite,
                IndexingStrategy = normalized.Indexing,
                ShardingStrategy = normalized.Sharding,
                MoveToShard = normalized.MoveToShard,
                Filter = _formFilter
            };

            _expandedDraftId = null;
            _error = null;
            RefreshPagination();
        }

        private void CancelEdit()
        {
            _expandedDraftId = null;
        }

        // === SELECTION ===

        private int GetSelectedCount()
        {
            return _selectedLiveIds.Count + _selectedDraftIds.Count;
        }

        private bool AreAllSelectedDrafts()
        {
            return _selectedLiveIds.Count == 0 && _selectedDraftIds.Count > 0;
        }

        private bool AllSelectedHaveDontShard()
        {
            return _selectedDraftIds.All(id =>
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                return draft?.ShardingStrategy == ShardingStrategy.DontShard;
            });
        }

        // Matches the form's "Locked (Overwrite = No)" rule via the central DraftOptionRules helper.
        // Bulk Set buttons for Indexing/Sharding/Move-to are disabled if any selected draft is locked.
        private bool AnySelectedHasOverwriteFalse()
        {
            return _selectedDraftIds.Any(id =>
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                return draft != null && DraftOptionRules.IsIndexingLocked(draft.Overwrite, MigrationJob.IsSimulatedRun);
            });
        }

        private void ToggleSelectAll(ChangeEventArgs e)
        {
            var isChecked = (bool)(e.Value ?? false);
            if (isChecked)
            {
                _selectedLiveIds = _liveUnits.Where(u => !_toRemoveIds.Contains(u.Id)).Select(u => u.Id).ToHashSet();
                _selectedDraftIds = _drafts.Select(d => d.Id).ToHashSet();
            }
            else
            {
                _selectedLiveIds.Clear();
                _selectedDraftIds.Clear();
            }
        }

        private bool IsAllSelected()
        {
            var liveCount = _liveUnits.Count(u => !_toRemoveIds.Contains(u.Id));
            return liveCount > 0 && _selectedLiveIds.Count == liveCount && _selectedDraftIds.Count == _drafts.Count;
        }

        // === BULK ACTIONS ===

        private void BulkRemove()
        {
            foreach (var id in _selectedLiveIds)
            {
                QueueForRemoval(id);
            }

            var draftsToRemove = _selectedDraftIds.ToList();
            foreach (var id in draftsToRemove)
            {
                RemoveDraft(id);
            }

            _selectedLiveIds.Clear();
            _selectedDraftIds.Clear();
        }

        private void BulkSetOverwrite(bool value)
        {
            foreach (var id in _selectedDraftIds)
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                if (draft == null) continue;

                var normalized = DraftOptionRules.Normalize(
                    value, draft.IndexingStrategy, draft.ShardingStrategy, draft.MoveToShard,
                    MigrationJob.IsSimulatedRun);
                var index = _drafts.IndexOf(draft);
                _drafts[index] = draft with
                {
                    Overwrite = normalized.Overwrite,
                    IndexingStrategy = normalized.Indexing,
                    ShardingStrategy = normalized.Sharding,
                    MoveToShard = normalized.MoveToShard
                };
            }
            RefreshPagination();
        }

        private void BulkSetIndexing(IndexingStrategy strategy)
        {
            foreach (var id in _selectedDraftIds)
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                if (draft == null || DraftOptionRules.IsIndexingLocked(draft.Overwrite, MigrationJob.IsSimulatedRun)) continue;

                var index = _drafts.IndexOf(draft);
                _drafts[index] = draft with { IndexingStrategy = strategy };
            }
            RefreshPagination();
        }

        private void BulkSetSharding(ShardingStrategy strategy)
        {
            foreach (var id in _selectedDraftIds)
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                if (draft == null || DraftOptionRules.IsShardingLocked(draft.Overwrite, MigrationJob.IsSimulatedRun)) continue;

                var index = _drafts.IndexOf(draft);
                // Switching away from DontShard invalidates MoveToShard; run through Normalize.
                var normalized = DraftOptionRules.Normalize(
                    draft.Overwrite, draft.IndexingStrategy, strategy, draft.MoveToShard,
                    MigrationJob.IsSimulatedRun);
                _drafts[index] = draft with
                {
                    ShardingStrategy = normalized.Sharding,
                    MoveToShard = normalized.MoveToShard
                };
            }
            RefreshPagination();
        }

        private void BulkSetMoveToAuto()
        {
            foreach (var id in _selectedDraftIds)
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                if (draft == null || DraftOptionRules.IsMoveToShardLocked(draft.Overwrite, draft.ShardingStrategy, MigrationJob.IsSimulatedRun)) continue;

                var index = _drafts.IndexOf(draft);
                _drafts[index] = draft with { MoveToShard = null };
            }
            RefreshPagination();
        }

        private void BulkSetMoveToShard(string shardId)
        {
            foreach (var id in _selectedDraftIds)
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                if (draft == null || DraftOptionRules.IsMoveToShardLocked(draft.Overwrite, draft.ShardingStrategy, MigrationJob.IsSimulatedRun)) continue;

                var index = _drafts.IndexOf(draft);
                _drafts[index] = draft with { MoveToShard = shardId };
            }
            RefreshPagination();
        }

        private void BulkClearFilter()
        {
            foreach (var id in _selectedDraftIds)
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                if (draft == null) continue;

                var index = _drafts.IndexOf(draft);
                _drafts[index] = draft with { Filter = null };
            }
            RefreshPagination();
        }

        // === CLUSTER NODES ===

        private async Task EnsureClusterNodesLoaded()
        {
            if (_clusterNodes.Count > 0 || _loadingNodes) return;

            _loadingNodes = true;
            try
            {
                _clusterNodes = await MongoHelper.GetClusterNodesAsync(TargetConnectionString);
            }
            catch (Exception ex)
            {
                MigrationJobContext.AddVerboseLog($"Failed to load cluster nodes: {ex.Message}");
            }
            finally
            {
                _loadingNodes = false;
            }
        }

        // === NAVIGATION ===

        private int GetPendingAddCount() => _drafts.Count;
        private int GetPendingRemoveCount() => _toRemoveIds.Count;

        private void ShowSummary()
        {
            _showSummary = true;
        }

        private void BackToList()
        {
            _showSummary = false;
        }

        private void Cancel()
        {
            if (GetPendingAddCount() > 0 || GetPendingRemoveCount() > 0)
            {
                _showCancelConfirmation = true;
            }
            else
            {
                ConfirmCancel();
            }
        }

        private async void ConfirmCancel()
        {
            _showCancelConfirmation = false;
            await OnCancelled.InvokeAsync();
        }

        private void CancelCancelConfirmation()
        {
            _showCancelConfirmation = false;
            StateHasChanged();
        }

        // === SUMMARY VIEW ===

        private bool HasConflicts()
        {
            return _drafts.Any(d =>
            {
                var ns = $"{d.DatabaseName}.{d.CollectionName}";
                return _toRemoveIds.Any(id =>
                {
                    var u = _liveUnits.FirstOrDefault(x => x.Id == id);
                    return u != null && $"{u.DatabaseName}.{u.CollectionName}" == ns;
                });
            });
        }

        private string TruncateFilter(string filter)
        {
            const int maxLen = 50;
            return filter.Length > maxLen ? filter.Substring(0, maxLen) + "..." : filter;
        }

        private async Task ApplyChanges()
        {
            _isApplying = true;
            _error = null;

            try
            {
                // Step 1: Remove collections. Sync-back guard: clear change-stream resume tokens
                // before removal so a later re-add of the same namespace starts a fresh stream.
                foreach (var id in _toRemoveIds)
                {
                    var unit = _liveUnits.FirstOrDefault(u => u.Id == id);
                    if (unit != null)
                    {
                        unit.ResumeToken = null;
                        unit.OriginalResumeToken = null;
                        unit.CSLastResumeTokenWithChange = null;
                        unit.CSLastChangeUTCTime = null;
                        unit.SyncBackResumeToken = null;
                        unit.SyncBackOriginalResumeToken = null;
                        unit.SyncBackCSLastResumeTokenWithChange = null;
                        unit.SyncBackCSLastChangeUTCTime = null;
                        unit.Remove();
                    }
                }

                // Step 2: Add drafts as new MigrationUnits
                foreach (var draft in _drafts)
                {
                    var unit = new MigrationUnit(MigrationJob, draft.DatabaseName, draft.CollectionName, new List<MigrationChunk>());
                    draft.ApplyToMigrationUnit(unit);

                    MigrationJob.MigrationUnitBasics ??= new List<MigrationUnitBasic>();
                    MigrationJob.MigrationUnitBasics.Add(unit.GetBasic());

                    unit.Persist();
                }

                // Step 3: Save job
                if (!MigrationJobContext.SaveMigrationJob(MigrationJob))
                {
                    _error = "Failed to save migration job.";
                    return;
                }

                // Step 4: Fire callback and close
                await OnCollectionsCommitted.InvokeAsync();
            }
            catch (Exception ex)
            {
                _error = $"Error applying changes: {ex.Message}";
            }
            finally
            {
                _isApplying = false;
            }
        }

        // === UPLOAD FILE ===

        private async Task OnUploadNamespacesFile(InputFileChangeEventArgs e)
        {
            _error = null;
            var file = e.File;
            if (file == null) return;

            // Cap at 1 MB to protect against accidental huge uploads.
            const long maxBytes = 1024 * 1024;
            if (file.Size > maxBytes)
            {
                _error = $"Upload rejected: file is {file.Size} bytes, max {maxBytes}.";
                return;
            }

            try
            {
                using var stream = file.OpenReadStream(maxBytes);
                using var reader = new System.IO.StreamReader(stream);
                var contents = await reader.ReadToEndAsync();
                _formNamespaces = string.IsNullOrWhiteSpace(_formNamespaces)
                    ? contents
                    : _formNamespaces.TrimEnd() + Environment.NewLine + contents;
            }
            catch (Exception ex)
            {
                _error = $"Failed to read uploaded file: {ex.Message}";
            }
        }

        // === FILTER ===

        private void OnFilterChanged()
        {
            RefreshPagination();
        }

        private void ClearFilter()
        {
            _paginationHelper.FilterText = string.Empty;
            RefreshPagination();
        }
    }
}
