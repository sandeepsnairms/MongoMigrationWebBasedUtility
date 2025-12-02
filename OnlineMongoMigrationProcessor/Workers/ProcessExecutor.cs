using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Xml.Linq;

#pragma warning disable CS8600
namespace OnlineMongoMigrationProcessor.Workers
{
    internal class ProcessExecutor
    {
        private Log _log;
        private Process? _process = null;
        private readonly object _processLock = new object();
        private CancellationToken _cancellationToken;
        private static readonly Dictionary<string, System.Timers.Timer> _percentageTimers = new Dictionary<string, System.Timers.Timer>();
        private static readonly object _timerLock = new object();
        private const int PERCENTAGE_UPDATE_INTERVAL_MS = 5000; // 5 seconds
        private ActiveMigrationUnitsCache _muCache;
        public ProcessExecutor(Log log, ActiveMigrationUnitsCache muCache)
        {
            _log = log;
            _muCache = muCache;
		}

        /// <summary>
        /// Executes a process with the given executable path and arguments.
        /// </summary>
        /// <param name="jobList">The MigrationJobContext.MigrationJob list for saving state.</param>
        /// <param name="mu">Migration mu.</param>
        /// <param name="chunk">Migration chunk.</param>
        /// <param name="chunkIndex">Index of the chunk.</param>
        /// <param name="basePercent">Base percentage for progress calculation.</param>
        /// <param name="contribFactor">Contribution factor for progress calculation.</param>
        /// <param name="targetCount">Target document count.</param>
        /// <param name="exePath">The full path to the executable file.</param>
        /// <param name="arguments">The arguments to pass to the executable.</param>
        /// <param name="cancellationToken">Cancellation token for graceful shutdown.</param>
        /// <param name="onProcessStarted">Callback when process starts with PID.</param>
        /// <param name="onProcessEnded">Callback when process ends with PID.</param>
        /// <returns>True if the process completed successfully, otherwise false.</returns>
        public bool Execute(
            MigrationUnit mu,
            MigrationChunk chunk,
            int chunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            string exePath,
            string arguments,
            string outputFilePath,
            CancellationToken cancellationToken,
            Action<int>? onProcessStarted = null,
            Action<int>? onProcessEnded = null)
        {
            _cancellationToken = cancellationToken;
            string processType = exePath.ToLower().Contains("restore") ? "MongoRestore" : "MongoDump";

            EnsurePercentageTimerRunning(mu, processType, _log);

            try
            {
                lock (_processLock)
                {
                    _process = new Process
                    {
                        StartInfo = new ProcessStartInfo
                        {
                            FileName = exePath,
                            Arguments = arguments,
                            RedirectStandardOutput = true,
                            RedirectStandardError = true,
                            RedirectStandardInput = true,   // needed for restore streaming
                            UseShellExecute = false,
                            CreateNoWindow = true
                        }
                    };
                }

                StringBuilder outputBuffer = new StringBuilder();
                StringBuilder errorBuffer = new StringBuilder();

                _process.OutputDataReceived += (sender, args) =>
                {
                    if (!string.IsNullOrEmpty(args.Data))
                    {
                        outputBuffer.AppendLine(args.Data);
                        _log.WriteLine($"{processType} Log: {Helper.RedactPii(args.Data)}");
                    }
                };

                _process.ErrorDataReceived += (sender, args) =>
                {
                    if (!string.IsNullOrEmpty(args.Data))
                    {
                        _log.WriteLine($"{processType} Log: {Helper.RedactPii(args.Data)}", LogType.Debug);
                        errorBuffer.AppendLine(args.Data);
                        ProcessConsoleOutput(args.Data, processType, mu, chunk, chunkIndex, basePercent, contribFactor, targetCount);
                    }
                };

                _process.Start();
                int processId = _process.Id;
                onProcessStarted?.Invoke(processId);
                _log.WriteLine($"{processType} process started: PID {processId} for chunk {chunkIndex}");

                if (processType == "MongoDump")
                {
                    // stdout is streamed to file → do NOT call BeginOutputReadLine
                    _process.BeginErrorReadLine();   // we still need progress messages from stderr
                }
                else
                {
                    // restore → read console normally
                    _process.BeginOutputReadLine();
                    _process.BeginErrorReadLine();
                }

                // -------------------------
                // Dump => write stdout -> file
                // Restore => read file -> stdin
                // -------------------------
                if (processType == "MongoDump")
                {
                    using var fileStream = new FileStream(
                        outputFilePath, FileMode.Create, FileAccess.Write, FileShare.None,
                        bufferSize: 81920, useAsync: true);

                    _process.StandardOutput.BaseStream.CopyToAsync(fileStream, cancellationToken).Wait(cancellationToken);
                    fileStream.Flush();
                }
                else // MongoRestore
                {
                    using var fileStream = new FileStream(
                        outputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read,
                        bufferSize: 81920, useAsync: true);

                    fileStream.CopyToAsync(_process.StandardInput.BaseStream, cancellationToken).Wait(cancellationToken);
                    _process.StandardInput.Close(); // signal EOF to mongo restore
                }

                // cancellation-aware process wait
                while (!_process.WaitForExit(1000))
                {
                    if (_cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            _process.Kill(entireProcessTree: true);
                            _log.WriteLine($"{processType} process {processId} terminated due to cancellation.");
                        }
                        catch (Exception ex)
                        {
                            _log.WriteLine($"Error killing process {processId}: {Helper.RedactPii(ex.Message)}", LogType.Error);
                        }

                        onProcessEnded?.Invoke(processId);
                        string key = $"{mu.DatabaseName}.{mu.CollectionName}.{processType}";
                        _percentageTimers[key].Enabled = false;
                        return false;
                    }
                }

                onProcessEnded?.Invoke(processId);

                bool success = _process.ExitCode == 0;
                if (!success)
                    _log.WriteLine($"{processType} process {processId} exited with code {_process.ExitCode}", LogType.Error);

                return success;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error executing {processType}: {Helper.RedactPii(ex.Message)}", LogType.Error);
                return false;
            }
        }


        private void ProcessConsoleOutput(string data, string processType, MigrationUnit mu, MigrationChunk chunk,int chunkIndex, double basePercent, double contribFactor, long targetCount)
        {
            string percentValue = ExtractPercentage(data);
            string docsProcessed = ExtractDocCount(data);

            double percent = 0;
            int count;

            if (!string.IsNullOrEmpty(percentValue))
                double.TryParse(percentValue, out percent);

            if (!string.IsNullOrEmpty(docsProcessed) && int.TryParse(docsProcessed, out count) && count > 0)
            {
                percent = Math.Round((double)count / targetCount * 100, 3);
                
                // Update chunk counts so timer can calculate overall progress
                if (processType == "MongoRestore")
                {
                    chunk.RestoredSuccessDocCount = count;
                }
                else
                {
                    chunk.DumpResultDocCount = count;
                }
            }
            else if (percent > 0 && targetCount > 0)
            {
                // When only percentage is reported (no doc count), calculate the doc count
                // This happens with mongorestore which often reports percentage but not document count
                long calculatedCount = (long)(percent / 100.0 * targetCount);
                
                if (processType == "MongoRestore")
                {
                    chunk.RestoredSuccessDocCount = calculatedCount;
                }
                else
                {
                    chunk.DumpResultDocCount = calculatedCount;
                }
            }
            //mongorestore doesn't report on doc count sometimes. hence we need to calculate  based on targetCount percent
            if (percent == 100 & processType == "MongoRestore")
            {
                chunk.RestoredSuccessDocCount = targetCount - (chunk.RestoredFailedDocCount + chunk.SkippedAsDuplicateCount);

                MigrationJobContext.SaveMigrationUnit(mu,false);
            }

            if (percent > 0 && targetCount>0)
            {
                _log.ShowInMonitor($"{processType} for {mu.DatabaseName}.{mu.CollectionName} Chunk[{chunkIndex}] : {percent}%");
                
                // Immediately calculate and update percentage from all chunks
                if (processType == "MongoRestore")
                {
                    mu.RestorePercent = CalculateOverallPercentFromAllChunks(mu, isRestore: true, log: _log);
                    if (mu.RestorePercent >= 99.99)
                    {
                        mu.RestoreComplete = true;
                        _muCache.RemoveMigrationUnit(mu.Id);
                    }
                }
                else
                {
                    mu.DumpPercent = CalculateOverallPercentFromAllChunks(mu, isRestore: false, log: _log);
                    if (mu.DumpPercent >= 99.99)
                        mu.DumpComplete = true;
                }

                MigrationJobContext.SaveMigrationUnit(mu,true);

                // Ensure timer is running for this migration mu to handle percentage updates
                EnsurePercentageTimerRunning(mu, processType, _log);
            }
            else
            {
                if (processType == "MongoRestore")
                {
                    var (restoredCount, failedCount, restorePercent) = ExtractRestoreCounts(data);
                    if (restoredCount > 0 || failedCount > 0)
                    {
                        chunk.RestoredSuccessDocCount = restoredCount;
                        chunk.RestoredFailedDocCount = failedCount;
                    }
                    if (restoredCount == 0 && failedCount == 0 && restorePercent ==100)
                    {
                        chunk.IsUploaded = true;
                    }

                }
                if (!data.Contains("continuing through error: Duplicate key violation on the requested collection"))
                {
                    _log.WriteLine($"{processType} Response for {mu.DatabaseName}.{mu.CollectionName}: {Helper.RedactPii(data)}");
                }
            }
        }

        /// <summary>
        /// Calculates overall percent from all chunks by checking their current state.
        /// Used by timer to recalculate overall progress for dump or restore operations.
        /// </summary>
        private static double CalculateOverallPercentFromAllChunks(MigrationUnit mu, bool isRestore, Log log)
        {
            double totalPercent = 0;
            long totalDocs = Helper.GetMigrationUnitDocCount(mu);
            
            if (totalDocs == 0) return 0;

            string strLog=$"DumpResultDocCount/DumpQueryDocCount - chunkPercent - Contrib -  TotalPercent";

            for (int i = 0; i < mu.MigrationChunks.Count; i++)
            {
                var c = mu.MigrationChunks[i];
                
                if (c.DumpQueryDocCount == 0)
                {
                    continue;
                }
                
                double chunkContrib = (double)c.DumpQueryDocCount / totalDocs;
                double chunkPercent = 0;
                if (isRestore)
                {
                    
                    if (c.IsUploaded == true)
                    {
                        // Completed chunk: 100%
                        totalPercent += 100 * chunkContrib;
                    }
                    else if (c.RestoredSuccessDocCount > 0)
                    {
                        // In-progress chunk: calculate from restored count
                        chunkPercent = Math.Min(100, (double)c.RestoredSuccessDocCount / c.DumpQueryDocCount * 100);
                        totalPercent += chunkPercent * chunkContrib;
                    }
                    
                    // else: not started, contributes 0%
                }
                else // Dump
                {
                    if (c.IsDownloaded == true)
                    {
                        // Completed chunk: 100%
                        totalPercent += 100 * chunkContrib;
                    }
                    else if (c.DumpResultDocCount > 0)
                    {
                        // In-progress chunk: calculate from dumped count
                        chunkPercent = Math.Min(100, (double)c.DumpResultDocCount / c.DumpQueryDocCount * 100);
                        totalPercent += chunkPercent * chunkContrib;
                    }
                    // else: not started, contributes 0%
                    
                }
                strLog= $"{strLog}\n [{i}] {c.DumpResultDocCount}/{c.DumpQueryDocCount} - {chunkPercent} - {chunkContrib} - {totalPercent}";
            }  
            log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} Total {totalPercent} IsRestore{isRestore}: {strLog}", LogType.Verbose);
            return Math.Min(100, totalPercent);
        }

        /// <summary>
        /// Ensures a timer is running for the given migration mu to periodically recalculate percentages.
        /// Timer runs every 5 seconds and stops when all chunks are complete.
        /// </summary>
        private static void EnsurePercentageTimerRunning(MigrationUnit mu, string processType, Log log)
        {
            string key = $"{mu.DatabaseName}.{mu.CollectionName}.{processType}";
            
            lock (_timerLock)
            {
                // Check if timer already exists and is enabled
                if (_percentageTimers.ContainsKey(key) && _percentageTimers[key].Enabled)
                {
                    return; // Timer already running
                }
                
                // Create new timer
                var timer = new System.Timers.Timer(PERCENTAGE_UPDATE_INTERVAL_MS);
                timer.AutoReset = true;
                timer.Elapsed += (sender, e) =>
                {
                    try
                    {
                        bool hasActiveChunks = false;
                        
                        if (processType == "MongoRestore")
                        {
                            // Check if there are any active or pending restore chunks
                            // Active: RestoredSuccessDocCount > 0 and not uploaded
                            // Pending: IsDownloaded (dump complete) but not IsUploaded (restore not complete)
                            foreach (var chunk in mu.MigrationChunks)
                            {
                                if (chunk.IsUploaded != true && (chunk.RestoredSuccessDocCount > 0 || chunk.IsDownloaded == true))
                                {
                                    hasActiveChunks = true;
                                    break;
                                }
                            }
                            
                            if (hasActiveChunks)
                            {
                                // Recalculate overall restore percent from all chunks
                                double previousPercent = mu.RestorePercent;
                                mu.RestorePercent = CalculateOverallPercentFromAllChunks(mu, isRestore: true, log: log);
                                if (mu.RestorePercent >= 99.99)
                                    mu.RestoreComplete = true;

                                MigrationJobContext.SaveMigrationUnit(mu,true);
                            }
                        }
                        else // MongoDump
                        {
                            // Check if there are any active or pending dump chunks
                            // Active: DumpResultDocCount > 0 and not downloaded
                            // Pending: DumpQueryDocCount > 0 (chunk initialized) but not IsDownloaded
                            foreach (var chunk in mu.MigrationChunks)
                            {
                                if (chunk.IsDownloaded != true && chunk.DumpQueryDocCount > 0)
                                {
                                    hasActiveChunks = true;
                                    break;
                                }
                            }
                            
                            if (hasActiveChunks)
                            {
                                // Recalculate overall dump percent from all chunks
                                double previousPercent = mu.DumpPercent;
                                mu.DumpPercent = CalculateOverallPercentFromAllChunks(mu, isRestore: false, log: log);
                                if (mu.DumpPercent >= 99.99)
                                    mu.DumpComplete = true;

                                MigrationJobContext.SaveMigrationUnit(mu,true);
                            }
                        }
                        
                        // Stop timer if no active chunks
                        if (!hasActiveChunks)
                        {
                            lock (_timerLock)
                            {
                                if (_percentageTimers.ContainsKey(key))
                                {
                                    _percentageTimers[key].Stop();
                                    _percentageTimers[key].Dispose();
                                    _percentageTimers.Remove(key);
                                }
                            }
                        }
                    }
                    catch
                    {
                        // Ignore errors in timer callback
                    }
                };
                
                _percentageTimers[key] = timer;
                timer.Start();
            }
        }

        private string ExtractPercentage(string input)
        {
            // Regular expression to match the percentage value in the format (x.y%)
            var match = Regex.Match(input, @"\(([\d.]+)%\)");
            if (match.Success)
            {
                return match.Groups[1].Value; // Extract the percentage value without the parentheses and %
            }
            return string.Empty;
        }

        private string ExtractDocCount(string input)
        {

            var match = Regex.Match(input, @"\s+(\d+)$");
            if (match.Success)
            {
                return match.Groups[1].Value; // Extract the doc count value 
            }
            else
            {
                return ExtractDumpedDocumentCount(input).ToString();
            }

        }

        public (int RestoredCount, int FailedCount, double percentage) ExtractRestoreCounts(string input)
        {
            // Regular expressions to capture the counts
            var restoredMatch = Regex.Match(input, @"(\d+)\s+document\(s\)\s+restored\s+successfully");
            var failedMatch = Regex.Match(input, @"(\d+)\s+document\(s\)\s+failed\s+to\s+restore");

            // Extract counts with default value of 0 if no match
            int restoredCount = restoredMatch.Success ? int.Parse(restoredMatch.Groups[1].Value) : 0;
            int failedCount = failedMatch.Success ? int.Parse(failedMatch.Groups[1].Value) : 0;

            double percentage=0;
            if (restoredCount==0 && failedCount==0)
            {
                var match = Regex.Match(input, @"\(([\d.]+)%\)");

                if (match.Success)
                {
                    percentage = double.Parse(match.Groups[1].Value);
                    //Console.WriteLine($"Percentage: {percentage}%");
                }
            }

            return (restoredCount, failedCount, percentage);
        }

        public int ExtractDumpedDocumentCount(string input)
        {
            // Define the regex pattern to match "done" followed by document count
            string pattern = @"\bdone dumping.*\((\d+)\s+documents\)";
            //string pattern = @"\bdone dumping .*?\((\d+)\s+documents\)";

            var match = Regex.Match(input, pattern);

            // Check if the regex matched
            if (match.Success)
            {
                // Parse and return the document count
                return int.Parse(match.Groups[1].Value);
            }

            // Return 0 if no match found
            return 0;
        }

        //public void Terminate()
        //{
        //    lock (_processLock)
        //    {
        //        if (_process != null && !_process.HasExited)
        //        {
        //            try
        //            {
        //                _process.Kill(entireProcessTree: true);
        //                _log.WriteLine("Process terminated via Terminate() method");
        //            }
        //            catch (Exception ex)
        //            {
        //                _log.WriteLine($"Error terminating process: {Helper.RedactPii(ex.Message)}", LogType.Error);
        //            }
        //        }
        //    }
        //}
    }
}

