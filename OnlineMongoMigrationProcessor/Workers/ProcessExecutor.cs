using System;
using System.Collections.Generic;
using System.Diagnostics;
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

		public ProcessExecutor(Log log)
        {
            _log = log;
		}
		
		/// <summary>
		/// Executes a process with the given executable path and arguments.
		/// </summary>
		/// <param name="jobList">The job list for saving state.</param>
		/// <param name="mu">Migration unit.</param>
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
			JobList jobList, 
			MigrationUnit mu, 
			MigrationChunk chunk, 
			int chunkIndex, 
			double basePercent, 
			double contribFactor, 
			long targetCount, 
			string exePath, 
			string arguments,
			CancellationToken cancellationToken,
			Action<int>? onProcessStarted = null,
			Action<int>? onProcessEnded = null)
        {
			_cancellationToken = cancellationToken;
            string processType = exePath.ToLower().Contains("restore") ? "MongoRestore" : "MongoDump";
            
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
                        ProcessConsoleOutput(args.Data, processType, mu, chunk, chunkIndex, basePercent, contribFactor, targetCount, jobList);
                    }
                };

                _process.Start();
                _process.BeginOutputReadLine();
                _process.BeginErrorReadLine();

                int processId = _process.Id;
                onProcessStarted?.Invoke(processId);
                _log.WriteLine($"{processType} process started: PID {processId} for chunk {chunkIndex}");

                // Wait for process to exit, checking cancellation periodically
                while (!_process.WaitForExit(1000))
                {
                    if (_cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            _process.Kill(entireProcessTree: true);
                            _log.WriteLine($"{processType} process {processId} terminated due to cancellation.");
                            onProcessEnded?.Invoke(processId);
                            return false;
                        }
                        catch (Exception ex)
                        {
                            _log.WriteLine($"Error killing process {processId}: {Helper.RedactPii(ex.Message)}", LogType.Error);
                        }
                    }
                }

                onProcessEnded?.Invoke(processId);
                
                bool success = _process.ExitCode == 0;
                if (!success)
                {
                    _log.WriteLine($"{processType} process {processId} exited with code {_process.ExitCode}", LogType.Error);
                }
                
                return success;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error executing {processType}: {Helper.RedactPii(ex.Message)}", LogType.Error);
                return false;
            }
        }

        private void ProcessConsoleOutput(string data, string processType, MigrationUnit mu, MigrationChunk chunk,int chunkIndex, double basePercent, double contribFactor, long targetCount, JobList jobList)
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
            //mongorestore doesn't report on doc count sometimes. hence we need to calculate  based on targetCount percent
            if (percent == 100 & processType == "MongoRestore")
            {
                chunk.RestoredSuccessDocCount = targetCount - (chunk.RestoredFailedDocCount + chunk.SkippedAsDuplicateCount);
                jobList.Save();
            }

            if (percent > 0 && targetCount>0)
            {
                _log.ShowInMonitor($"{processType} for {mu.DatabaseName}.{mu.CollectionName} Chunk[{chunkIndex}] : {percent}%");
                
                // Immediately calculate and update percentage from all chunks
                if (processType == "MongoRestore")
                {
                    mu.RestorePercent = CalculateOverallPercentFromAllChunks(mu, isRestore: true);
                    if (mu.RestorePercent >= 99.99)
                        mu.RestoreComplete = true;
                }
                else
                {
                    mu.DumpPercent = CalculateOverallPercentFromAllChunks(mu, isRestore: false);
                    if (mu.DumpPercent >= 99.99)
                        mu.DumpComplete = true;
                }
                jobList.Save();
                
                // Ensure timer is running for this migration unit to handle percentage updates
                EnsurePercentageTimerRunning(mu, jobList, processType);
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
                    _log.WriteLine($"{processType} Response: {Helper.RedactPii(data)}");
                }
            }
        }

        /// <summary>
        /// Calculates overall percent from all chunks by checking their current state.
        /// Used by timer to recalculate overall progress for dump or restore operations.
        /// </summary>
        private static double CalculateOverallPercentFromAllChunks(MigrationUnit mu, bool isRestore)
        {
            double totalPercent = 0;
            long totalDocs = Helper.GetMigrationUnitDocCount(mu);
            
            if (totalDocs == 0) return 0;
            
            for (int i = 0; i < mu.MigrationChunks.Count; i++)
            {
                var c = mu.MigrationChunks[i];
                double chunkContrib = (double)c.DumpQueryDocCount / totalDocs;
                
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
                        double chunkPercent = Math.Min(100, (double)c.RestoredSuccessDocCount / c.DumpQueryDocCount * 100);
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
                        double chunkPercent = Math.Min(100, (double)c.DumpResultDocCount / c.DumpQueryDocCount * 100);
                        totalPercent += chunkPercent * chunkContrib;
                    }
                    // else: not started, contributes 0%
                }
            }
            
            return Math.Min(100, totalPercent);
        }

        /// <summary>
        /// Ensures a timer is running for the given migration unit to periodically recalculate percentages.
        /// Timer runs every 5 seconds and stops when all chunks are complete.
        /// </summary>
        private static void EnsurePercentageTimerRunning(MigrationUnit mu, JobList jobList, string processType)
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
                            // Check if there are any active restore chunks
                            foreach (var chunk in mu.MigrationChunks)
                            {
                                if (chunk.IsUploaded != true && chunk.RestoredSuccessDocCount > 0)
                                {
                                    hasActiveChunks = true;
                                    break;
                                }
                            }
                            
                            if (hasActiveChunks)
                            {
                                // Recalculate overall restore percent from all chunks
                                double previousPercent = mu.RestorePercent;
                                mu.RestorePercent = CalculateOverallPercentFromAllChunks(mu, isRestore: true);
                                if (mu.RestorePercent >= 99.99)
                                    mu.RestoreComplete = true;
                                jobList.Save();
                            }
                        }
                        else // MongoDump
                        {
                            // Check if there are any active dump chunks
                            foreach (var chunk in mu.MigrationChunks)
                            {
                                if (chunk.IsDownloaded != true && chunk.DumpResultDocCount > 0)
                                {
                                    hasActiveChunks = true;
                                    break;
                                }
                            }
                            
                            if (hasActiveChunks)
                            {
                                // Recalculate overall dump percent from all chunks
                                double previousPercent = mu.DumpPercent;
                                mu.DumpPercent = CalculateOverallPercentFromAllChunks(mu, isRestore: false);
                                if (mu.DumpPercent >= 99.99)
                                    mu.DumpComplete = true;
                                jobList.Save();
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

        /// <summary>
        /// Terminates the currently running process, if any.
        /// Note: This method is deprecated. Use CancellationToken instead.
        /// </summary>
        [Obsolete("Use CancellationToken for graceful cancellation")]
        public void Terminate()
        {
            lock (_processLock)
            {
                if (_process != null && !_process.HasExited)
                {
                    try
                    {
                        _process.Kill(entireProcessTree: true);
                        _log.WriteLine("Process terminated via Terminate() method");
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"Error terminating process: {Helper.RedactPii(ex.Message)}", LogType.Error);
                    }
                }
            }
        }
    }
}

