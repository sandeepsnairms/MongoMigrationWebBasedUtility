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
                if (processType == "MongoRestore")
                {
                    mu.RestorePercent = Math.Min(100,basePercent + percent * contribFactor);
                    if (mu.RestorePercent == 100)
                        mu.RestoreComplete = true;
                }
                else
                {
                    mu.DumpPercent = Math.Min(100, basePercent + percent * contribFactor);
                    if (mu.DumpPercent == 100)
                        mu.DumpComplete = true;
                }
                jobList.Save();
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

