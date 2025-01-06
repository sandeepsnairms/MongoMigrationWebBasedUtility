using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor
{
    public static class Helper
    {
        public static async Task<string> EnsureMongoToolsAvailableAsync(string toolsDestinationFolder,string toolsDownloadUrl)
        {
            try
            {
                string toolsLaunchFolder = Path.Combine(toolsDestinationFolder, Path.GetFileNameWithoutExtension(toolsDownloadUrl), "bin");

                string mongodumpPath = Path.Combine(toolsLaunchFolder, "mongodump.exe");
                string mongorestorePath = Path.Combine(toolsLaunchFolder, "mongorestore.exe");

                // Check if tools exist
                if (File.Exists(mongodumpPath) && File.Exists(mongorestorePath))
                {
                    Log.WriteLine("Environment ready to use.");
                    Log.Save();
                    return toolsLaunchFolder;
                }

                Log.WriteLine("Downloading tools...");

                // Download ZIP file
                string zipFilePath = Path.Combine(toolsDestinationFolder, "mongo-tools.zip");
                Directory.CreateDirectory(toolsDestinationFolder);

                using (HttpClient client = new HttpClient())
                {
                    using (var response = await client.GetAsync(toolsDownloadUrl))
                    {
                        response.EnsureSuccessStatusCode();
                        await using (var fs = new FileStream(zipFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
                        {
                            await response.Content.CopyToAsync(fs);
                        }
                    }
                }


                // Extract ZIP file
                ZipFile.ExtractToDirectory(zipFilePath, toolsDestinationFolder, overwriteFiles: true);
                File.Delete(zipFilePath);

                if (File.Exists(mongodumpPath) && File.Exists(mongorestorePath))
                {
                    Log.WriteLine("Environment ready to use.");
                    Log.Save();
                    return toolsLaunchFolder;
                }
                Log.WriteLine("Environment failed.", LogType.Error);
                Log.Save();
                return string.Empty;
            }
            catch (Exception ex)
            {
                Log.WriteLine($"Error: {ex}", LogType.Error);
                Log.Save();
                return string.Empty;
            }
        }
    }
}
