using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Mvc;
using OnlineMongoMigrationProcessor;
using OnlineMongoMigrationProcessor.Context;
using SharpCompress.Common;
using System.IO;
using System.Text;

[ApiController]
[Route("api/[controller]")]

public class FileController : ControllerBase
{
   

    [HttpGet("download/log/{Id}")]
    public IActionResult DownloadFile(string Id)
    {
        string fileSharePath = $"{Helper.GetWorkingFolder()}migrationlogs"; // UNC path to your file share
        string filePath;
        
        filePath = Path.Combine(fileSharePath, Id + ".bin");
       
        //if (!System.IO.File.Exists(filePath))
        //{
        //  return NotFound("File not found.");      
        //}

        var fileBytes = new Log().DownloadLogsAsJsonBytes(Id, 0, 0);
        var contentType = "application/octet-stream";
        return File(fileBytes, contentType, $"{Id}.txt");

    }

    [HttpGet("download/Jobs")]
    public IActionResult DownloadFile()
    {
        string fileSharePath = $"{Helper.GetWorkingFolder()}migrationjobs"; // UNC path to your file share
        var filePath = Path.Combine(fileSharePath, "list.json");

        if (!System.IO.File.Exists(filePath))
        {
            return NotFound("File not found.");
        }

        var fileBytes = System.IO.File.ReadAllBytes(filePath);
        var contentType = "application/octet-stream";

        return File(fileBytes, contentType, "jobList.json");
    }

    [HttpGet("download/migrationunit/{jobId}/{migrationUnitId}")]
    public IActionResult DownloadMigrationUnit(string jobId, string migrationUnitId)
    {
        var filePath = $"migrationjobs\\{jobId}\\{migrationUnitId}.json";

        // Use the persistence storage to read the document
        if (MigrationJobContext.Store == null || !MigrationJobContext.Store.DocumentExists(filePath))
        {
            return NotFound("Migration unit file not found.");
        }

        var jsonContent = MigrationJobContext.Store.ReadDocument(filePath);
        
        if (string.IsNullOrEmpty(jsonContent))
        {
            return NotFound("Migration unit file is empty or could not be read.");
        }

        // Pretty print the JSON
        var jsonObject = Newtonsoft.Json.JsonConvert.DeserializeObject(jsonContent);
        var prettyJson = Newtonsoft.Json.JsonConvert.SerializeObject(jsonObject, Newtonsoft.Json.Formatting.Indented);
        
        var fileBytes = Encoding.UTF8.GetBytes(prettyJson);
        var contentType = "application/json";

        return File(fileBytes, contentType, $"{migrationUnitId}.json");
    }
}
