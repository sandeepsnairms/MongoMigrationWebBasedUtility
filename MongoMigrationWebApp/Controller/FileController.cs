using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Mvc;
using OnlineMongoMigrationProcessor;
using SharpCompress.Common;
using System.IO;

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
}
