using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Mvc;
using OnlineMongoMigrationProcessor;
using SharpCompress.Common;
using System.IO;

[ApiController]
[Route("api/[controller]")]

public class FileController : ControllerBase
{
   

    [HttpGet("download/log/{fileName}")]
    public IActionResult DownloadFile(string fileName)
    {
        string fileSharePath = $"{Helper.GetWorkingFolder()}migrationlogs"; // UNC path to your file share
        bool isBackup = false;
        string filePath;
        if (fileName.EndsWith(".txt"))
        {
            filePath = Path.Combine(fileSharePath, fileName);
            isBackup = true;
        }
        else
        {
            filePath = Path.Combine(fileSharePath, fileName + ".bin");
        }

        if (!System.IO.File.Exists(filePath))
        {
          return NotFound("File not found.");      
        }

        if (isBackup)
        {
            var fileBytes = System.IO.File.ReadAllBytes(filePath);
            var contentType = "application/octet-stream";
            return File(fileBytes, contentType, fileName);
        }
        else
        {
            
            var fileBytes = new Log().DownloadLogsAsJsonBytes(filePath);
            var contentType = "application/octet-stream";
            return File(fileBytes, contentType, fileName);
        }
    }

    [HttpGet("download/jobList")]
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
