using Microsoft.AspNetCore.Mvc;
using System.IO;
using OnlineMongoMigrationProcessor;

[ApiController]
[Route("api/[controller]")]

public class FileController : ControllerBase
{
   

    [HttpGet("download/log/{fileName}")]
    public IActionResult DownloadFile(string fileName)
    {
        string _fileSharePath = $"{Helper.GetWorkingFolder()}migrationlogs"; // UNC path to your file share
        var filePath = Path.Combine(_fileSharePath, fileName + ".txt");

        if (!System.IO.File.Exists(filePath))
        {
            return NotFound("File not found.");
        }

        var fileBytes = System.IO.File.ReadAllBytes(filePath);
        var contentType = "application/octet-stream";

        return File(fileBytes, contentType, fileName);
    }

    [HttpGet("download/jobs")]
    public IActionResult DownloadFile()
    {
        string _fileSharePath = $"{Helper.GetWorkingFolder()}migrationjobs"; // UNC path to your file share
        var filePath = Path.Combine(_fileSharePath, "list.json");

        if (!System.IO.File.Exists(filePath))
        {
            return NotFound("File not found.");
        }

        var fileBytes = System.IO.File.ReadAllBytes(filePath);
        var contentType = "application/octet-stream";

        return File(fileBytes, contentType, "jobs.json");
    }
}
