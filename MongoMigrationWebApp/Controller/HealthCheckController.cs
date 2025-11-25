using Microsoft.AspNetCore.Mvc;
using MongoMigrationWebApp.Service;

namespace MongoMigrationWebApp.Controller
{
    [ApiController]
    [Route("api/[controller]")]
    public class HealthCheckController : ControllerBase
    {
        // JobManager is registered as a singleton in Program.cs, so this will be the shared instance
        private readonly JobManager _jobManager;

        public HealthCheckController(JobManager jobManager)
        {
            _jobManager = jobManager;
        }

        /// <summary>
        /// Health check endpoint that returns the currently running job ID
        /// </summary>
        /// <returns>Health check response with running job information</returns>
        [HttpGet]
        public IActionResult Get()
        {
            var runningJobId = _jobManager.GetRunningJobId();
            
            var response = new
            {
                Status = "Healthy",
                Timestamp = DateTime.UtcNow,
                RunningJobId = string.IsNullOrEmpty(runningJobId) ? null : runningJobId,
                HasRunningJob = !string.IsNullOrEmpty(runningJobId)
            };

            return Ok(response);
        }

        /// <summary>
        /// Get the currently running job ID only
        /// </summary>
        /// <returns>The running job ID or empty string</returns>
        [HttpGet("runningjob")]
        public IActionResult GetRunningJobId()
        {
            var runningJobId = _jobManager.GetRunningJobId();
            
            return Ok(new { JobId = runningJobId });
        }
    }
}
