using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using MongoMigrationWebApp.Service;
using OnlineMongoMigrationProcessor;

// Helper method for critical error logging that works even under memory pressure
static void LogCriticalError(string source, Exception? ex, bool isTerminating = false)
{
    var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss UTC");
    var logEntry = $"""
        ===========================================
        CRITICAL ERROR - {source}
        ===========================================
        Timestamp: {timestamp}
        Is Terminating: {isTerminating}
        Exception Type: {ex?.GetType().FullName ?? "Unknown"}
        Message: {ex?.Message ?? "No message"}
        Stack Trace:
        {ex?.StackTrace ?? "No stack trace"}
        
        """;

    // Log to console
    Console.WriteLine(logEntry);

    // Log to file - use try-catch to handle potential file I/O issues under memory pressure
    try
    {
        var logFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "critical_errors.log");
        File.AppendAllText(logFile, logEntry + Environment.NewLine);
    }
    catch
    {
        // If file logging fails, at least we have console output
        Console.WriteLine($"WARNING: Failed to write critical error to file at {timestamp}");
    }
}

// Global exception handlers
AppDomain.CurrentDomain.UnhandledException += (sender, eventArgs) =>
{
    var ex = eventArgs.ExceptionObject as Exception;
    
    // Special handling for OutOfMemoryException
    if (ex is OutOfMemoryException)
    {
        LogCriticalError("AppDomain - OUT OF MEMORY", ex, eventArgs.IsTerminating);
        
        // Try emergency GC before termination
        try
        {
            GC.Collect(2, GCCollectionMode.Aggressive, true, true);
            GC.WaitForPendingFinalizers();
            Console.WriteLine("Emergency GC completed during OOM handling");
        }
        catch
        {
            Console.WriteLine("Emergency GC failed during OOM handling");
        }
    }
    else
    {
        LogCriticalError("AppDomain - UNHANDLED EXCEPTION", ex, eventArgs.IsTerminating);
    }
};

TaskScheduler.UnobservedTaskException += (sender, eventArgs) =>
{
    var ex = eventArgs.Exception;
    
    // Check for OutOfMemoryException in the aggregate
    bool hasOOM = ex.InnerExceptions.Any(innerEx => innerEx is OutOfMemoryException);
    
    if (hasOOM)
    {
        LogCriticalError("TaskScheduler - OUT OF MEMORY DETECTED", ex);
        
        // Try emergency GC
        try
        {
            GC.Collect(2, GCCollectionMode.Aggressive, true, true);
            GC.WaitForPendingFinalizers();
            Console.WriteLine("Emergency GC completed during Task OOM handling");
        }
        catch
        {
            Console.WriteLine("Emergency GC failed during Task OOM handling");
        }
    }
    else
    {
        LogCriticalError("TaskScheduler - UNOBSERVED TASK EXCEPTION", ex);
    }
    
    // Mark as observed to prevent process termination
    eventArgs.SetObserved();
};

try
{
    var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllersWithViews();

// Register HttpClient and dynamically set the base address using NavigationManager
builder.Services.AddScoped(sp =>
{
    // Retrieve NavigationManager from the service provider
    var navigationManager = sp.GetRequiredService<NavigationManager>();

    // Create and configure HttpClient with dynamic base address
    var client = new HttpClient
    {
        BaseAddress = new Uri(navigationManager.BaseUri)  // Use NavigationManager's BaseUri
    };

    return client;
});

    builder.Configuration.AddEnvironmentVariables();
    builder.Services.AddRazorPages();
builder.Services.AddServerSideBlazor();
builder.Services.AddSingleton(builder.Configuration);
builder.Services.AddSingleton<JobManager>();
builder.Services.AddScoped<FileService>();

var app = builder.Build();

// _configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();

app.UseStaticFiles();

app.UseRouting();

app.MapBlazorHub();
app.MapFallbackToPage("/_Host");

app.MapControllers(); // Ensure controllers are mapped

    Console.WriteLine("Application starting...");
    
    // Log initial memory state
    try
    {
        var gcMemory = GC.GetTotalMemory(false);
        var workingSet = Environment.WorkingSet;
        Console.WriteLine($"STARTUP MEMORY: GC={gcMemory / 1024 / 1024}MB, WorkingSet={workingSet / 1024 / 1024}MB");
    }
    catch
    {
        Console.WriteLine("Failed to log startup memory diagnostics");
    }
    
    app.Run();
    Console.WriteLine("Application stopped gracefully.");
}
catch (Exception ex)
{
    // Special handling for OutOfMemoryException
    if (ex is OutOfMemoryException)
    {
        LogCriticalError("MAIN - OUT OF MEMORY APPLICATION CRASH", ex);
        
        // Log memory diagnostics if possible
        try
        {
            var gcMemory = GC.GetTotalMemory(false);
            var workingSet = Environment.WorkingSet;
            Console.WriteLine($"MEMORY DIAGNOSTICS: GC={gcMemory:N0} bytes, WorkingSet={workingSet:N0} bytes");
        }
        catch
        {
            Console.WriteLine("Failed to get memory diagnostics during OOM");
        }
    }
    else
    {
        LogCriticalError("MAIN - FATAL APPLICATION CRASH", ex);
    }
    
    // Re-throw to ensure the process exits with error code
    throw;
}
