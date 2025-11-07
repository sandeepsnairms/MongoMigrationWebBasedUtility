using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using MongoMigrationWebApp.Service;
using OnlineMongoMigrationProcessor;

// Global exception handlers
AppDomain.CurrentDomain.UnhandledException += (sender, eventArgs) =>
{
    var ex = eventArgs.ExceptionObject as Exception;
    Console.WriteLine("===========================================");
    Console.WriteLine("UNHANDLED EXCEPTION in AppDomain");
    Console.WriteLine("===========================================");
    Console.WriteLine($"Is Terminating: {eventArgs.IsTerminating}");
    Console.WriteLine($"Exception: {ex?.GetType().FullName}");
    Console.WriteLine($"Message: {ex?.Message}");
    Console.WriteLine($"Stack Trace:\n{ex?.StackTrace}");
    Console.WriteLine("===========================================");
};

TaskScheduler.UnobservedTaskException += (sender, eventArgs) =>
{
    Console.WriteLine("===========================================");
    Console.WriteLine("UNOBSERVED TASK EXCEPTION");
    Console.WriteLine("===========================================");
    Console.WriteLine($"Exception: {eventArgs.Exception.GetType().FullName}");
    Console.WriteLine($"Message: {eventArgs.Exception.Message}");
    
    foreach (var innerEx in eventArgs.Exception.InnerExceptions)
    {
        Console.WriteLine($"\nInner Exception:");
        Console.WriteLine($"  Type: {innerEx.GetType().FullName}");
        Console.WriteLine($"  Message: {innerEx.Message}");
        Console.WriteLine($"  Stack Trace:\n{innerEx.StackTrace}");
    }
    Console.WriteLine("===========================================");
    
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
    app.Run();
    Console.WriteLine("Application stopped gracefully.");
}
catch (Exception ex)
{
    Console.WriteLine("===========================================");
    Console.WriteLine("FATAL ERROR - Application crashed!");
    Console.WriteLine("===========================================");
    Console.WriteLine($"Exception Type: {ex.GetType().FullName}");
    Console.WriteLine($"Message: {ex.Message}");
    Console.WriteLine($"Stack Trace:\n{ex.StackTrace}");
    
    if (ex.InnerException != null)
    {
        Console.WriteLine("\n--- Inner Exception ---");
        Console.WriteLine($"Type: {ex.InnerException.GetType().FullName}");
        Console.WriteLine($"Message: {ex.InnerException.Message}");
        Console.WriteLine($"Stack Trace:\n{ex.InnerException.StackTrace}");
    }
    
    // Check for AggregateException
    if (ex is AggregateException aggEx)
    {
        Console.WriteLine($"\n--- AggregateException with {aggEx.InnerExceptions.Count} inner exceptions ---");
        int i = 0;
        foreach (var innerEx in aggEx.InnerExceptions)
        {
            Console.WriteLine($"\nInner Exception {++i}:");
            Console.WriteLine($"  Type: {innerEx.GetType().FullName}");
            Console.WriteLine($"  Message: {innerEx.Message}");
            Console.WriteLine($"  Stack Trace:\n{innerEx.StackTrace}");
        }
    }
    
    Console.WriteLine("===========================================");
    
    // Re-throw to ensure the process exits with error code
    throw;
}
