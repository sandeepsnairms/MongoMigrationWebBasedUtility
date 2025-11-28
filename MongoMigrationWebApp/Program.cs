using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using MongoMigrationWebApp.Service;
using OnlineMongoMigrationProcessor;
using System.Security.AccessControl;

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

try
{
    //Map environment variables to configuration keys
    var stateStoreCSorPath = Environment.GetEnvironmentVariable("StateStoreConnectionStringOrPath");
    if (!string.IsNullOrEmpty(stateStoreCSorPath))
    {
        builder.Configuration["StateStore:ConnectionStringOrPath"] = stateStoreCSorPath;
    }
    else
    {
        throw new Exception("StateStoreConnectionStringOrPath environment variable is not set.");
    }
    var appId = Environment.GetEnvironmentVariable("StateStoreAppID");
    if (!string.IsNullOrEmpty(appId))
    {
        builder.Configuration["StateStore:AppID"] = appId;
    }
    bool.TryParse( Environment.GetEnvironmentVariable("StateStoreUseLocalDisk"), out var useLocal);
    builder.Configuration["StateStore:UseLocalDisk"] = useLocal.ToString();
}
catch
{
    // Ignore errors
}

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

app.Run();

