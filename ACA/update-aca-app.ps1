# Azure Container Apps - Application Update Script
# Updates only the application image without resetting environment variables and secrets

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$true)]
    [string]$ContainerAppName,
    
    [Parameter(Mandatory=$true)]
    [string]$AcrName,
    
    [Parameter(Mandatory=$false)]
    [string]$ImageTag = "latest"
)

$ErrorActionPreference = "Stop"

Write-Host "`n=== Azure Container App - Image Update ===" -ForegroundColor Cyan
Write-Host "Resource Group: $ResourceGroupName" -ForegroundColor White
Write-Host "Container App: $ContainerAppName" -ForegroundColor White
Write-Host "ACR: $AcrName" -ForegroundColor White
Write-Host "Image Tag: $ImageTag" -ForegroundColor White
Write-Host ""

# Step 1: Build and push new image to ACR
Write-Host "Step 1: Building and pushing Docker image to ACR..." -ForegroundColor Yellow
Write-Host "Note: Warnings about packing source code and excluding .git files are normal and expected." -ForegroundColor Gray

$ErrorActionPreference = 'Continue'
az acr build `
    --registry $AcrName `
    --resource-group $ResourceGroupName `
    --image "$($ContainerAppName):$($ImageTag)" `
    --file ../MongoMigrationWebApp/Dockerfile `
    ..

if ($LASTEXITCODE -ne 0) {
    Write-Host "`nError: Failed to build and push Docker image" -ForegroundColor Red
    exit 1
}
$ErrorActionPreference = 'Stop'

Write-Host "`nDocker image built and pushed successfully." -ForegroundColor Green

# Step 2: Update Container App with new image
Write-Host "`nStep 2: Updating Container App with new image..." -ForegroundColor Yellow
Write-Host "Note: Warnings about cryptography or UserWarnings are normal and can be ignored." -ForegroundColor Gray

$imageName = "$AcrName.azurecr.io/$($ContainerAppName):$($ImageTag)"

$ErrorActionPreference = 'Continue'
az containerapp update `
    --name $ContainerAppName `
    --resource-group $ResourceGroupName `
    --image $imageName `
    2>&1 | Where-Object { $_ -notmatch 'cryptography' -and $_ -notmatch 'UserWarning' }

if ($LASTEXITCODE -ne 0) {
    Write-Host "`nError: Failed to update Container App" -ForegroundColor Red
    exit 1
}
$ErrorActionPreference = 'Stop'

Write-Host "`n=== Update Complete ===" -ForegroundColor Cyan
Write-Host "The Container App '$ContainerAppName' has been updated with image: $imageName" -ForegroundColor Green
Write-Host "Environment variables and secrets remain unchanged." -ForegroundColor Green
Write-Host ""

# Retrieve and display the application URL
Write-Host "Retrieving application URL..." -ForegroundColor Yellow
$ErrorActionPreference = 'Continue'
$appUrl = az containerapp show `
    --name $ContainerAppName `
    --resource-group $ResourceGroupName `
    --query "properties.configuration.ingress.fqdn" `
    --output tsv `
    2>&1 | Where-Object { $_ -notmatch 'cryptography' -and $_ -notmatch 'UserWarning' -and $_ -notmatch 'WARNING:' }
$ErrorActionPreference = 'Stop'

if ($appUrl) {
    Write-Host ""
    Write-Host "===========================================" -ForegroundColor Green
    Write-Host "  Application updated successfully!" -ForegroundColor Green
    Write-Host "===========================================" -ForegroundColor Green
    Write-Host "  Launch URL: https://$appUrl" -ForegroundColor Cyan
    Write-Host "===========================================" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "Unable to retrieve application URL. Please check the Azure Portal." -ForegroundColor Yellow
}
