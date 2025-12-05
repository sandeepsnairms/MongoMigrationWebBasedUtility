# Azure Container Apps Deployment Script
# Deploys the MongoDB Migration Web-Based Utility to Azure Container Apps

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$true)]
    [string]$ContainerAppName,
    
    [Parameter(Mandatory=$false)]
    [string]$AcrName = "",
    
    [Parameter(Mandatory=$false)]
    [string]$StateStoreAppID = "",
    
    [Parameter(Mandatory=$true)]
    [ValidateSet('eastus','eastus2','westus2','westus','centralus','northcentralus','southcentralus','westcentralus','northeurope','westeurope','francecentral','germanywestcentral','uksouth','ukwest','switzerlandnorth','norwayeast','eastasia','southeastasia','japaneast','japanwest','koreacentral','australiaeast','australiasoutheast','centralindia','southindia','brazilsouth','canadacentral','canadaeast','uaenorth','southafricanorth','swedencentral')]
    [string]$Location,
    
    [Parameter(Mandatory=$false)]
    [string]$StorageAccountName = "",
    
    [Parameter(Mandatory=$false)]
    [string]$ImageTag = "latest",
    
    [Parameter(Mandatory=$false)]
    [string]$AcrRepository = "",
    
    [Parameter(Mandatory=$false)]
    [ValidateRange(1, 32)]
    [int]$VCores = 8,
    
    [Parameter(Mandatory=$false)]
    [ValidateRange(2, 64)]
    [int]$MemoryGB = 32
)

$ErrorActionPreference = "Stop"

# Generate ACR name if not provided
if ([string]::IsNullOrEmpty($AcrName)) {
    $AcrName = ($ContainerAppName -replace '-', '').ToLower() + 'acr'
    if ($AcrName.Length -gt 50) {
        $AcrName = $AcrName.Substring(0, 50)
    }
    Write-Host "Using generated ACR name: $AcrName" -ForegroundColor Cyan
}

# Generate StateStoreAppID if not provided
if ([string]::IsNullOrEmpty($StateStoreAppID)) {
    $StateStoreAppID = $ContainerAppName
    Write-Host "Using ContainerAppName as StateStoreAppID: $StateStoreAppID" -ForegroundColor Cyan
}

# Generate ACR repository name if not provided
if ([string]::IsNullOrEmpty($AcrRepository)) {
    $AcrRepository = $ContainerAppName
    Write-Host "Using ContainerAppName as ACR repository: $AcrRepository" -ForegroundColor Cyan
}

# Generate storage account name if not provided
if ([string]::IsNullOrEmpty($StorageAccountName)) {
    $StorageAccountName = ($ContainerAppName -replace '-', '').ToLower() + 'stor'
    if ($StorageAccountName.Length -gt 24) {
        $StorageAccountName = $StorageAccountName.Substring(0, 24)
    }
    Write-Host "Using generated storage account name: $StorageAccountName" -ForegroundColor Cyan
}

$supportedRegions = @(
    'eastus','eastus2','westus2','westus','centralus','northcentralus','southcentralus','westcentralus',
    'northeurope','westeurope','francecentral','germanywestcentral','uksouth','ukwest',
    'switzerlandnorth','norwayeast','eastasia','southeastasia','japaneast','japanwest','koreacentral',
    'australiaeast','australiasoutheast','centralindia','southindia','brazilsouth','canadacentral',
    'canadaeast','uaenorth','southafricanorth','swedencentral'
)

if ($supportedRegions -contains $Location) {
    Write-Host "Location '$Location' supports Azure Container Apps" -ForegroundColor Green
} else {
    Write-Host "Location '$Location' may not support Azure Container Apps" -ForegroundColor Red
    Write-Host "Recommended regions include eastus, westus2, northeurope, eastasia" -ForegroundColor Yellow
}

Write-Host "`nStep 1: Deploying infrastructure (ACR, Storage Account, Managed Identity, Container Apps Environment)..." -ForegroundColor Yellow

$bicepParams = @(
    "deployment", "group", "create",
    "--resource-group", $ResourceGroupName,
    "--template-file", "aca_main.bicep",
    "--parameters",
        "containerAppName=$ContainerAppName",
        "acrName=$AcrName",
        "acrRepository=$AcrRepository",
        "location=$Location",
        "storageAccountName=$StorageAccountName",
        "vCores=$VCores",
        "memoryGB=$MemoryGB"
)

az @bicepParams

Write-Host "`nStep 2: Checking if Docker image exists in ACR..." -ForegroundColor Yellow

# Check if the image exists in ACR
$ErrorActionPreference = 'Continue'
$imageExists = az acr repository show-tags `
    --name $AcrName `
    --repository $AcrRepository `
    --query "contains(@, '$ImageTag')" `
    --output tsv 2>$null
$ErrorActionPreference = 'Stop'

if ($imageExists -eq 'true') {
    Write-Host "Image '${AcrRepository}:${ImageTag}' found in ACR. Skipping build." -ForegroundColor Green
} else {
    Write-Host "Image '${AcrRepository}:${ImageTag}' not found in ACR. Building and pushing..." -ForegroundColor Yellow
    Write-Host "Note: Warnings about packing source code and excluding .git files are normal and expected." -ForegroundColor Gray
    
    $ErrorActionPreference = 'Continue'
    az acr build `
        --registry $AcrName `
        --resource-group $ResourceGroupName `
        --image "$($AcrRepository):$($ImageTag)" `
        --file ../MongoMigrationWebApp/Dockerfile `
        ..
    $ErrorActionPreference = 'Stop'
    
    Write-Host "Docker image built and pushed successfully." -ForegroundColor Green
}

Write-Host "`nStep 3: Prompting for StateStore connection string..." -ForegroundColor Yellow
$secureConnString = Read-Host -Prompt "The StateStore keeps track of migration job details in a DocumentDB. You may use the same database as the Target DocumentDB or a separate one. Enter the connection string for the StateStore." -AsSecureString
$connString = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureConnString)
)

Write-Host "`nStep 4: Deploying Container App with application image..." -ForegroundColor Yellow

$finalBicepParams = @(
    "deployment", "group", "create",
    "--resource-group", $ResourceGroupName,
    "--template-file", "aca_main.bicep",
    "--parameters",
        "containerAppName=$ContainerAppName",
        "acrName=$AcrName",
        "acrRepository=$AcrRepository",
        "location=$Location",
        "storageAccountName=$StorageAccountName",
        "vCores=$VCores",
        "memoryGB=$MemoryGB",
        "stateStoreAppID=$StateStoreAppID",
        "stateStoreConnectionString=`"$connString`"",
        "aspNetCoreEnvironment=Development",
        "imageTag=$ImageTag"
)

az @finalBicepParams

Remove-Variable connString, secureConnString -ErrorAction Ignore

Write-Host "`n=== Deployment Complete ===" -ForegroundColor Cyan

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
    Write-Host "==========================================" -ForegroundColor Green
    Write-Host "  Application deployed successfully!" -ForegroundColor Green
    Write-Host "==========================================" -ForegroundColor Green
    Write-Host "  Launch URL: https://$appUrl" -ForegroundColor Cyan
    Write-Host "==========================================" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "Unable to retrieve application URL. Please check the Azure Portal." -ForegroundColor Yellow
}
