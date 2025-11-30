# Azure Container Apps Deployment Script
# Deploys the MongoDB Migration Web-Based Utility to Azure Container Apps

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$true)]
    [string]$ContainerAppName,
    
    [Parameter(Mandatory=$true)]
    [string]$AcrName,
    
    [Parameter(Mandatory=$true)]
    [string]$StateStoreAppID,
    
    [Parameter(Mandatory=$true)]
    [ValidateSet('eastus','eastus2','westus2','westus','centralus','northcentralus','southcentralus','westcentralus','northeurope','westeurope','francecentral','germanywestcentral','uksouth','ukwest','switzerlandnorth','norwayeast','eastasia','southeastasia','japaneast','japanwest','koreacentral','australiaeast','australiasoutheast','centralindia','southindia','brazilsouth','canadacentral','canadaeast','uaenorth','southafricanorth','swedencentral')]
    [string]$Location,
    
    [Parameter(Mandatory=$false)]
    [string]$ImageTag = "latest",
    
    [Parameter(Mandatory=$false)]
    [string]$DnsNameLabel = "",
    
    [Parameter(Mandatory=$false)]
    [ValidateRange(1, 32)]
    [int]$VCores = 8,
    
    [Parameter(Mandatory=$false)]
    [ValidateRange(2, 64)]
    [int]$MemoryGB = 32,
    
    [Parameter(Mandatory=$false)]
    [bool]$EnableApplicationGateway = $false
)

$ErrorActionPreference = "Stop"

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

Write-Host "`nStep 1: Deploying base infrastructure (ACR, Managed Identity, Networking, Container Apps Environment)..." -ForegroundColor Yellow

$bicepParams = @(
    "deployment", "group", "create",
    "--resource-group", $ResourceGroupName,
    "--template-file", "aca_main.bicep",
    "--parameters",
        "containerAppName=$ContainerAppName",
        "acrName=$AcrName",
        "location=$Location",
        "vCores=$VCores",
        "memoryGB=$MemoryGB",
        "enableApplicationGateway=$EnableApplicationGateway",
        "imageTag=$ImageTag"
)

if ($DnsNameLabel) {
    $bicepParams += "dnsNameLabel=$DnsNameLabel"
}

az @bicepParams

Write-Host "`nStep 2: Building and pushing Docker image to ACR..." -ForegroundColor Yellow

$ErrorActionPreference = 'Continue'
az acr build `
    --registry $AcrName `
    --resource-group $ResourceGroupName `
    --image "$($ContainerAppName):$($ImageTag)" `
    --file MongoMigrationWebApp/Dockerfile `
    .
$ErrorActionPreference = 'Stop'

Write-Host "`nStep 3: Prompting for StateStore connection string..." -ForegroundColor Yellow
$secureConnString = Read-Host -Prompt "The StateStore keeps track of migration job details in a DocumentDB. You may use the same database as the Target DocumentDB or a separate one. Enter the connection string for the StateStore." -AsSecureString
$connString = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureConnString)
)

Write-Host "`nStep 4: Final Container App deployment..." -ForegroundColor Yellow

$finalBicepParams = @(
    "deployment", "group", "create",
    "--resource-group", $ResourceGroupName,
    "--template-file", "aca_main.bicep",
    "--parameters",
        "containerAppName=$ContainerAppName",
        "acrName=$AcrName",
        "location=$Location",
        "vCores=$VCores",
        "memoryGB=$MemoryGB",
        "enableApplicationGateway=$EnableApplicationGateway",
        "stateStoreAppID=$StateStoreAppID",
        "stateStoreConnectionString=`"$connString`"",
        "aspNetCoreEnvironment=Development",
        "imageTag=$ImageTag"
)

if ($DnsNameLabel) {
    $finalBicepParams += "dnsNameLabel=$DnsNameLabel"
}

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
    2>&1 | Where-Object { $_ -notmatch 'cryptography' -and $_ -notmatch 'UserWarning' }
$ErrorActionPreference = 'Stop'

if ($appUrl) {
    Write-Host ""
    Write-Host "===========================================" -ForegroundColor Green
    Write-Host "  Application deployed successfully!" -ForegroundColor Green
    Write-Host "===========================================" -ForegroundColor Green
    Write-Host "  Launch URL: https://$appUrl" -ForegroundColor Cyan
    Write-Host "===========================================" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "Unable to retrieve application URL. Please check the Azure Portal." -ForegroundColor Yellow
}
