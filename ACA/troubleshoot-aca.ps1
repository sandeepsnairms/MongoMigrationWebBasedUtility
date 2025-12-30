#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Troubleshoot Azure Container Apps deployment issues
.DESCRIPTION
    This script helps diagnose common issues with ACA deployments
.PARAMETER ResourceGroupName
    The name of the resource group
.PARAMETER Location
    Azure region for deployment
.PARAMETER AcrName
    Azure Container Registry name to check
#>

param(
    [Parameter(Mandatory = $true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory = $true)]
    [string]$Location,
    
    [Parameter(Mandatory = $false)]
    [string]$AcrName = "mongomigacr$(Get-Random -Minimum 1000 -Maximum 9999)"
)

Write-Host "=== Azure Container Apps Deployment Troubleshooting ===" -ForegroundColor Cyan

# Check if logged into Azure
Write-Host "1. Checking Azure login status..." -ForegroundColor Yellow
try {
    $account = az account show --query "name" -o tsv 2>$null
    if ($account) {
        Write-Host "✓ Logged into Azure account: $account" -ForegroundColor Green
    } else {
        throw "Not logged in"
    }
} catch {
    Write-Host "✗ Not logged into Azure. Please run 'az login'" -ForegroundColor Red
    exit 1
}

# Check Container Apps extension
Write-Host "2. Checking Container Apps CLI extension..." -ForegroundColor Yellow
$containerAppExtension = az extension list --query "[?name=='containerapp'].name" -o tsv
if ($containerAppExtension) {
    Write-Host "✓ Container Apps extension is installed" -ForegroundColor Green
} else {
    Write-Host "⚠ Container Apps extension not found. Installing..." -ForegroundColor Yellow
    az extension add --name containerapp --upgrade
}

# Check region support for Container Apps
Write-Host "3. Checking Container Apps support in region '$Location'..." -ForegroundColor Yellow
$supportedRegions = az provider show --namespace Microsoft.App --query "resourceTypes[?resourceType=='managedEnvironments'].locations[]" -o tsv
if ($supportedRegions -contains $Location) {
    Write-Host "✓ Container Apps supported in $Location" -ForegroundColor Green
} else {
    Write-Host "✗ Container Apps not supported in $Location" -ForegroundColor Red
    Write-Host "Supported regions: $($supportedRegions -join ', ')" -ForegroundColor Yellow
    Write-Host "Consider using: eastus, westus2, northeurope, or eastasia" -ForegroundColor Yellow
}

# Check workload profiles availability
Write-Host "4. Checking workload profiles support..." -ForegroundColor Yellow
try {
    $workloadProfiles = az containerapp env workload-profile list-supported --location $Location --query "[].name" -o tsv 2>$null
    if ($workloadProfiles -like "*D4*") {
        Write-Host "✓ D4 workload profile supported in $Location" -ForegroundColor Green
    } else {
        Write-Host "⚠ D4 workload profile may not be supported in $Location" -ForegroundColor Yellow
        Write-Host "Available profiles: $($workloadProfiles -join ', ')" -ForegroundColor Yellow
    }
} catch {
    Write-Host "⚠ Could not check workload profile support" -ForegroundColor Yellow
}

# Check ACR name availability
Write-Host "5. Checking ACR name availability..." -ForegroundColor Yellow
$acrAvailable = az acr check-name --name $AcrName --query "nameAvailable" -o tsv
if ($acrAvailable -eq "true") {
    Write-Host "✓ ACR name '$AcrName' is available" -ForegroundColor Green
} else {
    Write-Host "✗ ACR name '$AcrName' is not available" -ForegroundColor Red
    $suggestedName = "mongomigacr$(Get-Random -Minimum 10000 -Maximum 99999)"
    Write-Host "Try using: $suggestedName" -ForegroundColor Yellow
}

# Check resource group
Write-Host "6. Checking resource group..." -ForegroundColor Yellow
$rgExists = az group exists --name $ResourceGroupName
if ($rgExists -eq "true") {
    Write-Host "✓ Resource group '$ResourceGroupName' exists" -ForegroundColor Green
} else {
    Write-Host "⚠ Resource group '$ResourceGroupName' does not exist" -ForegroundColor Yellow
    Write-Host "It will be created during deployment" -ForegroundColor Yellow
}

# Check subscription quotas
Write-Host "7. Checking subscription quotas..." -ForegroundColor Yellow
try {
    $quotas = az vm list-usage --location $Location --query "[?name.value=='cores'].{Name:name.localizedValue, Current:currentValue, Limit:limit}" -o table
    Write-Host "Core quotas in $Location :" -ForegroundColor Yellow
    Write-Host $quotas
} catch {
    Write-Host "⚠ Could not retrieve quota information" -ForegroundColor Yellow
}

# Validate Bicep template
Write-Host "8. Validating Bicep template..." -ForegroundColor Yellow
if (Test-Path "ACA/aca_main.bicep") {
    try {
        $validation = az deployment group validate `
            --resource-group $ResourceGroupName `
            --template-file ACA/aca_main.bicep `
            --parameters acrName=$AcrName `
            --parameters location=$Location `
            --only-show-errors 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✓ Bicep template validation passed" -ForegroundColor Green
        } else {
            Write-Host "✗ Bicep template validation failed:" -ForegroundColor Red
            Write-Host $validation -ForegroundColor Red
        }
    } catch {
        Write-Host "✗ Error validating template: $_" -ForegroundColor Red
    }
} else {
    Write-Host "✗ ACA/aca_main.bicep not found" -ForegroundColor Red
}

Write-Host "`n=== Troubleshooting Summary ===" -ForegroundColor Cyan
Write-Host "If deployment still fails, try:" -ForegroundColor Yellow
Write-Host "1. Use a different region from the supported list" -ForegroundColor White
Write-Host "2. Use a unique ACR name with random numbers" -ForegroundColor White
Write-Host "3. Check that your subscription has sufficient quota" -ForegroundColor White
Write-Host "4. Try deployment without Application Gateway first" -ForegroundColor White
Write-Host "5. Contact Azure support if region-specific issues persist" -ForegroundColor White

Write-Host "`nSuggested deployment command:" -ForegroundColor Cyan
Write-Host "./deploy-to-aca.ps1 -ResourceGroupName '$ResourceGroupName' -Location '$Location' -AcrName '$AcrName'" -ForegroundColor Green