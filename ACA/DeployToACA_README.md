# Deploy to Azure Container Apps (ACA)

This guide explains how to deploy the MongoDB Migration Web-Based Utility to Azure Container Apps using the `aca_main.bicep` template. ACA provides enterprise-grade dedicated compute resources and persistent storage for high-performance migrations.

## Deployment Scripts Overview

Two PowerShell scripts are provided for different deployment scenarios:

1. **`deploy-to-aca.ps1`** - Full initial deployment
   - Deploys complete infrastructure (ACR, Container Apps Environment, Storage)
   - Prompts for DocumentDB connection string
   - Configures all environment variables and secrets
   - Use for first-time deployment or when changing infrastructure

2. **`update-aca-app.ps1`** - Application updates only
   - Updates only the Docker image
   - Preserves existing environment variables and secrets
   - Faster deployment without infrastructure changes
   - Use for deploying code changes after initial setup

## Prerequisites

**Note**: This guide assumes you have cloned the repository to `c:\Work\GitHub\repos\MongoMigrationWebBasedUtility`.

- Azure CLI installed and logged in (`az login`)
- An Azure subscription with appropriate permissions
- Resource group created (`az group create -n <rg-name> -l <location>`)
- **Azure DocumentDB account** created for state storage (you'll need the connection string)
- **Supported Azure region** - Container Apps with dedicated workload profiles are not available in all regions

**Note**: Docker is not required locally. Images are built directly in Azure Container Registry (ACR) using `az acr build`.

### Supported Regions for Container Apps with Dedicated Plans

To get the latest list of regions that support Azure Container Apps, run:

```powershell
# Check all regions where Container Apps managed environments are available
az provider show --namespace Microsoft.App --query "resourceTypes[?resourceType=='managedEnvironments'].locations" -o table

# Check regions that support dedicated workload profiles
az containerapp env workload-profile list-supported -l <your-location>
```

**Commonly supported regions include**: `eastus`, `eastus2`, `westus2`, `westus3`, `northeurope`, `westeurope`, `australiaeast`, `southeastasia`, `canadacentral`

**Note**: If you encounter deployment errors, verify region support using the commands above or try switching to a well-supported region like `eastus`.

## Quick Start (Automated Deployment)

For a streamlined deployment experience, use the provided PowerShell script:

```powershell
# Navigate to the repository directory
cd c:\Work\GitHub\repos\MongoMigrationWebBasedUtility

# Run the deployment script with default 8 vCores and 32GB RAM
.\deploy-to-aca.ps1 `
  -ResourceGroupName "MongoMigrationRGTest" `
  -ContainerAppName "mongomigration" `
  -AcrName "mongomigrationacr" `
  -StateStoreAppID "aca_server1" `
  -Location "eastus" `
  -ImageTag "latest"
```

The script will:
1. Deploy infrastructure using Bicep (ACR, Container Apps Environment, Storage Account)
2. Build and push Docker image to ACR
3. Securely prompt for DocumentDB connection string
4. Deploy Container App with final configuration and environment variables
5. Display the application URL with a clickable launch link

### Subsequent Application Updates

After initial deployment, use the update script for faster deployments:

```powershell
# Update application code without changing infrastructure or secrets
.\update-aca-app.ps1 `
  -ResourceGroupName "MongoMigrationRGProd" `
  -ContainerAppName "mongomigration-prod" `
  -AcrName "mongomigprod1234" `
  -ImageTag "v1.1"
```

The update script:
1. Builds and pushes new Docker image to ACR
2. Updates Container App with new image
3. Preserves all environment variables and secrets
4. Displays the application URL with a clickable launch link

### Configurable Resource Sizing

The script supports flexible resource allocation to match your workload requirements:

```powershell
# Small workload (development/testing)
-VCores 4 -MemoryGB 16

# Default production workload  
# (uses defaults: 8 vCores, 32GB - no parameters needed)

# Large workload (high-performance migration)
-VCores 16 -MemoryGB 64

# Maximum workload (enterprise-scale migration)
-VCores 32 -MemoryGB 64
```

---

## Manual Deployment Steps

If you prefer to run each step manually, follow the detailed steps below.

### Step 1: Deploy Infrastructure with Bicep

Deploy the Bicep template to create all required resources:

```powershell
az deployment group create `
  --resource-group <resource-group-name> `
  --template-file aca_main.bicep `
  --parameters containerAppName=<container-app-name> acrName=<acr-name>
```

This will create:
- Managed Identity
- Azure Container Registry (ACR)
- Container Apps Environment with Configurable Dedicated Plan (1-32 vCores, 2-64GB)
- Storage Account with 100GB Azure File Share

### Step 2: Build and Push Docker Image to ACR

Build your Docker image directly in Azure Container Registry:

```powershell
# Navigate to the repository root
cd c:\Work\GitHub\repos\MongoMigrationWebBasedUtility

# Build and push the image to ACR
az acr build `
  --registry <acrName> `
  --resource-group <resource-group-name> `
  --image <containerAppName>:latest `
  --file MongoMigrationWebApp/Dockerfile `
  .
```

### Step 3: Deploy Container App with Environment Variables

Deploy the final Container App configuration with environment variables:

```powershell
# Securely get connection string
$secureConnString = Read-Host -Prompt "Enter DocumentDB Connection String" -AsSecureString
$connString = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureConnString))

# Deploy final configuration
az deployment group create `
  --resource-group <resource-group-name> `
  --template-file aca_main.bicep `
  --parameters containerAppName=<container-app-name> acrName=<acr-name> stateStoreAppID=<your-app-id> stateStoreConnectionString="`"$connString`"" aspNetCoreEnvironment=Development imageTag=latest

# Clear the variable from memory
Remove-Variable connString, secureConnString
```

## Parameters Reference

### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `ResourceGroupName` | Name of the Azure resource group | `MongoMigrationRG` |
| `ContainerAppName` | Name of the Container App | `mongomigration` |
| `AcrName` | Name of the Azure Container Registry (must be globally unique, alphanumeric only) | `mongomigrationacr001` |
| `StateStoreAppID` | Application identifier for state storage | `aca_server1` |
| `Location` | Azure region (must support Container Apps) | `eastus` |

### Optional Parameters

| Parameter | Type | Default | Range | Description |
|-----------|------|---------|-------|-------------|
| `VCores` | int | `8` | 1-32 | Number of vCores for the container |
| `MemoryGB` | int | `32` | 2-64 | Memory in GB for the container |
| `ImageTag` | string | `latest` | - | Docker image tag to deploy |

## Resource Configurations

Azure Container Apps with Dedicated Plan provides configurable high-performance compute:

### Configurable Resources
- **CPU**: 1-32 vCores (default: 8 vCores, dedicated)
- **Memory**: 2-64GB RAM (default: 32GB)
- **Storage**: 100GB persistent Azure File Share
- **Replicas**: Fixed at 1 replica (auto-scaling not supported)
- **Networking**: Private VNet integration with optional public ingress

### Resource Configuration Examples

**Development/Testing (4 vCores, 16GB):**
```powershell
-VCores 4 -MemoryGB 16
```

**Default Production (8 vCores, 32GB):**
```powershell
# Uses defaults, no additional parameters needed
```

**High-Performance (16 vCores, 64GB):**
```powershell
-VCores 16 -MemoryGB 64
```

**Maximum Performance (32 vCores, 64GB):**
```powershell
-VCores 32 -MemoryGB 64
```

**Key Features:**
- ✅ Configurable high CPU and memory (up to 32 vCores, 64GB)
- ✅ Persistent storage that survives deployments
- ✅ Enterprise networking with VNet integration
- ✅ Built-in ingress controller with HTTPS
- ✅ Managed identity for secure resource access

**Note**: Auto-scaling is not supported. The application runs with a fixed single replica to ensure data consistency during migration operations.

## Complete Deployment Examples

### Development Setup (Small Workload)

```powershell
# Deploy with reduced resources for development and testing
.\deploy-to-aca.ps1 `
  -ResourceGroupName "rg-mongomig-dev" `
  -ContainerAppName "mongomigration-dev" `
  -AcrName "mongomigdevacr" `
  -StateStoreAppID "dev-migration-01" `
  -Location "eastus" `
  -VCores 4 `
  -MemoryGB 16 `
  -ImageTag "dev"
```

### High-Performance Migration Setup (Large Workload)

```powershell
# Deploy with maximum resources for large migrations
.\deploy-to-aca.ps1 `
  -ResourceGroupName "rg-mongomig-perf" `
  -ContainerAppName "mongomigration-perf" `
  -AcrName "mongomigperfacr" `
  -StateStoreAppID "perf-migration-01" `
  -Location "eastus" `
  -VCores 32 `
  -MemoryGB 64 `
  -ImageTag "latest"
```

## Network Security (Optional)

### Setting Up Private Endpoints and VNET Integration

For enhanced security, you can configure private endpoints and VNET integration using the Azure Portal to restrict access to your Container App and associated resources.

#### Prerequisites for Private Networking
- An existing Azure Virtual Network (VNet) with available address space
- Subnet dedicated for Container Apps (recommended: /23 or larger)
- Subnet for private endpoints (recommended: /28 or larger)

#### Step 1: Configure Container Apps Environment with VNET Integration

1. **Navigate to Container Apps Environment** in Azure Portal
   - Go to your Container Apps Environment resource
   - Select **Networking** from the left menu

2. **Enable VNET Integration** (if not already enabled)
   - Click **Change VNET configuration**
   - Select **Use my own virtual network**
   - Choose your existing VNet or create a new one
   - Select or create a subnet for the Container Apps Environment
     - Recommended subnet size: /23 (512 addresses)
     - This subnet will be delegated to `Microsoft.App/environments`
   - Click **Save**

3. **Configure Ingress Settings**
   - Go to your Container App resource
   - Select **Ingress** from the left menu
   - Under **Ingress traffic**, select:
     - **Limited to Container Apps Environment** for private-only access
     - **Accepting traffic from anywhere** for public access (default)
   - Click **Save**

#### Step 2: Set Up Private Endpoint for Azure Container Registry (ACR)

1. **Navigate to your ACR** in Azure Portal
   - Go to your Azure Container Registry resource
   - Select **Networking** from the left menu

2. **Create Private Endpoint**
   - Click on **Private endpoint connections** tab
   - Click **+ Private endpoint**
   
3. **Configure Private Endpoint (Basics)**
   - **Subscription**: Select your subscription
   - **Resource group**: Same as your Container App
   - **Name**: `pe-<acr-name>`
   - **Region**: Same as your ACR
   - Click **Next: Resource**

4. **Configure Resource**
   - **Connection method**: Connect to an Azure resource in my directory
   - **Resource type**: Microsoft.ContainerRegistry/registries
   - **Resource**: Select your ACR
   - **Target sub-resource**: registry
   - Click **Next: Virtual Network**

5. **Configure Virtual Network**
   - **Virtual network**: Select your VNet
   - **Subnet**: Select a subnet for private endpoints
   - **Private IP configuration**: Dynamic
   - **Application security group**: (Optional)
   - Click **Next: DNS**

6. **Configure DNS**
   - **Integrate with private DNS zone**: Yes
   - **Private DNS zone**: (Auto-created) privatelink.azurecr.io
   - Click **Next: Tags**, then **Review + create**

7. **Disable Public Access** (Optional)
   - Return to ACR **Networking**
   - Under **Public network access** tab
   - Select **Disabled**
   - Click **Save**

#### Step 3: Set Up Private Endpoint for Storage Account

1. **Navigate to your Storage Account** in Azure Portal
   - Go to your Storage Account resource
   - Select **Networking** from the left menu

2. **Create Private Endpoint**
   - Click on **Private endpoint connections**
   - Click **+ Private endpoint**

3. **Configure Private Endpoint (Basics)**
   - **Name**: `pe-<storage-account-name>-file`
   - **Region**: Same as your storage account
   - Click **Next: Resource**

4. **Configure Resource**
   - **Resource type**: Microsoft.Storage/storageAccounts
   - **Resource**: Select your storage account
   - **Target sub-resource**: **file** (for Azure Files)
   - Click **Next: Virtual Network**

5. **Configure Virtual Network**
   - **Virtual network**: Select your VNet
   - **Subnet**: Select a subnet for private endpoints
   - Click **Next: DNS**

6. **Configure DNS**
   - **Integrate with private DNS zone**: Yes
   - **Private DNS zone**: (Auto-created) privatelink.file.core.windows.net
   - Click **Review + create**

7. **Disable Public Access** (Optional)
   - Return to Storage Account **Networking**
   - Under **Firewalls and virtual networks**
   - Select **Disabled** for public network access
   - Or select **Enabled from selected virtual networks and IP addresses**
     - Add your Container Apps subnet
   - Click **Save**

#### Step 4: Set Up Private Endpoint for Azure DocumentDB (StateStore)

1. **Navigate to your DocumentDB Account** in Azure Portal
   - Go to your Azure DocumentDB (Cosmos DB) account
   - Select **Networking** from the left menu

2. **Create Private Endpoint**
   - Click on **Private endpoint connections** tab
   - Click **+ Private endpoint**

3. **Configure Private Endpoint (Basics)**
   - **Name**: `pe-<documentdb-name>`
   - **Region**: Same as your DocumentDB
   - Click **Next: Resource**

4. **Configure Resource**
   - **Resource type**: Microsoft.DocumentDB/databaseAccounts
   - **Resource**: Select your DocumentDB account
   - **Target sub-resource**: **MongoDB** (or **Sql** depending on your API)
   - Click **Next: Virtual Network**

5. **Configure Virtual Network**
   - **Virtual network**: Select your VNet
   - **Subnet**: Select a subnet for private endpoints
   - Click **Next: DNS**

6. **Configure DNS**
   - **Integrate with private DNS zone**: Yes
   - **Private DNS zone**: (Auto-created) privatelink.mongo.cosmos.azure.com
   - Click **Review + create**

7. **Configure Firewall** (Optional)
   - Return to DocumentDB **Networking**
   - Under **Public network access**
   - Select **Disabled** for full private access
   - Or select **Selected networks** and add your Container Apps subnet
   - Click **Save**

#### Step 5: Verify Private Endpoint Connections

1. **Check DNS Resolution**
   ```powershell
   # From a VM in the same VNet, test DNS resolution
   nslookup <acr-name>.azurecr.io
   nslookup <storage-account-name>.file.core.windows.net
   nslookup <documentdb-name>.mongo.cosmos.azure.com
   
   # Should resolve to private IP addresses (10.x.x.x or 172.x.x.x or 192.168.x.x)
   ```

2. **Test Connectivity**
   - Deploy or restart your Container App
   - Check logs to ensure it can access ACR, Storage, and DocumentDB
   - Verify no public internet access is being used

#### Benefits of Private Endpoints

- ✅ **Enhanced Security**: All traffic stays within Azure backbone network
- ✅ **No Public Exposure**: Resources not accessible from the internet
- ✅ **Compliance**: Meets regulatory requirements for private data access
- ✅ **Network Isolation**: Reduces attack surface
- ✅ **Data Exfiltration Protection**: Prevents unauthorized data transfer

#### Important Considerations

- **Cost**: Private endpoints incur additional costs (~$0.01/hour per endpoint)
- **DNS**: Ensure proper DNS configuration for private endpoint resolution
- **Subnet Planning**: Plan subnet sizes carefully to accommodate growth
- **Managed Identity**: Container Apps use managed identity to access resources, which works seamlessly with private endpoints
- **Troubleshooting**: If connection issues occur, verify NSG rules and DNS resolution

## Persistent Storage

### Migration Data Storage
- **Path**: `/app/migration-data` (accessible via `ResourceDrive` environment variable)
- **Capacity**: 100GB Azure File Share
- **Persistence**: Data survives container restarts, deployments, and scaling events
- **Performance**: Standard_LRS storage for consistent I/O performance

### Using Persistent Storage in Application
Your application can access the persistent storage using the environment variable:

```csharp
var resourceDrive = Environment.GetEnvironmentVariable("ResourceDrive"); // Returns: "/app/migration-data"
// Store migration files in this directory - they persist across deployments
```

## Deployment Outputs

After successful deployment, the template provides these outputs:

| Output | Description |
|--------|-------------|
| `containerAppEnvironmentId` | Container Apps Environment resource ID |
| `containerAppFQDN` | Direct Container App FQDN |
| `containerAppUrl` | Application URL (HTTP or HTTPS based on setup) |
| `managedIdentityId` | Resource ID of the managed identity |
| `managedIdentityClientId` | Client ID for the managed identity |
| `storageAccountName` | Storage account name for migration data |
| `fileShareName` | File share name (migration-data) |
| `resourceDrivePath` | Mount path for persistent storage |

View outputs:
```powershell
az deployment group show `
  --resource-group <resource-group-name> `
  --name <deployment-name> `
  --query properties.outputs
```

## Updating the Application

### Quick Update (Recommended)

For updating only the application image without resetting environment variables or secrets, use the dedicated update script:

```powershell
# Update application with new code changes
.\update-aca-app.ps1 `
  -ResourceGroupName "MongoMigrationRGTest" `
  -ContainerAppName "mongomigration" `
  -AcrName "mongomigrationacr" `
  -ImageTag "v1.1"
```

The update script will:
1. Build and push the new Docker image to ACR
2. Update the Container App with the new image
3. Preserve all existing environment variables and secrets
4. Display the application URL with a clickable launch link

**Advantages of using the update script:**
- ✅ Faster deployment (skips infrastructure provisioning)
- ✅ No need to re-enter connection strings or secrets
- ✅ Environment variables remain unchanged
- ✅ Simpler command with fewer parameters required

### Update with Full Redeployment

Deploy updated configuration with all parameters (use this when you need to change environment variables or infrastructure):
```powershell
az deployment group create `
  --resource-group <resource-group-name> `
  --template-file aca_main.bicep `
  --parameters containerAppName=<container-app-name> acrName=<acr-name> imageTag=v2.0 # ... other parameters
```

### Manual Image Update

If you prefer manual control over each step:

1. Build and push new image:
```powershell
az acr build --registry <acrName> `
  --resource-group <resource-group-name> `
  --image <containerAppName>:v2.0 `
  --file MongoMigrationWebApp/Dockerfile `
  .
```

2. Update Container App:
```powershell
az containerapp update `
  --name <containerAppName> `
  --resource-group <resource-group-name> `
  --image <acrName>.azurecr.io/<containerAppName>:v2.0
```

**Note**: For most scenarios, using `update-aca-app.ps1` is recommended over manual updates as it handles both steps and provides better error handling.

## Monitoring and Troubleshooting

### Common Deployment Issues

### Troubleshooting Script

Before deploying, you can run the troubleshooting script to check for common issues:

```powershell
./troubleshoot-aca.ps1 -ResourceGroupName "my-rg" -Location "eastus"
```

This script will:
- Verify Azure login and CLI extensions
- Check region support for Container Apps
- Validate workload profile availability  
- Check ACR name availability
- Validate Bicep template syntax
- Display subscription quotas

#### 1. Template Deployment Validation Error
**Error**: `The template deployment 'aca_main' is not valid according to the validation procedure`

**Possible Causes & Solutions**:
- **Region not supported**: Use a supported region like `eastus`, `westus2`, or `northeurope`
- **ACR name conflict**: ACR names must be globally unique - try adding random numbers
- **Resource limits**: Check if your subscription has quotas for Container Apps

```powershell
# Check Container Apps availability in region
az provider show --namespace Microsoft.App --query "resourceTypes[?resourceType=='managedEnvironments'].locations" -o table

# Generate unique ACR name
$uniqueAcrName = "mongomigacr$(Get-Random -Minimum 1000 -Maximum 9999)"
```

#### 2. Workload Profile Not Available
**Error**: Related to D4 workload profile

**Solution**: The Bicep template now includes both Consumption and Dedicated profiles. If D4 is not available, it will fall back to Consumption profile with lower resource limits.

#### 3. Network Configuration Issues
**Error**: VNet or subnet configuration problems

**Solution**: Ensure the region supports Container Apps networking features.

### View Container App Logs
```powershell
az containerapp logs show `
  --name <containerAppName> `
  --resource-group <resource-group-name> `
  --follow
```

### Check Container App Status
```powershell
az containerapp show `
  --name <containerAppName> `
  --resource-group <resource-group-name> `
  --query "properties.provisioningState"
```

### Access Container App
```powershell
# Get the URL
az containerapp show `
  --name <containerAppName> `
  --resource-group <resource-group-name> `
  --query "properties.configuration.ingress.fqdn" `
  --output tsv
```

### Access Persistent Storage
```powershell
# List files in Azure File Share
az storage file list `
  --account-name <storageAccountName> `
  --share-name migration-data `
  --output table
```

## Cost Optimization Tips

1. **Right-size resources** - Start with smaller vCores/memory configurations and increase only if needed
2. **Monitor resource usage** - Use Azure Monitor to track CPU and memory utilization
3. **Right-size storage** - 100GB File Share, expand only if needed
4. **Choose appropriate regions** - Some regions have lower Container Apps pricing
5. **Use dedicated plan efficiently** - Dedicated plan provides consistent performance for predictable costs
6. **Clean up resources** - Remove Container Apps and storage when migrations are complete

## Security Features

### Container Apps Security
- ✅ **Managed Identity** - No passwords or keys needed for ACR and storage access
- ✅ **Secure Environment Variables** - Connection strings stored as secrets
- ✅ **VNet Integration** - Private networking with optional public ingress
- ✅ **ACR Integration** - Secure image pull using managed identity
- ✅ **HTTPS by Default** - Built-in TLS termination

## Additional Resources

- [Azure Container Apps Documentation](https://learn.microsoft.com/azure/container-apps/)
- [Container Apps Pricing](https://azure.microsoft.com/pricing/details/container-apps/)
- [Azure Container Registry Documentation](https://learn.microsoft.com/azure/container-registry/)
- [Azure File Share Documentation](https://learn.microsoft.com/azure/storage/files/)
- [Managed Identity Documentation](https://learn.microsoft.com/azure/active-directory/managed-identities-azure-resources/)