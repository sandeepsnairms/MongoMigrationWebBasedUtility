# Deploy to Azure Container Apps (ACA)

This guide explains how to deploy the MongoDB Migration Web-Based Utility to Azure Container Apps using the `aca_main.bicep` template. ACA provides enterprise-grade auto-scaling, dedicated compute resources, and persistent storage for high-performance migrations.

## Prerequisites

- Azure CLI installed and logged in (`az login`)
- Docker installed (for building images)
- An Azure subscription with appropriate permissions
- Resource group created (`az group create -n <rg-name> -l <location>`)
- **Azure DocumentDB account** created for state storage (you'll need the connection string)
- **Supported Azure region** - Container Apps with dedicated workload profiles are not available in all regions

### Supported Regions for Container Apps with Dedicated Plans

Container Apps with dedicated workload profiles (D4) are currently available in these regions:
- East US (`eastus`)
- East US 2 (`eastus2`) 
- West US 2 (`westus2`)
- West US 3 (`westus3`)
- North Europe (`northeurope`)
- West Europe (`westeurope`)
- Australia East (`australiaeast`)
- Southeast Asia (`southeastasia`)
- Canada Central (`canadacentral`)

**Note**: If you encounter deployment errors, try switching to one of these regions.

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
  -ImageTag "latest"
```

### Production Deployment with Application Gateway

```powershell
# Deploy with HTTPS, WAF protection, and enterprise security
# Note: Use a globally unique ACR name and supported region
.\deploy-to-aca.ps1 `
  -ResourceGroupName "MongoMigrationRGProd" `
  -ContainerAppName "mongomigration-prod" `
  -AcrName "mongomigprod$(Get-Random -Minimum 1000 -Maximum 9999)" `
  -StateStoreAppID "prod-migration-01" `
  -EnableApplicationGateway $true `
  -DnsNameLabel "mongomig-prod" `
  -Location "eastus" `
  -VCores 16 `
  -MemoryGB 64 `
  -ImageTag "v1.0"
```

The script will:
1. Deploy infrastructure using Bicep (ACR, Container Apps Environment, VNet, Storage Account)
2. Build and push Docker image to ACR
3. Securely prompt for DocumentDB connection string
4. Deploy Container App with final configuration and environment variables
5. Display the application URL (HTTP or HTTPS based on Application Gateway setting)

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
  --parameters containerAppName=<container-app-name> `
               acrName=<acr-name> `
               enableApplicationGateway=false
```

This will create:
- Managed Identity
- Azure Container Registry (ACR)
- Container Apps Environment with Configurable Dedicated Plan (1-32 vCores, 2-64GB)
- Storage Account with 100GB Azure File Share
- VNet and Application Gateway (if enabled)

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
$secureConnString = Read-Host -Prompt "Enter Cosmos DB Connection String" -AsSecureString
$connString = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto([System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureConnString))

# Deploy final configuration
az deployment group create `
  --resource-group <resource-group-name> `
  --template-file aca_main.bicep `
  --parameters containerAppName=<container-app-name> `
               acrName=<acr-name> `
               stateStoreAppID=<your-app-id> `
               stateStoreConnectionString=$connString `
               aspNetCoreEnvironment=Development `
               imageTag=latest

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
| `EnableApplicationGateway` | bool | `false` | - | Enable HTTPS and WAF protection |
| `DnsNameLabel` | string | Auto-generated | - | DNS name label for Application Gateway |

## Resource Configurations

Azure Container Apps with Dedicated Plan provides configurable high-performance compute:

### Configurable Resources
- **CPU**: 1-32 vCores (default: 8 vCores, dedicated)
- **Memory**: 2-64GB RAM (default: 32GB)
- **Storage**: 100GB persistent Azure File Share
- **Auto-scaling**: 1-3 replicas based on CPU utilization (70% threshold)
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

**Key Advantages over Container Instances:**
- ✅ Configurable high CPU and memory (up to 32 vCores, 64GB vs fixed 4 cores, 16GB)
- ✅ Auto-scaling based on load
- ✅ Persistent storage that survives deployments
- ✅ Enterprise networking with VNet integration
- ✅ Built-in ingress controller with HTTPS
- ✅ Application Gateway integration for WAF protection

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

### Production Setup with Application Gateway

```powershell
# Deploy with HTTPS, WAF, and enterprise security
.\deploy-to-aca.ps1 `
  -ResourceGroupName "rg-mongomig-prod" `
  -ContainerAppName "mongomigration-prod" `
  -AcrName "mongomigprodacr" `
  -StateStoreAppID "prod-migration-01" `
  -EnableApplicationGateway $true `
  -DnsNameLabel "mongomig-production" `
  -Location "eastus" `
  -ImageTag "v1.0"
```

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
| `applicationGatewayPublicIp` | Application Gateway public IP (when enabled) |
| `applicationGatewayFQDN` | Application Gateway FQDN (when enabled) |

View outputs:
```powershell
az deployment group show `
  --resource-group <resource-group-name> `
  --name <deployment-name> `
  --query properties.outputs
```

## Updating the Application

### Update with New Image

Deploy updated configuration:
```powershell
az deployment group create `
  --resource-group <resource-group-name> `
  --template-file aca_main.bicep `
  --parameters containerAppName=<container-app-name> `
               acrName=<acr-name> `
               imageTag=v2.0 `
               # ... other parameters
```

### Manual Image Update

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

**Solution**: Ensure the region supports Container Apps networking features, or set `-EnableApplicationGateway $false` for simpler deployment.

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

### Check Auto-scaling Status
```powershell
az containerapp replica list `
  --name <containerAppName> `
  --resource-group <resource-group-name>
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

1. **Use auto-scaling** - Container Apps automatically scales down to minimum replicas (1)
2. **Monitor resource usage** - Adjust CPU scaling threshold based on workload patterns
3. **Right-size storage** - 100GB File Share, expand only if needed
4. **Choose appropriate regions** - Some regions have lower Container Apps pricing
5. **Use dedicated plan efficiently** - D4 plan provides consistent performance for predictable costs
6. **Clean up resources** - Remove Container Apps and storage when migrations are complete

## Security Features

### Container Apps Security
- ✅ **Managed Identity** - No passwords or keys needed for ACR and storage access
- ✅ **Secure Environment Variables** - Connection strings stored as secrets
- ✅ **VNet Integration** - Private networking with optional public ingress
- ✅ **ACR Integration** - Secure image pull using managed identity
- ✅ **HTTPS by Default** - Built-in TLS termination

### Application Gateway Security (when enabled)
- ✅ **Web Application Firewall (WAF)** - OWASP 3.2 rule set protection
- ✅ **TLS Termination** - HTTPS encryption for client connections
- ✅ **Azure AD Integration Ready** - Support for authentication and authorization
- ✅ **DDoS Protection** - Built-in protection against volumetric attacks
- ✅ **IP Filtering** - Control access by source IP ranges

## Comparison: ACA vs ACI

| Feature | Azure Container Apps (ACA) | Azure Container Instances (ACI) |
|---------|----------------------------|----------------------------------|
| **Max CPU (Dedicated)** | 32 vCores (configurable 1-32) | 4 cores |
| **Max Memory (Dedicated)** | 64 GB (configurable 2-64GB) | 16 GB |
| **Default Resources** | 8 vCores, 32GB | 4 cores, 16GB |
| **Resource Flexibility** | ✅ Configurable at deployment | ❌ Fixed sizing options |
| **Persistent Storage** | ✅ 100GB Azure File Share | ❌ Ephemeral only |
| **Auto-scaling** | ✅ CPU-based (1-3 replicas) | ❌ Manual scaling |
| **HTTPS Built-in** | ✅ Native ingress controller | ❌ Requires load balancer |
| **VNet Integration** | ✅ Native support | ⚠️ Limited support |
| **Application Gateway** | ✅ Integrated WAF | ⚠️ External configuration |
| **Best For** | Production workloads, enterprise | Simple batch jobs, testing |
| **Pricing Model** | Dedicated plan (predictable) | Per-second billing |
| **Complexity** | Enterprise features | Simple deployment |

**Recommendation for MongoDB Migration**: Use ACA with dedicated plan for production workloads requiring high performance, persistent storage, and enterprise security features.

## Application Gateway Features (Production Setup)

When `enableApplicationGateway` is set to `true`:

### Security Features
- **WAF Protection**: OWASP 3.2 rule set blocks common attacks
- **TLS Termination**: HTTPS encryption for all client connections
- **DDoS Protection**: Built-in volumetric attack protection

### Performance Features
- **Load Balancing**: Distributes traffic across Container App replicas
- **Health Probes**: Automatic health checking and failover
- **Connection Draining**: Graceful handling of deployments

### Enterprise Features
- **Custom Domain Ready**: Support for custom SSL certificates
- **Azure AD Integration**: Ready for enterprise authentication
- **Network Security Groups**: Fine-grained traffic control

## Additional Resources

- [Azure Container Apps Documentation](https://learn.microsoft.com/azure/container-apps/)
- [Container Apps Pricing](https://azure.microsoft.com/pricing/details/container-apps/)
- [Azure Container Registry Documentation](https://learn.microsoft.com/azure/container-registry/)
- [Azure Application Gateway Documentation](https://learn.microsoft.com/azure/application-gateway/)
- [Azure File Share Documentation](https://learn.microsoft.com/azure/storage/files/)
- [Managed Identity Documentation](https://learn.microsoft.com/azure/active-directory/managed-identities-azure-resources/)