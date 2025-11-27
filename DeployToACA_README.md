# Deploy to Azure Container Apps (ACA)

This guide explains how to deploy the MongoDB Migration Web-Based Utility to Azure Container Apps using the `aca_main.bicep` template.

## Prerequisites

- Azure CLI installed and logged in (`az login`)
- Docker installed (for building images)
- An Azure subscription with appropriate permissions
- Resource group created (`az group create -n <rg-name> -l <location>`)
- **Azure DocumentDB account** created for state storage (you'll need the connection string)

## Deployment Steps

### Step 0: Clone the Repository

Clone the MongoDB Migration Web-Based Utility repository to your local machine (assuming you are in `c:\Work\GitHub\repos\`):

```powershell
# Navigate to your repos directory
cd c:\Work\GitHub\repos\

# Clone the repository
git clone https://github.com/AzureCosmosDB/MongoMigrationWebBasedUtility.git

# Navigate to the repository directory
cd MongoMigrationWebBasedUtility
```

### Step 1: Deploy Infrastructure with Bicep

Deploy the Bicep template to create all required resources:

```powershell
az deployment group create `
  --resource-group <resource-group-name> `
  --template-file aca_main.bicep `
  --parameters appName=<app-name> `
               acrName=<acr-name> `
               stateStoreAppId=<unique-app-id> `
               stateStoreConnectionString=<cosmos-db-connection-string>
```

**Required Parameters:**
- `stateStoreAppId`: A unique identifier for this migration server instance (e.g., `migration-server-01`). Use a different ID for each instance if deploying multiple servers.
- `stateStoreConnectionString`: A Cosmos DB for MongoDB (DocumentDB) connection string used to store the migration state information.

This will create:
- Managed Identity
- Azure Container Registry (ACR)
- Container App Environment
- Container App (will initially fail to start without an image)

Refer to[Parameter Reference](#parameters-reference) learn about the various parameters.

### Step 2: Build and Push Docker Image to ACR

After deploying the infrastructure, build and push your Docker image to the Azure Container Registry.

You have two options for building and pushing your Docker image:

#### Option A: Build in Azure (Recommended)

Build your Docker image directly in Azure Container Registry (no local Docker required):

```powershell
# Navigate to the repository root
cd c:\Work\GitHub\repos\MongoMigrationWebBasedUtility

# Build and push the image to ACR
az acr build --registry <acrName> `
  --image <appName>:latest `
  --file MongoMigrationWebApp/Dockerfile `
  .
```

**Benefits:**
- No local Docker installation required
- Faster builds using Azure's infrastructure
- Automatically pushes to ACR after build
- Works from any machine with Azure CLI

#### Option B: Build Locally and Push

Build the Docker image locally and push it to Azure Container Registry:

```powershell
# Navigate to the repository root
cd c:\Work\GitHub\repos\MongoMigrationWebBasedUtility

# Build the Docker image locally
docker build -f MongoMigrationWebApp/Dockerfile -t <appName>:latest .

# Login to Azure Container Registry
az acr login --name <acrName>

# Tag the image for ACR
docker tag <appName>:latest <acrName>.azurecr.io/<appName>:latest

# Push the image to ACR
docker push <acrName>.azurecr.io/<appName>:latest
```

**Prerequisites for local build:**
- Docker Desktop installed and running
- Sufficient disk space for build (recommended: 5GB+)
- Azure CLI logged in (`az login`)

**Note**: The ACR name must be globally unique and contain only alphanumeric characters (3-50 characters).

#### Understanding the Build Process

The Dockerfile performs these steps:
1. **Base Stage**: Uses .NET 9.0 runtime as base
2. **Build Stage**: Restores and builds the application
   - Restores `MongoMigrationWebApp.csproj`
   - Restores `OnlineMongoMigrationProcessor.csproj`
   - Builds in Release configuration
3. **Publish Stage**: Publishes the application
4. **Final Stage**: 
   - Installs MongoDB Database Tools (mongodump, mongorestore, etc.)
   - Copies published application
   - Configures entrypoint
   - Exposes ports 8080 and 8081

**Build Time**: Expect 5-10 minutes for first build (downloads dependencies), 2-5 minutes for subsequent builds.

### Step 3: Update Container App with Image

After the image is built and pushed, update the Container App to use the new image:

```powershell
az containerapp update `
  --name <appName> `
  --resource-group <resource-group-name> `
  --image <acrName>.azurecr.io/<appName>:latest
```

The Container App will now start successfully with your application.

## Parameters Reference

### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------||
| `appName` | Name of the Container App | `mongo-migration-app` |
| `acrName` | Name of the Azure Container Registry (must be globally unique, alphanumeric only) | `mongomigrationacr001` |
| `stateStoreAppId` | Unique identifier for this migration server instance | `migration-server-01` |
| `stateStoreConnectionString` | Cosmos DB for MongoDB connection string for state storage | `mongodb://...` |

### Optional Parameters

| Parameter | Type | Default | Description | Valid Values |
|-----------|------|---------|-------------|--------------|
| `location` | string | Resource group location | Azure region for all resources | Any Azure region (e.g., `eastus`, `westus2`, `northeurope`) |
| `environmentPlan` | string | `Consumption` | Container App Environment plan type | `Consumption` or `Dedicated` |
| `cpuCores` | string | `0.5` | CPU cores allocated to the container | `0.25`, `0.5`, `1.0`, `1.5`, `2.0`, `2.5`, `3.0`, `3.5`, `4.0` |
| `memorySize` | string | `1.0` | Memory in GB allocated to the container | `0.5`, `1.0`, `1.5`, `2.0`, `3.0`, `4.0`, `6.0`, `8.0` |
| `imageTag` | string | `latest` | Docker image tag to deploy | Any valid tag (e.g., `latest`, `v1.0`, `prod`) |

## Environment Plan Options

### Consumption Plan (Default)

Best for development, testing, and low-traffic workloads.

**Characteristics:**
- Pay only for what you use
- Automatic scaling to zero
- Lower cost for intermittent workloads
- Shared infrastructure

**CPU and Memory Combinations:**

| CPU Cores | Memory Options (GB) | Use Case |
|-----------|---------------------|----------|
| 1.0 | 2.0 | Minimal workloads, testing |
| 2.0 | 4.0 | Light  workloads, Maximum available in consumption tier, use Dedicated for faster migrations |

**Example Deployment:**
```powershell
az deployment group create `
  --resource-group myResourceGroup `
  --template-file aca_main.bicep `
  --parameters appName=mongo-migration `
               acrName=mongomigacr001 `
               stateStoreAppId=migration-server-01 `
               stateStoreConnectionString='<your-cosmos-db-connection-string>' `
               environmentPlan=Consumption `
               cpuCores=0.5 `
               memorySize=1.0
```

### Dedicated Plan

Best for production workloads requiring consistent performance and dedicated resources.

**Characteristics:**
- Dedicated compute resources
- Predictable performance
- Higher cost but more control
- Suitable for high-traffic applications

**CPU and Memory Combinations:**

| CPU Cores | Memory Options (GB) | Use Case |
|-----------|---------------------|----------|
| 2.0 - 4.0 | 4.0 - 8.0 | Small dedicated workloads |
| 4.0 - 8.0 | 16.0 - 32.0 | Large dedicated workloads |

**Example Deployment:**
```powershell
az deployment group create `
  --resource-group myResourceGroup `
  --template-file aca_main.bicep `
  --parameters appName=mongo-migration `
               acrName=mongomigacr001 `
               stateStoreAppId=migration-server-01 `
               stateStoreConnectionString='<your-cosmos-db-connection-string>' `
               environmentPlan=Dedicated `
               cpuCores=2.0 `
               memorySize=4.0
```

## Complete Deployment Examples

### Example 1: Development Environment
```powershell
# Build and push image
az acr build --registry mongomigdevacr `
  --image mongo-migration:dev `
  --file MongoMigrationWebApp/Dockerfile `
  .

# Deploy with minimal resources
az deployment group create `
  --resource-group rg-mongomig-dev `
  --template-file aca_main.bicep `
  --parameters appName=mongo-migration-dev `
               acrName=mongomigdevacr `
               stateStoreAppId=dev-migration-01 `
               stateStoreConnectionString='<your-cosmos-db-connection-string>' `
               environmentPlan=Consumption `
               cpuCores=1.0 `
               memorySize=2.0 `
               imageTag=dev `
               location=eastus
```


### Example 3: Environment with Dedicated Plan
```powershell
# Build and push image
az acr build --registry mongomigentacr `
  --image mongo-migration:v1.0 `
  --file MongoMigrationWebApp/Dockerfile `
  .

# Deploy with dedicated resources
az deployment group create `
  --resource-group rg-mongomig-enterprise `
  --template-file aca_main.bicep `
  --parameters appName=mongo-migration-ent `
               acrName=mongomigentacr `
               stateStoreAppId=prod-migration-01 `
               stateStoreConnectionString='<your-cosmos-db-connection-string>' `
               environmentPlan=Dedicated `
               cpuCores=4.0 `
               memorySize=8.0 `
               imageTag=v1.0 `
               location=westus2
```

## Deployment Outputs

After successful deployment, the template provides these outputs:

| Output | Description |
|--------|-------------|
| `managedIdentityId` | Resource ID of the managed identity |
| `managedIdentityClientId` | Client ID for the managed identity |
| `acrLoginServer` | Login server URL for the Container Registry |
| `containerAppFQDN` | Fully qualified domain name of the container app |
| `containerAppUrl` | Full HTTPS URL to access the application |

View outputs:
```powershell
az deployment group show `
  --resource-group <resource-group-name> `
  --name <deployment-name> `
  --query properties.outputs
```

## Updating the Application

### Update with New Image

1. Build and push new image:
```powershell
az acr build --registry <acrName> `
  --image <appName>:v2.0 `
  --file MongoMigrationWebApp/Dockerfile `
  .
```

2. Update the container app:
```powershell
az containerapp update `
  --name <appName> `
  --resource-group <resource-group-name> `
  --image <acrName>.azurecr.io/<appName>:v2.0
```

### Update Resource Allocation

Redeploy with different parameters:
```powershell
az deployment group create `
  --resource-group <resource-group-name> `
  --template-file aca_main.bicep `
  --parameters appName=<app-name> `
               acrName=<acr-name> `
               cpuCores=2.0 `
               memorySize=4.0
```

## Monitoring and Troubleshooting

### View Container App Logs
```powershell
az containerapp logs show `
  --name <appName> `
  --resource-group <resource-group-name> `
  --follow
```

### Check Container App Status
```powershell
az containerapp show `
  --name <appName> `
  --resource-group <resource-group-name> `
  --query properties.runningStatus
```

### Access Container App
```powershell
# Get the URL
az containerapp show `
  --name <appName> `
  --resource-group <resource-group-name> `
  --query properties.configuration.ingress.fqdn `
  --output tsv
```

## Cost Optimization Tips

1. **Use Consumption Plan** for development and variable workloads
2. **Scale to zero** when not in use (Consumption plan only)
3. **Right-size resources** - start small and scale up as needed
4. **Use Basic ACR SKU** for development (already configured in template)
5. **Monitor usage** and adjust CPU/memory based on actual needs

## Security Features

- ✅ **Managed Identity** - No passwords or keys needed
- ✅ **ACR Integration** - Secure image pull using managed identity
- ✅ **HTTPS Only** - All traffic encrypted in transit
- ✅ **No Admin User** - ACR admin account disabled
- ✅ **Single Replica** - Predictable and controlled deployment

## Additional Resources

- [Azure Container Apps Documentation](https://learn.microsoft.com/azure/container-apps/)
- [Container Apps Pricing](https://azure.microsoft.com/pricing/details/container-apps/)
- [Azure Container Registry Documentation](https://learn.microsoft.com/azure/container-registry/)
- [Managed Identity Documentation](https://learn.microsoft.com/azure/active-directory/managed-identities-azure-resources/)
