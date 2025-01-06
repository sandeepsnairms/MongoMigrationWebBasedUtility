# Migration Tool for Azure Cosmos DB for MongoDB (vCore-Based)

Simplify your migration journey to **Azure Cosmos DB for MongoDB (vCore-based)** with a tool designed for efficiency, reliability, and ease of use. Whether migrating data online or offline, this tool provides a seamless experience tailored to your needs.

## Key Features

- **Flexible Migration Options**  
  Supports both online and offline migrations to suit your business requirements.

- **User-Friendly Interface**  
  No steep learning curve—simply provide your connection strings and specify the collections to migrate.

- **Automatic Resume**  
  Migration resumes automatically in case of connection loss, ensuring uninterrupted reliability.

- **Private Deployment**  
  Deploy the tool within your private virtual network (vNet) for enhanced security. (Update `main.bicep` for vNet configuration.)

- **Standalone Solution**  
  Operates independently, with no dependencies on other Azure resources.

- **Scalable Performance**  
  Select your Azure Web App pricing plan based on performance requirements:  
  - Default: **B1**  
  - Recommended for large workloads: **Premium v3 P1V3** (Update `main.bicep` accordingly.)

- **Customizable**  
  Modify the provided C# code to suit your specific use cases.

---

Effortlessly migrate your MongoDB collections while maintaining control, security, and scalability. Begin your migration today and unlock the full potential of Azure Cosmos DB!

## Deployment Steps

### Clone the GitHub Repository

Clone the repository:  
`https://github.com/sandeepsnairms/cosmos-mongo-migrate-tovcore`

### Deploy the App to Azure Web App

1. Open PowerShell.
2. Navigate to the cloned project folder.
3. Refer to [Enable Private Endpoint](#steps-to-enable-private-endpoint-on-the-azure-web-app-optional) if required.
4. Run the following commands in PowerShell:

   ```powershell
   # Variables to be updated
   $resourceGroupName = <Replace with Existing Resource Group Name>
   $webAppName = <Replace with Web App Name>
   $projectFolderPath = <Replace with path to cloned repo on local>


   # Paths - No changes required
   $projectFilePath = "$projectFolderPath\MongoMigrationWebApp\MongoMigrationWebApp.csproj"
   $publishFolder = "$projectFolderPath\publish"
   $zipPath = "$publishFolder\app.zip"

   # Login to Azure
   az login

   # Set subscription (optional)
   # az account set --subscription "your-subscription-id"

   # Deploy Azure Web App
   Write-Host "Deploying Azure Web App..."
   az deployment group create --resource-group $resourceGroupName --template-file main.bicep --parameters location=WestUs3 webAppName=$webAppName

   # Build the Blazor app
   Write-Host "Building Blazor app..."
   dotnet publish $projectFilePath -c Release -o $publishFolder

   # Archive published files
   Compress-Archive -Path "$publishFolder\*" -DestinationPath $zipPath -Update

   # Deploy files to Azure Web App
   Write-Host "Deploying to Azure Web App..."
   az webapp deploy --resource-group $resourceGroupName --name $webAppName --src-path $zipPath --type zip

   Write-Host "Deployment completed successfully!"
   ```

5. Open `https://<WebAppName>.azurewebsites.net` to access the tool.

## Steps to Enable Private Endpoint on the Azure Web App (Optional)

### Prerequisites

Ensure you have:

- An existing **Azure Virtual Network (VNet)**.
- A subnet dedicated to private endpoints (e.g., `PrivateEndpointSubnet`).
- Permissions to configure networking and private endpoints in Azure.

### Enable Private Endpoint

#### 1. Navigate to the Web App

- Open the **Azure Portal**.
- In the left-hand menu, select **App Services**.
- Click the desired web app.

#### 2. Access Networking Settings

- In the web app's blade, select **Networking**.
- Under the **Private Endpoint** section, click **+ Private Endpoint**.

#### 3. Create the Private Endpoint

Follow these steps in the **Add Private Endpoint** Advanced wizard:

##### a. Basics

- **Name**: Enter a name for the private endpoint (e.g., `WebAppPrivateEndpoint`).
- **Region**: Ensure it matches your VNet’s region.

##### b. Resource

- **Resource Type**: Select `Microsoft.Web/sites` for the web app.
- **Resource**: Choose your web app.
- **Target Sub-resource**: Select `sites`.

##### c. Configuration

- **Virtual Network**: Select your VNet.
- **Subnet**: Choose the `PrivateEndpointSubnet`.
- **Integrate with private DNS zone**: Enable this to link the private endpoint with an Azure Private DNS zone (recommended).

#### 4. Review and Create

- Click **Next** to review the configuration.
- Click **Create** to finalize.

#### 5. Verify Private Endpoint Connection

- Return to the **Networking** tab of your web app.
- Verify the private endpoint status under **Private Endpoint Connections** as `Approved`.

### Configure DNS (Optional but Recommended)

If using a private DNS zone:

1. **Validate DNS Resolution**:
   Run the following command to ensure DNS resolves to the private IP:

   ```bash
   nslookup <WebAppName>.azurewebsites.net
   ```

2. **Update Custom DNS Servers (if applicable)**:
   Configure custom DNS servers to resolve the private endpoint using Azure Private DNS.

### Test Connectivity

1. Deploy a **VM in the same VNet**.
2. From the VM, access the web app via its URL (e.g., `https://<WebAppName>.azurewebsites.net`).
3. Confirm that the web app is accessible only within the VNet.

