# Migration Tool for Azure Cosmos DB for MongoDB (vCore-Based)

Streamline your migration to Azure Cosmos DB for MongoDB (vCore-based) with a tool built for efficiency, reliability, and ease of use. Whether you're migrating data online or offline, this tool delivers a seamless experience tailored to your requirements. While it leverages `mongodump` and `mongorestore` internally for data transfer, you don’t need to learn or use these command-line tools yourself.

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

You can deploy the utility either by compiling the source files or by using the precompiled binaries.

### Prerequisites

1. Azure Subscription
1. Azure CLI Installed
1. PowerShell


### Deploy using Source Files (option 1)

This option involves cloning the repository and building the C# project source files locally on a Windows machine. If you’re not comfortable working with code, consider using Option 2 below.

1. Install [.NET SDK](https://dotnet.microsoft.com/en-us/download/dotnet/6.0)
2. Clone the repository: `https://github.com/AzureCosmosDB/MongoMigrationWebBasedUtility`
2. Open PowerShell.
2. Navigate to the cloned project folder.
3. Run the following commands in PowerShell:

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

   # Deploy Azure Web App using Source Code (Option 1)
   Write-Host "Deploying Azure Web App..."
   az deployment group create --resource-group $resourceGroupName --template-file main.bicep --parameters location=WestUs3 webAppName=$webAppName


   # Configure Nuget Path. Execute only once on a machine
   dotnet nuget add source https://api.nuget.org/v3/index.json -n nuget.org


   # Build the Blazor app
   Write-Host "Building Blazor app..."
   dotnet publish $projectFilePath -c Release -o $publishFolder -warnaserror:none --nologo


   # Archive published files
   Compress-Archive -Path "$publishFolder\*" -DestinationPath $zipPath -Update

   # Deploy files to Azure Web App
   Write-Host "Deploying to Azure Web App..."
   az webapp deploy --resource-group $resourceGroupName --name $webAppName --src-path $zipPath --type zip

   Write-Host "Deployment completed successfully!"
   ```

4. Open `https://<WebAppName>.azurewebsites.net` to access the tool.
5. [Enable the use of a single public IP for consistent firewall rules](#integrating-azure-web-app-with-a-vnet-to-use-a-single-public-ip-optional) or [Enable Private Endpoint](#steps-to-enable-private-endpoint-on-the-azure-web-app-optional) if required.

### Deploy using precompiled binaries (option 2)

1. Download `app.zip` from the latest release at `https://github.com/AzureCosmosDB/MongoMigrationWebBasedUtility/releases`
2. Open PowerShell.
3. Run the following commands in PowerShell:

   ```powershell
   # Variables to be updated
   $resourceGroupName = <Replace with Existing Resource Group Name>
   $webAppName = <Replace with Web App Name>
   $zipPath = <Replace with full path of downloaded zip file on local>

   # Login to Azure
   az login

   # Set subscription (optional)
   # az account set --subscription "your-subscription-id"

   # Deploy Azure Web App using Source Code (Option 1)
   Write-Host "Deploying Azure Web App..."
   az deployment group create --resource-group $resourceGroupName --template-file main.bicep --parameters location=WestUs3 webAppName=$webAppName

   # Deploy files to Azure Web App
   Write-Host "Deploying to Azure Web App..."
   az webapp deploy --resource-group $resourceGroupName --name $webAppName --src-path $zipPath --type zip

   Write-Host "Deployment completed successfully!"
   ```

4. Open `https://<WebAppName>.azurewebsites.net` to access the tool.
5. [Enable the use of a single public IP for consistent firewall rules](#integrating-azure-web-app-with-a-vnet-to-use-a-single-public-ip-optional) or [Enable Private Endpoint](#steps-to-enable-private-endpoint-on-the-azure-web-app-optional) if required.

## Integrating Azure Web App with a VNet to Use a Single Public IP (Optional)

### Steps

#### 1. Create a VNet (If Not Already Existing)

1. **Go to the Azure Portal**:
   - Navigate to the **Create a resource** section.

2. **Create a Virtual Network**:
   - Search for **Virtual Network** and click **Create**.
   - Provide a **Name** for the VNet.
   - Choose the desired **Region** (ensure it matches the Web App's region for integration).
   - Define the **Address Space** (e.g., `10.0.0.0/16`).

3. **Add a Subnet**:
   - In the **Subnet** section, create a new subnet.
   - Provide a **Name** (e.g., `WebAppSubnet`).
   - Define the **Subnet Address Range** (e.g., `10.0.1.0/24`).
   - Set the **Subnet Delegation** to `Microsoft.Web` for VNet integration.

4. **Create the VNet**:
   - Click **Review + Create** and then **Create**.

#### 1. Enable VNet Integration for the Web App

1. **Go to the Web App**:
   - Navigate to your Azure Web App in the Azure Portal.

2. **Enable VNet Integration**:
   - In the left-hand menu, select **Networking**.
   - Under **Outbound Traffic**, click **VNet Integration**.
   - Click **Add VNet** and choose an existing VNet and subnet.
   - Ensure the subnet is delegated to **Microsoft.Web**.

3. **Save** the configuration.

#### 2. Configure a NAT Gateway for the VNet

1. **Create a Public IP Address**:
   - Go to **Create a resource** in the Azure Portal.
   - Search for **Public IP Address** and click **Create**.
   - Assign a name and ensure it's set to **Static**.
   - Complete the setup.

2. **Create a NAT Gateway**:
   - Navigate to **Create a resource** > **Networking** > **NAT Gateway**.
   - Assign a name and link it to the **Public IP Address** created earlier.
   - Attach the NAT Gateway to the same subnet used for the Web App.

3. **Save** the configuration.

#### 3. Configure Outbound IP Rules

1. **Identify Web App Outbound IPs**:
   - In the Web App's **Properties** section, note the **Outbound IP Addresses**.

2. **Update Firewall Rules**:
   - In your firewall (e.g., Azure Firewall, third-party), allow traffic from the single public IP address used by the NAT Gateway.


#### 4. Test the Configuration

   - Test access to MongoDB Source and Destination that have been configured with the new IP in their firewall rules.



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

