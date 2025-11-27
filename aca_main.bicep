@description('Location for all resources')
param location string = resourceGroup().location

@description('Name of the Container App')
param appName string

@description('Name of the Container Registry')
param acrName string

@description('Container App Environment plan (Consumption or Dedicated)')
@allowed([
  'Consumption'
  'Dedicated'
])
param environmentPlan string = 'Consumption'

@description('CPU cores for the container (e.g., 0.25, 0.5, 1.0, 2.0)')
param cpuCores string = '0.5'

@description('Memory in GB for the container (e.g., 0.5, 1.0, 2.0, 4.0)')
param memorySize string = '1.0'

@description('Docker image tag')
param imageTag string = 'latest'

@description('StateStore App ID - Identifies one migration server instance, use different ID for each instance')
param stateStoreAppId string

@description('StateStore Connection String - DocumentDB connection string used to store state information')
@secure()
param stateStoreConnectionString string

// Managed Identity
resource managedIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${appName}-identity'
  location: location
}

// Azure Container Registry
resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: acrName
  location: location
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: false
    publicNetworkAccess: 'Enabled'
  }
}

// Assign AcrPull role to Managed Identity
resource acrPullRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(acr.id, managedIdentity.id, 'AcrPull')
  scope: acr
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d') // AcrPull role
    principalId: managedIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

// Container App Environment
resource containerAppEnvironment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: '${appName}-env'
  location: location
  properties: {
    workloadProfiles: environmentPlan == 'Dedicated' ? [
      {
        name: 'Consumption'
        workloadProfileType: 'Consumption'
      }
    ] : null
  }
}

// Container App
resource containerApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: appName
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${managedIdentity.id}': {}
    }
  }
  properties: {
    managedEnvironmentId: containerAppEnvironment.id
    configuration: {
      ingress: {
        external: true
        targetPort: 8080
        transport: 'http'
        allowInsecure: false
      }
      registries: [
        {
          server: acr.properties.loginServer
          identity: managedIdentity.id
        }
      ]
    }
    template: {
      containers: [
        {
          name: appName
          image: '${acr.properties.loginServer}/${appName}:${imageTag}'
          resources: {
            cpu: json(cpuCores)
            memory: '${memorySize}Gi'
          }
          env: [
            {
              name: 'ASPNETCORE_ENVIRONMENT'
              value: 'Production'
            }
            {
              name: 'ASPNETCORE_HTTP_PORTS'
              value: '8080'
            }
            {
              name: 'StateStore_AppID'
              value: stateStoreAppId
            }
            {
              name: 'StateStore_ConnectionString'
              value: stateStoreConnectionString
            }
          ]
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
  dependsOn: [
    acrPullRoleAssignment
  ]
}

// Outputs
output managedIdentityId string = managedIdentity.id
output managedIdentityClientId string = managedIdentity.properties.clientId
output acrLoginServer string = acr.properties.loginServer
output containerAppFQDN string = containerApp.properties.configuration.ingress.fqdn
output containerAppUrl string = 'https://${containerApp.properties.configuration.ingress.fqdn}'
