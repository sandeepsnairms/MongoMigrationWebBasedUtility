@description('Location for all resources')
param location string = resourceGroup().location

@description('Name of the Container App')
param containerAppName string

@description('Name of the Azure Container Registry')
param acrName string

@description('Docker image tag to deploy')
param imageTag string = 'latest'

@description('Enable Application Gateway for HTTPS and WAF protection')
param enableApplicationGateway bool = false

@description('Application Gateway name')
param applicationGatewayName string = '${containerAppName}-appgw'

@description('Application Gateway SKU')
param applicationGatewaySku string = 'Standard_v2'

@description('Application Gateway capacity')
param applicationGatewayCapacity int = 2

@secure()
@description('StateStore connection string for the container')
param stateStoreConnectionString string = ''

@description('StateStore App ID for the container')
param stateStoreAppID string = ''

@description('Number of vCores for the container')
@minValue(1)
@maxValue(32)
param vCores int = 8

@description('Memory in GB for the container')
@minValue(2)
@maxValue(64)
param memoryGB int = 32

@description('ASP.NET Core environment setting')
param aspNetCoreEnvironment string = 'Development'

@description('DNS name label for the container app (optional)')
param dnsNameLabel string = ''

// Variables for dynamic workload profile selection
var workloadProfileType = vCores <= 4 ? 'D4' : vCores <= 8 ? 'D8' : vCores <= 16 ? 'D16' : 'D32'
var workloadProfileName = 'Dedicated'

// Managed Identity for Container App and ACR access
resource managedIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${containerAppName}-identity'
  location: location
}

// Virtual Network for Application Gateway and Container Apps
resource vnet 'Microsoft.Network/virtualNetworks@2023-05-01' = if (enableApplicationGateway) {
  name: '${containerAppName}-vnet'
  location: location
  properties: {
    addressSpace: {
      addressPrefixes: ['10.0.0.0/16']
    }
    subnets: [
      {
        name: 'appgw-subnet'
        properties: {
          addressPrefix: '10.0.1.0/24'
          privateEndpointNetworkPolicies: 'Disabled'
          privateLinkServiceNetworkPolicies: 'Disabled'
        }
      }
      {
        name: 'containerapp-subnet'
        properties: {
          addressPrefix: '10.0.2.0/23'
          privateEndpointNetworkPolicies: 'Disabled'
          privateLinkServiceNetworkPolicies: 'Disabled'
        }
      }
    ]
  }
}

// Public IP for Application Gateway
resource appGwPublicIp 'Microsoft.Network/publicIPAddresses@2023-05-01' = if (enableApplicationGateway) {
  name: '${containerAppName}-appgw-pip'
  location: location
  sku: {
    name: 'Standard'
    tier: 'Regional'
  }
  properties: {
    publicIPAllocationMethod: 'Static'
    dnsSettings: {
      domainNameLabel: dnsNameLabel != '' ? dnsNameLabel : '${containerAppName}-${uniqueString(resourceGroup().id)}'
    }
  }
}

// Managed Identity for Application Gateway
resource appGwManagedIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = if (enableApplicationGateway) {
  name: '${containerAppName}-appgw-identity'
  location: location
}

// Application Gateway
resource applicationGateway 'Microsoft.Network/applicationGateways@2023-05-01' = if (enableApplicationGateway) {
  name: applicationGatewayName
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${appGwManagedIdentity.id}': {}
    }
  }
  properties: {
    sku: {
      name: applicationGatewaySku
      tier: applicationGatewaySku == 'WAF_v2' ? 'WAF_v2' : 'Standard_v2'
      capacity: applicationGatewayCapacity
    }
    gatewayIPConfigurations: [
      {
        name: 'appGwIpConfig'
        properties: {
          subnet: {
            id: vnet.?properties.?subnets[?0].?id ?? ''
          }
        }
      }
    ]
    frontendIPConfigurations: [
      {
        name: 'appGwPublicFrontendIp'
        properties: {
          publicIPAddress: {
            id: appGwPublicIp.id
          }
        }
      }
    ]
    frontendPorts: [
      {
        name: 'port80'
        properties: {
          port: 80
        }
      }
    ]
    backendAddressPools: [
      {
        name: 'appGwBackendPool'
        properties: {
          backendAddresses: []
        }
      }
    ]
    backendHttpSettingsCollection: [
      {
        name: 'appGwBackendHttpSettings'
        properties: {
          port: 80
          protocol: 'Http'
          cookieBasedAffinity: 'Disabled'
          connectionDraining: {
            enabled: false
            drainTimeoutInSec: 1
          }
          requestTimeout: 20
          probe: {
            id: resourceId('Microsoft.Network/applicationGateways/probes', applicationGatewayName, 'healthProbe')
          }
        }
      }
    ]
    httpListeners: [
      {
        name: 'appGwHttpListener'
        properties: {
          frontendIPConfiguration: {
            id: resourceId('Microsoft.Network/applicationGateways/frontendIPConfigurations', applicationGatewayName, 'appGwPublicFrontendIp')
          }
          frontendPort: {
            id: resourceId('Microsoft.Network/applicationGateways/frontendPorts', applicationGatewayName, 'port80')
          }
          protocol: 'Http'
        }
      }
    ]
    requestRoutingRules: [
      {
        name: 'rule1'
        properties: {
          priority: 1
          ruleType: 'Basic'
          httpListener: {
            id: resourceId('Microsoft.Network/applicationGateways/httpListeners', applicationGatewayName, 'appGwHttpListener')
          }
          backendAddressPool: {
            id: resourceId('Microsoft.Network/applicationGateways/backendAddressPools', applicationGatewayName, 'appGwBackendPool')
          }
          backendHttpSettings: {
            id: resourceId('Microsoft.Network/applicationGateways/backendHttpSettingsCollection', applicationGatewayName, 'appGwBackendHttpSettings')
          }
        }
      }
    ]
    probes: [
      {
        name: 'healthProbe'
        properties: {
          protocol: 'Http'
          path: '/'
          interval: 30
          timeout: 30
          unhealthyThreshold: 3
          pickHostNameFromBackendHttpSettings: false
          minServers: 0
          match: {
            statusCodes: ['200-399']
          }
        }
      }
    ]
    enableHttp2: false
    webApplicationFirewallConfiguration: applicationGatewaySku == 'WAF_v2' ? {
      enabled: true
      firewallMode: 'Prevention'
      ruleSetType: 'OWASP'
      ruleSetVersion: '3.2'
      disabledRuleGroups: []
      exclusions: []
      requestBodyCheck: true
      maxRequestBodySizeInKb: 128
      fileUploadLimitInMb: 100
    } : null
  }
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
  }
}

// ACR Pull Role Assignment to Managed Identity
resource acrPullRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(acr.id, managedIdentity.id, 'acrPull')
  scope: acr
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
    principalId: managedIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

// Storage Account for persistent migration files
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: take('${replace(containerAppName, '-', '')}stor', 24)
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    accessTier: 'Hot'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
  }
}

// File Share for migration data (100GB)
resource fileShare 'Microsoft.Storage/storageAccounts/fileServices/shares@2023-01-01' = {
  name: '${storageAccount.name}/default/migration-data'
  properties: {
    shareQuota: 100
    enabledProtocols: 'SMB'
  }
}

// Container Apps Environment with Dedicated Plan
resource containerAppEnvironment 'Microsoft.App/managedEnvironments@2023-05-01' = {
  name: '${containerAppName}-env-${workloadProfileType}'
  location: location
  properties: {
    vnetConfiguration: enableApplicationGateway ? {
      infrastructureSubnetId: vnet.?properties.?subnets[?1].?id ?? ''
      internal: false
    } : null
    workloadProfiles: [
      {
        name: 'Consumption'
        workloadProfileType: 'Consumption'
      }
      {
        name: workloadProfileName
        workloadProfileType: workloadProfileType
        minimumCount: 1
        maximumCount: 3
      }
    ]
  }
}

// Storage configuration for Container Apps Environment
resource storageConfiguration 'Microsoft.App/managedEnvironments/storages@2023-05-01' = {
  parent: containerAppEnvironment
  name: 'migration-storage'
  properties: {
    azureFile: {
      accountName: storageAccount.name
      accountKey: storageAccount.listKeys().keys[0].value
      shareName: 'migration-data'
      accessMode: 'ReadWrite'
    }
  }
}

// Container App
resource containerApp 'Microsoft.App/containerApps@2023-05-01' = {
  name: containerAppName
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${managedIdentity.id}': {}
    }
  }
  properties: {
    managedEnvironmentId: containerAppEnvironment.id
    workloadProfileName: workloadProfileName
    configuration: {
      secrets: stateStoreConnectionString != '' ? [
        {
          name: 'statestore-connection'
          value: stateStoreConnectionString
        }
      ] : []
      registries: [
        {
          server: acr.properties.loginServer
          identity: managedIdentity.id
        }
      ]
      ingress: {
        external: !enableApplicationGateway
        targetPort: 8080
        allowInsecure: true
        traffic: [
          {
            weight: 100
            latestRevision: true
          }
        ]
      }
    }
    template: {
      containers: [
        {
          name: containerAppName
          image: stateStoreAppID == '' ? 'nginx:latest' : '${acr.properties.loginServer}/${containerAppName}:${imageTag}'
          resources: {
            cpu: vCores
            memory: '${memoryGB}Gi'
          }
          volumeMounts: [
            {
              volumeName: 'migration-data-volume'
              mountPath: '/app/migration-data'
            }
          ]
          env: concat([
            {
              name: 'ASPNETCORE_ENVIRONMENT'
              value: aspNetCoreEnvironment
            }
            {
              name: 'ASPNETCORE_HTTP_PORTS'
              value: '8080'
            }
            {
              name: 'StateStoreAppID'
              value: stateStoreAppID
            }
            {
              name: 'ResourceDrive'
              value: '/app/migration-data'
            }
          ], stateStoreConnectionString != '' ? [
            {
              name: 'StateStoreConnectionStringOrPath'
              secretRef: 'statestore-connection'
            }
          ] : [])
        }
      ]
      volumes: [
        {
          name: 'migration-data-volume'
          storageType: 'AzureFile'
          storageName: 'migration-storage'
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
  dependsOn: [
    storageConfiguration
  ]
}

// Outputs
@description('Container Apps Environment ID')
output containerAppEnvironmentId string = containerAppEnvironment.id

@description('Container App FQDN')
output containerAppFQDN string = stateStoreAppID != '' ? containerApp.properties.configuration.ingress.fqdn : 'not-ready'

@description('Container App URL')
output containerAppUrl string = stateStoreAppID == '' ? 'not-ready' : (enableApplicationGateway ? 'https://${appGwPublicIp.?properties.?dnsSettings.?fqdn ?? ''}' : 'https://${containerApp.properties.configuration.ingress.fqdn}')

@description('Managed Identity Resource ID')
output managedIdentityId string = managedIdentity.id

@description('Managed Identity Client ID')
output managedIdentityClientId string = managedIdentity.properties.clientId

@description('Application Gateway Public IP (when enabled)')
output applicationGatewayPublicIp string = appGwPublicIp.?properties.?ipAddress ?? ''

@description('Application Gateway FQDN (when enabled)')
output applicationGatewayFQDN string = appGwPublicIp.?properties.?dnsSettings.?fqdn ?? ''

@description('Application Gateway Backend Pool ID (when enabled)')
output applicationGatewayBackendPoolId string = applicationGateway.?properties.?backendAddressPools[?0].?id ?? ''

@description('Storage Account Name for migration data')
output storageAccountName string = storageAccount.name

@description('File Share Name for migration data')
output fileShareName string = 'migration-data'

@description('Resource Drive Mount Path in container')
output resourceDrivePath string = '/app/migration-data'
