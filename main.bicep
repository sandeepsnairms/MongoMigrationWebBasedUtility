param location string
param webAppName string


var appServicePlanName = '${webAppName}SP'

resource appServicePlan 'Microsoft.Web/serverfarms@2022-03-01' = {
  name: appServicePlanName
  location: location
  sku: {
    name: 'B1'   // Use P1V3 for large workloads
    tier: 'Basic' // Use PremiumV2 for large workloads
  }
}

resource webApp 'Microsoft.Web/sites@2022-03-01' = {
  name: webAppName
  location: location
  properties: {
    serverFarmId: appServicePlan.id    
}

