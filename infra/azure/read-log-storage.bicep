param readLogStorageAccountName string
param location string
param ownership object
param operatorObjectId string
param operatorType string
param runtimeIdentityId string
param runtimePrincipalId string

resource readLogStorage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: readLogStorageAccountName
  location: location
  tags: ownership
  kind: 'StorageV2'
  sku: { name: 'Standard_LRS' }
  properties: {
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    publicNetworkAccess: 'Enabled'
    networkAcls: {
      bypass: 'AzureServices'
      defaultAction: 'Allow'
    }
  }
}

resource operatorReadExport 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(readLogStorage.id, operatorObjectId, 'read-log-reader')
  scope: readLogStorage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: operatorObjectId
    principalType: operatorType == 'User' ? 'User' : 'ServicePrincipal'
  }
}

resource runtimeReadExport 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(readLogStorage.id, runtimeIdentityId, 'runtime-read-log-reader')
  scope: readLogStorage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: runtimePrincipalId
    principalType: 'ServicePrincipal'
  }
}

resource runtimeReadExportReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(readLogStorage.id, runtimeIdentityId, 'runtime-read-log-arm-reader')
  scope: readLogStorage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'acdd72a7-3385-48ef-bd42-f606fba81ae7')
    principalId: runtimePrincipalId
    principalType: 'ServicePrincipal'
  }
}

output storageAccountId string = readLogStorage.id
