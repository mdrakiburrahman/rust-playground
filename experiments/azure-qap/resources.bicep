targetScope = 'resourceGroup'

param runId string
param storageAccountName string
param clusterName string
param identityName string
param generation string
param sandboxId string
param location string = resourceGroup().location

var suffix = replace(runId, '-', '')
var databaseName = 'qapexp-${suffix}'
var dataName = 'qapdata-${suffix}'
var checkpointsName = 'qapcontrol-${suffix}'

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storageAccountName
}
resource service 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' existing = {
  parent: storage
  name: 'default'
}
resource data 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: service
  name: dataName
  properties: {
    publicAccess: 'None'
    metadata: {
      arrowcolexperiment: runId
      sandboxid: sandboxId
      project: 'rust-playground'
    }
  }
}
resource checkpoints 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: service
  name: checkpointsName
  properties: {
    publicAccess: 'None'
    metadata: {
      arrowcolexperiment: runId
      sandboxid: sandboxId
      project: 'rust-playground'
      resetstate: 'ready-v1'
      resetgeneration: generation
      occupancy: 'released'
      publisherregistryversion: '1'
      publishers: '{}'
    }
  }
}
resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: identityName
}
resource reader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(data.id, identity.id, 'qap-experiment-reader')
  scope: data
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}
resource cluster 'Microsoft.Kusto/clusters@2024-04-13' existing = {
  name: clusterName
}
resource database 'Microsoft.Kusto/clusters/databases@2024-04-13' = {
  parent: cluster
  name: databaseName
  location: location
  kind: 'ReadWrite'
  properties: {
    softDeletePeriod: 'P1D'
    hotCachePeriod: 'P1D'
  }
}
resource databaseReader 'Microsoft.Kusto/clusters/databases/principalAssignments@2024-04-13' = {
  parent: database
  name: 'experiment-query-identity'
  properties: {
    principalId: identity.properties.clientId
    principalType: 'App'
    role: 'Viewer'
    tenantId: tenant().tenantId
  }
}

output experimentRunId string = runId
output databaseId string = database.id
output dataId string = data.id
output checkpointsId string = checkpoints.id
output readerRoleId string = reader.id
