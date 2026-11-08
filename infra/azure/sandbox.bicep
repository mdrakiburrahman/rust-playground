param location string
param identityLocation string = location
@allowed(['stamped', 'schema-evolution'])
param azureMode string = 'stamped'
param ownership object
param identityTags object
param storageAccountName string
param clusterName string
param identityName string
param registryName string
param aksConfig object
param publisherFederatedCredentials array
param databaseName string
param dataContainer string
param checkpointContainer string
param experimentContainer string
param experimentDatabase string
param logAnalyticsWorkspaceName string
param readLogStorageAccountName string
param controlMetadata object
param checkpointExists bool
param operatorObjectId string
param operatorKustoPrincipalId string
param operatorType string
param clusterSku string
param minimumInstances int
param maximumInstances int
param clusterTags object

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: identityName
  location: identityLocation
  tags: identityTags
}

module aks './aks.bicep' = if (azureMode == 'stamped') {
  name: 'arrowcol-aks-${ownership.sandboxId}'
  params: {
    location: location
    ownership: ownership
    clusterName: clusterName
    registryName: registryName
    sharedIdentityName: identityName
    operatorObjectId: operatorObjectId
    operatorType: operatorType
    aksConfig: aksConfig
    publisherFederatedCredentials: publisherFederatedCredentials
  }
  dependsOn: [identity]
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: ownership
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    isHnsEnabled: true
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

// Disposable data must really disappear on reset, not remain as retained copies.
resource blobs 'Microsoft.Storage/storageAccounts/blobServices@2025-08-01' = {
  parent: storage
  name: 'default'
  properties: {
    // $web is anonymously readable at the static website endpoint even when blob public access is disabled.
    // Only the sanitized benchmark summary is published there.
    staticWebsite: {
      enabled: azureMode == 'stamped'
      indexDocument: 'index.html'
      errorDocument404Path: '404.html'
    }
    isVersioningEnabled: false
    deleteRetentionPolicy: {
      enabled: false
    }
    containerDeleteRetentionPolicy: {
      enabled: false
    }
    changeFeed: {
      enabled: false
    }
    restorePolicy: {
      enabled: false
    }
  }
}

resource website 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = if (azureMode == 'stamped') {
  parent: blobs
  name: '$web'
  properties: {
    publicAccess: 'None'
    metadata: {
      sandboxid: ownership.sandboxId
      project: 'rust-playground'
    }
  }
}

resource data 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobs
  name: dataContainer
  properties: {
    publicAccess: 'None'
    metadata: {
      sandboxid: ownership.sandboxId
      project: 'rust-playground'
    }
  }
}

resource checkpoints 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = if (!checkpointExists) {
  parent: blobs
  name: checkpointContainer
  properties: {
    publicAccess: 'None'
    metadata: union(controlMetadata, {
      sandboxid: ownership.sandboxId
      project: 'rust-playground'
    })
  }
}

resource checkpointContainerScope 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' existing = {
  parent: blobs
  name: checkpointContainer
}

// Experiment fixtures live under runs/<runId>/<operation>/ and are wiped by reset.
resource experiment 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobs
  name: experimentContainer
  properties: {
    publicAccess: 'None'
    metadata: {
      sandboxid: ownership.sandboxId
      project: 'rust-playground'
    }
  }
}

resource readData 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(data.id, identity.id, 'blob-reader')
  scope: data
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource readExperiment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(experiment.id, identity.id, 'blob-reader')
  scope: experiment
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource runtimeDataWriter 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (azureMode == 'stamped') {
  name: guid(data.id, identity.id, 'runtime-data-contributor')
  scope: data
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource runtimeExperimentWriter 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (azureMode == 'stamped') {
  name: guid(experiment.id, identity.id, 'runtime-blob-contributor')
  scope: experiment
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource runtimeWebsiteWriter 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (azureMode == 'stamped') {
  name: guid(website.id, identity.id, 'runtime-static-site-contributor')
  scope: website
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource runtimeControlWriter 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (azureMode == 'stamped') {
  name: guid(checkpointContainerScope.id, identity.id, 'runtime-control-contributor')
  scope: checkpointContainerScope
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
  dependsOn: [checkpoints]
}

resource writeData 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, operatorObjectId, 'blob-contributor')
  scope: storage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: operatorObjectId
    principalType: operatorType == 'User' ? 'User' : 'ServicePrincipal'
  }
}

resource cluster 'Microsoft.Kusto/clusters@2024-04-13' = {
  name: clusterName
  location: location
  tags: union(clusterTags, ownership, azureMode == 'schema-evolution' ? { 'NRMS.KustoPlatform.Classification.1P': 'Corp' } : {})
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  sku: {
    name: clusterSku
    tier: 'Standard'
    capacity: minimumInstances
  }
  properties: {
    engineType: 'V3'
    enableAutoStop: true
    enableDiskEncryption: true
    enableStreamingIngest: false
    publicNetworkAccess: 'Enabled'
    optimizedAutoscale: {
      isEnabled: true
      version: 1
      minimum: minimumInstances
      maximum: maximumInstances
    }
  }
}

resource operator 'Microsoft.Kusto/clusters/principalAssignments@2024-04-13' = {
  parent: cluster
  name: 'sandbox-operator'
  properties: {
    principalId: operatorKustoPrincipalId
    principalType: operatorType
    role: 'AllDatabasesAdmin'
    tenantId: tenant().tenantId
  }
}

resource database 'Microsoft.Kusto/clusters/databases@2024-04-13' = {
  parent: cluster
  name: databaseName
  location: location
  kind: 'ReadWrite'
  properties: {
    softDeletePeriod: 'P7D'
    hotCachePeriod: 'P1D'
  }
}

resource reader 'Microsoft.Kusto/clusters/databases/principalAssignments@2024-04-13' = {
  parent: database
  name: 'sandbox-query-identity'
  properties: {
    principalId: identity.properties.clientId
    principalType: 'App'
    role: 'Viewer'
    tenantId: tenant().tenantId
  }
}

// Kusto rejects concurrent control operations on one cluster, so experiment objects follow the main database.
resource experimentDb 'Microsoft.Kusto/clusters/databases@2024-04-13' = {
  parent: cluster
  name: experimentDatabase
  location: location
  kind: 'ReadWrite'
  properties: {
    softDeletePeriod: 'P7D'
    hotCachePeriod: 'P1D'
  }
  dependsOn: [
    reader
  ]
}

resource experimentReader 'Microsoft.Kusto/clusters/databases/principalAssignments@2024-04-13' = {
  parent: experimentDb
  name: 'sandbox-query-identity'
  properties: {
    principalId: identity.properties.clientId
    principalType: 'App'
    role: 'Viewer'
    tenantId: tenant().tenantId
  }
}

resource runtimeExperimentAdmin 'Microsoft.Kusto/clusters/databases/principalAssignments@2024-04-13' = if (azureMode == 'stamped') {
  parent: experimentDb
  name: 'qap-runtime-identity'
  properties: {
    principalId: identity.properties.clientId
    principalType: 'App'
    role: 'Admin'
    tenantId: tenant().tenantId
  }
  dependsOn: [experimentReader]
}

resource runtimeClusterReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (azureMode == 'stamped') {
  name: guid(cluster.id, identity.id, 'runtime-cluster-reader')
  scope: cluster
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'acdd72a7-3385-48ef-bd42-f606fba81ae7')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource workspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: logAnalyticsWorkspaceName
  location: location
  tags: ownership
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
  }
}

module readLogStorage './read-log-storage.bicep' = if (!empty(readLogStorageAccountName)) {
  name: 'readLogStorage'
  params: {
    readLogStorageAccountName: readLogStorageAccountName
    location: location
    ownership: ownership
    operatorObjectId: operatorObjectId
    operatorType: operatorType
    runtimeIdentityId: identity.id
    runtimePrincipalId: identity.properties.principalId
  }
}

resource storageReads 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'arrowcol-storage-reads'
  scope: blobs
  properties: {
    workspaceId: workspace.id
    storageAccountId: empty(readLogStorageAccountName) ? null : readLogStorage!.outputs.storageAccountId
    logAnalyticsDestinationType: 'Dedicated'
    logs: [
      {
        category: 'StorageRead'
        enabled: true
      }
    ]
  }
}

resource readLogs 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(workspace.id, operatorObjectId, 'log-analytics-reader')
  scope: workspace
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '73c42c96-874c-492b-b04d-ab87d138a893')
    principalId: operatorObjectId
    principalType: operatorType == 'User' ? 'User' : 'ServicePrincipal'
  }
}

resource runtimeReadLogs 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (azureMode == 'stamped') {
  name: guid(workspace.id, identity.id, 'runtime-log-analytics-reader')
  scope: workspace
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '73c42c96-874c-492b-b04d-ab87d138a893')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

output experimentContainer string = experiment.name
output experimentDatabase string = experimentDb.name
output logAnalyticsWorkspaceId string = workspace.id
output logAnalyticsCustomerId string = workspace.properties.customerId
output readLogStorageAccountName string = readLogStorageAccountName

output aksClusterName string = azureMode == 'stamped' ? aks!.outputs.clusterName : ''
output aksOidcIssuer string = azureMode == 'stamped' ? aks!.outputs.oidcIssuer : ''
output identityClientId string = identity.properties.clientId
output registryLoginServer string = azureMode == 'stamped' ? aks!.outputs.registryLoginServer : ''
