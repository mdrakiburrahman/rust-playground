param location string
param ownership object
param storageAccountName string
param clusterName string
param identityName string
param databaseName string
param dataContainer string
param checkpointContainer string
param experimentContainer string
param experimentDatabase string
param logAnalyticsWorkspaceName string
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
  location: location
  tags: ownership
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
  }
}

// Disposable data must really disappear on reset, not remain as retained copies.
resource blobs 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
  properties: {
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
  tags: union(clusterTags, ownership)
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

// Blob reads only; the experiment filters StorageBlobLogs by run path and the Kusto identity.
resource storageReads 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'arrowcol-storage-reads'
  scope: blobs
  properties: {
    workspaceId: workspace.id
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

output experimentContainer string = experiment.name
output experimentDatabase string = experimentDb.name
output logAnalyticsWorkspaceId string = workspace.id
output logAnalyticsCustomerId string = workspace.properties.customerId
