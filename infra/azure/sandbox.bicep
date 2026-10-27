param location string
param ownership object
param storageAccountName string
param clusterName string
param identityName string
param databaseName string
param dataContainer string
param checkpointContainer string
param controlMetadata object
param checkpointExists bool
param operatorObjectId string
param operatorType string
param clusterSku string
param minimumInstances int
param maximumInstances int

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

resource readData 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(data.id, identity.id, 'blob-reader')
  scope: data
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
  tags: ownership
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
    principalId: operatorObjectId
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
