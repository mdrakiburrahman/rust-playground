targetScope = 'subscription'

param location string
param resourceGroupName string = 'rust-playground'
param sandboxId string
param storageAccountName string
param clusterName string
param identityName string
param databaseName string = 'arrowcol'
param dataContainer string = 'arrowcol'
param checkpointContainer string = 'checkpoints'
param controlMetadata object
param checkpointExists bool = false
param operatorObjectId string
param operatorKustoPrincipalId string
@allowed(['User', 'App'])
param operatorType string = 'User'
param clusterSku string = 'Standard_E4ads_v5'
@minValue(2)
param minimumInstances int = 2
@minValue(2)
@maxValue(8)
param maximumInstances int = 4

var ownership = {
  project: 'rust-playground'
  purpose: 'disposable-arrowcol-sandbox'
  sandboxId: sandboxId
}

resource group 'Microsoft.Resources/resourceGroups@2024-03-01' = {
  name: resourceGroupName
  location: location
  tags: ownership
}

module sandbox './sandbox.bicep' = {
  name: 'arrowcol-sandbox'
  scope: group
  params: {
    location: location
    ownership: ownership
    storageAccountName: storageAccountName
    clusterName: clusterName
    identityName: identityName
    databaseName: databaseName
    dataContainer: dataContainer
    checkpointContainer: checkpointContainer
    controlMetadata: controlMetadata
    checkpointExists: checkpointExists
    operatorObjectId: operatorObjectId
    operatorKustoPrincipalId: operatorKustoPrincipalId
    operatorType: operatorType
    clusterSku: clusterSku
    minimumInstances: minimumInstances
    maximumInstances: maximumInstances
  }
}
