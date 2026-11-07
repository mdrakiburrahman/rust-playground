targetScope = 'subscription'

param location string
param resourceGroupName string = 'arrow-collector-rg'
param sandboxId string
param storageAccountName string
param clusterName string
param identityName string
param registryName string
param aksConfig object
param publisherFederatedCredentials array
param databaseName string = 'arrowcol'
param dataContainer string = 'arrowcol'
param checkpointContainer string = 'checkpoints'
param experimentContainer string = 'experimentqap'
param experimentDatabase string = 'experimentqap'
param logAnalyticsWorkspaceName string = 'arrow-collector-1'
param readLogStorageAccountName string = ''
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
@description('Extra tags for the Kusto cluster only, e.g. tenant policy classification tags.')
param clusterTags object = {}

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
    registryName: registryName
    aksConfig: aksConfig
    publisherFederatedCredentials: publisherFederatedCredentials
    databaseName: databaseName
    dataContainer: dataContainer
    checkpointContainer: checkpointContainer
    experimentContainer: experimentContainer
    experimentDatabase: experimentDatabase
    logAnalyticsWorkspaceName: logAnalyticsWorkspaceName
    readLogStorageAccountName: readLogStorageAccountName
    controlMetadata: controlMetadata
    checkpointExists: checkpointExists
    operatorObjectId: operatorObjectId
    operatorKustoPrincipalId: operatorKustoPrincipalId
    operatorType: operatorType
    clusterSku: clusterSku
    minimumInstances: minimumInstances
    maximumInstances: maximumInstances
    clusterTags: clusterTags
  }
}

output experimentContainer string = sandbox.outputs.experimentContainer
output experimentDatabase string = sandbox.outputs.experimentDatabase
output logAnalyticsWorkspaceId string = sandbox.outputs.logAnalyticsWorkspaceId
output logAnalyticsCustomerId string = sandbox.outputs.logAnalyticsCustomerId
output readLogStorageAccountName string = sandbox.outputs.readLogStorageAccountName

output aksClusterName string = sandbox.outputs.aksClusterName
output aksOidcIssuer string = sandbox.outputs.aksOidcIssuer
output identityClientId string = sandbox.outputs.identityClientId
output registryLoginServer string = sandbox.outputs.registryLoginServer
