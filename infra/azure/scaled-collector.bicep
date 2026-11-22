param clusterName string
param nodeSubnetId string
param poolName string
@allowed(['Standard_E64ds_v6', 'Standard_E192ids_v6'])
param vmSize string
param runId string
@secure()
param ownerToken string

resource cluster 'Microsoft.ContainerService/managedClusters@2026-05-01' existing = {
  name: clusterName
}

resource pool 'Microsoft.ContainerService/managedClusters/agentPools@2026-05-01' = {
  parent: cluster
  name: poolName
  properties: {
    tags: {
      'arrowcol-oat-run-id': runId
      'arrowcol-oat-owner-token': ownerToken
    }
    osDiskSizeGB: 100
    enableAutoScaling: true
    count: 0
    minCount: 0
    maxCount: 1
    maxPods: 30
    vmSize: vmSize
    osType: 'Linux'
    mode: 'User'
    vnetSubnetID: nodeSubnetId
    enableFIPS: true
    osSKU: 'AzureLinux'
    nodeTaints: [
      'arrow-collector/role=arrowcol:NoSchedule'
      'arrowcol-oat/run-id=${runId}:NoSchedule'
    ]
    nodeLabels: {
      type: 'benchmark'
      'arrow-collector/role': 'arrowcol'
      'arrowcol-oat/run-id': runId
    }
    securityProfile: { sshAccess: 'Disabled' }
  }
}
