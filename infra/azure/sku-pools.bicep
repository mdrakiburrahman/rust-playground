param clusterName string
param nodeSubnetId string
param userPools array

resource cluster 'Microsoft.ContainerService/managedClusters@2026-05-01' existing = {
  name: clusterName
}

resource containerStorage 'Microsoft.KubernetesConfiguration/extensions@2025-03-01' = {
  scope: cluster
  name: 'acstor'
  properties: {
    autoUpgradeMinorVersion: true
    extensionType: 'microsoft.azurecontainerstoragev2'
    releaseTrain: 'Stable'
    scope: { cluster: { releaseNamespace: 'kube-system' } }
  }
}

@batchSize(1)
resource pools 'Microsoft.ContainerService/managedClusters/agentPools@2026-05-01' = [for pool in userPools: {
  parent: cluster
  name: pool.name
  properties: {
    osDiskSizeGB: 100
    enableAutoScaling: true
    count: pool.initialCount
    minCount: pool.minCount
    maxCount: pool.maxCount
    maxPods: pool.maxPods
    vmSize: pool.vmSize
    osType: 'Linux'
    mode: 'User'
    vnetSubnetID: nodeSubnetId
    enableFIPS: true
    osSKU: 'AzureLinux'
    availabilityZones: pool.availabilityZones
    nodeTaints: ['arrow-collector/role=${pool.role}:NoSchedule']
    nodeLabels: { type: 'benchmark', 'arrow-collector/role': pool.role, 'arrow-collector/sku': pool.vmSize }
    upgradeSettings: { maxSurge: '33%' }
    securityProfile: { sshAccess: 'Disabled' }
  }
}]
