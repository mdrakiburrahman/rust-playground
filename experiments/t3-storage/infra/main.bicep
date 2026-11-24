targetScope = 'subscription'

param resourceGroupName string
param location string
param stagingLocation string
param prefix string
param sshPublicKey string
param uploaderObjectId string
param uploaderIp string
param resourceTags object = {}
param outboundIpTags array = []
param vmSize string = 'Standard_D8ads_v6'
param imageVersion string
param shareQuotaGiB int = 8192

resource group 'Microsoft.Resources/resourceGroups@2024-03-01' = {
  name: resourceGroupName
  location: location
  tags: union(resourceTags, { purpose: 'storage-spike' })
}

module lab './lab.bicep' = {
  name: 'storage-spike'
  scope: group
  params: {
    location: location
    stagingLocation: stagingLocation
    prefix: prefix
    sshPublicKey: sshPublicKey
    uploaderObjectId: uploaderObjectId
    uploaderIp: uploaderIp
    tags: union(resourceTags, { purpose: 'storage-spike' })
    outboundIpTags: outboundIpTags
    vmSize: vmSize
    imageVersion: imageVersion
    shareQuotaGiB: shareQuotaGiB
  }
}

output inventory object = lab.outputs.inventory
