param location string
param stagingLocation string
@minLength(2)
@maxLength(12)
param prefix string
param sshPublicKey string
param uploaderObjectId string
param uploaderIp string
param tags object
param outboundIpTags array
param vmSize string
param imageVersion string
@minValue(100)
@maxValue(102400)
param shareQuotaGiB int
param adminUsername string = 'spike'

var suffix = uniqueString(resourceGroup().id)
var accountKinds = ['FileStorage', 'BlockBlobStorage', 'StorageV2', 'StorageV2']
var accountSkus = ['Premium_ZRS', 'Premium_ZRS', 'Standard_ZRS', 'Standard_LRS']
var accountRoles = ['file', 'blob', 'blob', 'blob']
var blobContributor = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
var rolePairs = [for i in range(0, 6): { vm: i / 3, account: i % 3 + 1 }]
var dnsNames = ['privatelink.file.${environment().suffixes.storage}', 'privatelink.blob.${environment().suffixes.storage}']

resource egressIp 'Microsoft.Network/publicIPAddresses@2024-05-01' = {
  name: '${prefix}-egress'
  location: location
  tags: tags
  sku: { name: 'Standard' }
  properties: { publicIPAllocationMethod: 'Static', ipTags: outboundIpTags }
}

resource nat 'Microsoft.Network/natGateways@2024-05-01' = {
  name: '${prefix}-nat'
  location: location
  tags: tags
  sku: { name: 'Standard' }
  properties: { publicIpAddresses: [{ id: egressIp.id }], idleTimeoutInMinutes: 10 }
}

resource nsg 'Microsoft.Network/networkSecurityGroups@2024-05-01' = {
  name: '${prefix}-nsg'
  location: location
  tags: tags
  properties: {
    securityRules: [
      {
        name: 'allow-lab'
        properties: { priority: 100, direction: 'Inbound', access: 'Allow', protocol: '*', sourcePortRange: '*', destinationPortRange: '*', sourceAddressPrefix: 'VirtualNetwork', destinationAddressPrefix: 'VirtualNetwork' }
      }
      {
        name: 'deny-other-inbound'
        properties: { priority: 4096, direction: 'Inbound', access: 'Deny', protocol: '*', sourcePortRange: '*', destinationPortRange: '*', sourceAddressPrefix: '*', destinationAddressPrefix: '*' }
      }
    ]
  }
}

resource network 'Microsoft.Network/virtualNetworks@2024-05-01' = {
  name: '${prefix}-vnet'
  location: location
  tags: tags
  properties: {
    addressSpace: { addressPrefixes: ['10.88.0.0/16'] }
    subnets: [
      {
        name: 'lab'
        properties: {
          addressPrefix: '10.88.0.0/24'
          defaultOutboundAccess: false
          natGateway: { id: nat.id }
          networkSecurityGroup: { id: nsg.id }
          privateEndpointNetworkPolicies: 'Disabled'
          serviceEndpoints: [{ service: 'Microsoft.Storage' }]
        }
      }
    ]
  }
}

resource accounts 'Microsoft.Storage/storageAccounts@2025-01-01' = [for i in range(0, 4): {
  name: i == 3 && stagingLocation != location ? '${prefix}${i}${take(uniqueString(resourceGroup().id, stagingLocation), 8)}' : '${prefix}${i}${take(suffix, 8)}'
  location: i == 3 ? stagingLocation : location
  tags: tags
  kind: accountKinds[i]
  sku: { name: accountSkus[i] }
  properties: {
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    publicNetworkAccess: i == 3 ? 'Enabled' : 'Disabled'
    networkAcls: {
      bypass: 'None'
      defaultAction: 'Deny'
      ipRules: i == 3 ? [{ value: uploaderIp, action: 'Allow' }] : []
      virtualNetworkRules: i == 3 ? [{ id: '${network.id}/subnets/lab', action: 'Allow' }] : []
    }
  }
}]

resource fileService 'Microsoft.Storage/storageAccounts/fileServices@2025-01-01' = {
  parent: accounts[0]
  name: 'default'
}

resource share 'Microsoft.Storage/storageAccounts/fileServices/shares@2025-01-01' = {
  parent: fileService
  name: 'wal'
  properties: { enabledProtocols: 'NFS', shareQuota: shareQuotaGiB, rootSquash: 'RootSquash' }
}

resource blobServices 'Microsoft.Storage/storageAccounts/blobServices@2025-01-01' = [for i in range(1, 3): {
  parent: accounts[i]
  name: 'default'
}]

resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' = [for i in range(0, 3): {
  parent: blobServices[i]
  name: 'spike'
  properties: { publicAccess: 'None' }
}]

resource dns 'Microsoft.Network/privateDnsZones@2024-06-01' = [for name in dnsNames: {
  name: name
  location: 'global'
  tags: tags
}]

resource links 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = [for i in range(0, 2): {
  parent: dns[i]
  name: '${prefix}-link'
  location: 'global'
  properties: { registrationEnabled: false, virtualNetwork: { id: network.id } }
}]

resource endpoints 'Microsoft.Network/privateEndpoints@2024-05-01' = [for i in range(0, 3): {
  name: '${prefix}-pe-${i}'
  location: location
  tags: tags
  properties: {
    subnet: { id: '${network.id}/subnets/lab' }
    privateLinkServiceConnections: [{ name: 'storage', properties: { privateLinkServiceId: accounts[i].id, groupIds: [accountRoles[i]] } }]
  }
}]

resource zoneGroups 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-05-01' = [for i in range(0, 3): {
  parent: endpoints[i]
  name: 'default'
  properties: { privateDnsZoneConfigs: [{ name: 'storage', properties: { privateDnsZoneId: dns[i == 0 ? 0 : 1].id } }] }
}]

resource nics 'Microsoft.Network/networkInterfaces@2024-05-01' = [for i in range(0, 2): {
  name: '${prefix}-nic-${i}'
  location: location
  tags: tags
  properties: {
    enableAcceleratedNetworking: true
    ipConfigurations: [{ name: 'primary', properties: { privateIPAllocationMethod: 'Dynamic', subnet: { id: '${network.id}/subnets/lab' } } }]
  }
}]

resource vms 'Microsoft.Compute/virtualMachines@2024-11-01' = [for i in range(0, 2): {
  name: '${prefix}-vm-${i}'
  location: location
  zones: [string(i + 1)]
  tags: tags
  identity: { type: 'SystemAssigned' }
  properties: {
    hardwareProfile: { vmSize: vmSize }
    securityProfile: { securityType: 'TrustedLaunch', uefiSettings: { secureBootEnabled: true, vTpmEnabled: true } }
    osProfile: {
      computerName: 'spike${i}'
      adminUsername: adminUsername
      linuxConfiguration: { disablePasswordAuthentication: true, provisionVMAgent: true, ssh: { publicKeys: [{ path: '/home/${adminUsername}/.ssh/authorized_keys', keyData: sshPublicKey }] } }
    }
    storageProfile: {
      imageReference: { publisher: 'Canonical', offer: 'ubuntu-24_04-lts', sku: 'server', version: imageVersion }
      osDisk: { createOption: 'FromImage', diskSizeGB: 128, managedDisk: { storageAccountType: 'Premium_LRS' } }
      dataDisks: [{ lun: 0, name: '${prefix}-oracle-${i}', diskSizeGB: 128, createOption: 'Empty', caching: 'None', managedDisk: { storageAccountType: 'Premium_LRS' } }]
    }
    networkProfile: { networkInterfaces: [{ id: nics[i].id }] }
    diagnosticsProfile: { bootDiagnostics: { enabled: true } }
  }
}]

resource vmRoles 'Microsoft.Authorization/roleAssignments@2022-04-01' = [for pair in rolePairs: {
  name: guid(accounts[pair.account].id, vms[pair.vm].id, blobContributor)
  scope: accounts[pair.account]
  properties: { principalId: vms[pair.vm].identity.principalId, principalType: 'ServicePrincipal', roleDefinitionId: blobContributor }
}]

resource uploadRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(accounts[3].id, uploaderObjectId, blobContributor)
  scope: accounts[3]
  properties: { principalId: uploaderObjectId, principalType: 'User', roleDefinitionId: blobContributor }
}

var vmNames = [for i in range(0, 2): vms[i].name]
var storageAccounts = [for i in range(0, 4): accounts[i].name]
var storageEndpoints = [for i in range(0, 4): {
  blob: 'https://${accounts[i].name}.blob.${environment().suffixes.storage}/'
  file: 'https://${accounts[i].name}.file.${environment().suffixes.storage}/'
}]

output inventory object = {
  resourceGroup: resourceGroup().name
  vmNames: vmNames
  storageAccounts: storageAccounts
  storageEndpoints: storageEndpoints
  nfsSource: '${accounts[0].name}.file.${environment().suffixes.storage}:/${accounts[0].name}/${share.name}'
  shareQuotaGiB: shareQuotaGiB
  vmSize: vmSize
  imageVersion: imageVersion
}
