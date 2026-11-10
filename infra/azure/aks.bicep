param location string
param ownership object
param clusterName string
param registryName string
param sharedIdentityName string
param operatorObjectId string
param operatorType string
param aksConfig object
param publisherFederatedCredentials array

resource sharedIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: sharedIdentityName
}

var network = aksConfig.network
var pool = aksConfig.systemPool
var networkContributorRole = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4d97b98b-1d4f-4787-a291-c67834d212e7')
var managedIdentityOperatorRole = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'f1a07417-d97a-45cb-824c-7a7467783830')
var federatedCredentialContributorRole = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7e559ce2-48d7-4b27-9128-fa1b247f1308')
var acrPullRole = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
var acrPushRole = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '8311e382-0749-4cb8-b61a-304f252e45ec')
var readerRole = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'acdd72a7-3385-48ef-bd42-f606fba81ae7')
var aksClusterAdminRole = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'b1ff04bb-8a4e-4dc4-8eb5-8693973ce19b')

resource nsg 'Microsoft.Network/networkSecurityGroups@2024-05-01' = {
  name: '${clusterName}-nsg'
  location: location
  tags: ownership
  properties: { securityRules: [] }
}

resource vnet 'Microsoft.Network/virtualNetworks@2024-05-01' = {
  name: '${clusterName}-vnet'
  location: location
  tags: ownership
  properties: {
    addressSpace: { addressPrefixes: [network.vnetIpv4Cidr, network.vnetIpv6Cidr] }
  }
}

resource nodeSubnet 'Microsoft.Network/virtualNetworks/subnets@2024-05-01' = {
  parent: vnet
  name: pool.name
  properties: {
    addressPrefixes: [network.nodeSubnetIpv4Cidr, network.nodeSubnetIpv6Cidr]
    networkSecurityGroup: { id: nsg.id }
    defaultOutboundAccess: false
  }
}

resource apiServerSubnet 'Microsoft.Network/virtualNetworks/subnets@2024-05-01' = {
  parent: vnet
  name: 'apiserver-subnet'
  // Subnet writes share the VNet lock; wait for the system pool subnet.
  dependsOn: [nodeSubnet]
  properties: {
    addressPrefixes: [network.apiServerSubnetIpv4Cidr]
    delegations: [{ name: 'aks-api-server', properties: { serviceName: 'Microsoft.ContainerService/managedClusters' } }]
    networkSecurityGroup: { id: nsg.id }
    defaultOutboundAccess: false
  }
}

resource nodeSubnetNetworkRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(nodeSubnet.id, sharedIdentity.id, 'network-contributor')
  scope: nodeSubnet
  properties: { roleDefinitionId: networkContributorRole, principalId: sharedIdentity.properties.principalId, principalType: 'ServicePrincipal' }
}

resource apiSubnetNetworkRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(apiServerSubnet.id, sharedIdentity.id, 'network-contributor')
  scope: apiServerSubnet
  properties: { roleDefinitionId: networkContributorRole, principalId: sharedIdentity.properties.principalId, principalType: 'ServicePrincipal' }
}

resource identityOperatorRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(sharedIdentity.id, sharedIdentity.id, 'managed-identity-operator')
  scope: sharedIdentity
  properties: { roleDefinitionId: managedIdentityOperatorRole, principalId: sharedIdentity.properties.principalId, principalType: 'ServicePrincipal' }
}

resource runFederationRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(sharedIdentity.id, sharedIdentity.id, 'run-federation-contributor')
  scope: sharedIdentity
  properties: { roleDefinitionId: federatedCredentialContributorRole, principalId: sharedIdentity.properties.principalId, principalType: 'ServicePrincipal' }
}

resource egressIpv4Prefix 'Microsoft.Network/publicIPPrefixes@2024-05-01' = {
  name: '${clusterName}-egress-ipv4-prefix'
  location: location
  tags: ownership
  sku: { name: 'Standard' }
  properties: { prefixLength: 30, publicIPAddressVersion: 'IPv4', ipTags: network.egressIpv4IpTags }
}

resource egressIpv6Prefix 'Microsoft.Network/publicIPPrefixes@2024-05-01' = {
  name: '${clusterName}-egress-ipv6-prefix'
  location: location
  tags: ownership
  sku: { name: 'Standard' }
  properties: { prefixLength: 126, publicIPAddressVersion: 'IPv6', ipTags: network.egressIpv6IpTags }
}

resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: registryName
  location: location
  tags: ownership
  sku: { name: 'Basic' }
  properties: { adminUserEnabled: false, publicNetworkAccess: 'Enabled' }
}

resource registryPullRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, sharedIdentity.id, 'acr-pull')
  scope: registry
  properties: { roleDefinitionId: acrPullRole, principalId: sharedIdentity.properties.principalId, principalType: 'ServicePrincipal' }
}

resource registryPushRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, sharedIdentity.id, 'acr-push')
  scope: registry
  properties: { roleDefinitionId: acrPushRole, principalId: sharedIdentity.properties.principalId, principalType: 'ServicePrincipal' }
}

resource registryReaderRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, sharedIdentity.id, 'registry-reader')
  scope: registry
  properties: { roleDefinitionId: readerRole, principalId: sharedIdentity.properties.principalId, principalType: 'ServicePrincipal' }
}

var userAgentPoolProfiles = [for userPool in aksConfig.userPools: {
  name: userPool.name
  osDiskSizeGB: 100
  enableAutoScaling: true
  count: userPool.initialCount
  minCount: userPool.minCount
  maxCount: userPool.maxCount
  maxPods: userPool.maxPods
  vmSize: userPool.vmSize
  osType: 'Linux'
  mode: 'User'
  vnetSubnetID: nodeSubnet.id
  enableFIPS: true
  osSKU: 'AzureLinux'
  availabilityZones: userPool.availabilityZones
  nodeTaints: ['arrow-collector/role=${userPool.role}:NoSchedule']
  nodeLabels: { type: 'benchmark', 'arrow-collector/role': userPool.role }
  upgradeSettings: { maxSurge: '33%' }
  securityProfile: { sshAccess: 'Disabled' }
}]

resource aks 'Microsoft.ContainerService/managedClusters@2026-05-01' = {
  name: clusterName
  location: location
  tags: ownership
  sku: { name: 'Base', tier: aksConfig.skuTier }
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: { '${sharedIdentity.id}': {} }
  }
  dependsOn: [nodeSubnetNetworkRole, apiSubnetNetworkRole, identityOperatorRole, registryPullRole]
  properties: {
    supportPlan: aksConfig.skuTier == 'Premium' ? 'AKSLongTermSupport' : 'KubernetesOfficial'
    kubernetesVersion: aksConfig.version
    dnsPrefix: '${clusterName}-dns'
    agentPoolProfiles: concat([{
      name: pool.name
      osDiskSizeGB: 100
      enableAutoScaling: true
      count: pool.initialCount
      minCount: pool.minCount
      maxCount: pool.maxCount
      maxPods: pool.maxPods
      vmSize: pool.vmSize
      osType: 'Linux'
      mode: 'System'
      vnetSubnetID: nodeSubnet.id
      enableFIPS: true
      osSKU: 'AzureLinux'
      availabilityZones: pool.availabilityZones
      nodeTaints: ['CriticalAddonsOnly=true:NoSchedule']
      nodeLabels: { type: 'general' }
      upgradeSettings: { maxSurge: '33%' }
      securityProfile: { sshAccess: 'Disabled' }
    }], userAgentPoolProfiles)
    aadProfile: { managed: true, enableAzureRBAC: true }
    oidcIssuerProfile: { enabled: true }
    securityProfile: { workloadIdentity: { enabled: true } }
    networkProfile: {
      networkPlugin: 'azure'
      networkPluginMode: 'overlay'
      networkDataplane: aksConfig.?networkDataplane ?? 'azure'
      networkPolicy: (aksConfig.?networkDataplane ?? 'azure') == 'cilium' ? 'cilium' : 'none'
      outboundType: 'loadBalancer'
      loadBalancerSku: 'Standard'
      loadBalancerProfile: { outboundIPPrefixes: { publicIPPrefixes: [{ id: egressIpv4Prefix.id }, { id: egressIpv6Prefix.id }] } }
      ipFamilies: ['IPv4', 'IPv6']
      podCidrs: [network.podIpv4Cidr, network.podIpv6Cidr]
      serviceCidrs: [network.serviceIpv4Cidr, network.serviceIpv6Cidr]
      dnsServiceIP: network.dnsServiceIp
    }
    apiServerAccessProfile: {
      enablePrivateCluster: false
      enablePrivateClusterPublicFQDN: false
      enableVnetIntegration: true
      subnetId: apiServerSubnet.id
    }
    autoUpgradeProfile: { nodeOSUpgradeChannel: 'NodeImage', upgradeChannel: 'patch' }
    disableLocalAccounts: false
    identityProfile: {
      kubeletidentity: {
        resourceId: sharedIdentity.id
        clientId: sharedIdentity.properties.clientId
        objectId: sharedIdentity.properties.principalId
      }
    }
  }
}

resource operatorClusterAdminRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(aks.id, operatorObjectId, 'aks-cluster-admin')
  scope: aks
  properties: {
    roleDefinitionId: aksClusterAdminRole
    principalId: operatorObjectId
    principalType: operatorType == 'User' ? 'User' : 'ServicePrincipal'
  }
}

resource qapFederatedCredential 'Microsoft.ManagedIdentity/userAssignedIdentities/federatedIdentityCredentials@2023-01-31' = {
  parent: sharedIdentity
  name: 'azure-qap-${clusterName}'
  dependsOn: [publisherFederatedCredentialsResource]
  properties: {
    audiences: ['api://AzureADTokenExchange']
    issuer: aks.properties.oidcIssuerProfile.issuerURL
    subject: 'system:serviceaccount:${aksConfig.namespace}:${aksConfig.serviceAccount}'
  }
}

@batchSize(1)
resource publisherFederatedCredentialsResource 'Microsoft.ManagedIdentity/userAssignedIdentities/federatedIdentityCredentials@2023-01-31' = [for fic in publisherFederatedCredentials: {
  parent: sharedIdentity
  name: fic.name
  properties: { audiences: ['api://AzureADTokenExchange'], issuer: fic.issuer, subject: fic.subject }
}]

output clusterName string = aks.name
output oidcIssuer string = aks.properties.oidcIssuerProfile.issuerURL
output registryLoginServer string = registry.properties.loginServer
output identityClientId string = sharedIdentity.properties.clientId
