// Candidate only. Review the matching what-if; do not deploy before source/image readiness and the next scoped authorization.
using './sku-pools.bicep'

param clusterName = 'arrow-collector-68590'
param nodeSubnetId = '/subscriptions/68fa0545-661c-4152-93f2-fc8ae1c231b8/resourceGroups/arrow-collector-rg/providers/Microsoft.Network/virtualNetworks/arrow-collector-68590-vnet/subnets/systempool'
param userPools = [
  {
    name: 'benchd64'
    role: 'client'
    vmSize: 'Standard_D64ds_v6'
    initialCount: 0
    minCount: 0
    maxCount: 8
    maxPods: 250
    availabilityZones: []
  }
  {
    name: 'supportd64'
    role: 'support'
    vmSize: 'Standard_D64ds_v6'
    initialCount: 0
    minCount: 0
    maxCount: 1
    maxPods: 250
    availabilityZones: []
  }
  {
    name: 'arrowe192'
    role: 'arrowcol'
    vmSize: 'Standard_E192ids_v6'
    initialCount: 0
    minCount: 0
    maxCount: 1
    maxPods: 250
    availabilityZones: []
  }
]
