function object(value: unknown): Record<string, any> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('ARM pool configuration is UNKNOWN.');
  return value as Record<string, any>;
}
const poolWritable = new Set(['count', 'vmSize', 'osDiskSizeGB', 'osDiskType', 'kubeletDiskType', 'vnetSubnetID',
  'podSubnetID', 'maxPods', 'osType', 'osSKU', 'type', 'mode', 'availabilityZones', 'enableAutoScaling', 'minCount',
  'maxCount', 'enableNodePublicIP', 'nodePublicIPPrefixID', 'nodeLabels', 'nodeTaints', 'orchestratorVersion',
  'upgradeSettings', 'enableEncryptionAtHost', 'enableUltraSSD', 'enableFIPS', 'kubeletConfig', 'linuxOSConfig',
  'scaleDownMode', 'powerState', 'creationData', 'securityProfile', 'networkProfile', 'gpuInstanceProfile',
  'capacityReservationGroupID', 'tags']);
const poolReadonly = new Set(['provisioningState', 'currentOrchestratorVersion', 'nodeImageVersion', 'status', 'eTag']);


export function buildAksPoolScalePayload(resource: unknown, target: { count: number; autoscale: boolean; minimum: number | null; maximum: number | null }): Record<string, unknown> {
  const selected = object(resource), properties = object(selected.properties);
  for (const key of Object.keys(properties)) if (!poolWritable.has(key) && !poolReadonly.has(key))
    throw new Error('Unrecognized pool configuration cannot be safely preserved during scale: ' + key);
  const preserved: Record<string, unknown> = Object.fromEntries(Object.entries(properties).filter(([key]) => poolWritable.has(key)));
  if (typeof preserved.vmSize !== 'string' || !['System', 'User'].includes(String(preserved.mode)))
    throw new Error('Pool identity/configuration is incomplete for safe scale.');
  const name = typeof selected.id === 'string' ? selected.id.split('/').at(-1) : undefined;
  if (target.autoscale !== (preserved.mode === 'User') ||
      (target.autoscale ? !['benchd64', 'benche64'].includes(String(name)) : name !== 'systempool'))
    throw new Error('Scale pool mode differs from the exact selected target.');
  if (!target.autoscale) { delete preserved.minCount; delete preserved.maxCount; }
  return { properties: { ...preserved, count: target.count, enableAutoScaling: target.autoscale,
    ...(target.autoscale ? { minCount: target.minimum, maxCount: target.maximum } : {}) } };
}
