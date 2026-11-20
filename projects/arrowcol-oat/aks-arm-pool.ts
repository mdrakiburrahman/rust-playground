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

export function fleetPoolConfiguration(resource: unknown): Record<string, unknown> {
  const selected = object(resource), properties = object(selected.properties);
  for (const key of Object.keys(selected))
    if (!['id', 'name', 'type', 'location', 'etag', 'eTag', 'properties', 'tags', 'systemData'].includes(key))
      throw new Error('Unrecognized fleet pool resource configuration.');
  for (const key of Object.keys(properties))
    if (!poolWritable.has(key) && !poolReadonly.has(key)) throw new Error('Unrecognized fleet pool configuration.');
  return {
    ...(selected.location === undefined ? {} : { location: selected.location }),
    ...(selected.tags === undefined ? {} : { tags: selected.tags }),
    properties: Object.fromEntries(Object.entries(properties).filter(([key]) =>
      poolWritable.has(key) && !['count', 'minCount', 'maxCount'].includes(key))),
  };
}

export function buildFleetPoolScalePayload(resource: unknown, approved: { resourceId: string; sku: string; maxCount: number },
  count: number): Record<string, unknown> {
  const selected = object(resource), properties = object(selected.properties);
  fleetPoolConfiguration(selected);
  if (typeof selected.id !== 'string' || selected.id.toLowerCase() !== approved.resourceId.toLowerCase() ||
      properties.mode !== 'User' || properties.osType !== 'Linux' || properties.vmSize !== approved.sku ||
      properties.enableAutoScaling !== true || !Number.isSafeInteger(approved.maxCount) || approved.maxCount < 1 ||
      !Number.isSafeInteger(count) || count < 0 || count > approved.maxCount)
    throw new Error('Fleet scale requires an exact approved autoscaled Linux User pool.');
  return { ...(selected.tags === undefined ? {} : { tags: selected.tags }),
    properties: { ...Object.fromEntries(Object.entries(properties).filter(([key]) => poolWritable.has(key))),
      count, minCount: count, maxCount: count === 0 ? approved.maxCount : count, enableAutoScaling: true } };
}


export function buildAksPoolScalePayload(resource: unknown, target: { count: number; autoscale: boolean; minimum: number | null; maximum: number | null },
  scope?: 'direct-first-floor'): Record<string, unknown> {
  const selected = object(resource), properties = object(selected.properties);
  for (const key of Object.keys(properties)) if (!poolWritable.has(key) && !poolReadonly.has(key))
    throw new Error('Unrecognized pool configuration cannot be safely preserved during scale: ' + key);
  const preserved: Record<string, unknown> = Object.fromEntries(Object.entries(properties).filter(([key]) => poolWritable.has(key)));
  if (typeof preserved.vmSize !== 'string' || !['System', 'User'].includes(String(preserved.mode)))
    throw new Error('Pool identity/configuration is incomplete for safe scale.');
  const name = typeof selected.id === 'string' ? selected.id.split('/').at(-1) : undefined;
  if (target.autoscale !== (preserved.mode === 'User') ||
      (scope === 'direct-first-floor' ? name !== 'arrowe192' || !target.autoscale || target.count !== 0 ||
        target.minimum !== 1 || target.maximum !== 1 :
        target.autoscale ? !['benchd64', 'benche64'].includes(String(name)) : name !== 'systempool'))
    throw new Error('Scale pool mode differs from the exact selected target.');
  if (!target.autoscale) { delete preserved.minCount; delete preserved.maxCount; }
  return { ...(scope === 'direct-first-floor' && selected.tags !== undefined ? { tags: selected.tags } : {}),
    properties: { ...preserved, count: target.count, enableAutoScaling: target.autoscale,
    ...(target.autoscale ? { minCount: target.minimum, maxCount: target.maximum } : {}) } };
}
