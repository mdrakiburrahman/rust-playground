import assert from 'node:assert/strict';
import { test } from 'node:test';
import { CAMPAIGN_LGTM_IMAGE, CAMPAIGN_MONITORING_NAMESPACE, CAMPAIGN_OTLP_HTTP_ENDPOINT,
  renderCampaignMonitoring } from './aks-monitoring.js';

test('campaign LGTM stays in a dedicated namespace with pinned image and cluster-internal OTLP service', () => {
  const resources = renderCampaignMonitoring();
  assert.deepEqual(resources.map(resource => resource.kind), ['Namespace', 'Deployment', 'Service']);
  assert.equal(resources[0]!.metadata.name, CAMPAIGN_MONITORING_NAMESPACE);
  assert.equal(resources[1]!.metadata.namespace, CAMPAIGN_MONITORING_NAMESPACE);
  assert.equal(resources[2]!.metadata.namespace, CAMPAIGN_MONITORING_NAMESPACE);
  const deployment = resources[1]!.spec as { template: { spec: { containers: Array<{ image: string }> } } };
  assert.equal(deployment.template.spec.containers[0]!.image, CAMPAIGN_LGTM_IMAGE);
  assert.match(CAMPAIGN_LGTM_IMAGE, /@sha256:[a-f0-9]{64}$/);
  assert.equal(CAMPAIGN_OTLP_HTTP_ENDPOINT, 'http://perf69-lgtm.perf69-monitoring.svc.cluster.local:4318');
  assert.equal(resources[2]!.spec && (resources[2]!.spec as { type: string }).type, 'ClusterIP');
});

test('campaign monitor refuses alternate and shared namespace names', () => {
  for (const namespace of ['default', 'azure-qap', 'host10', 'perf69']) {
    assert.throws(() => renderCampaignMonitoring(namespace), /fixed/);
  }
});
