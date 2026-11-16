export const CAMPAIGN_MONITORING_NAMESPACE = 'perf69-monitoring';
export const CAMPAIGN_OTLP_HTTP_ENDPOINT = `http://perf69-lgtm.${CAMPAIGN_MONITORING_NAMESPACE}.svc.cluster.local:4318`;
export const CAMPAIGN_LGTM_IMAGE = 'grafana/otel-lgtm@sha256:57c04cbd8e7022f32bb84c4ac005d79baf8b08e2c5be44facf11f5f1f9944ba2';

export interface KubernetesResource {
  apiVersion: string;
  kind: string;
  metadata: { name: string; namespace?: string; labels?: Record<string, string> };
  [key: string]: unknown;
}
const labels = { campaign: 'perf69', 'app.kubernetes.io/part-of': 'perf69-monitoring' };

/** Render only the isolated, cluster-internal campaign monitor; never applies it to a cluster. */
export function renderCampaignMonitoring(namespace = CAMPAIGN_MONITORING_NAMESPACE): KubernetesResource[] {
  if (namespace !== CAMPAIGN_MONITORING_NAMESPACE) throw new Error('Campaign LGTM namespace is fixed and must not overlap a shared namespace.');
  return [
    {
      apiVersion: 'v1', kind: 'Namespace', metadata: { name: namespace, labels },
    },
    {
      apiVersion: 'apps/v1', kind: 'Deployment',
      metadata: { name: 'perf69-lgtm', namespace, labels },
      spec: {
        replicas: 1,
        selector: { matchLabels: { app: 'perf69-lgtm' } },
        strategy: { type: 'Recreate' },
        template: {
          metadata: { labels: { ...labels, app: 'perf69-lgtm' } },
          spec: {
            automountServiceAccountToken: false,
            containers: [{
              name: 'lgtm', image: CAMPAIGN_LGTM_IMAGE, imagePullPolicy: 'IfNotPresent',
              ports: [
                { name: 'otlp-grpc', containerPort: 4317, protocol: 'TCP' },
                { name: 'otlp-http', containerPort: 4318, protocol: 'TCP' },
                { name: 'grafana', containerPort: 3000, protocol: 'TCP' },
              ],
              resources: { requests: { cpu: '250m', memory: '512Mi' }, limits: { cpu: '1', memory: '2Gi' } },
              startupProbe: { tcpSocket: { port: 'grafana' }, periodSeconds: 10, failureThreshold: 30 },
              readinessProbe: { tcpSocket: { port: 'grafana' }, periodSeconds: 5, failureThreshold: 3 },
              livenessProbe: { tcpSocket: { port: 'grafana' }, periodSeconds: 10, failureThreshold: 3 },
            }],
          },
        },
      },
    },
    {
      apiVersion: 'v1', kind: 'Service', metadata: { name: 'perf69-lgtm', namespace, labels },
      spec: {
        type: 'ClusterIP', selector: { app: 'perf69-lgtm' },
        ports: [
          { name: 'otlp-grpc', port: 4317, targetPort: 'otlp-grpc', protocol: 'TCP' },
          { name: 'otlp-http', port: 4318, targetPort: 'otlp-http', protocol: 'TCP' },
          { name: 'grafana', port: 3000, targetPort: 'grafana', protocol: 'TCP' },
        ],
      },
    },
  ];
}

import { pathToFileURL } from 'node:url';
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  console.log(renderCampaignMonitoring().map(resource => JSON.stringify(resource)).join('\n---\n'));
}
