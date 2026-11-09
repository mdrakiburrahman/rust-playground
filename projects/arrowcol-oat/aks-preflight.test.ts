import assert from 'node:assert/strict';
import { chmod, mkdtemp, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { formatPreflightReport, runAksPreflight } from './aks-preflight.js';

test('AKS preflight inspects without mutation and never reports credential contents', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'arrowcol-oat-preflight-'));
  const assertionPath = path.join(root, 'assertion.jwt');
  const runtimePath = path.join(root, 'runtime.json');
  const bootstrapPath = path.join(root, 'bootstrap.env');
  const configPath = path.join(root, 'oat.json');
  const subscription = '68fa0545-661c-4152-93f2-fc8ae1c231b8';
  const clientId = 'e23159c9-5be9-427c-9edf-dd08e026ec46';
  const principalId = '00000000-0000-0000-0000-000000000001';
  const context = 'arrow-collector-00000';
  const apiServer = 'https://arrow-collector-00000.example.test:443';
  const secretValue = 'preflight-secret-canary';
  const accessToken = 'preflight-token-canary';
  const calls: Array<{ command: string; args: string[] }> = [];

  try {
    await writeFile(assertionPath, 'private assertion');
    await chmod(assertionPath, 0o600);
    await writeFile(runtimePath, JSON.stringify({
      subscriptionId: subscription,
      workloadIdentityClientId: clientId,
      stamp: '00000',
      images: {
        sqlserver: 'arrowcollector00000.azurecr.io/sqlserver@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        catalog: 'arrowcollector00000.azurecr.io/catalog@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        router: 'arrowcollector00000.azurecr.io/router@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
        dataflow: 'arrowcollector00000.azurecr.io/dataflow@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
      },
    }));
    await writeFile(bootstrapPath, '');
    await writeFile(configPath, JSON.stringify({
      platform: {
        kind: 'aks',
        runtime_config_file: 'runtime.json',
        bootstrap_env_file: 'bootstrap.env',
      },
      stamp: '00000',
      qap_endpoint: 'https://arrow-collector-00000.canadacentral.kusto.windows.net',
      qap_database: 'approved-qap',
      dmv_client_image: 'arrowcollector00000.azurecr.io/dmv@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
      result_url: 'abfss://experiments@arrowcollector00000.dfs.core.windows.net/results.parquet',
      website_url: 'abfss://$web@arrowcollector00000.dfs.core.windows.net/results.parquet',
    }));

    const runCommand = (command: string, args: string[]) => {
      calls.push({ command, args: [...args] });
      const output = (value: unknown) => ({ stdout: typeof value === 'string' ? value : JSON.stringify(value), exitCode: 0 });
      if (command === 'cargo') return output('validated');
      if (command === 'kubelogin') return output('kubelogin version v0.2.22');
      if (command === 'getent') return output('10.0.0.1 arrow-collector-00000.canadacentral.kusto.windows.net');
      if (command === 'kubectl') {
        if (args[0] === 'config' && args[1] === 'current-context') return output(context);
        if (args[0] === 'config' && args[1] === 'view') {
          return output({
            clusters: [{ cluster: { server: apiServer } }],
            users: [{ user: { exec: { args: ['--disable-environment-override'] } } }],
          });
        }
        if (args.some((arg) => arg === '--raw=/version')) return output({ major: '1', minor: '35' });
        if (args.some((arg) => arg.includes('/apis/metrics.k8s.io/'))) return output({ items: [] });
        if (args.includes('ns')) return output({ status: { phase: 'Active' } });
        if (args.includes('sa')) return output({ metadata: { annotations: { 'azure.workload.identity/client-id': clientId } } });
        if (args.includes('secret')) {
          const data: Record<string, string> = {};
          for (const key of ['password', 'edge-hmac', 'internal-hmac', 'generation']) {
            data[key] = Buffer.from(secretValue).toString('base64');
          }
          return output({ data });
        }
        if (args.includes('deploy')) return output({ items: [] });
        if (args.includes('svc')) return output({ spec: { type: 'ClusterIP', ports: [{ port: 50050 }] } });
        if (args.includes('endpointslices')) return output({ items: [{ endpoints: [{ conditions: { ready: true } }] }] });
        if (args.includes('apiservice')) return output({ status: { conditions: [{ type: 'Available', status: 'True' }] } });
        if (args.includes('pods')) return output({ items: [] });
        if (args.includes('ds')) return output({ status: { desiredNumberScheduled: 0, numberReady: 0 } });
        if (args.includes('networkpolicies')) return output({ items: [] });
        throw new Error('Unexpected kubectl read');
      }
      if (command === 'az') {
        if (args[0] === 'account' && args[1] === 'show') return output({ id: subscription, tenantId: 'tenant-0001' });
        if (args[0] === 'account' && args[1] === 'get-access-token') return output(accessToken);
        if (args[0] === 'acr' && args[1] === 'repository' && args[2] === 'show') {
          const image = args[args.indexOf('--image') + 1] ?? '';
          return output({ digest: image.split('@')[1] });
        }
        if (args[0] === 'aks' && args[1] === 'show') {
          return output({
            fqdn: new URL(apiServer).hostname,
            oidcIssuerProfile: { issuerUrl: 'https://issuer.example.test/' },
            networkProfile: { networkPolicy: 'azure' },
          });
        }
        if (args[0] === 'identity' && args[1] === 'show') return output({ clientId, principalId });
        if (args.includes('federated-credential')) return output([]);
        if (args[0] === 'storage' && args[1] === 'account') return output({ id: '/subscriptions/test/storageAccounts/arrowcollector00000' });
        if (args[0] === 'storage' && args[1] === 'container') return output({ name: args[args.indexOf('-n') + 1] });
        if (args[0] === 'storage' && args[1] === 'blob' && args[2] === 'list') return output([]);
        if (args.includes('service-properties')) return output({ staticWebsite: { enabled: true } });
        if (args[0] === 'role' && args[1] === 'assignment') return output([]);
        throw new Error('Unexpected Azure read');
      }
      throw new Error('Unexpected command');
    };
    const fetcher: typeof fetch = async () => new Response('{"Tables":[]}', { status: 200 });
    const report = await runAksPreflight('oat.json', {
      workspaceRoot: root,
      environment: {
        ARROWCOL_MODE: 'azure',
        AZURE_CLIENT_ID: clientId,
        AZURE_TENANT_ID: 'tenant-0001',
        AZURE_SUBSCRIPTION_ID: subscription,
        AZURE_FEDERATED_TOKEN_FILE: assertionPath,
      },
      runCommand,
      fetcher,
      now: () => Date.parse('2026-01-01T00:00:00Z'),
    });
    const formatted = formatPreflightReport(report);
    assert.equal(report.ready, false);
    assert.equal(report.checks.length, 12);
    assert.equal(report.nextCommand, 'npx --no-install arrowcol-oat --mode aks --preflight-only');
    assert.equal(report.checks.find((check) => check.name === 'Kubernetes context and credentials')?.ready, true);
    assert.equal(report.checks.find((check) => check.name === 'Fixed publisher identity')?.ready, true);
    assert.equal(report.checks.find((check) => check.name === 'Required secrets')?.ready, true);
    assert.equal(report.checks.find((check) => check.name === 'AKS deployments')?.ready, false);
    assert.equal(report.checks.find((check) => check.name === 'Immutable ACR images')?.ready, true);
    assert.equal(calls.filter(({ command, args }) => command === 'az' && args[0] === 'acr').length, 5);
    assert.equal(report.checks.find((check) => check.name === 'Fresh pod metrics')?.ready, false);
    assert.equal(report.checks.find((check) => check.name === 'Network-policy enforcement proof')?.ready, false);
    assert.equal(formatted.includes(secretValue), false);
    assert.equal(formatted.includes(Buffer.from(secretValue).toString('base64')), false);
    assert.equal(formatted.includes(accessToken), false);
    assert.equal(calls.filter(({ command }) => command === 'az').every(({ args }) => args.includes('--subscription')), true);
    assert.equal(calls.filter(({ command, args }) => command === 'kubectl' && args.includes('get'))
      .every(({ args }) => args.includes('--request-timeout=30s')), true);
    const mutation = calls.some(({ command, args }) => {
      if (command === 'kubectl') return args.some((arg) => ['apply', 'create', 'delete', 'patch', 'scale', 'port-forward'].includes(arg));
      if (command !== 'az') return false;
      if (args[0] === 'login') return true;
      if (args[0] === 'aks') return ['get-credentials', 'create', 'delete', 'update', 'scale'].includes(args[1] ?? '');
      if (args[0] === 'storage') return ['create', 'delete', 'update', 'set'].includes(args[2] ?? '');
      if (args[0] === 'identity') return ['create', 'delete', 'update'].includes(args[1] ?? '') ||
        (args[1] === 'federated-credential' && ['create', 'delete', 'update'].includes(args[2] ?? ''));
      if (args[0] === 'role' && args[1] === 'assignment') return ['create', 'delete'].includes(args[2] ?? '');
      return false;
    });
    assert.equal(mutation, false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
