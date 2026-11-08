import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { exchange_workload_identity_token } from './bootstrap.js';

test('workload identity exchanges the projected token for the requested Azure scope', async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'azure-workload-identity-'));
  const tokenFile = path.join(directory, 'token');
  fs.writeFileSync(tokenFile, 'projected-assertion');
  const originalFetch = globalThis.fetch;
  let requestUrl = '';
  let requestBody = new URLSearchParams();
  globalThis.fetch = async (input, init) => {
    requestUrl = String(input);
    requestBody = new URLSearchParams(init?.body as URLSearchParams);
    return new Response(JSON.stringify({ access_token: 'access-token', token_type: 'Bearer', expires_in: 3600 }),
      { status: 200, headers: { 'content-type': 'application/json' } });
  };
  try {
    const result = await exchange_workload_identity_token('https://storage.azure.com/', 'client-id', 'tenant-id', tokenFile);
    assert.equal(result.token, 'access-token');
    assert.equal(result.expiresIn, 3600);
    assert.equal(requestUrl, 'https://login.microsoftonline.com/tenant-id/oauth2/v2.0/token');
    assert.equal(requestBody.get('client_id'), 'client-id');
    assert.equal(requestBody.get('scope'), 'https://storage.azure.com/.default');
    assert.equal(requestBody.get('client_assertion'), 'projected-assertion');
    assert.equal(requestBody.get('grant_type'), 'client_credentials');
  } finally {
    globalThis.fetch = originalFetch;
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test('workload identity exchange does not expose identity-provider response bodies', async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'azure-workload-identity-'));
  const tokenFile = path.join(directory, 'token');
  fs.writeFileSync(tokenFile, 'projected-assertion');
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response('private assertion detail', { status: 403 });
  try {
    await assert.rejects(
      exchange_workload_identity_token('https://storage.azure.com/', 'client-id', 'tenant-id', tokenFile),
      error => error instanceof Error && error.message === 'Workload identity token exchange failed with HTTP 403.' &&
        !error.message.includes('private assertion detail'),
    );
  } finally {
    globalThis.fetch = originalFetch;
    fs.rmSync(directory, { recursive: true, force: true });
  }
});