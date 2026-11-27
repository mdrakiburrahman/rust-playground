import assert from 'node:assert/strict';
import test from 'node:test';
import { parseDeltaVisibility } from './aks-qap-observer.js';
import type { HttpReceipt } from '../../infra/azure/bootstrap.js';

const prefix = 'runs/fixture/table/_delta_log/';
function listing(versions: number[], next = '', exactPrefix = prefix): HttpReceipt {
  const body = `<EnumerationResults><Prefix>${exactPrefix}</Prefix><Blobs>${
    versions.map(version => `<Blob><Name>${prefix}${String(version).padStart(20, '0')}.json</Name></Blob>`).join('')
  }</Blobs><NextMarker>${next}</NextMarker></EnumerationResults>`;
  return { status: 200, bodyBase64: Buffer.from(body).toString('base64'),
    headers: { 'x-ms-request-id': 'storage-receipt-123' } };
}

test('commit visibility requires a complete current exact-prefix inventory, not a moving aggregate', () => {
  const observed = parseDeltaVisibility(listing([0, 1, 2]), prefix);
  assert.equal(observed.version, 2);
  assert.equal(observed.logFiles, 3);
  assert.equal(observed.requestId, 'storage-receipt-123');
  assert.match(observed.responseSha256!, /^[a-f0-9]{64}$/);
  for (const receipt of [
    listing([0, 2]), listing([0, 1, 1]), listing([]), listing([0, 1], 'more'),
    listing([0, 1], '', 'another-prefix/'), { ...listing([0]), status: 206 },
    { ...listing([0]), headers: {} },
  ]) assert.throws(() => parseDeltaVisibility(receipt, prefix), /Delta visibility/);
});

test('historical, duplicate, and escaped listing entries cannot bound a current commit', () => {
  const base = Buffer.from(listing([0]).bodyBase64, 'base64').toString();
  for (const body of [
    base.replace('</Blob>', '<Deleted>false</Deleted></Blob>'),
    base.replace('</Blobs>', `<Blob><Name>${prefix}../00000000000000000001.json</Name></Blob></Blobs>`),
    base.replace('</Blobs>', `<Blob><Name>${prefix}unexpected.json</Name></Blob></Blobs>`),
    base.replace('</Prefix>', '</Prefix><Prefix>other/</Prefix>'),
  ]) assert.throws(() => parseDeltaVisibility({ ...listing([0]), bodyBase64: Buffer.from(body).toString('base64') }, prefix));
});
