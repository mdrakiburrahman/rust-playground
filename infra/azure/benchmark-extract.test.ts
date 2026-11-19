import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { collectorReceipt, rawField, terminalManifest } from './benchmark-extract.js';

test('preserves nanosecond u64 values exactly in extracted manifests and receipts', () => {
  const manifest = '{"first_send_unix_ns":1795104000123456789,"last_send_unix_ns":1795104000987654321}';
  const line = `OAT_CLIENT_METRICS {"phase":"completed","other":{"x":"brace }"},"logical_batch_manifest":${manifest}}`;
  assert.equal(terminalManifest(line), manifest);
  const receipt = '{"fields":{"message":"Delta logical publication audit receipt","ns":1795104000123456789}}';
  assert.equal(collectorReceipt(receipt), true);
  assert.equal(rawField(rawField(receipt, 'fields')!, 'ns'), '1795104000123456789');
});

test('rejects duplicate manifest fields and missing terminal manifests', () => {
  assert.throws(() => terminalManifest('OAT_CLIENT_METRICS {"phase":"completed","logical_batch_manifest":{},"logical_batch_manifest":{}}'), /Duplicate/);
  assert.throws(() => terminalManifest('OAT_CLIENT_METRICS {"phase":"failed"}'), /lack a manifest/);
  assert.equal(terminalManifest('OAT_CLIENT_METRICS {"phase":"prepared"}'), undefined);
});
