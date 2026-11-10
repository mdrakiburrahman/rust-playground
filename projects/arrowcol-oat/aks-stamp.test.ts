import assert from 'node:assert/strict';
import test from 'node:test';
import { parse } from 'yaml';
import { renderStampConfig } from './aks-stamp.js';

test('persisted stamp splice preserves every other byte, comment and CRLF', () => {
  const source = '# comment\r\nstamp:  89152   # selection\r\naks: { version: 1.35.6 }\r\n';
  assert.equal(renderStampConfig(source, { stamp: '12345' }), source.replace('89152', '12345'));
});

for (const original of ['89152', "'89152'", '"89152"']) {
  test(`quoted zero stamp remains five digits when replacing ${original}`, () => {
    const source = `stamp: ${original} # retained\naks: {}\n`;
    const result = renderStampConfig(source, { stamp: '00000' });
    assert.equal(parse(result).stamp, '00000');
    assert.ok(result.endsWith(' # retained\naks: {}\n'));
  });
}

test('temporary network candidate changes only its own rendering, never the source', () => {
  const source = "stamp: '00000'\naks:\n  networkDataplane: cilium\n";
  const candidate = parse(renderStampConfig(source, { stamp: '12345', network: 'azure' }));
  assert.equal(String(candidate.stamp), '12345');
  assert.equal(candidate.aks.networkDataplane, 'azure');
  assert.equal(renderStampConfig(source, { stamp: '12345' }), source.replace('00000', '12345'));
});

test('invalid, duplicate, nested-only and non-scalar stamp inputs fail closed', () => {
  for (const source of ['stamp: [1]\n', 'stamp: 1\nstamp: 2\n', 'aks: { stamp: 1 }\n', '[]', 'stamp: true\n', 'stamp: &s [1]\n'])
    assert.throws(() => renderStampConfig(source, { stamp: '12345' }), /stamp/i);
  for (const stamp of ['1', '123456', '01234'])
    assert.throws(() => renderStampConfig('stamp: 1\n', { stamp }), /five digits/);
});
