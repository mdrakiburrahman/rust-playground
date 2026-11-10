import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { runInNewContext } from 'node:vm';

test('viewer keeps unknown measurements distinct from zero and unverified pinning distinct from violations', async () => {
  const html = await readFile('docs/experiments/arrowcol-oat/index.html', 'utf8');
  const start = html.indexOf('function number(');
  const end = html.indexOf('const facets =', start);
  assert.ok(start >= 0 && end > start);
  const display = runInNewContext(`${html.slice(start, end)}; ({ show, pinningLabel })`) as {
    show(value: unknown): string;
    pinningLabel(row: { status: string; pinning_ok: boolean }): string;
  };
  assert.equal(display.show(null), '\u2014');
  assert.equal(display.show(0), '0');
  assert.equal(display.pinningLabel({ status: 'failed', pinning_ok: false }), 'Unverified');
  assert.equal(display.pinningLabel({ status: 'pinning_violation', pinning_ok: false }), 'Violation');
  assert.equal(display.pinningLabel({ status: 'succeeded', pinning_ok: true }), 'OK');
  assert.equal(display.pinningLabel({ status: 'failed', pinning_ok: true }), 'OK');
  assert.ok(html.includes('const pinning = pinningLabel(row);'));
});
