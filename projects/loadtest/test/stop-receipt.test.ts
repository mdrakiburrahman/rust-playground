import assert from 'node:assert/strict';
import { test } from 'node:test';

import { stopReceipt } from '../src/lifecycle.js';

test('ordinary local stop retains the original receipt without inventing a cloud generation', () => {
  assert.deepEqual(stopReceipt(), { stopped: true, scope: 'workspace-owned-loadtest', dataPreserved: true });
});

test('cloud stop receipt carries exactly the captured canonical generation without normalization', () => {
  const generation = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';
  assert.deepEqual(stopReceipt(generation), {
    stopped: true, scope: 'workspace-owned-loadtest', dataPreserved: true, resetGeneration: generation,
  });
  for (const invalid of ['', generation.toUpperCase(), ` ${generation}`, `${generation}\n`, 'not-a-generation']) {
    assert.throws(() => stopReceipt(invalid), /canonical lowercase/u);
  }
});
