import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import vm from 'node:vm';

const page = await readFile(new URL('../../docs/experiments/arrowcol-oat/index.html', import.meta.url), 'utf8');

test('heatmap bounds are measured once per column and rendered rows are capped', () => {
  const helpers = page.match(/function heatBounds\([\s\S]*?\n}\nfunction heat\([\s\S]*?\n}/)?.[0];
  assert.ok(helpers, 'heat bounds and rendering helpers must exist');
  const context = vm.createContext({ number: (value: unknown) => typeof value === 'number' && Number.isFinite(value) ? value : null });
  vm.runInContext(`${helpers}\nglobalThis.helpers = { heatBounds, heat };`, context);
  const bounds = (context as { helpers: { heatBounds(values: number[]): { min: number; max: number; count: number }; heat(value: number, bounds: { min: number; max: number; count: number }): string } }).helpers;
  let visited = 0;
  const values = [3, 9, 6].map(value => value);
  const measured = bounds.heatBounds(new Proxy(values, {
    get(target, property, receiver) {
      if (property === Symbol.iterator) {
        return function* () {
          for (const value of target) {
            visited++;
            yield value;
          }
        };
      }
      return Reflect.get(target, property, receiver);
    },
  }));
  assert.deepEqual({ ...measured }, { min: 3, max: 9, count: 3 });
  assert.equal(visited, values.length);
  assert.notEqual(bounds.heat(9, measured), '');

  const render = page.match(/function render\(\) \{[\s\S]*?\n}\nasync function loadResults/)?.[0];
  assert.ok(render, 'render function must exist');
  assert.match(page, /const MAX_RENDERED_ROWS = 250;/);
  assert.match(render, /heatBounds\(/);
  assert.match(render, /rows\.slice\(0, MAX_RENDERED_ROWS\)\.map\(/);
});
