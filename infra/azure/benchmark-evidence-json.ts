export function requireEvidence(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

export function evidenceObject(value: unknown): Record<string, unknown> {
  requireEvidence(typeof value === 'object' && value !== null && !Array.isArray(value), 'Expected evidence object');
  return value as Record<string, unknown>;
}

export function evidenceList(value: unknown): unknown[] {
  requireEvidence(Array.isArray(value), 'Expected evidence list');
  return value;
}

export function evidenceText(value: unknown): string {
  requireEvidence(typeof value === 'string', 'Expected evidence string');
  return value;
}

export function uint(value: unknown, label: string, maximum = (1n << 64n) - 1n): bigint {
  requireEvidence(typeof value === 'bigint' && value >= 0n && value <= maximum, `Invalid ${label}`);
  return value;
}

const tuple = Symbol('debug tuple');
export function debugTuple(value: unknown): unknown[] {
  return evidenceList((evidenceObject(value) as { [tuple]?: unknown })[tuple]);
}

export function parseEvidence(input: string, debug = false): unknown {
  if (debug) requireEvidence(/^[\x00-\x7f]*$/.test(input), 'Non-ASCII receipt debug list');
  const token = /[ \t\r\n]*("(?:[^"\\\x00-\x1f]|\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))*"|(?:-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)|true|false|null|[\[\]{}(),:])/y;
  let at = 0;
  function peek(): string {
    token.lastIndex = at;
    return token.exec(input)?.[1] ?? '';
  }
  function consume(): string {
    const next = peek();
    requireEvidence(next, `Invalid evidence syntax at ${at}`);
    at = token.lastIndex;
    return next;
  }
  function closing(end: string): void {
    requireEvidence(consume() === end, `Expected ${end}`);
  }
  function value(depth: number): unknown {
    requireEvidence(depth < 100, 'Evidence nesting limit exceeded');
    const next = consume();
    if (next === '[' || (debug && next === '(')) {
      const end = next === '[' ? ']' : ')';
      const items = [];
      if (peek() !== end) {
        items.push(value(depth + 1));
        while (peek() === ',') { consume(); items.push(value(depth + 1)); }
      }
      closing(end);
      return next === '(' ? { [tuple]: items } : items;
    }
    if (!debug && next === '{') {
      const object: Record<string, unknown> = Object.create(null);
      if (peek() !== '}') {
        do {
          const quoted = consume();
          requireEvidence(quoted.startsWith('"'), 'Expected JSON object key');
          const key = evidenceText(JSON.parse(quoted));
          requireEvidence(!Object.hasOwn(object, key), `Duplicate JSON key: ${key}`);
          closing(':');
          object[key] = value(depth + 1);
          if (peek() !== ',') break;
          consume();
        } while (true);
      }
      closing('}');
      return object;
    }
    if (next.startsWith('"')) return JSON.parse(next) as unknown;
    if (/^-?\d+$/.test(next)) return BigInt(next);
    if (!debug && /^-?\d/.test(next)) {
      const n = Number(next);
      requireEvidence(Number.isFinite(n), 'Nonfinite JSON number');
      return n;
    }
    if (!debug && next === 'true') return true;
    if (!debug && next === 'false') return false;
    if (!debug && next === 'null') return null;
    throw new Error(`Unsupported evidence token ${next}`);
  }
  const parsed = value(0);
  requireEvidence(/^[ \t\r\n]*$/.test(input.slice(at)), 'Trailing evidence content');
  return parsed;
}
