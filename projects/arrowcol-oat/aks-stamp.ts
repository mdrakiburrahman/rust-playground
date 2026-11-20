import { isMap, isScalar, parseDocument } from 'yaml';

export function renderStampConfig(source: string, updates: { stamp: string; network?: 'azure' | 'cilium' }): string {
  if (!/^(00000|[1-9]\d{4})$/.test(updates.stamp))
    throw new Error('Selected stamp must be exactly five digits.');
  const document = parseDocument(source);
  if (document.errors.length || !isMap(document.contents)) throw new Error('Invalid stamp YAML mapping.');
  const node = document.get('stamp', true);
  if (!isScalar(node) || !node.range || !['PLAIN', 'QUOTE_SINGLE', 'QUOTE_DOUBLE'].includes(node.type ?? '') ||
      !/^(00000|[1-9]\d{0,5})$/.test(String(node.value)))
    throw new Error('Stamp YAML must contain a single scalar stamp.');
  const replacement = node.type === 'QUOTE_DOUBLE' ? JSON.stringify(updates.stamp) :
    node.type === 'QUOTE_SINGLE' || updates.stamp === '00000' ? `'${updates.stamp}'` : updates.stamp;
  const result = source.slice(0, node.range[0]) + replacement + source.slice(node.range[1]);
  if (updates.network === undefined) return result;
  if (updates.network !== 'azure' && updates.network !== 'cilium') throw new Error('Invalid stamp network.');
  const candidate = parseDocument(result);
  candidate.setIn(['aks', 'networkDataplane'], updates.network);
  return candidate.toString();
}
