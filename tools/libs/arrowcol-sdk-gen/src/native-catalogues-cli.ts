import { parseArgs } from 'node:util';
import { nativeManager, type NativeProvider } from './native-catalogues.js';

const { values } = parseArgs({
  options: {
    action: { type: 'string', default: 'status' },
    provider: { type: 'string' }, catalog: { type: 'string' }, schema: { type: 'string' }, table: { type: 'string' },
  },
});
const action = values.action;
if (!['up', 'stop', 'status', 'inspect', 'verify', 'enable-build'].includes(action)) {
  throw new Error('Use --action=up|stop|status|inspect|verify|enable-build --provider=unity|hive');
}
if (values.provider !== 'unity' && values.provider !== 'hive') throw new Error('Select --provider=unity|hive');
const provider: NativeProvider = values.provider;
const manager = nativeManager(process.cwd());
const result = action === 'enable-build' ? await manager.enableBuild(provider)
  : action === 'up' ? await manager.up(provider) : action === 'stop' ? await manager.stop(provider)
  : action === 'status' ? await manager.status(provider) : action === 'verify' ? await manager.verify(provider)
    : await manager.inspect(provider, values.catalog, values.schema, values.table);
console.log(JSON.stringify(result, null, 2));
