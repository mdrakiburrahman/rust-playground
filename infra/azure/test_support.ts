import { Config, Azure, TAGS, ControlConflict, HttpFailure, canonical_json, uuid,
  type BlobEntry, type Metadata } from './bootstrap.js';
export const SUB = '00000000-0000-0000-0000-000000000001';
export const WORKSPACE_CUSTOMER = '00000000-0000-0000-0000-00000000c0de';
export type ExperimentObjects = { views: string[]; functions: string[]; external: string[]; tables: string[] };
const EXPERIMENT_KQL: [string, string, keyof ExperimentObjects][] = [
  ['.show materialized-views', '.drop materialized-view', 'views'], ['.show functions', '.drop function', 'functions'],
  ['.show external tables', '.drop external table', 'external'], ['.show tables', '.drop table', 'tables'],
];
export class FakeAzure extends Azure {
  tags: Record<string, string>;
  databases: any[]; containers: any[];
  data_metadata: Metadata;
  experiment_metadata: Metadata;
  experiment_objects: ExperimentObjects = { views: [], functions: [], external: [], tables: [] };
  diagnostic = true;
  settings: Record<string, any> = {};
  items: Record<string, BlobEntry[]>;
  directories: Record<string, string[]>;
  metadata: Metadata;
  cluster_state = 'Running';
  checkpoint_exists = true;
  etag = '"one"';
  events: any[][] = [];
  logs = new Map<string, Buffer | null>();
  constructor() {
    super(new Config({ AZURE_SUBSCRIPTION_ID: SUB }));
    const c = this.c;
    this.tags = { ...TAGS, sandboxId: c.sandbox };
    this.databases = [{ name: c.database }];
    this.containers = [c.data, c.checkpoints].map(name => ({ name, properties: {} }));
    this.data_metadata = { sandboxid: c.sandbox };
    this.experiment_metadata = { sandboxid: c.sandbox };
    this.items = { [c.data]: [{ name: 'table/data.parquet', snapshot: null, version: null, deleted: false }], [c.checkpoints]: [] };
    this.directories = { [c.data]: [], [c.checkpoints]: [] };
    this.metadata = { sandboxid: c.sandbox, resetstate: 'ready-v1', resetgeneration: uuid(),
      occupancy: 'released', publisherregistryversion: '1', publishers: '{}', custom: 'preserved' };
  }
  with_experiment(objects: Partial<ExperimentObjects> = {}): this {
    const c = this.c;
    this.databases.push({ name: c.experimentDatabase });
    this.containers.push({ name: c.experimentContainer, properties: {} });
    this.items[c.experimentContainer] = [{ name: 'runs/run-1/optimize/_delta_log/00000000000000000000.json', snapshot: null, version: null, deleted: false }];
    this.directories[c.experimentContainer] = ['runs', 'runs/run-1', 'runs/run-1/optimize', 'runs/run-1/optimize/_delta_log'];
    this.experiment_objects = { ...this.experiment_objects, ...objects };
    return this;
  }
  override async arm(method: string, resourcePath: string, ..._args: any[]): Promise<any> {
    if (resourcePath.includes('/diagnosticSettings/'))
      return this.diagnostic ? { properties: { logs: [{ category: 'StorageRead', enabled: true }] } } : null;
    if (resourcePath.endsWith('/blobServices/default')) return { properties: this.settings };
    if (resourcePath.endsWith('/containers/' + this.c.checkpoints))
      return this.checkpoint_exists ? { properties: { metadata: { ...this.metadata } } } : null;
    if (resourcePath.endsWith('/containers/' + this.c.data)) return { properties: { metadata: { ...this.data_metadata } } };
    if (resourcePath.endsWith('/containers/' + this.c.experimentContainer))
      return { properties: { metadata: { ...this.experiment_metadata } } };
    return { tags: { ...this.tags }, properties: { isHnsEnabled: true, uri: 'https://cluster.example',
      principalId: SUB, state: this.cluster_state, customerId: WORKSPACE_CUSTOMER, retentionInDays: 30 } };
  }
  override async kql(command: string, database?: string): Promise<any> {
    this.events.push(['kql', command, ...database ? [database] : []]);
    if (database !== this.c.experimentDatabase) return {};
    for (const [show, drop, kind] of EXPERIMENT_KQL) {
      if (command.startsWith(show)) return { Tables: [{ Rows: this.experiment_objects[kind].map(name => [name]) }] };
      const match = command.startsWith(drop + ' ') && /\['([^']+)'\] ifexists$/.exec(command);
      if (match) this.experiment_objects[kind] = this.experiment_objects[kind].filter(name => name !== match[1]);
    }
    return {};
  }
  override async collection(resourcePath: string): Promise<any[]> {
    return resourcePath.endsWith('/databases') ? this.databases : this.containers;
  }
  override async blobs(container: string): Promise<BlobEntry[]> {
    return this.items[container].map(item => ({ etag: `"${item.name}"`, ...item }));
  }
  override async paths(container: string): Promise<any[]> {
    return this.directories[container].map(name => ({ name, isDirectory: true, etag: `"${name}"` }));
  }
  override async delete_directory(container: string, name: string, etag: string): Promise<void> {
    this.events.push(['DELETE_DIRECTORY', name]);
    if (etag !== `"${name}"`) throw new HttpFailure(412, 'HTTP 412: condition not met');
    this.directories[container] = this.directories[container].filter(item => item !== name);
  }
  override async head_control(): Promise<[Metadata, string]> { return [{ ...this.metadata }, this.etag]; }
  override async set_control(metadata: Metadata, etag: string): Promise<void> {
    if (etag !== this.etag) throw new ControlConflict('HTTP 412: metadata changed');
    this.metadata = { ...metadata }; this.etag = uuid();
    this.events.push(['control', metadata.resetstate]);
  }
  override async cli(...args: string[]): Promise<any> {
    this.events.push(['cli', ...args]);
    if (args.includes('/stop?api-version=2024-04-13') || args.some(arg => arg.includes('/stop?')))
      this.cluster_state = 'Stopped';
    if (args.some(arg => arg.includes('/start?'))) this.cluster_state = 'Running';
    return {};
  }
  override async delete_database(): Promise<[number, string | undefined]> {
    this.events.push(['arm-delete-db']); this.databases = this.databases.filter(item => item.name !== this.c.database); return [200, undefined];
  }
  override async provider_operation(_url: string): Promise<string> {
    this.events.push(['arm-operation']); return 'Succeeded';
  }
  override async blob(method: string, container: string, name = '', _query?: Record<string, string>,
    _body?: string | Buffer | null, headers: Record<string, string> = {}, _missing_ok = false): Promise<[Buffer | null, Record<string, string>]> {
    this.events.push([method, name]);
    if (method === 'GET') return [this.logs.get(name) ?? null, {}];
    if (method === 'DELETE') {
      const current = (await this.blobs(container)).find(item => item.name === name);
      if (current && headers['If-Match'] !== undefined && current.etag !== headers['If-Match'])
        throw new HttpFailure(412, 'HTTP 412: condition not met');
      this.items[container] = this.items[container].filter(item => item.name !== name);
    }
    return [Buffer.alloc(0), {}];
  }
  registry() { return JSON.parse(this.metadata.publishers); }
  register(publisher = uuid(), state = 'active'): string {
    this.metadata.publishers = canonical_json({ ...this.registry(), [publisher]: { generation: this.metadata.resetgeneration, state } });
    return publisher;
  }
}
