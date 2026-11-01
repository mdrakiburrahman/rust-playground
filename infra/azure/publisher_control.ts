/** Cooperative same-container publisher authority. Each transition performs at most one CAS. */
import { Failure, canonical, canonical_json, publisher_registry, require_released,
  type Metadata, type Registry } from './authority.js';

export const READY = 'ready-v1';
export const FENCED = 'fenced-v1';
export const CLEANING = 'cleaning-v1';
export const STATES = [READY, FENCED, CLEANING];
export const MAX_METADATA_BYTES = 8192;
export class UnknownTransition extends Failure {}
export const canonical_uuid = canonical;
export interface Authority {
  c: { sandbox: string };
  head_control(): Promise<[Metadata, string]>;
  set_control(metadata: Metadata, etag: string): Promise<void>;
}
export function encode_registry(metadata: Metadata, registry: Registry): Metadata {
  const updated = { ...metadata, publishers: canonical_json(registry) };
  publisher_registry(updated);
  const size = Object.entries(updated).reduce((n, [k, v]) =>
    n + Buffer.byteLength(`x-ms-meta-${k}: ${v}\r\n`), 0);
  if (size > MAX_METADATA_BYTES)
    throw new Failure('Container metadata budget exhausted; admission fails closed.');
  return updated;
}
export class PublisherControl {
  constructor(public azure: Authority) {}
  async read(): Promise<[Metadata, Registry, string]> {
    const [metadata, etag] = await this.azure.head_control();
    if (metadata.sandboxid !== this.azure.c.sandbox)
      throw new Failure('Publisher authority sandbox ownership mismatch.');
    if (['ready', 'fenced'].includes(metadata.resetstate) || !('publisherregistryversion' in metadata))
      throw new Failure('Legacy or unversioned reset authority; no implicit migration. Re-create the disposable sandbox authority explicitly.');
    if (!STATES.includes(metadata.resetstate))
      throw new Failure('Publisher authority requires a known versioned v1 state.');
    canonical(metadata.resetgeneration);
    if (!['released', 'testing', 'stopping'].includes(metadata.occupancy))
      throw new Failure('Publisher authority requires explicit occupancy released/testing/stopping.');
    if (metadata.occupancy !== 'released') canonical(metadata.occupancyid);
    const registry = publisher_registry(metadata);
    if (Object.values(registry).some(entry => entry.generation !== metadata.resetgeneration))
      throw new Failure('Publisher registry contains a different authority generation.');
    encode_registry(metadata, registry);
    return [metadata, registry, etag];
  }
  async cas(metadata: Metadata, registry: Registry, etag: string): Promise<Metadata> {
    const updated = encode_registry(metadata, registry);
    try { await this.azure.set_control(updated, etag); }
    catch (error) {
      if (!(error instanceof Failure)) throw error;
      throw new UnknownTransition('Publisher metadata CAS did not produce a definitive success; do not admit mutation or infer drain. Preserve publisher identity and reconcile authority read-only.', { cause: error });
    }
    return updated;
  }
  async admit(publisher: string, generation: string, bootstrap = false, occupant?: string): Promise<Metadata> {
    canonical(publisher); canonical(generation);
    const [metadata, registry, etag] = await this.read();
    if (occupant !== undefined && (metadata.occupancy !== 'testing' || metadata.occupancyid !== canonical(occupant)))
      throw new Failure('Test occupancy owner changed before admission; refused before I/O.');
    if (metadata.resetgeneration !== generation) throw new Failure('Admission generation changed.');
    if (metadata.resetstate !== (bootstrap ? FENCED : READY))
      throw new Failure('Publisher admission is fenced, cleanup-owned or incompatible.');
    if (bootstrap) {
      require_released(metadata);
      if (Object.keys(registry).length) throw new Failure('Bootstrap requires exclusive empty publisher registry.');
    } else if (metadata.occupancy === 'stopping')
      throw new Failure('A cluster stop holds the sandbox; publisher admission refused.');
    if (publisher in registry) throw new Failure('Publisher identity is already registered; no implicit retry/adoption.');
    registry[publisher] = { generation, state: 'active' };
    return this.cas(metadata, registry, etag);
  }
  async claim_exclusive(operator: string, generation: string): Promise<Metadata> {
    canonical(operator); canonical(generation);
    const [metadata, registry, etag] = await this.read();
    if (metadata.resetgeneration !== generation) throw new Failure('Exclusive operator generation changed.');
    if (metadata.resetstate !== READY) throw new Failure('Exclusive operator custody requires ready-v1.');
    require_released(metadata);
    if (Object.keys(registry).length) throw new Failure('Exclusive operator custody requires an empty publisher registry.');
    metadata.resetstate = FENCED;
    return this.cas(metadata, { [operator]: { generation, state: 'active' } }, etag);
  }
  async fence(generation: string): Promise<Metadata> {
    canonical(generation);
    const [metadata, registry, etag] = await this.read();
    require_released(metadata);
    if (metadata.resetgeneration !== generation) throw new Failure('Cannot fence a different generation.');
    if (![READY, FENCED].includes(metadata.resetstate)) throw new Failure('Cannot fence while cleanup owns authority.');
    metadata.resetstate = FENCED;
    return this.cas(metadata, registry, etag);
  }
  async member(publisher: string, generation: string): Promise<[string, Metadata, Registry, string]> {
    canonical(publisher); canonical(generation);
    const [metadata, registry, etag] = await this.read();
    if (metadata.resetgeneration !== generation || !(publisher in registry))
      throw new Failure('Publisher registration or generation no longer matches.');
    return [publisher, metadata, registry, etag];
  }
  async uncertain(publisher: string, generation: string): Promise<Metadata> {
    const [, metadata, registry, etag] = await this.member(publisher, generation);
    registry[publisher].state = 'uncertain';
    return this.cas(metadata, registry, etag);
  }
  async clean_release(publisher: string, generation: string, definitively_drained: boolean): Promise<Metadata> {
    const [, metadata, registry, etag] = await this.member(publisher, generation);
    if (metadata.resetstate === CLEANING) throw new Failure('The cleanup operator leaves only through finish_cleanup.');
    if (definitively_drained !== true) throw new Failure('Clean release requires definitive completion of every physical request.');
    if (registry[publisher].state !== 'active') throw new Failure('An uncertain publisher cannot be force-released or expire.');
    delete registry[publisher];
    return this.cas(metadata, registry, etag);
  }
  async drained(generation: string): Promise<[Metadata, string]> {
    const [metadata, registry, etag] = await this.read();
    if (metadata.resetstate !== FENCED || metadata.resetgeneration !== generation)
      throw new Failure('Drain proof requires the same fenced authority generation.');
    if (Object.keys(registry).length) throw new Failure('Active or uncertain registered publishers block cleanup.');
    require_released(metadata);
    return [metadata, etag];
  }
  async claim_cleanup(operator: string, generation: string): Promise<Metadata> {
    canonical(operator); canonical(generation);
    const [metadata, etag] = await this.drained(generation);
    metadata.resetstate = CLEANING;
    return this.cas(metadata, { [operator]: { generation, state: 'active' } }, etag);
  }
  async cleanup_operator(operator: string, generation: string): Promise<[string, Metadata, string]> {
    const [, metadata, registry, etag] = await this.member(operator, generation);
    require_released(metadata);
    if (metadata.resetstate !== CLEANING || Object.keys(registry).join(',') !== operator ||
        registry[operator].state !== 'active')
      throw new Failure('Caller does not hold exclusive definite cleanup authority.');
    return [operator, metadata, etag];
  }
  async abort_cleanup(operator: string, generation: string, nothing_deleted: boolean): Promise<Metadata> {
    const [, metadata, etag] = await this.cleanup_operator(operator, generation);
    if (nothing_deleted !== true) throw new Failure('Cleanup abort is permitted only before any deletion I/O.');
    metadata.resetstate = FENCED;
    return this.cas(metadata, {}, etag);
  }
  async finish_cleanup(operator: string, generation: string, next: string, definitively_cleaned: boolean): Promise<Metadata> {
    canonical(next);
    const [, metadata, etag] = await this.cleanup_operator(operator, generation);
    if (definitively_cleaned !== true) throw new Failure('Cleanup release requires definitive completion of every deletion.');
    if (next === generation) throw new Failure('Generation rotation requires a fresh UUID.');
    metadata.resetgeneration = next; metadata.resetstate = FENCED;
    return this.cas(metadata, {}, etag);
  }
  async abandon(generation: string, next: string, writers_stopped: boolean): Promise<Metadata> {
    canonical(generation); canonical(next);
    const [metadata, , etag] = await this.read();
    if (writers_stopped !== true) throw new Failure('Recovery requires proof that the workspace writers are stopped.');
    if (metadata.resetgeneration !== generation) throw new Failure('Recovery generation changed; nothing abandoned.');
    if (![READY, FENCED].includes(metadata.resetstate)) throw new Failure('Cleanup owns authority; use reset --resume instead.');
    if (metadata.occupancy === 'stopping') throw new Failure('A cluster stop holds the sandbox; recovery refused.');
    if (next === generation) throw new Failure('Generation rotation requires a fresh UUID.');
    metadata.resetstate = FENCED; metadata.resetgeneration = next; metadata.occupancy = 'released';
    delete metadata.occupancyid;
    return this.cas(metadata, {}, etag);
  }
  async bootstrap_owner(publisher: string, generation: string): Promise<[string, Metadata, string]> {
    const [, metadata, registry, etag] = await this.member(publisher, generation);
    require_released(metadata);
    if (metadata.resetstate !== FENCED || Object.keys(registry).join(',') !== publisher ||
        registry[publisher].state !== 'active')
      throw new Failure('Caller does not hold exclusive definite bootstrap authority.');
    return [publisher, metadata, etag];
  }
  async complete_bootstrap(publisher: string, generation: string, definitively_drained: boolean): Promise<Metadata> {
    const [, metadata, etag] = await this.bootstrap_owner(publisher, generation);
    if (definitively_drained !== true) throw new Failure('Bootstrap completion requires definitive completion of every request.');
    metadata.resetstate = READY;
    return this.cas(metadata, {}, etag);
  }
}
