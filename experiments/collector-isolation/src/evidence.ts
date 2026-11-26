import {createReadStream} from 'node:fs';
import {createInterface} from 'node:readline';
import type {Header} from './reduce.ts';

export type Obj = Record<string, unknown>;
export function obj(value: unknown): Obj | null { return typeof value === 'object' && value !== null && !Array.isArray(value) ? value as Obj : null; }
const integer = (v: unknown): number | null => typeof v === 'number' && Number.isSafeInteger(v) && v >= 0 ? v : null;
const sum = (ns: (number | null)[]) => ns.some(n=>n === null) ? null : integer(ns.reduce<number>((a,b)=>a+b!,0));
const perSecond = (n: number | null, h: Header) => n === null ? null : n*1000/(h.endMs-h.startMs);
export async function terminalClientLog(path: string): Promise<Obj> {
  const rl = createInterface({input:createReadStream(path),crlfDelay:Infinity});
  const terminal: Obj[] = [];
  for await (const line of rl) if (line.startsWith('OAT_CLIENT_METRICS ')) {
    const doc = obj(JSON.parse(line.slice('OAT_CLIENT_METRICS '.length)));
    if (doc && ['completed','failed'].includes(String(doc.phase))) terminal.push(doc);
  }
  if (terminal.length !== 1 || terminal[0].phase !== 'completed') throw Error(`Expected one successful terminal DMV snapshot: ${path}`);
  return terminal[0];
}
export function clientEvidence(clients: {id:string; report:Obj}[], expectedCount: number, h: Header, offeredField: string) {
  if (!Number.isSafeInteger(expectedCount) || expectedCount < 1 || clients.length !== expectedCount || new Set(clients.map(c=>c.id)).size !== clients.length) throw Error('Incomplete/duplicate client roster');
  if (!/^(?:offered|attempted|send_started)_[a-z_]*rows$/.test(offeredField)) throw Error('Offered field must be an explicit measurement_window offered/attempted row counter');
  const rows = clients.map(({id,report}) => {
    const w = obj(report.measurement_window);
    if (report.phase !== 'completed' || !w || w.start_unix_ms !== h.startMs || w.end_unix_ms !== h.endMs || (report.run_id !== undefined && report.run_id !== h.runId)) throw Error(`Client ${id} terminal window/identity mismatch`);
    const workers = Array.isArray(report.workers) ? report.workers.map(obj) : [];
    if (!workers.length || workers.includes(null)) throw Error(`Client ${id} has no worker receipt evidence`);
    const counts = (key: string) => sum(workers.map(v=>integer(v![key])));
    const completed = integer(w.completed_rows), diag = counts('diagnostic_streams'), durable = counts('durable_streams'), best = counts('best_effort_streams');
    // Stream counts are process-lifetime **type proof**, never window counts/rates.
    if (completed === null || durable === null || best === null || (h.level === 'L5' ? (diag ?? 0) !== 0 || (completed > 0 && best+durable === 0) :
      diag === null || best !== 0 || durable !== 0 || (completed > 0 && diag === 0))) throw Error(`Client ${id} receipt type conflicts with ${h.level}`);
    return {id, offeredRows:integer(w[offeredField]), completedRows:completed, configuredIpcFlightBytes:integer(w.completed_flight_data_bytes_configured_ipc_compression),
      uncompressedFlightBytes:integer(w.completed_flight_data_bytes_uncompressed), receiptTypeProofProcessLifetime:{diagnosticStreams:diag,durableStreams:durable,bestEffortStreams:best}};
  });
  const offered=sum(rows.map(v=>v.offeredRows)), completed=sum(rows.map(v=>v.completedRows));
  if (completed === null) throw Error('Client completion rows overflow');
  return {scope:'one exact half-open shared UTC interval; offers counted by send start; completions by receipt time, not necessarily the same cohort',
    rosterSource: 'caller-supplied per-cell client log IDs and expected count', offeredField:`measurement_window.${offeredField}`,
    completedField:'measurement_window.completed_rows', completionSemantics:h.level === 'L5' ? 'validated_admission_not_delta_commit' : 'explicit_diagnostic_discard_not_admission',
    completedRows:completed, offeredRows:offered, offeredRowsPerSecond:perSecond(offered,h), acceptedRowsPerSecond:h.level === 'L5' ? perSecond(completed,h) : null,
    diagnosticRowsPerSecond:h.level === 'L5' ? null : perSecond(completed,h), completedRowsPerSecond:perSecond(completed,h),
    configuredIpcFlightBytesPerSecond:perSecond(sum(rows.map(v=>v.configuredIpcFlightBytes)),h), uncompressedFlightBytesPerSecond:perSecond(sum(rows.map(v=>v.uncompressedFlightBytes)),h),
    offeredToCompletedRatio: offered !== null && completed > 0 ? offered/completed : null,
    unavailableOfferedReason:offered === null ? 'At least one exact-window send-start row counter absent/invalid; no cumulative or concurrency substitute.' : null, clients:rows};
}
export function committedEvidence(e: Obj,h: Header) {
  if (h.level !== 'L5' || e.scope !== 'new_ingestion_commits_finished_in_half_open_shared_window_not_client_cohort' ||
      e.measurement_start_unix_ms !== h.startMs || e.measurement_end_unix_ms !== h.endMs) throw Error('Official Delta evidence scope/window mismatch');
  const counters = [e.rows,e.files,e.input_bytes,e.parquet_bytes,e.commits].map(integer);
  if (e.unavailable_reason != null || counters.includes(null)) return {available:false, reason:String(e.unavailable_reason ?? 'Incomplete official commit evidence')};
  const [rows,files,input,physical,commits] = counters;
  return {available:true, scope:e.scope, rows, files, commits, rowsPerSecond:perSecond(rows,h), logicalInputBytesPerSecond:perSecond(input,h), parquetBytesPerSecond:perSecond(physical,h)};
}

// Producer: arrow_deltalake::telemetry::data_commit, emitted after the durability barrier.
export async function commitLogEvidence(path: string, h: Header) {
  if (h.level !== 'L5') throw Error('Delta commit logs apply only to L5');
  const stream = createInterface({input:createReadStream(path),crlfDelay:Infinity});
  const seen=new Map<number,string>(); let table:string|undefined, rows=0,files=0,input=0,parquet=0,commits=0;
  for await (const line of stream) {
    if (!line.startsWith('arrowcol_data_commit ')) continue;
    const event=obj(JSON.parse(line.slice('arrowcol_data_commit '.length)));
    if (!event || event.schema_version!==1 || event.outcome!=='committed' || typeof event.table_id!=='string' || !event.table_id) return {available:false, reason:'Incomplete or ambiguous Delta commit event in single-process log'};
    if (table && event.table_id!==table) throw Error('Multiple table identities in single-table Delta evidence');
    table=event.table_id;
    const [ms,version,n,f,ib,pb]=[event.finished_at_unix_ms,event.delta_version,event.rows,event.files,event.input_bytes,event.parquet_bytes].map(integer);
    if (ms===null || version===null || n===null || f===null || ib===null || pb===null || !n || !f || !pb) throw Error('Incomplete row, byte, or version evidence in Delta data commit');
    const fingerprint=JSON.stringify([ms,n,f,ib,pb]);
    if (seen.has(version)) {if (seen.get(version)!==fingerprint)throw Error('Conflicting evidence for Delta version'); continue;}
    seen.set(version,fingerprint);
    if (ms<h.startMs || ms>=h.endMs) continue;
    rows+=n;files+=f;input+=ib;parquet+=pb;commits++;
  }
  if (!seen.size) return {available:false,reason:'No attributable data-commit events: cannot distinguish empty/missing capture from zero commits'};
  return {...committedEvidence({scope:'new_ingestion_commits_finished_in_half_open_shared_window_not_client_cohort',measurement_start_unix_ms:h.startMs,measurement_end_unix_ms:h.endMs,
    rows,files,input_bytes:input,parquet_bytes:parquet,commits},h),source:'caller-supplied complete, dedicated, unfiltered one-collector log; deduplicated by table identity and version',tableId:table};
}
