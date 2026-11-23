import { AsyncLocalStorage } from 'node:async_hooks';
import { createHash } from 'node:crypto';
import { channel } from 'node:diagnostics_channel';
import { readFile, realpath, writeFile } from 'node:fs/promises';
import { basename, dirname, resolve } from 'node:path';
import { monitorEventLoopDelay, performance } from 'node:perf_hooks';
import { setTimeout as sleep } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';

const phases = ['create', 'dispatch', 'headers', 'trailers', 'error'] as const;
type Phase = typeof phases[number];
type Times = Record<Phase, number | null>;
type SocketMeta = { localPort?: number; remotePort?: number };
type Message = { request?: { method?: string; origin?: string; path?: string }; socket?: SocketMeta;
  connectParams?: { hostname?: string; port?: string | number } };
function optional<T>(read: () => T): T | null { try { return read(); } catch { return null; } }
function delta(end?: number, start?: number) {
  return end !== undefined && start !== undefined && Number.isFinite(end - start) && end >= start ? end - start : null;
}

// Never use an incomplete trace to classify why a request failed. A transport
// error can legitimately terminate any *prefix* of create/dispatch/headers.
export function validateTrace(t: Times, elapsed: number, fetched: boolean, consumed: boolean, ambiguous: boolean) {
  const ordered = [t.create, t.dispatch, t.headers, t.trailers, t.error].filter((n): n is number => n !== null);
  if (ambiguous || ordered.some((n,i) => !Number.isFinite(n) || n < 0 || n > elapsed || (i > 0 && n < ordered[i-1]!))
    || (t.error !== null && t.trailers !== null) || (t.trailers !== null && t.headers === null)
    || (t.headers !== null && t.dispatch === null) || (t.dispatch !== null && t.create === null)) return 'unknown_inconsistent';
  if (t.create === null || t.dispatch === null || (fetched && t.headers === null) || (consumed && t.trailers === null)
    || (!consumed && t.error === null && t.trailers === null)) return 'unknown_incomplete';
  return 'observed';
}

function attempt(start: number) {
  return { start, closed:false, ambiguous:false,
    times:{create:null,dispatch:null,headers:null,trailers:null,error:null} as Times,
    connection_id:null as number | null, connection_use:'unknown' as 'unknown'|'new'|'reused',
    local_port:null as number|null, remote_port:null as number|null };
}
type Attempt = ReturnType<typeof attempt>;

class Observer {
  private scope = new AsyncLocalStorage<Attempt>();
  private requests = new WeakMap<object,Attempt>();
  private fresh = new WeakSet<object>();
  private sockets = new WeakMap<object,number>();
  private nextId = 1;
  private cleanup: (() => void)[] = [];
  readonly delay = optional(() => {const d = monitorEventLoopDelay({resolution:10}); d.enable(); return d;});

  constructor(readonly url: URL) {
    const on = (name:string, handle:(m:Message)=>void) => {
      const c = channel('undici:'+name);
      const callback = (value:unknown) => {try {handle(value as Message);} catch { const a=this.scope.getStore(); if(a) a.ambiguous=true; }};
      c.subscribe(callback); this.cleanup.push(()=>c.unsubscribe(callback));
    };
    const mark = (a:Attempt, p:Phase) => {
      if (a.times[p] !== null) a.ambiguous=true;
      else a.times[p]=performance.now()-a.start;
    };
    on('request:create', ({request:r}) => {
      const a=this.scope.getStore();
      if (!a || a.closed || !r || r.method!=='GET' || String(r.origin)!==url.origin || r.path!==url.pathname) return;
      mark(a,'create'); this.requests.set(r,a);
    });
    on('client:connected', ({socket, connectParams:p}) => {
      if (socket && p?.hostname===url.hostname && String(p.port)===url.port) this.fresh.add(socket);
    });
    on('client:sendHeaders', ({request,socket}) => {
      const first = socket ? this.fresh.delete(socket) : false;
      const a=request && this.requests.get(request); if(!a || a.closed) return;
      mark(a,'dispatch'); if(!socket) return;
      const previous=this.sockets.get(socket);
      a.connection_use=previous!==undefined?'reused':first?'new':'unknown';
      a.connection_id=previous ?? this.nextId++; this.sockets.set(socket,a.connection_id);
      a.local_port=socket.localPort??null; a.remote_port=socket.remotePort??null;
    });
    for(const p of ['headers','trailers','error'] as const) on('request:'+p,({request})=>{
      const a=request && this.requests.get(request); if(a && !a.closed) mark(a,p);
    });
  }
  fetch(a:Attempt) { return this.scope.run(a,()=>fetch(this.url,{signal:AbortSignal.timeout(3000)})); }
  close(){ for(const f of this.cleanup) f(); this.scope.disable(); this.delay?.disable(); }
}

async function sample(o:Observer, due:number) {
  const cpu=optional(()=>process.cpuUsage()), resources=optional(()=>process.resourceUsage());
  const loop=optional(()=>performance.eventLoopUtilization()); o.delay?.reset();
  const started_at=new Date().toISOString(), start=performance.now(), a=attempt(start);
  let status:number|null=null, error:string|null=null, fetch_headers_ms:number|null=null, body_complete_ms:number|null=null;
  try {
    const response=await o.fetch(a); fetch_headers_ms=performance.now()-start; status=response.status;
    if(response.redirected) a.ambiguous=true; // Fetch semantics unchanged; no redirect target is logged.
    if(!response.ok) error=`HTTP ${status}`;
    else {
      const bytes = await response.arrayBuffer();
      body_complete_ms = performance.now() - start;
      if (bytes.byteLength > 1_048_576) error = 'pressure response exceeds 1 MiB';
    }
  } catch(e) {error=e instanceof Error && e.name==='TimeoutError'?'TimeoutError': 'fetch or body read failed';}
  const elapsed_ms=performance.now()-start; a.closed=true;
  const endCpu=optional(()=>process.cpuUsage()), endResources=optional(()=>process.resourceUsage());
  const elu=loop && optional(()=>performance.eventLoopUtilization(loop));
  const count=o.delay?.count??null;
  const validity=validateTrace(a.times,elapsed_ms,fetch_headers_ms!==null,body_complete_ms!==null,a.ambiguous
    || (fetch_headers_ms !== null && a.times.headers !== null && a.times.headers > fetch_headers_ms)
    || (body_complete_ms !== null && a.times.trailers !== null && a.times.trailers > body_complete_ms));
  return {started_at,elapsed_ms,status,error,start_monotonic_ms:start,late_start_ms:Math.max(0,start-due),fetch_headers_ms,body_complete_ms,
    trace:{validity,offset_ms:a.times,connection_id:a.connection_id,connection_use:validity==='observed'?a.connection_use:'unknown',local_port:a.local_port,remote_port:a.remote_port},
    observer:{cpu_user_us:delta(endCpu?.user,cpu?.user),cpu_system_us:delta(endCpu?.system,cpu?.system),
      voluntary_switches:delta(endResources?.voluntaryContextSwitches,resources?.voluntaryContextSwitches),
      involuntary_switches:delta(endResources?.involuntaryContextSwitches,resources?.involuntaryContextSwitches),
      event_loop_utilization:elu && Number.isFinite(elu.utilization)?elu.utilization:null,
      loop_delay_samples:count,loop_delay_p99_ms:count?o.delay!.percentile(99)/1e6:null,loop_delay_max_ms:count?o.delay!.max/1e6:null}};
}

// The CLI only accepts the published gateway. Loopback is for real HTTP tests
// of this same sampler; no test mode, timeout override or injected fetch in the CLI.
export async function collect(url:URL,seconds:number) {
  if(!['172.17.0.1','127.0.0.1'].includes(url.hostname) || url.protocol!=='http:' || !url.port || url.pathname!=='/v1/pressure'
    || url.href!==url.origin+'/v1/pressure' || !Number.isInteger(seconds) || seconds<1 || seconds>120) throw Error('invalid sampler input');
  const o=new Observer(url), observations=[]; const start=performance.now(); let due=start;
  try {while(performance.now()-start<seconds*1000){
    const s=await sample(o,due); observations.push(s); due=s.start_monotonic_ms+1000;
    const wait=Math.min(due,start+seconds*1000)-performance.now(); if(wait>0) await sleep(Math.ceil(wait));
  }} finally {o.close();}
  const successful=observations.filter(s=>s.error===null).map(s=>s.elapsed_ms).sort((a,b)=>a-b);
  const quantile=(p:number)=>successful[Math.ceil(successful.length*p)-1]??null;
  return {kind:'diagnostic-only',qualification:'not-evaluated',url:url.href,duration_requested_seconds:seconds,observations,
    success_count:successful.length,error_count:observations.length-successful.length,p50_ms:quantile(.5),p99_ms:quantile(.99),max_ms:quantile(1)};
}

export async function record(url:URL, seconds:number, output:string) {
  const bytes=await readFile(new URL(import.meta.url)), source=output+'.observer.ts';
  await writeFile(source,bytes,{flag:'wx',mode:0o400});
  const evidence=await collect(url,seconds);
  await writeFile(output,JSON.stringify({...evidence,observer_source:{file:basename(source),sha256:createHash('sha256').update(bytes).digest('hex')},node:process.version},null,2)+'\n',{flag:'wx'});
  return evidence.error_count===0 && evidence.max_ms!==null && evidence.max_ms<1500;
}
if(process.argv[1] && pathToFileURL(resolve(process.argv[1])).href===import.meta.url){
  const [text,seconds,output]=process.argv.slice(2); const url=new URL(text!);
  if(url.hostname!=='172.17.0.1' || !output || resolve(output)!==output || !output.startsWith('/workspaces/rust-playground/out/diagnostic-') || await realpath(dirname(output))!==dirname(output)) throw Error('invalid diagnostic sampler arguments');
  if(!await record(url,Number(seconds),output)) process.exitCode=1;
}
