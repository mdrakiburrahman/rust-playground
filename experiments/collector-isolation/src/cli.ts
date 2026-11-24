import {parseArgs} from 'node:util';
import {open, readFile, realpath, stat, writeFile} from 'node:fs/promises';
import {setTimeout as sleep} from 'node:timers/promises';
import {collectorMetrics, type Level} from './metrics.ts';
import {sampleLinux, systemInfo} from './linux.ts';
import {reduce, physicalEvidence, isolationEvidence, type Isolation, type Header, type Obs, type Sample} from './reduce.ts';
import {clientEvidence, committedEvidence, commitLogEvidence, terminalClientLog} from './evidence.ts';

const cmd = process.argv[2], string = {type:'string'} as const, repeated = {type:'string',multiple:true} as const;
function num(v: string | undefined, name: string, min: number, max=Number.MAX_SAFE_INTEGER) {
  if (!v || !/^\d+$/.test(v) || !Number.isSafeInteger(+v) || +v < min || +v > max) throw Error(`Invalid --${name} (${min}..${max})`);
  return +v;
}
const req = (v:string|undefined,name:string) => { if (!v) throw Error(`Missing --${name}`); return v; };
function devices(l: string[] | undefined) { const a=l?.flatMap(x=>x.split(',')) ?? []; if (a.some(x=>!/^[-.\w]+$/.test(x)) || a.length !== new Set(a).size) throw Error('Invalid/duplicate device'); return a; }
const readJson = async (s: string) => JSON.parse(await readFile(s,'utf8'));
if (cmd === 'sample') {
  const {values:v}=parseArgs({args:process.argv.slice(3),options:{level:string,cores:string,'run-id':string,'metrics-url':string,pid:string,nic:repeated,disk:repeated,'start-ms':string,'end-ms':string,'interval-ms':string,out:string}});
  const level = req(v.level,'level').toUpperCase() as Level;
  if (!/^L[0-5]$/.test(level)) throw Error('Expected L0..L5');
  const startMs=num(v['start-ms'],'start-ms',1), endMs=num(v['end-ms'],'end-ms',startMs+1), intervalMs=num(v['interval-ms']??'2000','interval-ms',250,60000);
  if (endMs-startMs < intervalMs || endMs-startMs > 3600_000 || Math.ceil((endMs-startMs)/intervalMs)>15000) throw Error('Use an interval <= the window, at most one hour');
  const addr=new URL(req(v['metrics-url'],'metrics-url'));
  if (!['http:','https:'].includes(addr.protocol) || addr.username || addr.password || addr.search || addr.hash) throw Error('Expected direct admin HTTP(S) URL without credentials/query');
  const isolatedAddr=new URL('/v1/isolation',addr);
  const response=async(url: URL)=>{const res=await fetch(url,{signal:AbortSignal.timeout(Math.min(intervalMs,1500)),redirect:'error'}); if(!res.ok) throw Error('HTTP '+res.status); return res;};
  const h: Header={declaredCores:v.cores ? num(v.cores,'cores',1,65536) : undefined,schema:'collector-isolation/v1',runId:req(v['run-id'],'run-id'),level,startMs,endMs,intervalMs,system:await systemInfo(num(v.pid,'pid',1),devices(v.nic),devices(v.disk))};
  const epoch=Date.now()-performance.now(), now=()=>epoch+performance.now();
  if (startMs < now()) throw Error('Window start already passed: arm before it starts');
  const file=await open(req(v.out,'out'),'wx');
  const observe=async <T>(fn:()=>Promise<T>): Promise<Obs<T>> => {const beginMs=now(); try{return {beginMs,value:await fn(),endMs:now()};} catch(e){return {beginMs,endMs:now(),value:null,error:e instanceof Error?e.message:String(e)};}};
  try {
    await file.writeFile(JSON.stringify(h)+'\n');
    for (let t=startMs;;) {
      await sleep(Math.max(0,t-now()));
      let promBodyBytes: number|null=null, isolationBodyBytes: number|null=null;
      const [prom,linux,isolation]=await Promise.all([
        observe(async()=>{const text=await (await response(addr)).text(); promBodyBytes=Buffer.byteLength(text); const metrics=collectorMetrics(text); if(metrics.received_rows_total===null)throw Error('Not a collector /metrics response'); return metrics;}),
        observe(()=>sampleLinux(h.system,t===startMs||t===endMs)),
        level === 'L5' ? undefined : observe(async()=>{const text=await (await response(isolatedAddr)).text(); isolationBodyBytes=Buffer.byteLength(text);return JSON.parse(text) as Isolation;})]);
      const cpu=process.cpuUsage();
      const s: Sample={targetMs:t,wallClockOffsetMs:Date.now()-now(),prom,linux,isolation,
        sampler:{atMs:now(),cpuUs:cpu.user+cpu.system,rssBytes:process.memoryUsage.rss(),promBodyBytes,isolationBodyBytes}}; await file.writeFile(JSON.stringify(s)+'\n');
      if(t===endMs)break;
      // Advance the absolute schedule, skipping missed interior reads instead of back-to-back catch-up GETs.
      t=Math.min(endMs,startMs+Math.max(1+Math.floor((t-startMs)/intervalMs),1+Math.floor((now()-startMs)/intervalMs))*intervalMs);
    }
  } finally {await file.close();}
} else if (cmd === 'reduce') {
  const {values:v}=parseArgs({args:process.argv.slice(3),options:{samples:string,client:repeated,'client-count':string,'offered-field':string,stages:string,'commit-log':string,'commit-window':string,out:string}});
  const [header,...samples]=(await readFile(req(v.samples,'samples'),'utf8')).trim().split('\n').map(l=>JSON.parse(l));
  const base=reduce(header,samples);
  const roster=v.client?.map(c=>{const sep=c.indexOf('='); if(sep<=0 || sep===c.length-1)throw Error('Expected --client ID=PATH'); return {id:c.slice(0,sep),path:c.slice(sep+1)};})??[];
  const paths=await Promise.all(roster.map(c=>realpath(c.path))); if(new Set(paths).size!==paths.length)throw Error('Duplicate client log path');
  const clients=roster.length || v['client-count'] ? clientEvidence(await Promise.all(roster.map(async(c,i)=>({id:c.id,report:await terminalClientLog(paths[i])}))),num(v['client-count'],'client-count',1),header,v['offered-field']??'offered_rows'):null;
  const diagnostic=isolationEvidence(samples,header);
  if (v['commit-log'] && v['commit-window']) throw Error('Choose one authoritative Delta evidence input');
  const committed = v['commit-window'] ? committedEvidence(await readJson(v['commit-window']),header) : v['commit-log'] ? await commitLogEvidence(v['commit-log'],header) : null;
  const report={...base,observer:{...base.observer,rawFileBytes:(await stat(req(v.samples,'samples'))).size},clients,diagnostic:diagnostic?{window:diagnostic.window,ingress:diagnostic.ingress}:null,physical:v.stages?physicalEvidence(await readJson(v.stages),header):diagnostic?.physical??null,committed};
  await writeFile(req(v.out,'out'),JSON.stringify(report,null,2)+'\n',{flag:'wx'});
} else if (cmd === 'chart') {
  const {values:v}=parseArgs({args:process.argv.slice(3),options:{report:repeated,kind:string,out:string}});
  if(!v.report?.length)throw Error('Expected --report PATH (repeat for matched runs)');
  const {chart}=await import('./chart.ts'); const kind=req(v.kind,'kind');
  if(kind!=='waterfall' && kind!=='resources') throw Error('Chart kind must be waterfall or resources');
  await chart(await Promise.all(v.report.map(readJson)),req(v.out,'out'),kind,v.report);
} else throw Error('Expected sample | reduce | chart. See experiments/collector-isolation/README.md');
