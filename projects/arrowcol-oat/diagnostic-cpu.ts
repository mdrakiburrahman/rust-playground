import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { get } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';

type Role = 'collector' | 'client' | 'support';
const role = (s: string): Role => s === 'dataflow-1' ? 'collector' : s === 'oat-client' ? 'client' : 'support';
type Member = { id: string; service: string; cpuset: string; state: string; started: string; finished: string; restarts: number };
type Plan = { project: string; masks: Record<string,string>; containers: Member[] };
function cpus(s: string): number[] {
  assert(typeof s === 'string' && s.length > 0, 'missing or empty cpuset');
  const ids: number[] = [];
  for (const field of s.trim().split(',')) {
    assert(/^\d+(-\d+)?$/.test(field), 'invalid cpuset');
    const [lo, hi = lo] = field.split('-').map(Number);
    assert(lo <= hi && hi < 4096, 'invalid cpu range');
    for (let i = lo; i <= hi; i++) ids.push(i);
  }
  assert.equal(new Set(ids).size, ids.length, 'duplicate cpu');
  return ids.sort((a,b) => a-b);
}
export function checkProject(plan: Plan, online: string) {
  const sets = { collector: cpus(plan.masks['dataflow-1']), client: cpus(plan.masks['oat-client']), support: [30,31] };
  assert.deepEqual(sets.client,[24,25,26,27,28,29]);
  const all = Object.values(sets).flat(), available = cpus(online);
  assert.equal(new Set(all).size,all.length,'role CPU masks overlap');
  assert(all.every(cpu=>available.includes(cpu)), 'offline CPU');
  for (const [name, mask] of Object.entries(plan.masks)) assert.deepEqual(cpus(mask),sets[role(name)]);
  assert.equal(new Set(plan.containers.map(c=>c.id)).size,plan.containers.length);
  for (const c of plan.containers) {
    assert(Object.hasOwn(plan.masks,c.service),'unclassified project container');
    assert.deepEqual(cpus(c.cpuset),sets[role(c.service)],'container cpuset mismatch');
    assert(['running','exited'].includes(c.state) && c.restarts === 0, 'unstable project container');
  }
  for (const s of ['router','dataflow-1','oat-client']) assert.equal(plan.containers.filter(c=>c.service===s && c.state==='running').length,1,`expected one running ${s}`);
  return sets;
}
// The diagnostic is local-only; never query an inherited remote Docker endpoint.
function engine(path: string): Promise<any> {
  return new Promise((resolve,reject)=>{
    const req=get({socketPath:'/var/run/docker.sock',path:'/v1.41'+path,signal:AbortSignal.timeout(3000)},res=>{
      const chunks: Buffer[]=[]; let bytes=0;
      res.on('error',reject); res.on('data',(b:Buffer)=>{bytes+=b.length; if(bytes>2**20) req.destroy(new Error('oversized Engine response')); else chunks.push(b);});
      res.on('end',()=>{try {assert.equal(res.statusCode,200);resolve(JSON.parse(Buffer.concat(chunks).toString()));} catch(e){reject(e);}});
    });req.on('error',reject);
  });
}
async function inspect(project:string,masks:Plan['masks']):Promise<Plan>{
  const filters=encodeURIComponent(JSON.stringify({label:[`com.docker.compose.project=${project}`]}));
  const listed=await engine(`/containers/json?all=1&filters=${filters}`);
  const containers=await Promise.all(listed.map(async({Id:id}:{Id:string}):Promise<Member>=>{
    assert(/^[a-f0-9]{64}$/.test(id));const c=await engine(`/containers/${id}/json`);
    assert.equal(c.Id,id);assert.equal(c.Config.Labels['com.docker.compose.project'],project);
    return {id,service:c.Config.Labels['com.docker.compose.service'],cpuset:c.HostConfig.CpusetCpus,state:c.State.Status,
      started:c.State.StartedAt,finished:c.State.FinishedAt,restarts:c.RestartCount};
  }));
  const plan={project,masks,containers:containers.sort((a,b)=>a.id.localeCompare(b.id))};
  const sets=checkProject(plan,await readFile('/sys/devices/system/cpu/online','utf8'));
  for(const [name,ids] of Object.entries(sets)) for(const id of ids){
    const siblings=cpus(await readFile(`/sys/devices/system/cpu/cpu${id}/topology/thread_siblings_list`,'utf8'));
    assert(siblings.includes(id) && !Object.entries(sets).some(([other,set])=>other!==name && set.some(cpu=>siblings.includes(cpu))), 'physical CPU overlap');
  }
  return plan;
}
export function cpuTicks(text:string){
  const ticks:Record<string,{busy:number;total:number}>={};
  for(const line of text.split('\n')){
    const m=/^cpu(\d+)\s+(.+)$/.exec(line);if(!m)continue;
    const f=m[2].trim().split(/\s+/).slice(0,8).map(Number); // guest and guest_nice are already in user/nice
    assert(f.length===8 && f.every(x=>Number.isSafeInteger(x)&&x>=0));
    const total=f.reduce((a,b)=>a+b,0);ticks[m[1]]={total,busy:total-f[3]-f[4]};
  }return ticks;
}
type Counter={at:number;ns:number|null};
type Sample={target:number;host_at:number;ticks:ReturnType<typeof cpuTicks>;counters:Record<string,Counter>};
const skew=1000;
async function sample(plan:Plan,target:number):Promise<Sample>{
  // Check membership ahead of each boundary. Sample just inside the last boundary so
  // a normally exiting client still has a live cgroup. Record actual times, never extrapolate.
  await sleep(Math.max(0,target-Date.now()-1500));
  assert.deepEqual(await inspect(plan.project,plan.masks),plan,'project membership or CPU configuration drift');
  await sleep(Math.max(0,target-Date.now()-150));
  const [host,rows]=await Promise.all([readFile('/proc/stat','utf8').then(text=>({host_at:Date.now(),ticks:cpuTicks(text)})),
    Promise.all(plan.containers.map(async c=>{
      if(c.state==='exited')return [c.id,{at:Date.now(),ns:null}] as const;
      const s=await engine(`/containers/${c.id}/stats?stream=false&one-shot=true`);
      const ns=s.cpu_stats?.cpu_usage?.total_usage,at=Date.parse(s.read);
      assert.equal(s.id,c.id);assert(Number.isSafeInteger(ns)&&ns>=0,'invalid Engine CPU counter');
      return [c.id,{at,ns}] as const;
    }))]);
  for(const t of [host.host_at,...rows.map(([,v])=>v.at)]) assert(Math.abs(t-target)<=skew,'CPU boundary skew exceeded');
  return {target,...host,counters:Object.fromEntries(rows)};
}
export function summarize(plan:Plan,start:Sample,end:Sample){
  const groups={collector:0,client:0,support:0};let router=0;
  const perContainer=plan.containers.map(c=>{
    const a=start.counters[c.id],b=end.counters[c.id];assert(b.at>a.at);
    let cores=0;
    if(c.state==='exited'){assert(a.ns===null && b.ns===null);}else{
      assert(a.ns!==null && b.ns!==null && b.ns>=a.ns,'missing or decreasing Engine counter');
      cores=(b.ns-a.ns)/((b.at-a.at)*1e6);
    }
    groups[role(c.service)]+=cores;if(c.service==='router')router+=cores;
    return {id:c.id,service:c.service,role:role(c.service),cores,scope:c.state==='exited'?'exited_before_window_no_cpu':'measured_counter_delta'};
  });
  const busy:Record<string,number>={};assert.deepEqual(Object.keys(start.ticks),Object.keys(end.ticks));
  for(const [id,b] of Object.entries(end.ticks)){const a=start.ticks[id],dt=b.total-a.total,db=b.busy-a.busy;
    assert(dt>0 && db>=0 && db<=dt,'invalid host CPU delta');busy[id]=db/dt;
  }
  const masks={collector:cpus(plan.masks['dataflow-1']),client:cpus(plan.masks['oat-client']),support:[30,31]};
  const sum=(ids:number[])=>ids.reduce((v,id)=>{assert(Object.hasOwn(busy,id));return v+busy[id];},0);
  const hostSetBusy = {collector:sum(masks.collector), client:sum(masks.client), support:sum(masks.support)};
  const residual=hostSetBusy.collector-groups.collector;
  const limited=(['client','support'] as const).filter(k=>groups[k]>=0.9*masks[k].length);
  return {start,end,per_container:perContainer,group_cores:groups,router_cores:router,host_busy_per_cpu:busy,host_busy_cores_by_set:hostSetBusy,
    non_collector_busy_on_collector_cpus:{estimated_cores:residual,status:residual<0?'invalid_negative_estimate':'unattributed_estimate',
      scope:'host busy less pinned collector cgroup; includes unpinned dockerd/containerd/docker-proxy/devcontainer, kernel and other work; no per-daemon attribution'},
    limited_by:limited,collector_cpu_evidence:!limited.length && residual>=0,host_isolated:false};
}
async function main(){const [op,arg,b,c]=process.argv.slice(2);
  if(op==='inspect'){console.log(JSON.stringify(await inspect(arg,JSON.parse(await readFile(b,'utf8')))));return;}
  assert.equal(op,'window');const plan:Plan=JSON.parse(arg);const lo=Number(b),hi=Number(c);assert(Number.isSafeInteger(lo)&&Number.isSafeInteger(hi)&&hi>lo);
  const start=await sample(plan,lo),end=await sample(plan,hi);console.log(JSON.stringify(summarize(plan,start,end)));
}
if(process.argv[1] && import.meta.url===pathToFileURL(process.argv[1]).href)main().catch(e=>{console.error(e);process.exitCode=1;});

// Recompute the verdict from the retained counters; never trust a summary flag in a log.
export function cpuWindowReceipt(rawPlan:unknown,raw:unknown,schedule:Record<string,unknown>){
  const plan=rawPlan as Plan, window=raw as ReturnType<typeof summarize>;
  assert.equal(window.start.target,schedule.measurement_start_unix_ms);
  assert.equal(window.end.target,schedule.measurement_end_unix_ms);
  for(const s of [window.start,window.end]){
    assert(Math.abs(s.host_at-s.target)<=skew);
    assert.deepEqual(Object.keys(s.counters).sort(),plan.containers.map(c=>c.id).sort());
    for(const c of Object.values(s.counters))assert(Math.abs(c.at-s.target)<=skew);
  }
  const computed=summarize(plan,window.start,window.end);assert.deepEqual(computed,window);return computed;
}
