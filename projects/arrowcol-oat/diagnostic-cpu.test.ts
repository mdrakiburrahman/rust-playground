import assert from 'node:assert/strict';
import test from 'node:test';
import {checkProject,cpuTicks,summarize,cpuWindowReceipt,readEngineCounter} from './diagnostic-cpu.js';
const masks:Record<string,string>={'dataflow-1':'0','oat-client':'24-29',router:'30,31',bootstrap:'30,31',sidecar:'30,31'};
const plan=()=>({project:'diagnostic-fixture',masks:{...masks},containers:Object.entries(masks).map(([service,cpuset],i)=>({service,cpuset,image:`sha256:${"a".repeat(64)}`,id:String(i),state:service==='bootstrap'?'exited':'running',started:'a',finished:'b',restarts:0}))});
test('readback includes exited containers, rejects absent, empty, mismatched and unclassified assignments',()=>{
  checkProject(plan(),'0-31');for(const edit of [ (c:any)=>delete c.cpuset,(c:any)=>{c.cpuset='';},(c:any)=>{c.cpuset='24-29';},(c:any)=>{c.service='unresolved';}]){
    const p=plan();edit(p.containers.find(c=>c.state==='exited'));assert.throws(()=>checkProject(p,'0-31'));
  }
  const p=plan();p.masks['dataflow-1']='0,31';assert.throws(()=>checkProject(p,'0-31'),/overlap/);assert.throws(()=>checkProject(plan(),'0-30'),/offline/);
});
test('CPU deltas include router once in shared support, detect inclusive saturation and retain negative residual',()=>{
  const p=plan();const sample=(t:number)=>({target:t,host_at:t,ticks:cpuTicks([0,24,25,26,27,28,29,30,31].map(i=>`cpu${i} ${t} 0 0 ${t} 0 0 0 0 99 0`).join('\n')),counters:Object.fromEntries(p.containers.map(c=>[c.id,{at:t,ns:c.state==='exited'?null:t*1e6*({'dataflow-1':1,'oat-client':5.4,sidecar:1,router:0.8}[c.service]??0)}]))});
  const start=sample(0),end=sample(1000);const w=summarize(p,start,end);
  assert.equal(w.router_cores,0.8);assert.equal(w.group_cores.support,1.8);assert.deepEqual(w.limited_by,['client','support']);assert.equal(w.collector_cpu_evidence,false);
  assert.equal(w.non_collector_busy_on_collector_cpus.estimated_cores,-0.5);assert.equal(w.non_collector_busy_on_collector_cpus.status,'invalid_negative_estimate');
  const schedule={measurement_start_unix_ms:0,measurement_end_unix_ms:1000};cpuWindowReceipt(p,w,schedule);assert.throws(()=>cpuWindowReceipt(p,{...w,collector_cpu_evidence:true},schedule));
});

test('binds Docker 27 one-shot stats without an echoed ID to an inspected full-ID request',async()=>{
  const p=plan(),c={...p.containers[0],id:'a'.repeat(64)};
  const inspected={Id:c.id,Config:{Labels:{'com.docker.compose.project':p.project,'com.docker.compose.service':c.service}},
    State:{Status:'running',StartedAt:c.started},RestartCount:0};
  // Relevant fields from big-3 Engine 27.5.1, /v1.41, stream=false&one-shot=true.
  const stats={read:'2026-11-23T06:34:32.1355767Z',cpu_stats:{cpu_usage:{total_usage:45300000}}};
  const probe=(identity:object,body:object)=>readEngineCounter(p.project,c,async path=>{
    if(path===`/containers/${c.id}/json`)return identity;
    assert.equal(path,`/containers/${c.id}/stats?stream=false&one-shot=true`);return body;
  });
  assert.deepEqual(await probe(inspected,stats),{at:Date.parse(stats.read),ns:45300000});
  for(const Id of [undefined,'b'.repeat(64)])await assert.rejects(probe({...inspected,Id},stats),/identity/);
  await assert.rejects(probe({...inspected,RestartCount:1},stats),/lifetime/);
  await assert.rejects(probe(inspected,{...stats,id:'b'.repeat(64)}),/identity/);
  await assert.rejects(probe(inspected,{...stats,read:null}),/sample time/);
  await assert.rejects(probe(inspected,{...stats,cpu_stats:{}}),/CPU counter/);
});

test('e75 pressure receipt accepts serialization drift in derived values only',()=>{
  const rows: [string,number,number,number,number][]=[["arrowcol-catalog",1795416733475,243614000,1795416793471,364835000],["lgtm",1795416733472,20664520000,1795416793475,24717454000],["dataflow-1",1795416733473,35021255000,1795416793474,170850559000],["router",1795416733476,15183032000,1795416793471,73956466000],["kusto",1795416733476,9352412000,1795416793472,12392789000],["delta-maintenance",1795416733474,243891000,1795416793473,491635000],["sqlserver",1795416733477,19042948000,1795416793473,37810313000],["oat-client",1795416733478,20526112000,1795416793471,87906615000]];
  const p=plan();p.containers=rows.map(([service],i)=>({...p.containers[0],id:String(i),state:'running',service}));
  const boundary=(end:boolean)=>{const target=end?1795416793616:1795416733616;return {target,host_at:target,ticks:cpuTicks([0,24,25,26,27,28,29,30,31].map(i=>'cpu'+i+' '+target+' 0 0 0 0 0 0 0').join('\n')),counters:Object.fromEntries(rows.map((r,i)=>[i,{at:end?r[3]:r[1],ns:end?r[4]:r[2]}]))};};
  const window=summarize(p,boundary(false),boundary(true)),schedule={measurement_start_unix_ms:window.start.target,measurement_end_unix_ms:window.end.target};
  assert.equal(window.group_cores.support,1.4168206119208677);assert.equal(window.router_cores,0.9796388699058255);
  window.group_cores.support=1.4168206119208675;window.router_cores=0.9796388699058256;
  cpuWindowReceipt(p,window,schedule);
  cpuWindowReceipt(p,{...window,router_cores:window.router_cores*(1+0.9e-12)},schedule);
  assert.throws(()=>cpuWindowReceipt(p,{...window,router_cores:window.router_cores*(1+1.1e-12)},schedule));
  assert.throws(()=>cpuWindowReceipt(p,{...window,limited_by:['support']},schedule));
  window.start.counters['0'].ns=0.25;assert.throws(()=>cpuWindowReceipt(p,window,schedule));
});
