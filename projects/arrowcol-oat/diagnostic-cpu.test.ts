import assert from 'node:assert/strict';
import test from 'node:test';
import {checkProject,cpuTicks,summarize,cpuWindowReceipt} from './diagnostic-cpu.js';
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
