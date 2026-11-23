import assert from 'node:assert/strict';
import test from 'node:test';
import {arms, armName, armWitness, type Arm} from './diagnostic-arm.js';

function fixture(name: Arm) {
  const {cores, flushes, commitFiles, collector} = arms[name];
  const cpus = Array.from({length: cores}, (_, i) => i);
  const cap = 64 * 1024**3;
  const tuning = {runtime:{cpu_workers:cores}, batching:{max_concurrent_flushes:flushes}, commit:{max_files_per_commit:commitFiles}, pressure:{rss_limit_bytes:cap}};
  const config = {platform:{local:{collector_image_override:{...collector}, expected_image_ids:{collector:collector.image_id}, collector_cpus:cpus, collector_memory_limit_bytes:cap}}, tunables:{collector:tuning}, dfe_worker_concurrency:[cores], overload_guard:{enabled:true,memory_signal:'cgroup_memory'}, measure_seconds:150};
  const masks = {'dataflow-1':cpus.join(','), router:'30,31'};
  const cpu = {engine_num_cores:cores, collector:{requested:cpus}, collector_memory_limit_bytes:cap, project_readback:{masks, containers:Object.entries(masks).map(([service,cpuset])=>({service,cpuset,image:service==='router'?'router-image':collector.image_id,state:'running',restarts:0}))}};
  const observed = {status:'verified_before_schedule',collector_image_id:collector.image_id,collector:structuredClone(tuning),client:{workers:14}};
  return {config,cpu,observed, witness:()=>armWitness(name,config,cpu,observed,collector.source_revision,{boot:'same'})};
}

test('only the declared effective diff reconstructs the candidate baseline', () => {
  const baseline = fixture('candidate-baseline').witness().baseline_sha256;
  for (const name of Object.keys(arms)) assert.equal(fixture(armName(name)).witness().baseline_sha256,baseline,name);
  const writer = fixture('writer-flush-6');
  writer.config.tunables.collector.batching.max_concurrent_flushes = 12;
  assert.throws(writer.witness);
  const cpu = fixture('cpu-16');
  cpu.cpu.engine_num_cores = 8;
  assert.throws(cpu.witness);
  const main = fixture('software-main');
  main.config.platform.local.collector_image_override.image_id = arms['candidate-baseline'].collector.image_id;
  assert.throws(main.witness);
  assert.throws(()=>armName('toString'));
  for (const drift of [
    (f:ReturnType<typeof fixture>)=>{f.config.measure_seconds++},
    (f:ReturnType<typeof fixture>)=>{f.observed.client.workers++},
    (f:ReturnType<typeof fixture>)=>{f.cpu.project_readback.masks.router='24'},
    (f:ReturnType<typeof fixture>)=>{f.observed.collector.pressure.rss_limit_bytes++},
  ]) {const f=fixture('commit-files-64');drift(f);assert.notEqual(f.witness().baseline_sha256,baseline);}
});

test('live treatment cannot admit without the actual matching baseline receipt and profile', async () => {
  const {checkReferenceCopies} = await import('./diagnostic-arm.js');
  const {createHash} = await import('node:crypto');
  const hash = (b:Buffer)=>createHash('sha256').update(b).digest('hex');
  const base = fixture('candidate-baseline').witness(), treatment = fixture('cpu-16').witness();
  const proof = {reference:{mode:'first',arm:'candidate-baseline'},arm_contract:base};
  const receipt = Buffer.from(JSON.stringify({run_id:'ref',snapshot_reconciled:true,collector_cpu_evidence:true,diagnostic_admission:proof,pre_schedule_admission:proof}));
  const profile = Buffer.from(JSON.stringify({reference_receipt_sha256:hash(receipt),reference_run_id:'ref'}));
  const ref={mode:'matched',arm:{name:'cpu-16',baseline_sha256:base.baseline_sha256},run_id:'ref',profile_sha256:hash(profile),receipt_sha256:hash(receipt)};
  checkReferenceCopies('cpu-16',treatment,ref,profile,receipt);
  assert.throws(()=>checkReferenceCopies('cpu-16',treatment,ref,profile,Buffer.from('{}')));
  assert.throws(()=>checkReferenceCopies('cpu-16',{...treatment,baseline_sha256:'a'.repeat(64)},ref,profile,receipt));
});
