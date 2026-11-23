import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {dirname,join,relative,resolve} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';

export const candidate={source_revision:'ad83c6376086f020a7d4e8cffd66994eedb537fe',image_id:'sha256:66c9d807973182594d6244a93f412a2b2c1da8b3bed30cfa528610ae50d67e68'};
export const mainCollector={source_revision:'3630d3e4301bc3ba8f9f69a308d077cebee6d12a',image_id:'sha256:23a418cf48ab1765b6ab7b1a1bd647ee2486357db37865027b60df1ca4ee4c49'};
// Closed manifest. No request-supplied JSON paths, pairs, values, or exclusions.
export const arms={
  'candidate-baseline':{kind:'reference',cores:8,flushes:12,commitFiles:128,batchWindow:'2s',collector:candidate},
  'software-main':{kind:'software-comparison',cores:8,flushes:12,commitFiles:128,batchWindow:'2s',collector:mainCollector},
  'cpu-16':{kind:'collector-cpu',cores:16,flushes:12,commitFiles:128,batchWindow:'2s',collector:candidate},
  'cpu-24':{kind:'collector-cpu',cores:24,flushes:12,commitFiles:128,batchWindow:'2s',collector:candidate},
  'writer-flush-6':{kind:'concurrent-flushes',cores:8,flushes:6,commitFiles:128,batchWindow:'2s',collector:candidate},
  'commit-files-64':{kind:'commit-files',cores:8,flushes:12,commitFiles:64,batchWindow:'2s',collector:candidate},
  'commit-window-1s':{kind:'commit-cadence',cores:8,flushes:12,commitFiles:128,batchWindow:'1s',collector:candidate},
} as const;
export type Arm=keyof typeof arms;
export function armName(v:unknown):Arm {assert(typeof v==='string' && Object.hasOwn(arms,v),'unknown diagnostic arm');return v as Arm;}
const mask=(n:number)=>Array.from({length:n},(_,i)=>i);
const sha=(v:string|Buffer)=>createHash('sha256').update(v).digest('hex');
function canonical(v:any):any {return Array.isArray(v)?v.map(canonical):v!==null&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;}

// Compare the *whole* effective config and inspected service roster after reversing
// precisely the declared treatment. Treat run-local destinations as relative addresses.
export function armWitness(name:Arm, raw:any, cpu:any, observed:any, source:string, context:object){
  const arm=arms[armName(name)],base=arms['candidate-baseline'];
  const config=structuredClone(raw),local=config.platform.local,t=config.tunables.collector;
  assert.deepEqual(local.collector_image_override,arm.collector);
  assert.equal(local.expected_image_ids.collector,arm.collector.image_id);
  assert.equal(observed.collector_image_id,arm.collector.image_id);assert.equal(source,arm.collector.source_revision);
  assert.deepEqual(local.collector_cpus,mask(arm.cores));assert.deepEqual(config.dfe_worker_concurrency,[arm.cores]);
  assert.equal(cpu.engine_num_cores,arm.cores);assert.deepEqual(cpu.collector.requested,mask(arm.cores));
  const effective=structuredClone(observed.collector);
  assert(observed.client && observed.status === "verified_before_schedule", "missing live readback");
  for(const c of [t,effective]){
    assert.equal(c.runtime.cpu_workers,arm.cores);assert.equal(c.batching.max_concurrent_flushes,arm.flushes);
    assert.equal(c.commit.batch_window,arm.batchWindow);
    assert.equal(c.commit.max_files_per_commit,arm.commitFiles);
  }
  assert.equal(config.overload_guard.memory_signal,'cgroup_memory');assert.equal(config.overload_guard.enabled,true);
  assert.equal(local.collector_memory_limit_bytes,64*1024**3);assert.equal(t.pressure.rss_limit_bytes,local.collector_memory_limit_bytes);
  assert.equal(cpu.collector_memory_limit_bytes,local.collector_memory_limit_bytes);
  const roster=cpu.project_readback.containers.map((c:any)=>{
    const collector=c.service==='dataflow-1';if(collector){assert.equal(c.image,arm.collector.image_id);assert.equal(c.cpuset,mask(arm.cores).join(','));}
    return [c.service,c.state,collector?base.collector.image_id:c.image,collector?mask(8).join(','):c.cpuset,c.restarts];
  }).sort((a:any[],b:any[])=>JSON.stringify(a).localeCompare(JSON.stringify(b)));
  const masks={...cpu.project_readback.masks};assert.equal(masks['dataflow-1'],mask(arm.cores).join(','));masks['dataflow-1']=mask(8).join(',');
  local.collector_image_override=base.collector;local.expected_image_ids.collector=base.collector.image_id;
  local.collector_cpus=mask(8);config.dfe_worker_concurrency=[8];t.runtime.cpu_workers=8;
  t.batching.max_concurrent_flushes=base.flushes;t.commit.max_files_per_commit=base.commitFiles;
  t.commit.batch_window=base.batchWindow;
  effective.commit.batch_window=base.batchWindow;
  effective.runtime.cpu_workers=base.cores;
  effective.batching.max_concurrent_flushes=base.flushes;
  effective.commit.max_files_per_commit=base.commitFiles;
  const normalized=canonical({config,roster,masks,context,effective,client:observed.client});
  return {name,manifest:arm,baseline_sha256:sha(JSON.stringify(normalized))};
}
// Re-open the run-owned copies in the runner process. A reference digest in the
// environment alone is not evidence that a baseline exists.
export function checkReferenceCopies(name: Arm, witness: ReturnType<typeof armWitness>, ref: any, profileBytes: Buffer, receiptBytes: Buffer) {
  assert.equal(ref.mode, 'matched');
  assert.equal(ref.arm.name, name);
  assert.equal(sha(profileBytes), ref.profile_sha256);
  assert.equal(sha(receiptBytes), ref.receipt_sha256);
  const profile = JSON.parse(profileBytes.toString('utf8'));
  const receipt = JSON.parse(receiptBytes.toString('utf8'));
  assert.equal(profile.reference_receipt_sha256, ref.receipt_sha256);
  assert.equal(profile.reference_run_id, ref.run_id);
  assert.equal(receipt.run_id, ref.run_id);
  assert.equal(receipt.snapshot_reconciled, true);
  assert.equal(receipt.collector_cpu_evidence, true);
  const base = {name: 'candidate-baseline', manifest: arms['candidate-baseline'], baseline_sha256: witness.baseline_sha256};
  assert.equal(ref.arm.baseline_sha256, base.baseline_sha256);
  for (const proof of [receipt.diagnostic_admission, receipt.pre_schedule_admission]) {
    assert.deepEqual(proof.reference, {mode:'first', arm:'candidate-baseline'});
    assert.deepEqual(proof.arm_contract, base);
  }
}
function run(){
  const [name,raw,cpu,observed,source]=process.argv.slice(2),config=JSON.parse(raw);
  const root=dirname(config.platform.compose_state_root),under=(p:string)=>{const r=relative(root,p);assert(r && !r.startsWith('..') && resolve(root,r)===p,'outside named diagnostic run');return r;};
  const output=dirname(fileURLToPath(config.result_url));
  const override=config.platform.compose_override_file;
  config.platform.compose_state_root=under(config.platform.compose_state_root);config.platform.compose_override_file=under(override);
  config.platform.local.snapshot_root=under(config.platform.local.snapshot_root);
  for(const key of ['result_url','website_url'])config[key]=under(fileURLToPath(config[key]));
  const context={host_boot:readFileSync('/proc/sys/kernel/random/boot_id','utf8').trim(),runtime:process.cwd(),parent:dirname(root),override_sha256:sha(readFileSync(override))};
  const arm=armName(name);
  const witness=armWitness(arm,config,JSON.parse(cpu),JSON.parse(observed),source,context);
  const ref=JSON.parse(process.env.OAT_DIAGNOSTIC_REFERENCE ?? 'null');
  if(arm === 'candidate-baseline') assert.deepEqual(ref,{mode:'first',arm});
  else checkReferenceCopies(arm,witness,ref,
    readFileSync(join(output,'diagnostic-comparison-profile.json')),
    readFileSync(join(output,'diagnostic-reference-receipt.json')));
  console.log(JSON.stringify(witness));
}
if(process.argv[1] && import.meta.url===pathToFileURL(process.argv[1]).href)run();
