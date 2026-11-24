import assert from 'node:assert/strict';
import test from 'node:test';
import {inflateSync} from 'node:zlib';
import {mkdir,writeFile,readFile,rm} from 'node:fs/promises';
import {randomUUID} from 'node:crypto';
import {join} from 'node:path';
import {chart,cpuSecondsPerMillionRows} from './chart.ts';
import {collectorMetrics,type Values} from './metrics.ts';
import {parseDevices,parseTask,parseTaskCounters,sampleLinux,systemInfo} from './linux.ts';
import {clientEvidence,committedEvidence,commitLogEvidence} from './evidence.ts';
import {reduce,physicalEvidence,isolationEvidence,type Header,type Sample} from './reduce.ts';

const h: Header={schema:'collector-isolation/v1',runId:'fixture',level:'L5',startMs:10000,endMs:14000,intervalMs:1000,system:{pid:7,tickHz:100,bootId:'fixture',interfaces:['lo'],disks:['vda']}};
const obs=<T>(ms:number,value:T)=>({beginMs:ms-2,endMs:ms+2,value});
const samples=():Sample[]=>[0,1,4].map((t,i)=>{
  const ms=h.startMs+t*1000,ticks=25*t,held=[0,8,0][i];
  const p:Values={received_rows_total:100+5*t,'reserved.global.bytes':held,'duration.commit.sum':t*.25,'groupSeconds.flush_slot':t*3,'groupSeconds.receipt_wait':t*1.5,'acquire_seconds_total.table.bytes':t*.75,'acquire_attempts_total.table.bytes':t*4,publication_backlog_available:1,
    'backlog.bytes':held, publication_settled_bytes_total:10*t,publication_settled_byte_seconds_total:5*t};
  const linux={process:{name:'collector',startTicks:'80',cpuTicks:ticks},netns:'net:[7]',threads:t===1?null:{
    7:{name:'worker',startTicks:'80',cpuTicks:ticks},9:{name:'recycled',startTicks:String(t),cpuTicks:40}},values:{cpuTicks:ticks,userTicks:20*t,systemTicks:5*t,minorFaults:100*t,majorFaults:0,'net.lo.rxBytes':11*t,'disk.vda.writeSectors':4*t}};
  return {targetMs:ms,wallClockOffsetMs:0,prom:obs(ms,p),linux:obs(ms,linux)};
});
test('duration-weighted backlog and byte-weighted residence share one successful-settlement population',()=>{
  const r=reduce(h,samples()); assert.equal(r.collector.receivedLogical.rowsPerSecond,5);assert.equal(r.collector.completedCommitBusyFraction,.25);
  assert.equal(r.littleLaw.sampledLogicalBytes?.timeWeightedMean,4);assert.equal(r.littleLaw.byteWeightedResidenceSeconds,.5);
  assert.equal(r.littleLaw.impliedBytesPerSecond,8);assert.equal(r.littleLaw.observedSettledBytesPerSecond,10);
  assert.equal(r.collector.flush.receiptWaitFractionOfCompletedSlotTime,.5);assert.equal(r.collector.capacityAcquisitions['table.bytes'].acquireSecondsPerElapsedSecond,.75);
  assert.equal(r.linux.processCores,.25);assert.equal(r.linux.scheduler.userCores,.2);assert.equal(r.linux.scheduler.systemCores,.05);assert.equal(r.linux.scheduler.systemFraction,.2);assert.equal(r.linux.scheduler.minorFaultsPerSecond,100);assert.equal(r.linux.scheduler.majorFaultsPerSecond,0);assert.equal(r.linux.scheduler.voluntarySwitchesPerSecond,null);assert.equal(r.linux.threads.matched?.length,1);assert.equal(r.linux.interfaces.lo?.rxBytesPerSecond,11);assert.equal(r.linux.disks.vda.writeBytesPerSecond,2048);
});
test('resets, unavailable backlog, late edge, and recycled process never fabricate an interval',()=>{
  const s=samples();s[1].prom.value!.received_rows_total=3;s[1].prom.value!.publication_backlog_available=0;
  assert.equal(reduce(h,s).collector.receivedLogical.rowsPerSecond,null);assert.equal(reduce(h,s).littleLaw.impliedBytesPerSecond,null);
  s[0].prom=obs(h.startMs+900,s[0].prom.value);assert.equal(reduce(h,s).window.collector.boundaryAligned,false);assert.equal(reduce(h,s).collector.completedCommitBusyFraction,null);
  s[2].linux.value!.process.startTicks='81';assert.throws(()=>reduce(h,s),/PID/);
});
test('NaN, ambiguous series and absent device counters stay unknown',()=>{
  const v=collectorMetrics('arrowcol_publication_backlog_available 0\narrowcol_publication_backlog_bytes NaN\narrowcol_received_rows_total 0\narrowcol_pipeline_stage_duration_seconds_sum{stage="commit",run="a"} 2\narrowcol_pipeline_stage_duration_seconds_sum{stage="commit",run="b"} 3');
  assert.equal(v['backlog.bytes'],null);assert.equal(v.received_rows_total,0);assert.equal(v['duration.commit.sum'],null);
  assert.equal(parseDevices(' lo: 2 1 0 1 0 0 0 0 4 3 2 1 0 0 0 0',null,h.system)['net.lo.txErrors'],2);assert.equal(parseDevices(null,null,h.system)['disk.vda.writeSectors'],null);
  assert.throws(()=>parseTask('7 (not valid)'),/Invalid/);
  assert.deepEqual(parseTaskCounters('7 (a b) S 1 7 7 0 -1 4194560 41 3 2 1 13 5 0 0 20 0 9 0 80'),{userTicks:13,systemTicks:5,minorFaults:41,majorFaults:2});
});
test('linux sampler can read one real snapshot without assuming a physical NIC or disk',async()=>{
  const s=await systemInfo(process.pid,[],[]),value=await sampleLinux(s,true);assert.ok(Object.keys(value.threads!).length>0);assert.ok(value.process.startTicks.length);assert.ok(value.values.cpuTicks!==null);assert.ok((value.values.userTicks??-1)>=0&&(value.values.minorFaults??-1)>0&&(value.values.voluntarySwitches??-1)>=0);
});
test('client exact send starts never fall back to lifetime attempts, require complete distinct terminal roster and receipt kind',()=>{
  const window={start_unix_ms:h.startMs,end_unix_ms:h.endMs,completed_rows:20,offered_rows:40};
  const report={phase:'completed',measurement_window:window,workers:[{attempted_rows:999,durable_streams:0,best_effort_streams:0,diagnostic_streams:7}]};
  const diag={...h,level:'L0' as const}, one=[{id:'a',report}]; const c=clientEvidence(one,1,diag,'offered_rows');
  assert.equal(c.offeredToCompletedRatio,2);assert.equal(c.diagnosticRowsPerSecond,5);assert.equal(c.acceptedRowsPerSecond,null);
  assert.equal(clientEvidence(one,1,diag,'attempted_rows').offeredRowsPerSecond,null);assert.throws(()=>clientEvidence(one,2,diag,'offered_rows'),/roster/);
  assert.throws(()=>clientEvidence(one,1,h,'offered_rows'),/receipt type/);
  assert.throws(()=>clientEvidence(one,1,{...diag,startMs:h.startMs-1},'offered_rows'),/window/);
});
test('matches isolated snapshots to actual source window, physical bytes and measured ingress only',()=>{
  const header={...h,level:'L2' as const};const s=samples();
  s.forEach((o,i)=>{o.isolation=obs(o.targetMs,{ingress:{scope:'transport_poll_excluding_async_wait',rows:i*50,ipc_body_bytes:i*80},physical:{level:'L2',scope:'per_batch_physical_stage',object_store:null,encode:{rows:i*50,bytes:i*20},fsync:null,readback:null,upload:null}})});
  const result=isolationEvidence(s,header)!;assert.equal(result.ingress.rowsPerSecond,25);assert.equal(result.ingress.ipcBodyBytesPerSecond,40);assert.equal(result.physical!.stages.encode.parquetBytesPerSecond,10);assert.equal(result.physical!.stages.fsync.rowsPerSecond,null);
  assert.throws(()=>physicalEvidence({run_id:'other',level:'L2',start_unix_ms:10000,end_unix_ms:14000,samples:[]},header),/identity/);
  assert.throws(()=>committedEvidence({scope:'process_cumulative'},h),/scope/);
});
test('offline PNG includes measured panels; no output is overwritten',async()=>{
  const dir=join(process.cwd(),'.isolation-test-'+randomUUID());await mkdir(dir);try{
    const out=join(dir,'chart.png');const r={...reduce(h,samples()),clients:null,physical:null,committed:null};await chart([r],out,'resources');
    const b=await readFile(out);assert.equal(b.toString('hex',0,8),'89504e470d0a1a0a');
    const at=b.indexOf('IDAT'),size=b.readUInt32BE(at-4);assert.ok(b.includes(Buffer.from('collector-isolation\0')));const pixels=inflateSync(b.subarray(at+4,at+4+size));
    assert.equal(pixels.length,(b.readUInt32BE(16)*3+1)*b.readUInt32BE(20));assert.ok(pixels.includes(81));await assert.rejects(chart([r],out,'resources'),/EEXIST/);
  }finally{await rm(dir,{recursive:true,force:true});}
});
test('commit log deduplicates a version, excludes upper bound, and drives L5 CPU denominator independently of admission',async()=>{
  const dir=join(process.cwd(),'.isolation-test-'+randomUUID());await mkdir(dir);
  try {
    const f=join(dir,'collector.log');
    const event=(ms:number,version:number,rows:number)=>'arrowcol_data_commit '+JSON.stringify({schema_version:1,outcome:'committed',table_id:'one',finished_at_unix_ms:ms,delta_version:version,rows,files:1,input_bytes:100,parquet_bytes:10});
    await writeFile(f,[event(9999,1,200),event(10000,2,2),event(10000,2,2),event(13999,3,8),event(14000,4,999)].join('\n'));
    const committed=await commitLogEvidence(f,h);const r={...reduce(h,samples()),clients:null,physical:null,committed};
    assert.ok('rows' in committed);assert.equal(committed.rows,10);assert.equal(cpuSecondsPerMillionRows(r),100000);assert.equal(cpuSecondsPerMillionRows({...r,committed:null}),null);
    const reports=Array.from({length:12},(_,i)=>({...r,runId:'cell-'+i,declaredCores:i%2===0?4:8,level:`L${Math.floor(i/2)}` as Header['level']}));
    const png=join(dir,'paired.png');await chart(reports,png,'waterfall');const bytes=await readFile(png);assert.ok(bytes.includes(Buffer.from('"runId":"cell-11"')));
    await writeFile(f,'\n'+event(10000,2,101),{flag:'a'});await assert.rejects(commitLogEvidence(f,h),/Conflicting/);
  }finally{await rm(dir,{recursive:true,force:true});}
});
