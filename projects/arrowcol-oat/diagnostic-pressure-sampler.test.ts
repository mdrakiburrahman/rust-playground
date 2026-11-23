import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { channel } from 'node:diagnostics_channel';
import { mkdir, readFile, rm } from 'node:fs/promises';
import { createServer, type RequestListener } from 'node:http';
import { join } from 'node:path';
import { test } from 'node:test';
import { collect, record, validateTrace } from './diagnostic-pressure-sampler.js';

async function fixture(handle:RequestListener, run:(url:URL)=>Promise<void>) {
  const server=createServer(handle); await new Promise<void>(resolve=>server.listen(0,'127.0.0.1',resolve));
  try { const addr=server.address(); assert(addr && typeof addr==='object'); await run(new URL(`http://127.0.0.1:${addr.port}/v1/pressure`)); }
  finally {server.closeAllConnections(); await new Promise<void>((ok,fail)=>server.close(e=>e?fail(e):ok()));}
}

test('real pre-header abort retains one failed attempt and no fabricated headers', async()=>{
  let hits=0; await fixture(()=>{hits++;},async url=>{
    const e=await collect(url,1); assert.equal(hits,1); assert.equal(e.error_count,1); assert.equal(e.max_ms,null);
    const s=e.observations[0]!; assert.equal(s.error,'TimeoutError'); assert.equal(s.status,null);
    assert.notEqual(s.trace.offset_ms.dispatch,null); assert.equal(s.trace.offset_ms.headers,null); assert.equal(s.body_complete_ms,null);
    assert(s.elapsed_ms>=2900); assert(s.observer.loop_delay_samples!>0);
  });
});
test('real truncated response distinguishes delivered headers from body completion', async()=>{
  await fixture((_req,res)=>{res.writeHead(200,{'Content-Length':'9000'}); res.write('private-fixture-body'); setTimeout(()=>res.destroy(),50);},async url=>{
    const e=await collect(url,1), s=e.observations[0]!; assert.equal(e.error_count,1); assert.equal(s.status,200);
    assert.notEqual(s.trace.offset_ms.headers,null); assert.notEqual(s.fetch_headers_ms,null); assert.equal(s.trace.offset_ms.trailers,null); assert.equal(s.body_complete_ms,null);
    assert(!JSON.stringify(e).includes('private-fixture-body'));
  });
});
test('successful real requests agree with independent server sockets; evidence includes exact observer bytes',async()=>{
  const ports:number[]=[];
  await fixture((req,res)=>{ports.push(req.socket.remotePort!); res.setHeader('X-private','header-secret-fixture'); res.end('body-secret-fixture');},async url=>{
    const dir=join('out','diagnostic-observer-test-'+randomUUID()); await mkdir(dir,{recursive:true});
    try {const output=join(dir,'result.json'); assert.equal(await record(url,3,output),true);
      const text=await readFile(output,'utf8'), e=JSON.parse(text); assert(!text.includes('secret-fixture')); assert.equal(e.success_count,ports.length); assert(ports.length>=2);
      const ids=new Map<number,number>(); for(const [i,s] of e.observations.entries()){
        assert.equal(s.trace.validity,'observed'); assert.equal(s.trace.local_port,ports[i]); assert.notEqual(s.body_complete_ms,null);
        const old=ids.get(ports[i]!); if(old!==undefined){assert.equal(s.trace.connection_use,'reused');assert.equal(s.trace.connection_id,old);} else assert.equal(s.trace.connection_use,'new');
        ids.set(ports[i]!,s.trace.connection_id);
      }
      const copy=await readFile(join(dir,e.observer_source.file)); assert.deepEqual(copy,await readFile(new URL('./diagnostic-pressure-sampler.ts',import.meta.url)));
      assert.equal(e.observer_source.sha256,createHash('sha256').update(copy).digest('hex'));
    } finally {await rm(dir,{recursive:true,force:true});}
  });
});
test('contradictory dispatch evidence is unknown, never changes a successful HTTP verdict',async()=>{
  const c=channel('undici:client:sendHeaders');let injecting=false;
  const duplicate=(m:unknown)=>{if(!injecting){injecting=true;try{c.publish(m);}finally{injecting=false;}}};
  c.subscribe(duplicate);
  try {await fixture((_req,res)=>res.end('ok'),async url=>{const e=await collect(url,1);assert.equal(e.success_count,1);assert.equal(e.error_count,0);assert.equal(e.observations[0]!.trace.validity,'unknown_inconsistent');});}
  finally{c.unsubscribe(duplicate);}
});
test('missing channels and impossible order cannot provide a verified phase sequence',()=>{
  const empty={create:null,dispatch:null,headers:null,trailers:null,error:null};
  assert.equal(validateTrace(empty,12,true,true,false),'unknown_incomplete');
  assert.equal(validateTrace({...empty,create:1,dispatch:3,headers:2},12,true,false,false),'unknown_inconsistent');
});

test('concurrent fetch to the same endpoint is not attributed to the sampler',async()=>{
  let arrived!:()=>void; const first=new Promise<void>(ok=>{arrived=ok;});
  await fixture((_req,res)=>{arrived();setTimeout(()=>res.end('ok'),50);},async url=>{
    const collecting=collect(url,1); await first; await (await fetch(url)).arrayBuffer();
    const e=await collecting; assert.equal(e.error_count,0);assert(e.observations.every(s=>s.trace.validity==='observed'));
  });
});
