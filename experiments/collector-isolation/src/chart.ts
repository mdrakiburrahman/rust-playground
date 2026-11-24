import {writeFile} from 'node:fs/promises';
import {deflateSync} from 'node:zlib';
import {finite} from './metrics.ts';
import type {clientEvidence, committedEvidence} from './evidence.ts';
import type {physicalEvidence, reduce} from './reduce.ts';

type Report = ReturnType<typeof reduce> & {clients: ReturnType<typeof clientEvidence> | null; physical: ReturnType<typeof physicalEvidence> | null; committed: ReturnType<typeof committedEvidence> | null};
// Minimal bitmap labels keep offline rendering identical on headless hosts without fonts or browsers.
const glyph: Record<string,string> = {
 A:'0e11111f111111',B:'1e11111e11111e',C:'0f10101010100f',D:'1e11111111111e',E:'1f10101e10101f',F:'1f10101e101010',G:'0f10101711110f',H:'1111111f111111',I:'1f04040404041f',J:'0702020212120c',K:'11121418141211',L:'1010101010101f',M:'111b1515111111',N:'11191915131311',O:'0e11111111110e',P:'1e11111e101010',Q:'0e11111115120d',R:'1e11111e141211',S:'0f10100e01011e',T:'1f040404040404',U:'1111111111110e',V:'11111111110a04',W:'1111111515150a',X:'11110a040a1111',Y:'11110a04040404',Z:'1f01020408101f',
 '0':'0e11131519110e','1':'040c040404040e','2':'0e11010204081f','3':'1e01010601011e','4':'02060a121f0202','5':'1f10101e01011e','6':'0e10101e11110e','7':'1f010204080808','8':'0e11110e11110e','9':'0e11110f01010e','.':'00000000000c0c',',':'00000000000c08','/':'01010204081010','-':'0000001f000000','_':'0000000000001f',':':'000c0c000c0c00','?':'0e110102040004',' ':'00000000000000'};
function chunk(name:string,b:Buffer) {
  const tag=Buffer.from(name), out=Buffer.alloc(12+b.length);out.writeUInt32BE(b.length);tag.copy(out,4);b.copy(out,8);
  let crc=-1; for(const byte of out.subarray(4,8+b.length)){crc^=byte; for(let i=0;i<8;i++)crc=(crc>>>1)^((crc&1)?0xedb88320:0);}
  out.writeUInt32BE((~crc)>>>0,8+b.length);return out;
}
function image(width:number,height:number) {
  const stride=1+width*3,b=Buffer.alloc(stride*height,255);
  for(let y=0;y<height;y++)b[y*stride]=0;
  function rect(x:number,y:number,w:number,h:number,color:number[]) {for(let yy=Math.max(0,y);yy<Math.min(height,y+h);yy++)for(let xx=Math.max(0,x);xx<Math.min(width,x+w);xx++){
    const at=stride*yy+xx*3+1;b[at]=color[0];b[at+1]=color[1];b[at+2]=color[2];}}
  function text(x:number,y:number,s:string,size=2,color=[40,55,68]) {for(const ch of s.toUpperCase()) {const rows=(glyph[ch]??glyph['?']).match(/../g)!;
    rows.forEach((r,yy)=>{const bits=parseInt(r,16);for(let i=0;i<5;i++)if(bits&(16>>i))rect(x+i*size,y+yy*size,size,size,color);});x+=6*size;}}
  function png(provenance: unknown) {const header=Buffer.alloc(13);header.writeUInt32BE(width);header.writeUInt32BE(height,4);header[8]=8;header[9]=2;
    return Buffer.concat([Buffer.from([137,80,78,71,13,10,26,10]),chunk('IHDR',header),chunk('iTXt',Buffer.concat([Buffer.from('collector-isolation\0\0\0\0\0'),Buffer.from(JSON.stringify(provenance))])),chunk('IDAT',deflateSync(b)),chunk('IEND',Buffer.alloc(0))]);}
  return {rect,text,png};
}
export type Kind = 'waterfall' | 'resources';
const levels = ['L0','L1','L2','L3','L4','L5'] as const;
const cuts = ['FLIGHT DRAIN','ARROW DECODE','PARQUET ENCODE','LOCAL FSYNC','OBJECT UPLOAD','DELTA COMMIT'];
function rows(r: Report): number | null {
  return finite(r.level==='L5' ? (r.committed?.available ? r.committed.rows : null) :
    r.clients?.completionSemantics==='explicit_diagnostic_discard_not_admission' ? r.clients.completedRows : null);
}
export function cpuSecondsPerMillionRows(r: Report) {
  const n=rows(r), cpu=finite(r.linux.processCpuSeconds);
  return n!==null && n>0 && cpu!==null && r.window.linux.boundaryAligned ? cpu*1e6/n : null;
}
export async function chart(reports: Report[], out: string, kind: Kind, paths: string[]=[] ) {
  if (!reports.length || reports.length>12 || reports.some(r=>r.schema!=='collector-isolation/report-v1'||!levels.includes(r.level)) ||
      levels.some(l=>reports.filter(r=>r.level===l).length>2) || new Set(reports.map(r=>r.runId)).size!==reports.length) throw Error('Supply at most two distinct measured runs per isolation level, at most 12 total');
  const runs=levels.flatMap(l=>{const group=reports.filter(r=>r.level===l).sort((a,b)=>(a.declaredCores??0)-(b.declaredCores??0)||a.runId.localeCompare(b.runId));return group.length?group:[undefined];});
  const slotLevels=levels.flatMap(l=>Array<string>(Math.max(1,reports.filter(r=>r.level===l).length)).fill(l));
  const provenance=runs.flatMap((r,i)=>r?[{key:String(i+1).padStart(2,'0'),runId:r.runId,level:r.level,declaredCores:r.declaredCores,mainTaskAllowedCpuList:r.linux.scope.mainTaskAllowedCpuList,
    startUnixMs:r.window.startUnixMs,endUnixMs:r.window.endUnixMs,sourceReport:paths[reports.indexOf(r)]??null,outputRows:rows(r),processCpuSeconds:r.linux.processCpuSeconds}]:[]);
  const legend=provenance.flatMap(p=>(p.key+' '+p.level+' '+(p.declaredCores!=null?p.declaredCores+'C ARM / ':'')+p.runId).match(/.{1,100}/g)??[]);
  type Panel = {title:string; values:(number|null)[]};
  const panel=(title:string,f:(r:Report)=>number|null|undefined):Panel=>({title, values:runs.map(r=>r?finite(f(r)):null)});
  const panels:Panel[]=[];
  if (kind==='waterfall') {
    panels.push(panel('ROWS PER SECOND - ALL SIX LEVELS SHARE ONE LINEAR SCALE', r=>{const n=rows(r);return n===null?null:n/r.window.nominalSeconds;}));
  } else {
    panels.push(panel('COLLECTOR PROCESS CPU-SECONDS / MILLION OUTPUT ROWS',cpuSecondsPerMillionRows));
    const mb=(n:number|null|undefined)=>n!=null?n/1e6:null;
    for(const iface of new Set(reports.flatMap(r=>Object.keys(r.linux.interfaces)))) {
      for(const [direction,key] of [['RX','rxBytesPerSecond'],['TX','txBytesPerSecond']] as const) {
        panels.push(panel(`NETWORK NAMESPACE / ${iface} / ${direction} MB/S`,r=>mb(r.linux.interfaces[iface]?.[key])));
      }
    }
    for(const disk of new Set(reports.flatMap(r=>Object.keys(r.linux.disks)))) {
      for(const [direction,key] of [['READ','readBytesPerSecond'],['WRITE','writeBytesPerSecond']] as const) {
        panels.push(panel(`KERNEL BLOCK DEVICE / ${disk} / ${direction} MB/S`,r=>mb(r.linux.disks[disk]?.[key])));
      }
    }
  }
  if(!panels.some(p=>p.values.some(v=>v!==null)))throw Error('No measured values for requested chart; no placeholder PNG written');
  const band=50+27*runs.length,W=1160,H=176+panels.length*band+12*legend.length;
  const im=image(W,H); im.text(20,16,kind==='waterfall'?'SINGLE COLLECTOR - ISOLATION WATERFALL':'SINGLE COLLECTOR - OBSERVED RESOURCE ROOFLINE',3);
  im.text(20,57,'L0-L4: EXPLICIT DIAGNOSTIC DISCARDS. L5: NEW DURABLE DELTA ROWS IN THE WINDOW.');
  im.text(20,80,kind==='waterfall'?'L4 TO L5 IS NOT AN EQUIVALENT STAGE ADDITION: PRODUCTION PIPELINE AND BATCHING DIFFER.':'OBSERVED USE ONLY. NO BANDWIDTH CEILING MEASURED. MB/S IS DECIMAL. RX AND TX ARE SEPARATE.');
  let y=116;
  for(const p of panels) {
    im.rect(12,y-6,W-24,25,[230,240,247]);im.text(20,y,p.title.slice(0,90));y+=28;
    const max=Math.max(...p.values.map(v=>v??0));
    runs.forEach((run,i)=>{
      const val=p.values[i]; im.text(20,y,run?String(i+1).padStart(2,'0')+' '+run.level+' '+(run.declaredCores!=null?run.declaredCores+'C':'')+' '+cuts[Number(run.level[1])]: slotLevels[i]+' NOT MEASURED'); im.text(302,y,run?run.window.nominalSeconds+' S':'-');
      im.text(370,y,val===null?'N/A':Intl.NumberFormat('en-US',{maximumSignificantDigits:5}).format(val));
      if(val!==null&&max>0)im.rect(575,y+1,Math.round(val/max*545),13,slotLevels[i]==='L5'?[32,81,114]:[45,129,164]);y+=27;
    });y+=22;
  }
  im.text(20,y,'RUN KEY / CONFIGURED CORE ARM / RUN ID. SOURCE PATHS AND EXACT WINDOWS EMBEDDED IN PNG.',1); y+=14;
  for(const line of legend){im.text(20,y,line,1);y+=12;}
  im.text(20,H-23,'N/A = NO MATCHED MEASUREMENT. BARS START AT ZERO. SEE JSON FOR SOURCE SCOPES AND TIMESTAMPS.',1);
  await writeFile(out,im.png({chart:kind,observations:provenance}),{flag:'wx'});
}
