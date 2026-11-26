import {readFile, readdir, readlink} from 'node:fs/promises';
import {execFileSync} from 'node:child_process';
import {finite, type Values} from './metrics.ts';

export type Task = {name: string; startTicks: string; cpuTicks: number};
export type Linux = {process: Task; cpusAllowed?: string | null; netns: string | null; threads: Record<string, Task> | null; values: Values};
export type System = {pid: number; tickHz: number; bootId: string; interfaces: string[]; disks: string[]; cgroup?: {path: string; membership: string} | null};
async function optional(path: string): Promise<string | null> {
  try { return await readFile(path, 'utf8'); } catch { return null; }
}
export function parseTask(text: string): Task {
  const left = text.indexOf('('), right = text.lastIndexOf(')'), fields = text.slice(right + 2).trim().split(/\s+/);
  const ticks = Number(fields[11]) + Number(fields[12]); // field 14 (utime) and 15 (stime), not children
  if (left < 0 || right <= left || !/^\d+$/.test(fields[19]) || !Number.isSafeInteger(ticks) || ticks < 0) throw Error('Invalid Linux task stat');
  return {name: text.slice(left + 1, right), cpuTicks: ticks, startTicks: fields[19]};
}
/** Process-lifetime scheduler and fault counters from `/proc/PID/stat`, excluding children. */
export function parseTaskCounters(text: string): Values {
  const fields = text.slice(text.lastIndexOf(')') + 2).trim().split(/\s+/);
  return {userTicks: finite(Number(fields[11])), systemTicks: finite(Number(fields[12])),
    minorFaults: finite(Number(fields[7])), majorFaults: finite(Number(fields[9]))};
}
export function parseDevices(net: string | null, disk: string | null, system: Pick<System, 'interfaces' | 'disks'>): Values {
  const values: Values = {};
  for (const iface of system.interfaces) {
    const row = net?.split('\n').find(l => l.slice(0, l.lastIndexOf(':')).trim() === iface);
    const fields = row?.slice(row.lastIndexOf(':') + 1).trim().split(/\s+/);
    for (const [key, i] of Object.entries({rxBytes: 0, rxErrors: 2, rxDrops: 3, txBytes: 8, txErrors: 10, txDrops: 11})) {
      values[`net.${iface}.${key}`] = finite(Number(fields?.[i]));
    }
  }
  for (const device of system.disks) {
    const f = disk?.trim().split('\n').map(l => l.trim().split(/\s+/)).find(row => row[2] === device);
    for (const [key, i] of Object.entries({reads: 3, readSectors: 5, writes: 7, writeSectors: 9, inFlight: 11, busyMs: 12, weightedIoMs: 13})) {
      values[`disk.${device}.${key}`] = finite(Number(f?.[i]));
    }
  }
  return values;
}
async function findCgroup(pid: number) {
  const membership = (await optional('/proc/'+pid+'/cgroup'))?.match(/^0::([^\n]*)$/m)?.[1];
  if (membership === undefined) return null;
  const root = '/sys/fs/cgroup';
  for (const path of new Set([...(membership.startsWith('/') && !membership.split('/').includes('..') ? [root+membership.replace(/\/$/,'')] : []),root])) {
    if ((await optional(path+'/cgroup.procs'))?.split('\n').includes(String(pid))) return {path,membership};
  }
  return null;
}
async function cgroupValues(s: System): Promise<Values> {
  if (!s.cgroup) return {};
  const membership = (await optional('/proc/'+s.pid+'/cgroup'))?.match(/^0::([^\n]*)$/m)?.[1];
  if (membership !== s.cgroup.membership) return {};
  const [cpu, mem, current, limit, pressure] = await Promise.all(['cpu.stat','memory.events','memory.current','memory.max','cpu.pressure'].map(p=>optional(s.cgroup!.path+'/'+p)));
  const value=(text: string|null,key:string)=>finite(Number(text?.split("\n").find(line=>line.startsWith(key+" "))?.split(" ")[1]));
  const number=(text: string|null)=>text !== null && /^\d+$/.test(text.trim()) ? finite(Number(text)) : null;
  return {cgThrottledUs:value(cpu,'throttled_usec'), cgThrottledPeriods:value(cpu,'nr_throttled'),cgMemoryHigh:value(mem,'high'),
    cgMemoryOom:value(mem,'oom'),cgMemoryOomKills:value(mem,'oom_kill'), cgMemoryBytes:number(current),cgMemoryLimitBytes:number(limit),
    cgCpuSomeUs:finite(Number(pressure?.match(/^some .* total=(\d+)$/m)?.[1]))};
}
export async function systemInfo(pid: number, interfaces: string[], disks: string[]): Promise<System> {
  const tickHz = Number(execFileSync('getconf', ['CLK_TCK'], {encoding: 'utf8'}).trim());
  if (!(tickHz > 0 && Number.isSafeInteger(tickHz))) throw Error('No usable CLK_TCK');
  return {pid, tickHz, bootId: (await readFile('/proc/sys/kernel/random/boot_id', 'utf8')).trim(), interfaces, disks, cgroup:await findCgroup(pid)};
}
export async function sampleLinux(s: System, boundary: boolean): Promise<Linux> {
  const root = `/proc/${s.pid}`;
  async function taskStats(): Promise<Record<string, Task> | null> {
    if (!boundary) return null;
    const ids = await readdir(`${root}/task`);
    const results = await Promise.all(ids.map(async id => {
      const line = await optional(`${root}/task/${id}/stat`);
      return line === null ? [] : [[id, parseTask(line)]];
    }));
    return Object.fromEntries(results.flat());
  }
  const [stat, status, io, net, disk, threads, netns, cg] = await Promise.all([
    readFile(`${root}/stat`, 'utf8'), optional(`${root}/status`), optional(`${root}/io`),
    s.interfaces.length ? optional(`${root}/net/dev`) : null, s.disks.length ? optional('/proc/diskstats') : null,
    taskStats(), readlink(`${root}/ns/net`).catch(() => null), cgroupValues(s),
  ]);
  const values = {...parseDevices(net, disk, s),...cg};
  const p = parseTask(stat);
  values.cpuTicks = p.cpuTicks;
  Object.assign(values, parseTaskCounters(stat));
  for (const [key, field] of Object.entries({voluntarySwitches: 'voluntary_ctxt_switches', involuntarySwitches: 'nonvoluntary_ctxt_switches'})) {
    values[key] = finite(Number(status?.match(new RegExp(`^${field}:\\s*(\\d+)$`, 'm'))?.[1]));
  }
  for (const key of ['read_bytes', 'write_bytes', 'cancelled_write_bytes']) values[`io.${key}`] = finite(Number(io?.match(new RegExp(`^${key}:\\s*(\\d+)$`, 'm'))?.[1]));
  values.rssBytes = finite(Number(status?.match(/^VmRSS:\s*(\d+) kB$/m)?.[1]) * 1024);
  return {process: p, cpusAllowed:status?.match(/^Cpus_allowed_list:\s*([0-9,-]+)$/m)?.[1]??null, netns, threads, values};
}
