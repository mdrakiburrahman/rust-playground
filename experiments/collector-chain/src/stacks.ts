import path from 'node:path';

/** Compose project names of this experiment's runs: `collector-chain-<UTC date>t<UTC time>-<4 hex digits>`. */
export const PROJECT = /^collector-chain-\d{8}t\d{6}-[0-9a-f]{4}$/;

export interface Container {
  readonly name: string;
  readonly project: string;
  readonly ports: readonly number[];
}

/** Host TCP ports in `docker ps` Ports text such as `127.0.0.1:8080->8080/tcp, [::]:8080->8080/tcp, 4317-4318/tcp`. */
export function publishedPorts(text: string): number[] {
  const ports = new Set<number>();
  for (const entry of text.split(',')) {
    const match = /:(\d+)(?:-(\d+))?->\d+(?:-\d+)?\/tcp$/.exec(entry.trim());
    if (!match) continue;
    for (let port = Number(match[1]); port <= Number(match[2] ?? match[1]); port++) ports.add(port);
  }
  return [...ports];
}

/** Rows of `docker ps --all --format '{{.Names}}\t{{.Label "com.docker.compose.project"}}\t{{.Ports}}'`. */
export function parseContainers(output: string): Container[] {
  return output.split('\n').filter(line => line !== '').map(line => {
    const [name = '', project = '', ports = ''] = line.split('\t');
    return { name, project, ports: publishedPorts(ports) };
  });
}

/** Who publishes a wanted host port: earlier runs of this experiment, which may be replaced, and anything else, which must be left alone. */
export function portConflicts(containers: readonly Container[], wanted: readonly number[]): {
  earlier: Map<string, number[]>;
  foreign: { name: string; port: number }[];
} {
  const held = new Map<string, Set<number>>();
  const foreign: { name: string; port: number }[] = [];
  for (const { name, project, ports } of containers) {
    for (const port of ports.filter(port => wanted.includes(port))) {
      if (PROJECT.test(project)) held.set(project, (held.get(project) ?? new Set()).add(port));
      else foreign.push({ name, port });
    }
  }
  return { earlier: new Map([...held].map(([project, ports]) => [project, [...ports].sort((a, b) => a - b)])), foreign };
}

/** IPv4 addresses printed by `docker network inspect --format '{{range .Containers}}{{.IPv4Address}}{{"\n"}}{{end}}'`, e.g. `172.19.0.7/16`. */
export function networkAddresses(output: string): Set<string> {
  return new Set(output.split('\n').map(line => line.trim().split('/')[0] ?? '').filter(address => address !== ''));
}

/** Pids in `ps -ww -eo pid=,args=` output of `program` clients that send to any of `addresses`. */
export function clientPids(ps: string, program: string, addresses: ReadonlySet<string>): number[] {
  const pids: number[] = [];
  for (const line of ps.split('\n')) {
    const match = /^\s*(\d+)\s+(\S+)(.*)$/.exec(line);
    if (!match || path.posix.basename(match[2] ?? '') !== program) continue;
    const targets = [...(match[3] ?? '').matchAll(/--(?:arrow|otlp)-endpoint[=\s]+https?:\/\/([^:/\s]+)/g)].map(target => target[1] ?? '');
    if (targets.some(target => addresses.has(target))) pids.push(Number(match[1]));
  }
  return pids;
}
