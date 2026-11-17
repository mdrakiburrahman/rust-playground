import { createHash, randomUUID } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Lab, execute, privateDirectory, shellQuote } from './lab.js';
import type { Request } from './job.js';

export interface Artifact { name: string; sha256: string; binarySha256: string; transport?: 'blob' | 'arm' | 'source-arm'; localFile?: string }

export async function publishArtifact(lab: Lab): Promise<Artifact> {
  const inventory = await lab.inventory();
  const directory = await privateDirectory(lab.config.outDir);
  const stage = path.join(directory, 'artifact-stage');
  await mkdir(stage, { recursive: true, mode: 0o700 });
  await execute('docker', ['exec', lab.config.buildContainer, 'cargo', 'build', '--locked', '--release', '--manifest-path', 'experiments/t3-storage/crate/Cargo.toml', '--target-dir', 'target/storage-spike'], 15 * 60_000);
  await execute('docker', ['exec', lab.config.buildContainer, 'cp', '/workspaces/rust-playground/target/storage-spike/release/storage-spike', `/workspaces/rust-playground/${path.relative(process.cwd(), stage)}/storage-spike`]);
  const binarySha256 = createHash('sha256').update(await readFile(path.join(stage, 'storage-spike'))).digest('hex');
  await writeFile(path.join(stage, 'job.ts'), await readFile('experiments/t3-storage/src/job.ts'));
  await writeFile(path.join(stage, 'package.json'), '{"type":"module"}\n');
  await writeFile(path.join(stage, 'provenance.json'), JSON.stringify({ binarySha256, quiverCommit: 'a152f5e8256160d4ad8fe756f4cab0a5055ae7ac' }));
  const file = path.join(directory, 'artifact.tgz');
  await execute('tar', ['-czf', file, '-C', stage, 'storage-spike', 'job.ts', 'package.json', 'provenance.json']);
  const sha256 = createHash('sha256').update(await readFile(file)).digest('hex');
  const name = `artifacts/${sha256}/lab.tgz`;
  let published = false;
  let transport: 'blob' | 'arm' = 'blob';
  for (let attempt = 0; attempt < 20; attempt++) {
    try {
      await lab.az(['storage', 'blob', 'upload', '--account-name', inventory.storageAccounts[3], '--container-name', 'spike', '--name', name, '--file', await lab.cliPath(file), '--auth-mode', 'login', '--overwrite', 'false', '--metadata', `sha256=${sha256}`], 5 * 60_000);
      published = true;
      break;
    } catch (error) {
      if (/network rules of storage account/u.test(String(error))) {
        transport = 'arm';
        published = true;
        break;
      }
      const existing = await lab.az(['storage', 'blob', 'show', '--account-name', inventory.storageAccounts[3], '--container-name', 'spike', '--name', name, '--auth-mode', 'login', '--query', 'metadata.sha256']).catch(() => null);
      if (existing === sha256) { published = true; break; }
      if (attempt === 19 || !/AuthorizationPermissionMismatch|AuthorizationFailure|403|Forbidden/u.test(String(error))) throw error;
      await delay(15_000);
    }
  }
  if (!published) throw new Error('Artifact upload did not complete.');
  let artifact: Artifact = { name, sha256, binarySha256, transport };
  if (transport === 'arm') {
    const sourceFile = path.join(directory, 'artifact-source.tgz');
    await execute('tar', ['-czf', sourceFile, '-C', 'experiments/t3-storage', 'crate/Cargo.toml', 'crate/Cargo.lock', 'crate/src', 'src/job.ts']);
    const sourceSha256 = createHash('sha256').update(await readFile(sourceFile)).digest('hex');
    artifact = { name: `artifacts/${sourceSha256}/source.tgz`, sha256: sourceSha256, binarySha256, transport: 'source-arm', localFile: sourceFile };
  }
  await lab.save('artifact.json', artifact);
  return artifact;
}

export function bootstrapScript(artifact: Artifact, endpoint: string): string {
  const url = `${endpoint}spike/${artifact.name}`;
  return `set -eu
install -d -m 755 /var/lib/storage-spike
(
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y --no-install-recommends nfs-common ca-certificates curl jq build-essential pkg-config libssl-dev git protobuf-compiler strace sysstat xz-utils
curl -fsSL https://packages.microsoft.com/config/ubuntu/24.04/packages-microsoft-prod.deb -o /var/lib/storage-spike/packages-microsoft-prod.deb
dpkg -i /var/lib/storage-spike/packages-microsoft-prod.deb
apt-get update -qq
AZNFS_NONINTERACTIVE_INSTALL=1 DEBIAN_FRONTEND=noninteractive apt-get install -y aznfs
cd /var/lib/storage-spike
curl -fsSLO https://nodejs.org/dist/v24.22.3/node-v24.22.3-linux-x64.tar.xz
curl -fsSL https://nodejs.org/dist/v24.22.3/SHASUMS256.txt -o node-shasums.txt
grep ' node-v24.22.3-linux-x64.tar.xz$' node-shasums.txt | sha256sum --check
tar -xf node-v24.22.3-linux-x64.tar.xz
ln -sfn /var/lib/storage-spike/node-v24.22.3-linux-x64/bin/node /usr/local/bin/node
DEVICE=''
for candidate in /dev/disk/azure/scsi1/lun0 /dev/disk/azure/data/by-lun/0; do
  if test -b "$candidate"; then DEVICE="$candidate"; break; fi
done
test -n "$DEVICE"
TYPE=$(blkid -s TYPE -o value "$DEVICE" || true)
if test -z "$TYPE"; then mkfs.ext4 -L storage-spike-oracle "$DEVICE"; else test "$TYPE" = ext4; test "$(blkid -s LABEL -o value "$DEVICE")" = storage-spike-oracle; fi
install -d -m 755 /var/lib/storage-spike-oracle /mnt/storage-spike
if ! mountpoint -q /var/lib/storage-spike-oracle; then mount "$DEVICE" /var/lib/storage-spike-oracle; fi
UUID=$(blkid -s UUID -o value "$DEVICE")
if ! grep -q "UUID=$UUID " /etc/fstab; then printf 'UUID=%s /var/lib/storage-spike-oracle ext4 defaults,nofail 0 2\\n' "$UUID" >> /etc/fstab; fi
install -d -o spike -g spike -m 700 /var/lib/storage-spike-oracle/jobs /var/lib/storage-spike-oracle/local
chown spike:spike /var/lib/storage-spike-oracle /var/lib/storage-spike
${artifact.transport === 'arm' || artifact.transport === 'source-arm' ? '' : `TOKEN=$(curl --noproxy '*' -fsS -H Metadata:true 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com%2F' | jq -r .access_token)
curl -fsSL -H "Authorization: Bearer $TOKEN" -H 'x-ms-version: 2021-12-02' ${shellQuote(url)} -o lab.tgz`}
printf '%s  lab.tgz\\n' ${shellQuote(artifact.sha256)} | sha256sum --check
tar -xzf lab.tgz
${artifact.transport === 'source-arm' ? `cp src/job.ts job.ts
printf '%s\\n' '{"type":"module"}' > package.json
curl -fsSL https://static.rust-lang.org/rustup/dist/x86_64-unknown-linux-gnu/rustup-init -o rustup-init
curl -fsSL https://static.rust-lang.org/rustup/dist/x86_64-unknown-linux-gnu/rustup-init.sha256 -o rustup-init.sha256
sha256sum --check rustup-init.sha256
chmod 755 rustup-init
runuser -u spike -- ./rustup-init -y --profile minimal --default-toolchain 1.96.1 --no-modify-path
chown -R spike:spike crate
runuser -u spike -- env CARGO_TARGET_DIR=/var/lib/storage-spike/driver-target /home/spike/.cargo/bin/cargo clean --release --package storage-spike --manifest-path /var/lib/storage-spike/crate/Cargo.toml
runuser -u spike -- env CARGO_TARGET_DIR=/var/lib/storage-spike/driver-target /home/spike/.cargo/bin/cargo build --locked --release --manifest-path /var/lib/storage-spike/crate/Cargo.toml
install -m 755 driver-target/release/storage-spike storage-spike
SOURCE=$(find /home/spike/.cargo/git/checkouts -mindepth 2 -maxdepth 2 -type d -name a152f5e)
test "$(git -C "$SOURCE" rev-parse HEAD)" = a152f5e8256160d4ad8fe756f4cab0a5055ae7ac
ln -sfn "$SOURCE" /var/lib/storage-spike/otel-arrow` : ''}
chmod 755 storage-spike
${artifact.transport === 'source-arm' ? '' : `printf '%s  storage-spike\\n' ${shellQuote(artifact.binarySha256)} | sha256sum --check`}
node --version
./storage-spike --help
./storage-spike write --help | grep -q -- --barrier-before-ingest
dpkg-query -W aznfs
timedatectl show -p NTPSynchronized
printf 'GUEST_BINARY_SHA256:'
sha256sum storage-spike | cut -d ' ' -f 1
) >/var/lib/storage-spike/bootstrap.log 2>&1 || { tail -n 40 /var/lib/storage-spike/bootstrap.log; exit 1; }
tail -n 10 /var/lib/storage-spike/bootstrap.log
echo STORAGE_SPIKE_OK
`;
}

function refreshScript(artifact: Artifact): string {
  if (!/^[0-9a-f]{64}$/u.test(artifact.sha256)) throw new Error('Invalid source archive digest.');
  const source = `/var/lib/storage-spike/source-${artifact.sha256}`;
  return `set -eu
test -z "$(systemctl list-units 'storage-spike-*' --state=running --no-legend --no-pager)"
cd /var/lib/storage-spike
printf '%s  lab.tgz\\n' ${shellQuote(artifact.sha256)} | sha256sum --check
install -d -o spike -g spike -m 755 ${shellQuote(source)}
tar -xzf lab.tgz -C ${shellQuote(source)}
chown -R spike:spike ${shellQuote(source)}
# Archive timestamps can otherwise make an older shared-target build look fresh.
runuser -u spike -- env CARGO_TARGET_DIR=/var/lib/storage-spike/driver-target /home/spike/.cargo/bin/cargo clean --release --package storage-spike --manifest-path ${shellQuote(`${source}/crate/Cargo.toml`)}
runuser -u spike -- env CARGO_TARGET_DIR=/var/lib/storage-spike/driver-target /home/spike/.cargo/bin/cargo build --locked --release --manifest-path ${shellQuote(`${source}/crate/Cargo.toml`)} >/var/lib/storage-spike/refresh.log 2>&1 || { tail -n 30 /var/lib/storage-spike/refresh.log; exit 1; }
install -m 755 driver-target/release/storage-spike storage-spike.new
mv storage-spike.new storage-spike
cp ${shellQuote(`${source}/src/job.ts`)} job.ts
./storage-spike write --help | grep -q -- --barrier-before-ingest
printf 'GUEST_BINARY_SHA256:'
sha256sum storage-spike | cut -d ' ' -f 1
echo STORAGE_SPIKE_OK
`;
}

export async function bootstrap(lab: Lab, refresh = false): Promise<void> {
  const inventory = await lab.inventory();
  const artifact = await publishArtifact(lab);
  const bytes = artifact.transport === 'arm' || artifact.transport === 'source-arm' ? await readFile(artifact.localFile ?? path.join(lab.config.outDir, 'artifact.tgz')) : null;
  const digests = await Promise.all(inventory.vmNames.map(async vm => {
    if (bytes) {
      const chunks: string[] = [];
      for (let offset = 0; offset < bytes.length; offset += 1_048_576) {
        const chunk = bytes.subarray(offset, offset + 1_048_576);
        const file = `/var/lib/storage-spike/transfer-${artifact.sha256}/chunk-${offset}`;
        chunks.push(file);
        const digest = createHash('sha256').update(chunk).digest('hex');
        await lab.remote(vm, `install -d -m 700 ${shellQuote(path.posix.dirname(file))}\nprintf '%s' ${shellQuote(chunk.toString('base64'))} | base64 -d > ${shellQuote(file)}\nprintf '%s  %s\\n' ${shellQuote(digest)} ${shellQuote(file)} | sha256sum --check\necho STORAGE_SPIKE_OK`, `artifact-${vm}-${offset}`);
      }
      await lab.remote(vm, `cat ${chunks.map(shellQuote).join(' ')} > /var/lib/storage-spike/lab.tgz\nprintf '%s  /var/lib/storage-spike/lab.tgz\\n' ${shellQuote(artifact.sha256)} | sha256sum --check\necho STORAGE_SPIKE_OK`, `assemble-${vm}`);
    }
    const script = refresh && artifact.transport === 'source-arm' ? refreshScript(artifact) : bootstrapScript(artifact, inventory.storageEndpoints[3].blob!);
    const result = await lab.remote(vm, script, refresh ? 'refresh' : 'bootstrap');
    const line = result.split(/\r?\n/u).find(value => /^GUEST_BINARY_SHA256:[0-9a-f]{64}$/u.test(value));
    if (!line) throw new Error('Guest binary digest not verified.');
    return [vm, line.slice('GUEST_BINARY_SHA256:'.length)] as const;
  }));
  await lab.save('guest-binaries.json', Object.fromEntries(digests));
}

export async function startJob(lab: Lab, vm: string, input: Pick<Request, 'mode' | 'cache'> & Partial<Request>): Promise<string> {
  const inventory = await lab.inventory();
  const artifact = JSON.parse(await readFile(path.join(lab.config.outDir, 'artifact.json'), 'utf8')) as Artifact;
  const digests = JSON.parse(await readFile(path.join(lab.config.outDir, 'guest-binaries.json'), 'utf8')) as Record<string, string>;
  if (!/^[0-9a-f]{64}$/u.test(digests[vm] ?? '')) throw new Error('Guest binary provenance is missing.');
  const id = input.id ?? `job-${randomUUID()}`;
  if (!/^[a-z0-9-]{1,100}$/u.test(id)) throw new Error('Invalid job ID.');
  const request: Request = { ...input, id, vm, nfsSource: inventory.nfsSource, stagingEndpoint: inventory.storageEndpoints[3].blob!, binarySha256: digests[vm], sourceArchiveSha256: artifact.sha256 };
  await lab.save(`${id}-request.json`, request);
  const encoded = Buffer.from(JSON.stringify(request)).toString('base64');
  const script = `set -eu
install -d -o spike -g spike -m 700 /var/lib/storage-spike/requests
printf '%s' ${shellQuote(encoded)} | base64 -d > /var/lib/storage-spike/requests/${id}.json
chown spike:spike /var/lib/storage-spike/requests/${id}.json
systemd-run --unit storage-spike-${id} --property=User=spike --property=WorkingDirectory=/var/lib/storage-spike --property=RuntimeMaxSec=10800 --property=StandardOutput=journal --property=StandardError=journal /usr/local/bin/node --experimental-strip-types /var/lib/storage-spike/job.ts --request /var/lib/storage-spike/requests/${id}.json
systemctl show storage-spike-${id} -p ActiveState -p ExecMainStatus
echo STORAGE_SPIKE_OK
`;
  await lab.remote(vm, script, `start-${id}`);
  return id;
}
