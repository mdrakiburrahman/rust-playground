import { execFileSync, spawnSync } from "node:child_process";

const [image, revision, harness, ...args] = process.argv.slice(2);
if (!image || !/^sha256:[a-f0-9]{64}$/.test(image) ||
    !revision || !/^[a-f0-9]{40}$/.test(revision) || !harness) {
  throw new Error("expected immutable image ID, full revision, harness path, and parity arguments");
}
const imageFlags = args.flatMap((arg, index) => arg === "--collector-image-id" ? [args[index + 1]] : []);
const revisionFlags = args.flatMap((arg, index) => arg === "--collector-source-revision" ? [args[index + 1]] : []);
if (args[0] !== "parity" || imageFlags.length !== 1 || imageFlags[0] !== image ||
    revisionFlags.length !== 1 || revisionFlags[0] !== revision) {
  throw new Error("parity collector override must match preflight image and revision");
}
const raw = execFileSync("docker", ["image", "inspect", image], { encoding: "utf8" });
const inspected: unknown = JSON.parse(raw);
if (!Array.isArray(inspected) || inspected.length !== 1) throw new Error("invalid image inspection");
const entry = inspected[0];
if (entry?.Id !== image || entry?.Config?.Labels?.["org.opencontainers.image.revision"] !== revision ||
    JSON.stringify(entry?.Config?.Entrypoint) !== JSON.stringify([
      "/usr/bin/tini", "--", "/usr/local/bin/arrowcol-dataflow-rust",
    ])) {
  throw new Error("collector image identity, source revision, or entrypoint mismatch");
}
execFileSync("docker", [
  "run", "--rm", "--network=none", "--entrypoint", "/bin/sh", image, "-ec",
  "test -x /usr/bin/tini && test -x /usr/local/bin/arrowcol-dataflow-rust",
], { stdio: "inherit" });
const result = spawnSync(harness, args, { stdio: "inherit" });
if (result.error) throw result.error;
if (result.signal) throw new Error(`parity harness terminated by ${result.signal}`);
process.exitCode = result.status ?? 1;
