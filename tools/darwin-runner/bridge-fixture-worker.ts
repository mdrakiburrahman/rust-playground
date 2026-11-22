import type { ConfigValidator, ValidatedDispatch } from "./dispatch-contract.js";

type BridgeModule = typeof import("./bridge-listener.js");
type FixtureConfig = { compression_level: number; other: string };

function parseConfig(bytes: Buffer): FixtureConfig {
  const value: unknown = JSON.parse(bytes.toString("utf8"));
  if (typeof value !== "object" || value === null || Array.isArray(value)
    || Object.keys(value).sort().join(",") !== "compression_level,other") {
    throw new Error("fixture config must contain only compression_level and other");
  }
  const record = value as Record<string, unknown>;
  const level = record.compression_level;
  if (typeof level !== "number" || !Number.isInteger(level) || typeof record.other !== "string") {
    throw new Error("fixture config values are invalid");
  }
  return { compression_level: level, other: record.other };
}

const validateConfig: ConfigValidator = (baseline, candidate) => {
  const before = parseConfig(baseline);
  const after = parseConfig(candidate);
  if (before.compression_level !== 1 || after.compression_level === before.compression_level
    || before.other !== after.other) {
    throw new Error("candidate may change only the inert fixture compression level");
  }
  return after.compression_level;
};

const [socketPath, repoRoot, producerCommit, ...extra] = process.argv.slice(2);
if (!socketPath || !repoRoot || !producerCommit || extra.length !== 0) {
  throw new Error("usage: bridge-fixture-worker.ts <socket-path> <repo-root> <producer-commit>");
}

const bridge = await import(new URL("./bridge-listener.ts", import.meta.url).href) as BridgeModule;
const listener = await bridge.startTrustedBridgeListener({
  socketPath,
  repoRoot,
  producerCommit,
  configPath: "fixtures/compression.json",
  validateConfig,
  startWorkload: (dispatch: ValidatedDispatch) => {
    console.log(`TRUSTED_CALLBACK level=${dispatch.compressionLevel} commit=${dispatch.candidateCommit}`);
    return { candidateCommit: dispatch.candidateCommit, compressionLevel: dispatch.compressionLevel };
  },
});

console.log("TRUSTED_LISTENER_READY");
let closing = false;
const close = () => {
  if (closing) return;
  closing = true;
  void listener.close().then(() => process.exit(0), () => process.exit(1));
};
process.on("SIGTERM", close);
process.on("SIGINT", close);
