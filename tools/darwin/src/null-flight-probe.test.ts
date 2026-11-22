import assert from "node:assert/strict";
import test from "node:test";
import { clientArguments } from "./null-flight-probe.js";

test("probe forwards strict client settings without permitting endpoint or schedule overrides", () => {
  assert.deepEqual(clientArguments({ workers: 8, interval_us: 0, ipc_compression: "lz4_frame", tcp_nodelay: true }),
    ["--workers", "8", "--interval-us", "0", "--ipc-compression", "lz4_frame", "--tcp-nodelay", "true"]);
  for (const value of [{ endpoint: "http://remote:1" }, { schedule_file: "other" }, { edge_secret_file: "key" },
    { runtime_threads: Infinity }, { workers: [1, 2] }, { "bad-key": "value" }]) {
    assert.throws(() => clientArguments(value));
  }
});
