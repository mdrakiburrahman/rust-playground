import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

test("pressure replay only launches after immutable collector image checks", async () => {
  const root = await mkdtemp(join(tmpdir(), "pressure-preflight-"));
  const image = `sha256:${"a".repeat(64)}`;
  const revision = "b".repeat(40);
  const marker = join(root, "launched");
  const docker = join(root, "docker");
  const harness = join(root, "harness");
  const script = fileURLToPath(new URL("./pressure-run-preflight.ts", import.meta.url));
  const imageInfo = {
    Id: image,
    Config: {
      Labels: { "org.opencontainers.image.revision": revision },
      Entrypoint: ["/usr/bin/tini", "--", "/usr/local/bin/arrowcol-dataflow-rust"],
    },
  };
  try {
    await writeFile(docker, '#!/bin/sh\nif [ "$1" = image ]; then printf "%s" "$IMAGE_JSON"; else test "$BAD_BIN" != 1; fi\n', { mode: 0o755 });
    await writeFile(harness, '#!/bin/sh\nprintf launched > "$LAUNCH_MARKER"\n', { mode: 0o755 });
    const invoke = (info: unknown, badBin = false, override = image) => spawnSync(process.execPath, [
      "--import", import.meta.resolve("tsx"), script, image, revision, harness, "parity",
      "--collector-image-id", override, "--collector-source-revision", revision,
    ], {
      encoding: "utf8",
      env: {
        ...process.env, PATH: `${root}${delimiter}${process.env.PATH}`,
        IMAGE_JSON: JSON.stringify([info]), BAD_BIN: badBin ? "1" : "0", LAUNCH_MARKER: marker,
      },
    });

    for (const result of [
      invoke({ ...imageInfo, Id: `sha256:${"c".repeat(64)}` }),
      invoke({ ...imageInfo, Config: { ...imageInfo.Config, Labels: { "org.opencontainers.image.revision": "c".repeat(40) } } }),
      invoke({ ...imageInfo, Config: { ...imageInfo.Config, Entrypoint: ["/bin/sh"] } }),
      invoke(imageInfo, true),
      invoke(imageInfo, false, `sha256:${"c".repeat(64)}`),
    ]) {
      assert.notEqual(result.status, 0, result.stderr);
    }
    await assert.rejects(readFile(marker), { code: "ENOENT" });
    assert.equal(invoke(imageInfo).status, 0);
    assert.equal(await readFile(marker, "utf8"), "launched");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
