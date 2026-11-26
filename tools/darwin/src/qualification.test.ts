import assert from "node:assert/strict";
import test from "node:test";
import { collectorForBinding } from "./qualification.js";
import type { ArmBinding } from "./tier2.js";

test("resolves collector identity from the binding, not the A/A arm label", () => {
  const baseline = { imageId: `sha256:${"a".repeat(64)}`, revision: "a".repeat(40) };
  const candidate = { imageId: `sha256:${"b".repeat(64)}`, revision: "b".repeat(40) };
  const binding: ArmBinding = {
    collectorImageId: baseline.imageId, collectorRevision: baseline.revision, nonCollectorImages: {},
    controllerSha256: "", observerSha256: "", workloadSha256: "", normalizedConfigSha256: "", treatmentSha256: "",
  };
  assert.deepEqual(collectorForBinding(binding, baseline, candidate), baseline);
  assert.deepEqual(collectorForBinding({ ...binding, collectorImageId: candidate.imageId,
    collectorRevision: candidate.revision }, baseline, candidate), candidate);
  assert.throws(() => collectorForBinding({ ...binding, collectorRevision: candidate.revision }, baseline, candidate),
    /does not identify/);
});
