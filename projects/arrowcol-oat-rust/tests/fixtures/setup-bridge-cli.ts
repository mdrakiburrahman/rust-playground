import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

// Offline provider simulation at the real Rust CLI boundary; never invokes kubectl, npx or a provider.
type KubeObject = {
  apiVersion: string;
  kind: string;
  metadata: {
    name: string;
    namespace?: string;
    uid?: string;
    resourceVersion?: string;
    generation?: number;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
  };
};
type Inventory = {
  [key: string]: unknown;
  namespace: { name: string; uid: string | null; owner_token: string };
  resources: { uid: string | null; state: string }[];
};
type Request = {
  schema_version: number;
  kind: string;
  request_id: number;
  request_sha256: string;
  stdin_sha256: string;
  stdin: string | null;
  executable: string;
  argv: string[];
  authority: { context: string; server: string };
};
const sha = (text: string) => createHash("sha256").update(text, "utf8").digest("hex");
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.entries(value).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
      .map(([key, item]) => `${JSON.stringify(key)}:${canonical(item)}`).join(",")}}`;
  }
  const json = JSON.stringify(value);
  assert.notEqual(json, undefined);
  return json;
}

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "../../../..");
const binary = resolve(process.argv[2] ?? join(root, "target/debug/arrowcol-oat-rust"));
const selected = process.argv[3];
const cases = selected ? [selected] : [
  "nominal", "partial", "nonzero-partial", "duplicate", "unexpected", "truncated", "malformed",
  "namespace-replaced", "namespace-absent", "namespace-origin", "namespace-rv",
];
const golden = JSON.parse(await readFile(join(here, "setup-bridge.json"), "utf8")) as {
  request: { authority: {
    campaign_id: string; cell_run_id: string; owner_id: string; fencing_token: number;
    namespace: string; server: string; context: string;
  } };
};
const authority = golden.request.authority;
const namespace = authority.namespace as string;
const serviceAccount = "fixture-sa";
const image = `registry.k8s.io/pause@sha256:${"a".repeat(64)}`;
const manifests = [
  { apiVersion: "v1", kind: "Namespace", metadata: { name: namespace } },
  { apiVersion: "v1", kind: "ServiceAccount", metadata: { name: serviceAccount, namespace },
    automountServiceAccountToken: false },
  ...["client", "collector", "maintenance"].map(role => {
    const name = role === "maintenance" ? "delta-maintenance" : `arrowcol-${role}`;
    const app = role === "client" ? "arrowcol-oat-client" : name;
    return {
      apiVersion: "apps/v1", kind: "Deployment", metadata: { name, namespace, labels: { app } },
      spec: { replicas: 1, selector: { matchLabels: { app } },
        template: { metadata: { labels: { app } }, spec: {
          serviceAccountName: serviceAccount, automountServiceAccountToken: false,
          containers: [{ name: role, image, resources: {
            requests: { cpu: "10m", memory: "16Mi" }, limits: { cpu: "100m", memory: "64Mi" },
          } }],
        } } },
    };
  }),
];
function inventory(): Inventory {
  const step = () => ({ state: "not_started", deadline_unix_ms: null, reason_code: null });
  return {
    schema_version: 1, campaign_id: authority.campaign_id, owner_id: authority.owner_id,
    fencing_token: authority.fencing_token, orchestration_run_id: "00000000-0000-0000-0000-000000000003",
    cell_run_id: authority.cell_run_id, load_admission: "not_authorized",
    namespace: { name: namespace, uid: null, owner_token: authority.owner_id }, resources: [],
    fic: { resource_id: null, name: null, state: "planned", plan: {
      version: 1, runId: authority.cell_run_id, namespace, serviceAccount,
      identityId: "/subscriptions/offline/identities/fixture",
      resourceId: "/subscriptions/offline/identities/fixture/federatedIdentityCredentials/fixture-fic",
      clientId: "00000000-0000-0000-0000-000000000005",
      tenantId: "00000000-0000-0000-0000-000000000006",
      aksId: "/subscriptions/offline/clusters/fixture", server: authority.server,
      credentialName: "fixture-fic", issuer: "https://offline.invalid/issuer",
      subject: `system:serviceaccount:${namespace}:${serviceAccount}`, audience: "api://AzureADTokenExchange",
    } },
    qap: null, effective_stop_budgets: { maintenance_grace_seconds: 4800, maintenance_stop_timeout_ms: 5100000 },
    planned_cell_duration_ms: 0,
    cleanup: Object.fromEntries(["stop_admission", "bounded_drain", "evidence_capture", "stop_maintenance",
      "terminal_capture", "handoff"].map(name => [name, step()])),
  };
}
function observed(value: KubeObject, index: number): KubeObject {
  const result = structuredClone(value);
  result.metadata.uid = `00000000-0000-0000-0000-${String(100 + index).padStart(12, "0")}`;
  result.metadata.resourceVersion = "17";
  if (result.kind === "Deployment") result.metadata.generation = 1;
  return result;
}

for (const mode of cases) {
  const work = await mkdtemp(join(tmpdir(), "oat-cli-create-"));
  const input = join(work, "input.json");
  await writeFile(input, JSON.stringify({
    schema_version: 1, authority, original_deadline_unix_ms: Date.now() + 60_000,
    inventory: inventory(), service_account: serviceAccount, manifests,
    runtime_directory: join(work, "runtime"), stamp_config_path: "/offline/stamp.yaml",
    bootstrap_env_file: "/offline/private.env",
  }), { mode: 0o600 });
  const child = spawn(binary, ["--fixture-setup", input, "--fixture-bridge-stdio"], {
    cwd: root, stdio: ["pipe", "pipe", "pipe"],
  });
  const closed = new Promise<number | null>((resolveClose, reject) => {
    child.once("error", reject);
    child.once("close", resolveClose);
  });
  const timeout = setTimeout(() => child.kill("SIGKILL"), 10_000);
  let stderr = "";
  child.stderr.setEncoding("utf8").on("data", (chunk: string) => { stderr += chunk; });
  child.stdin.on("error", error => { if ((error as NodeJS.ErrnoException).code !== "EPIPE") throw error; });
  let lastInventory: Inventory | undefined;
  const created: KubeObject[] = [];
  let receipt: { load_admitted: boolean; inventory: Inventory } | undefined;
  let namespaceReads = 0;
  let creates = 0;
  try {
    for await (const line of createInterface({ input: child.stdout })) {
      const request = JSON.parse(line) as Request;
      if (request.kind === "fixture_setup_receipt") {
        receipt = JSON.parse(line);
        continue;
      }
      assert.equal(request.kind, "command_request");
      const { request_sha256, ...unsigned } = request;
      assert.equal(sha(canonical(unsigned)), request_sha256);
      assert.equal(sha(request.stdin ?? ""), request.stdin_sha256);
      assert.deepEqual(request.authority, authority);
      let output: string;
      let exitCode = 0;
      if (request.executable === "npx") {
        assert.ok(request.argv.includes("publish-inventory"));
        lastInventory = JSON.parse(request.stdin ?? "") as Inventory;
        assert.equal(lastInventory.load_admission, "not_authorized");
        const fic = lastInventory.fic as { state: string };
        output = JSON.stringify({ ok: true, receipt: {
          status: "persisted", campaign_id: lastInventory.campaign_id, owner_id: lastInventory.owner_id,
          fencing_token: lastInventory.fencing_token, orchestration_run_id: lastInventory.orchestration_run_id,
          cell_run_id: lastInventory.cell_run_id, namespace_uid: lastInventory.namespace.uid,
          resource_count: lastInventory.resources.length, fic_state: fic.state, qap_state: null,
          etag: '"offline-only-etag"',
        } });
      } else {
        assert.equal(request.executable, "kubectl");
        assert.deepEqual(request.argv.slice(0, 4), ["--context", authority.context, "--server", authority.server]);
        const args = request.argv.slice(4);
        if (args[0] === "get" && args[1] === "namespace") {
          if (created.length === 0) output = "";
          else {
            namespaceReads++;
            const current = structuredClone(created[0]);
            if (mode === "namespace-replaced") current.metadata.uid = "00000000-0000-0000-0000-000000000999";
            if (mode === "namespace-origin") current.metadata.labels!["arrowcol-oat/owner-token"] = "foreign";
            if (mode === "namespace-rv") delete current.metadata.resourceVersion;
            output = mode === "namespace-absent" ? "" : JSON.stringify(current);
          }
        } else if (args[0] === "get" && args.includes("--ignore-not-found")) {
          output = '{"apiVersion":"v1","kind":"List","items":[]}';
        } else if (args[0] === "create") {
          creates++;
          assert.ok(lastInventory, "planned inventory must precede creates");
          const body = JSON.parse(request.stdin ?? "") as KubeObject & { items?: KubeObject[] };
          if (body.kind === "Namespace") {
            const actual = observed(body, 0);
            created.push(actual);
            output = JSON.stringify(actual);
          } else {
            assert.equal(lastInventory.resources.length, 4);
            assert.ok(lastInventory.resources.every(item => item.state === "planned" && item.uid === null));
            const actual = (body.items ?? []).map((item, index) => observed(item, index + 1));
            created.push(...structuredClone(actual));
            if (mode === "duplicate") actual[3] = structuredClone(actual[0]);
            if (mode === "unexpected") actual[3].metadata.name = "foreign-deployment";
            const returned = ["partial", "nonzero-partial"].includes(mode) ? actual.slice(0, 2) : actual;
            // kubectl create Flatten().Visit invokes the JSON printer once per created object.
            output = returned.map(item => JSON.stringify(item, null, 4)).join("\n") + "\n";
            if (mode === "truncated") output = output.slice(0, -8);
            if (mode === "malformed") output += "not JSON";
            if (mode === "nonzero-partial") exitCode = 1;
          }
        } else if (args[0] === "get") {
          output = JSON.stringify({ apiVersion: "v1", kind: "List", items: created.slice(1) });
        } else {
          throw new Error(`unexpected command ${JSON.stringify(request.argv)}`);
        }
      }
      child.stdin.write(JSON.stringify({
        schema_version: 1, kind: "command_response", request_id: request.request_id,
        request_sha256, exit_code: exitCode, stdout: output, stderr: "", truncated: false,
      }) + "\n");
    }
    const code = await closed;
    console.log(JSON.stringify({ offline_cli_observation: mode, code, creates, namespaceReads,
      receiptNamespaceUid: receipt?.inventory.namespace.uid ?? null,
      simulatedNamespaceUid: mode === "namespace-replaced" ? "00000000-0000-0000-0000-000000000999" : created[0]?.metadata.uid,
    }));
    if (mode === "nominal" || mode === "stream-only") {
      assert.equal(code, 0, `genuine per-object create stdout must succeed:\n${stderr}`);
      assert.equal(receipt?.load_admitted, false);
      assert.equal(creates, 2);
      if (mode === "nominal") assert.ok(namespaceReads > 0, "successful setup must reobserve the Namespace");
    } else {
      assert.notEqual(code, 0, `${mode} must refuse successful setup; stderr=${stderr}`);
      assert.equal(receipt, undefined);
      assert.ok(lastInventory?.resources.every(item => item.uid === null && item.state === "planned"),
        "failed/partial setup must retain planned custody, not publish successful child identity");
      assert.equal(creates, 2, "must not replay a create after an uncertain or invalid result");
      if (mode.startsWith("namespace-")) assert.ok(namespaceReads > 0, "must actually read the Namespace");
    }
    console.log(`OFFLINE_CLI_${mode}: PASS (exit=${code}, creates=${creates}, namespace_readbacks=${namespaceReads})`);
  } finally {
    clearTimeout(timeout);
    child.stdin.destroy();
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    await closed;
    await rm(work, { recursive: true, force: true });
  }
}
