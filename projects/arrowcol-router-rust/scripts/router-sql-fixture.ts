import { spawnSync } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { hostname } from "node:os";
import { resolve, join } from "node:path";

// Provision only an owned Compose project; never use the shared arrowcol-infra stack.
const root = process.cwd();
const output = resolve(root, ".temp", `router-sql-${randomUUID()}`);
const project = `router-sql-${randomUUID()}`;
const infra = join(root, "projects", "arrowcol-infra");
mkdirSync(output, { recursive: true, mode: 0o700 });
const environment = { ...process.env };
function run(command: string, args: string[], capture = false): string {
  const result = spawnSync(command, args, {
    cwd: root, env: environment, encoding: "utf8",
    stdio: capture ? ["ignore", "pipe", "pipe"] : "inherit",
  });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} ${args[0]} failed (${result.status}): ${capture ? result.stderr : "see private execution log"}`);
  return result.stdout?.trim() ?? "";
}
run("git", ["check-ignore", output]);
run("docker", ["info"], true);
run("docker", ["inspect", hostname()], true);
const revision = run("git", ["rev-parse", "HEAD"], true);
const envFile = join(output, "compose.env");
const secrets = ["sa", "app", "migrator"].map((role) => {
  const path = join(output, `${role}.secret`);
  writeFileSync(path, `Aa1!${randomBytes(24).toString("hex")}\n`, { mode: 0o600 });
  return path;
});
writeFileSync(envFile, [
  `COMPOSE_PROJECT_NAME=${project}`, "SQLSERVER_PORT=0",
  `SQLSERVER_SA_PASSWORD_FILE=${secrets[0]}`,
  `ARROWCOL_APP_PASSWORD_FILE=${secrets[1]}`,
  `ARROWCOL_MIGRATOR_PASSWORD_FILE=${secrets[2]}`,
  "SQLSERVER_MEMORY_LIMIT_MB=2048",
].join("\n") + "\n", { mode: 0o600 });
const compose = [
  "compose", "--project-name", project, "--project-directory", infra,
  "--env-file", envFile, "-f", join(infra, "compose.yaml"),
];
let created = false;
let connected = false;
try {
  // Build before starting SQL, so compilation is not concurrent with measurements.
  run("cargo", ["test", "--release", "--locked", "-p", "arrowcol-router-rust",
    "--test", "pingora_integration", "--no-run"]);
  created = true;
  run("docker", [...compose, "up", "--build", "--detach", "--wait", "--wait-timeout", "600", "sqlserver"]);
  run("docker", [...compose, "run", "--rm", "--no-deps", "database-bootstrap"]);
  run("docker", [...compose, "run", "--rm", "--no-deps", "arrowcol-migrate"]);
  run("docker", ["network", "connect", `${project}_default`, hostname()]);
  connected = true;
  Object.assign(environment, {
    ARROWCOL_ROUTER_SQL_FIXTURE: "1", ARROWCOL_ROUTER_SQL_PROJECT: project,
    ARROWCOL_ROUTER_SQL_OUTPUT: output, ARROWCOL_SQLSERVER_TEST: "1",
    ARROWCOL_SQLSERVER_HOST: `${project}-sqlserver-1`, ARROWCOL_SQLSERVER_PORT: "1433",
    ARROWCOL_APP_PASSWORD_FILE: secrets[1], ARROWCOL_MIGRATOR_PASSWORD_FILE: secrets[2],
  });
  run("cargo", ["test", "--release", "--locked", "-p", "arrowcol-router-rust",
    "--test", "pingora_integration", "router_sql_fixture::router_sql_component",
    "--", "--ignored", "--exact", "--nocapture"]);
  run("cargo", ["test", "--release", "--locked", "-p", "arrowcol-routing", "--test", "live_sql",
    "live_sql_coordinates_router_races_and_fenced_drain_transfer", "--", "--ignored", "--exact", "--nocapture"]);
  const result = JSON.parse(readFileSync(join(output, "fixture-results.json"), "utf8"));
  writeFileSync(join(output, "provenance.json"), JSON.stringify({
    revision, project, host: hostname(), command: "npx --no-install tsx projects/arrowcol-router-rust/scripts/router-sql-fixture.ts",
    runtime: process.version, completed_at: new Date().toISOString(),
  }, null, 2), { mode: 0o600 });
  console.log(JSON.stringify({ output, revision, matched_pairs: result.matched_pairs, records: result.records }));
} finally {
  let cleanupError: unknown;
  if (connected) {
    try { run("docker", ["network", "disconnect", `${project}_default`, hostname()]); }
    catch (error) { cleanupError = error; }
  }
  if (created) {
    try { run("docker", [...compose, "down", "--volumes", "--timeout", "30"]); }
    catch (error) { cleanupError = error; }
  }
  for (const secret of secrets) unlinkSync(secret);
  console.log(`Private fixture evidence: ${output}`);
  if (cleanupError) throw cleanupError;
}
