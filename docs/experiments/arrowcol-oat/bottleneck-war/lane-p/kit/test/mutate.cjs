const fs = require('node:fs'), os = require('node:os'), path = require('node:path'), { spawnSync } = require('node:child_process');
const KIT = process.argv[2];
const mutants = [
  ['policy.mjs', "if (footerSha !== null) need(r.footer_sha256 === footerSha,", "if (false) need(r.footer_sha256 === footerSha,", 'score ignores the footer hash'],
  ['policy.mjs', "export function resumeProblems(prior, plan, current, output) {", "export function resumeProblems(prior, plan, current, output) { return [];", 'resume accepts any prior output'],
  ['policy.mjs', "if (r?.status !== 'ok') problems.push(`${at}: status", "if (false) problems.push(`${at}: status", 'resume accepts failed cells'],
  ['policy.mjs', "if (r?.plan_sha256 !== current.plan_sha256) problems", "if (false) problems", 'resume accepts a different plan'],
  ['policy.mjs', "need(r.duration === plan.duration && r.warmup === plan.warmup,", "need(true,", 'admission ignores row duration/warmup'],
  ['policy.mjs', "need(isObject(s) && finite(s.seconds) && Math.abs", "need(true || isObject(s) && finite(s.seconds) && Math.abs", 'admission ignores the measured window'],
  ['policy.mjs', "need(finite(r.committed_rps) && r.committed_rps > 0 &&", "need(true || finite(r.committed_rps) && r.committed_rps > 0 &&", 'admission accepts non-finite rates'],
  ['policy.mjs', "  need(r.dirty === '', 'identity.dirty", "  need(true, 'identity.dirty", 'identity accepts a dirty tree'],
  ['policy.mjs', "for (const key of ['config_sha256', 'harness_sha256', 'footer_sha256']) need(hex64(r[key])", "for (const key of []) need(hex64(r[key])", 'identity accepts missing hashes'],
  ['policy.mjs', "HARNESS_FILES.every(file => hex64(r.harness_files[file])) &&", "true &&", 'identity accepts malformed harness hashes'],
  ['policy.mjs', "`${where}: ${o.compression} compression level must be", "`${where}: ${o.compression} compression level must be", 'IDENTITY (control: no change)'],
  ['screen-plan.mjs', "if (!(Math.abs(drift) <= DRIFT_BOUND)) problems.push", "if (false) problems.push", 'admission ignores bookend drift'],
  ['confirm.mjs', "insist(footer === s.identity.footer_sha256,", "insist(true,", 'register ignores the fixture footer'],
  ['policy.mjs', "for (const key of Object.keys(data)) check(PLAN_FIELDS.includes(key),", "for (const key of []) check(PLAN_FIELDS.includes(key),", 'plan accepts unknown fields'],
  ['policy.mjs', "check(Object.hasOwn(ENV_RULES, name), `${where}: unknown or forbidden", "check(true, `${where}: unknown or forbidden", 'env accepts typos'],
];
mutants.push(['policy.mjs', 'check(o.compression_level >= range[0] && o.compression_level <= range[1],', 'check(true,', 'codec level range unchecked']);
mutants.push(['policy.mjs', 'need(isObject(built) && BINARIES.every', 'need(true || isObject(built) && BINARIES.every', 'identity ignores provenance binary hashes']);
let bad = 0;
for (const [file, from, to, label] of mutants) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'bwp-mut-'));
  fs.cpSync(KIT, dir, { recursive: true, filter: s => !/[\\/]fixture\.new$/.test(s) });
  const target = path.join(dir, file), text = fs.readFileSync(target, 'utf8');
  if (!text.includes(from)) { console.log(`MISSING_ANCHOR ${label}`); bad++; fs.rmSync(dir, { recursive: true, force: true }); continue; }
  fs.writeFileSync(target, text.replace(from, to));
  if (file !== 'prepare-harness.mjs' && ['policy.mjs', 'audit.mjs'].includes(file)) fs.copyFileSync(target, path.join(dir, 'fixture', file));
  const r = spawnSync(process.execPath, ['--test', path.join(dir, 'test', 'kit.test.mjs')], { encoding: 'utf8' });
  const failing = (r.stdout.match(/^not ok \d+ - (.*)$/gm) || []).map(s => s.replace(/^not ok \d+ - /, ''));
  const control = from === to;
  const killed = r.status !== 0;
  if (killed === control) bad++;
  console.log(`${control ? (killed ? 'CONTROL_FAILED' : 'CONTROL_OK') : (killed ? 'KILLED' : 'SURVIVED')} ${label}${failing.length ? ' <- ' + failing.join(' | ') : ''}`);
  fs.rmSync(dir, { recursive: true, force: true });
}
process.exit(bad ? 1 : 0);

