"""Offline shared-path report, derived from all retained paired-run values."""
from collections import defaultdict
from html.parser import HTMLParser
import json
import math
from pathlib import Path
import re
from statistics import fmean

from delta_benchmark import PHASES, digest, distribution, save
from publication_gate import REPO
from coverage import shared_delta


def load(path):
    return json.loads(Path(path).read_text())


def latency_breakdown(records):
    stages = {name: [] for name in ("before_identity_ms", "identity_to_final_ms",
                                  "final_to_terminal_ms", "terminal_ms", "post_identity_percent")}
    for record in records:
        if record["terminal"] != "final":
            continue
        identity, final, terminal = (record[key] for key in
            ("admission_identity_observed_ms", "final_receipt_ms", "terminal_ms"))
        assert all(math.isfinite(value) for value in (identity, final, terminal))
        assert 0 <= identity <= final <= terminal and terminal > 0
        values = (identity, final - identity, terminal - final, terminal,
                  100 * (final - identity) / terminal)
        for name, value in zip(stages, values):
            stages[name].append(value)
    return {"inputs": len(records), "n": len(stages["terminal_ms"]),
            **{name: {**distribution(values), "mean": fmean(values) if values else None}
               for name, values in stages.items()}}


def analyze(cohort):
    directory = REPO / "target/delta-benchmark" / cohort
    attempts = load(directory / "attempts.json")
    assert len(attempts) == 6 and all(a["status"] == "completed-accounted-phases" for a in attempts)
    runs = []
    for attempt in attempts:
        root = REPO / attempt["evidence"]
        raw = load(root / "benchmark-inputs.json")
        phases = load(root / "phases.json")
        reconciliation = load(root / "independent-reconciliation.json")
        assert len(raw) == 56 and all(r["payload_retries"] == 0 for r in raw)
        assert [p["phase"] for p in phases] == [p[0] for p in PHASES]
        for phase in phases:
            records = [r for r in raw if r["phase"] == phase["phase"]]
            assert distribution([r["terminal_ms"] for r in records if r["terminal"] == "final"]) == phase["terminal"]
            assert phase["completed_rows"] == sum(r["rows"] for r in records if r["completion"] != "unresolved")
            assert phase["rows_per_second"] == phase["completed_rows"] / phase["seconds"]
            phase["resolved_receipt"] = distribution(
                [r["resolved_receipt_ms"] for r in records if "resolved_receipt_ms" in r])
            phase["latency_breakdown"] = latency_breakdown(records)
        assert all(reconciliation[key]["missing"] == reconciliation[key]["extra_or_duplicate"] == 0
                   for key in ("main", "cas"))
        receipts = load(root / "sql-final-receipts.json")
        receipt_map = {r["operation_id"]: r for r in receipts}
        completed = [r for r in raw if r["completion"] != "unresolved"]
        assert len(receipts) == len(completed) + 2
        for record in completed:
            final = record["receipts"][-1]
            sql = receipt_map[final["operation"]]
            assert (sql["table_id"], sql["version"], sql["digest"]) == (
                record["table"], final["version"], final["digest"])
        samples = [json.loads(line) for line in (root / "observations/samples.jsonl").read_text().splitlines()]
        assert all("error" not in s for s in samples)
        last_input_ns = max(r["ended_ns"] for r in raw)
        samples = [s for s in samples if s["ended_ns"] <= last_input_ns]
        roles = defaultdict(lambda: {"cpu_seconds": 0, "memory_peak_bytes": 0, "samples": 0})
        pending = active = 0
        for sample in samples:
            for component in sample["containers"]:
                role = roles[component["service"]]
                role["samples"] += 1
                role["memory_peak_bytes"] = max(role["memory_peak_bytes"], component["memory_usage_bytes"] or 0)
                if component["cpu_interval"]:
                    role["cpu_seconds"] += component["cpu_interval"]["cpu_usage_ns"] / 1e9
            view = sample.get("control_observation", {})
            active = max(active, sum(m.get("active", 0) for m in view.get("members", [])))
            assignments = view.get("assignments", {})
            assignments = assignments.values() if isinstance(assignments, dict) else assignments
            pending = max(pending, sum(len(a.get("record", a).get("pending", [])) for a in assignments))
        sql_samples = [s for s in load(root / "sql-cgroup.json") if s["time_ns"] <= last_input_ns]
        roles["sql"] = {"cpu_seconds": (sql_samples[-1]["cpu_total_usage_ns"] - sql_samples[0]["cpu_total_usage_ns"]) / 1e9,
                        "memory_peak_bytes": max(s["memory_usage_bytes"] for s in sql_samples),
                        "samples": len(sql_samples)}
        native_observations, routing_observations = {}, {}
        first_input_ms = min(r["started_ns"] for r in raw) // 1_000_000
        for log in (root / "logs").glob("*.log"):
            observations, routes = [], []
            for line in log.read_text().splitlines():
                try:
                    value = json.loads(line)
                except (ValueError, TypeError):
                    continue
                if isinstance(value, dict) and value.get("event") == "flight_observation":
                    observations.append(value)
                if (isinstance(value, dict) and value.get("event") == "delta_route"
                        and first_input_ms <= value["observed_ms"] <= last_input_ns // 1_000_000):
                    routes.append(value)
            if routes:
                routing_observations[log.stem] = {
                    "lookups": len(routes), "cache_hits": sum(r["cache_hit"] for r in routes),
                    "cache_misses": sum(not r["cache_hit"] for r in routes),
                    "data_only_lookups": sum(r["data_only"] for r in routes)}
            if observations:
                gauges = ("decoded_batch_bytes", "gateway_input_bytes", "gateway_result_bytes", "mailbox_waiters", "writer_streams")
                native_observations[log.stem] = {
                    "samples": len(observations),
                    "high_water": {g: max(o.get(g, {}).get("lifetime_high_water", 0) for o in observations) for g in gauges},
                    "last_cache": observations[-1].get("cache"),
                }
        cost = load(root / "substrate-costs.json")
        substrate_cpu = sum(v["cpu_seconds"] for k, v in roles.items() if k.startswith("collector") or k in ("sql", "publisher"))
        workload_seconds = sum(p["seconds"] for p in phases)
        completed_rows = sum(p["completed_rows"] for p in phases)
        runs.append({**attempt, "phases": phases, "rows": sum(p["completed_rows"] for p in phases),
            "workload_seconds": workload_seconds,
            "workload_rows_per_second": completed_rows / workload_seconds,
            "input_span_seconds": (last_input_ns - min(r["started_ns"] for r in raw)) / 1e9,
            "matched_rows": sum(p["completed_rows"] for p in phases if p["comparable"]),
            "matched_offered_rows": sum(p["offered_rows"] for p in phases if p["comparable"]),
            "nonfinal_inputs": len(raw) - len(completed),
            "nonfinal_rpcs": sum(r["terminal"] != "final" for r in raw),
            "missing_submitted_rows": sum(reconciliation[key]["missing_submitted_rows"] for key in ("main", "cas")),
            "reconciliation": reconciliation, "roles": dict(roles), "substrate": cost,
            "substrate_cpu_seconds": substrate_cpu, "total_role_cpu_seconds": sum(v["cpu_seconds"] for v in roles.values()),
            "observed_pending_peak": pending, "observed_active_streams_peak": active,
            "native_observations": native_observations,
            "routing_observations": routing_observations,
            "identity_resolved_final": sum(r["completion"] == "identity-resolved-final" for r in raw),
            "transitions": load(root / "transitions.json")})
    for rep in (1, 2, 3):
        pair = [r for r in runs if r["repetition"] == rep]
        assert {r["arm"] for r in pair} == {"pingora", "adr"} and len(pair) == 2
        assert pair[0]["matched_offered_rows"] == pair[1]["matched_offered_rows"]
        for phase, jobs, concurrency, rows in PHASES:
            comparable = []
            for run in pair:
                value = next(p for p in run["phases"] if p["phase"] == phase)
                assert (value["jobs"], value["concurrency"], value["rows_per_job"]) == (jobs, concurrency, rows)
                comparable.append(value["comparable"])
                if value["comparable"]:
                    assert value["actual_gateways"] == (2 if phase == "gateways-two" else 1)
                    records = [r for r in load(REPO / run["evidence"] / "benchmark-inputs.json")
                               if r["phase"] == phase]
                    ports = {r["gateway_port"] for r in records}
                    assert len(ports) == value["actual_gateways"]
                    assert all(sum(r["gateway_port"] == port for r in records) == jobs // len(ports)
                               for port in ports)
            assert comparable[0] == comparable[1], "asymmetric comparison exclusion"
    save(directory / "analysis.json", runs)
    return directory, runs


def generate(cohort):
    from report import EDITORIAL_CSS, esc, table
    directory, runs = analyze(cohort)
    fmt = lambda n: "N/A" if n is None else f"{n:,.1f}"
    warm = [dict(run=r, phase=next(p for p in r["phases"] if p["phase"] == "warm")) for r in runs]
    maximum = max(p["phase"]["terminal"]["p50"] for p in warm)
    charts = []
    for item in warm:
        run, phase = item["run"], item["phase"]
        value = phase["terminal"]["p50"]
        charts.append(f'<div class="bar-row"><span>R{run["repetition"]} {esc(run["arm"])}</span>'
                      f'<div class="bar-track"><div class="bar {"adr" if run["arm"] == "adr" else ""}" '
                      f'data-value="{value}" data-max="{maximum}" style="width:{100 * value / maximum:.6f}%"></div></div>'
                      f'<output>{fmt(value)} ms</output></div>')
    latency_charts, latency_rows = [], []
    latency_max = max(item["phase"]["latency_breakdown"]["terminal_ms"]["mean"] for item in warm)
    post_shares = [item["phase"]["latency_breakdown"]["post_identity_percent"]["p50"] for item in warm]
    for item in warm:
        run, phase = item["run"], item["phase"]
        name = f'R{run["repetition"]} {run["arm"]}'
        values = phase["latency_breakdown"]
        segments = []
        for key, label in (("before_identity_ms", "Before admission ID"),
                           ("identity_to_final_ms", "Admission ID to final receipt"),
                           ("final_to_terminal_ms", "Final receipt to RPC end")):
            mean = values[key]["mean"]
            segments.append(
                f'<span class="latency-segment {key}" data-stage="{key}" data-ms="{mean}" '
                f'data-scale="{latency_max}" style="width:{100 * mean / latency_max:.6f}%" '
                f'title="{esc(label)}: {fmt(mean)} ms mean"></span>')
        label = (f'{name}: before admission ID {fmt(values["before_identity_ms"]["mean"])} ms; '
                 f'admission ID to final receipt {fmt(values["identity_to_final_ms"]["mean"])} ms; '
                 f'final receipt to RPC end {fmt(values["final_to_terminal_ms"]["mean"])} ms. '
                 f'{values["n"]} of {values["inputs"]} inputs, means.')
        latency_charts.append(
            f'<div class="bar-row"><span>{esc(name)}</span>'
            f'<div class="latency-track" role="img" aria-label="{esc(label)}">{"".join(segments)}</div>'
            f'<output>{fmt(values["terminal_ms"]["mean"])} ms</output></div>')
        latency_rows.append((name, f'{values["n"]}/{values["inputs"]}',
            fmt(values["before_identity_ms"]["p50"]), fmt(values["identity_to_final_ms"]["p50"]),
            fmt(values["final_to_terminal_ms"]["p50"]), fmt(values["post_identity_percent"]["p50"]) + "%",
            fmt(values["terminal_ms"]["mean"])))
    common_rows = runs[0]["matched_offered_rows"]
    resolved_count = sum(r["identity_resolved_final"] for r in runs)
    missing_rows = sum(r["missing_submitted_rows"] for r in runs)
    nonfinal_count = sum(r["nonfinal_inputs"] for r in runs)
    matched_pairs = sum(p["comparable"] for r in runs for p in r["phases"]) // 2
    repaired = matched_pairs == len(PHASES) * 3
    topology = (
        "Both arms served two gateways. Data-only Pingora resolves through the primary router, "
        "without authority credentials or an independent controller."
        if repaired else
        "Gateway scale-out was blocked in adapted Pingora: its second router rejected the live controller.")
    history_note = (
        "All submitted rows reconciled; both arms accumulated identical completed row counts."
        if not missing_rows else
        "Failures change later history sizes: later phase ratios are not a clean latency comparison "
        "of equally completed histories.")
    adr_warm_wins = sum(
        next(i["phase"]["terminal"]["p50"] for i in warm if i["run"]["repetition"] == rep and i["run"]["arm"] == "adr") <
        next(i["phase"]["terminal"]["p50"] for i in warm if i["run"]["repetition"] == rep and i["run"]["arm"] == "pingora")
        for rep in (1, 2, 3))
    paired_summary = []
    for repetition in (1, 2, 3):
        pair = {r["arm"]: r for r in runs if r["repetition"] == repetition}
        warm_pair = {arm: next(p for p in r["phases"] if p["phase"] == "warm") for arm, r in pair.items()}
        elephant = {arm: next(p for p in r["phases"] if p["phase"] == "elephant-table") for arm, r in pair.items()}
        order = " → ".join("Pingora" if r["arm"] == "pingora" else "ADR"
                           for r in runs if r["repetition"] == repetition)
        paired_summary.append((f"R{repetition}",
            order,
            fmt(warm_pair["pingora"]["rows_per_second"]), fmt(warm_pair["adr"]["rows_per_second"]),
            f'{elephant["pingora"]["completed_rows"] // elephant["pingora"]["rows_per_job"]}/{elephant["pingora"]["jobs"]}',
            f'{elephant["adr"]["completed_rows"] // elephant["adr"]["rows_per_job"]}/{elephant["adr"]["jobs"]}',
            f'{pair["pingora"]["missing_submitted_rows"]:,} · {pair["adr"]["missing_submitted_rows"]:,}'))
    requirements = [
        ("Table identity, schemas and authorization",
         "Route by immutable tenant/catalog/table identity, not schema fingerprint. Validate registered versions, evolution, Arrow dictionaries and Parquet compatibility. During registry outages, allow only authorized cached versions; reject unknown ones."),
        ("Durable ownership metadata",
         "Maintain one authoritative owner, process incarnation, local writer generation, increasing epoch, lifecycle and accepted commit history per table. Gateway caches and placement suggestions cannot grant write authority."),
        ("New streams and lazy placement",
         "Clients name a table through a stable endpoint, not a collector. Reuse its active owner; for an unassigned table choose an eligible, compatible collector and reserve capacity against concurrent placement races."),
        ("Pressure, hot tables and idle tables",
         "Use CPU, memory, backlog, progress and table cost, not just stream counts. Bound aggregate buffers/open writers; backpressure overload. More collectors cannot remove one hot table's single-writer limit."),
        ("Planned migration and rebalancing",
         "Persist a resumable move: reserve target, stop new table admission, drain with a deadline, fence source, recover accepted data, activate successor. Use cooldowns and move budgets; do not transfer live sockets."),
        ("Scale out and scale in",
         "Add/remove gateways without independent authorities. Add collectors with deliberate redistribution; drain/evacuate owned tables before retirement. Keep unrelated tables available where their dependencies permit."),
        ("Stalled, zombie or partitioned collector",
         "Treat heartbeat as liveness, not ingestion progress. A timeout is suspicion, not proof of death. Establish an authoritative fence before replacement; a resumed old writer must not publish new work."),
        ("Source or recovery-target failure",
         "Recover after crashes before/during drain, encoding or publication. Finish already accepted decisions; reject stale new proposals. If the target fails, choose another with a higher epoch, never roll ownership back."),
        ("Controller restart or competing controllers",
         "Resume persisted assignments, reservations and migration stages after restart. Serialize control identity and ownership transitions; competing controllers must not create split-brain writers."),
        ("Stale routes and gateway failure",
         "Resnapshot missed/expired assignment updates; stale routes cannot authorize commits. On gateway death mid-stream, clients reconnect to the stable endpoint. Gateways forward errors/receipts, never replay payloads or own a receipt ledger."),
        ("One local writer across runtimes",
         "Enforce table affinity and one effective writer generation across cores, overlapping runtimes and hot reconfiguration. Drain completion/encoding paths before retiring local state."),
        ("Delta publication and storage failure",
         "Delta is the final destination. Fence at publication, not just admission; keep staging invisible, materialize accepted versions in order, and ACK only after the selected durability boundary. Reject conflicting commits and resolve partial failures."),
        ("Lost replies and client retries",
         "Correlate completion with exact operation identity; an admission ID is not success. Retain unacknowledged Arrow units and reconstruct schemas/dictionaries on retry. Target at-least-once delivery; duplicates are allowed, lost acknowledged data is not."),
        ("Authority outage and safe recovery",
         "Fail closed for new grants/decisions when authority is unavailable. Reconcile ambiguous outcomes and accepted work without inventing success. Restore must prevent old deployments or reused epochs from regaining authority."),
        ("Security, observability and production scale",
         "Authenticate control/data access and prevent publication bypass. Measure routing, queueing, commit/recovery latency, pressure, failures and duplicates. Qualify many schemas and the stated million-client / PB-per-day target with bounded resources."),
    ]
    phase_descriptions = {
        "cold": "Fresh deployment, first assignments/table initialization; not a host/OS-cache flush.",
        "warm": "Reuse the two table assignments after cold; same small batch size.",
        "resource-pressure": "Larger batches and four concurrent inputs; no external CPU-stress injection in this cohort.",
        "elephant-table": "All eight large inputs target main: one hot table, one owner/mailbox.",
        "cooperative-migration": "Manually move main, wait for the new active owner, then send traffic; no offered load during cutover.",
        "collectors-four": "Register collector 4, then send traffic; membership growth, not automatic table redistribution.",
        "collectors-three": "Stop collector 4, wait for eligible membership/active assignments, then send traffic; not proof of busy-owner evacuation.",
        "gateways-two": "Add gateway 2; three inputs use each gateway. Pingora replica is data-only.",
        "gateways-one": "Stop gateway 2 between phases, then send through the primary; not a mid-stream kill.",
    }
    scenario_rows = [(i, name, jobs, concurrency, f"{rows:,}", phase_descriptions[name])
                     for i, (name, jobs, concurrency, rows) in enumerate(PHASES, 1)]
    duration_rows = [
        (f'R{r["repetition"]} {r["arm"]}', f'{r["rows"]:,}', fmt(r["workload_seconds"]),
         fmt(r["workload_rows_per_second"]), fmt(r["input_span_seconds"]))
        for r in runs]
    workload_total = sum(r["workload_seconds"] for r in runs)
    overview = f"""<div class="eyebrow">Fresh shared SQL / Delta · {esc(cohort)} · one host</div>
<h1>Same ingestion path.<br>Different placement lifecycle.</h1>
<p class="deck">ADAPTED Pingora/router-hosted placement versus ADR gateway/separate controller.
Both use identical frozen collectors, publisher, SQL configuration and ordinary Parquet-backed Delta.</p>
<section id="requirements"><h2>Requirements: scenarios the final design must support</h2>
<p class="note">Target behavior from the supplied ADR and the confirmed Delta destination, <strong>not an all-pass
claim for either PoC</strong>. Logical responsibilities need not be separate processes.
The <a href="#feature-coverage">evidence matrix</a> distinguishes implemented, partial and unimplemented behavior.</p>
{table(("Scenario / responsibility", "Required behavior"), requirements)}
<p class="note"><strong>Still to decide or qualify:</strong> production latency/recovery SLOs, controller/store HA,
cloud-storage and regional/backup recovery, client-crash coverage, and WAL versus no WAL.
Client-crash durability needs a durable source/outbox; early ACK needs verified recoverable durable buffering.
This PoC uses no ingestion WAL and ACKs after Delta publication. It does not qualify those production guarantees,
an autonomous rebalancer, a general progress-stall detector or production multi-tenant/schema scale.</p></section>
<div class="scope"><strong>Three paired repetitions, not a proxy microbenchmark.</strong>
Pingora’s SQL grants replace disk/cache write authority. Its least-pressure/reservation policy and ADR’s
assignment-count weighting remain different. {topology} No fencing bypass was used.</div>
<div class="stats"><div class="stat"><b>3 pairs</b><span>alternating arm order; every attempt retained</span></div>
<div class="stat"><b>{common_rows:,}</b><span>matched offered rows per arm/repetition</span></div>
<div class="stat"><b>{nonfinal_count} without final</b><span>{missing_rows:,} submitted rows absent; no payload replay</span></div>
<div class="stat"><b>P99 N/A</b><span>finite samples; no sustained-capacity claim</span></div></div>
<nav><a href="#requirements">Requirements</a><a href="#architecture">Designs</a><a href="#run-guide">Run guide</a>
<a href="#duration">Duration &amp; throughput</a><a href="#warm">Warm latency</a><a href="#latency-layers">Where time goes</a><a href="#insights">Interpretation</a>
<a href="#appendix">Evidence &amp; historical appendix</a></nav>
<section id="architecture"><h2>Where decisions live</h2><div class="split">
<article class="arch"><h3>Adapted Pingora</h3><div class="node"><strong>Client → Pingora router</strong>
<small>Primary: placement/control + cache; data replicas: remote resolution + cache</small></div><div class="route">↓ authenticated Flight</div>
<div class="node">Shared adr-flight collectors</div><div class="control"><strong>Router ↔ SQL authority</strong>
Ownership and accepted decisions; disk is not competing authority.</div></article>
<article class="arch adr"><h3>ADR</h3><div class="node"><strong>Client → ADR gateway</strong>
<small>Proxy + assignment cache</small></div><div class="route">↓ authenticated Flight</div>
<div class="node">Same adr-flight collectors</div><div class="control"><strong>Separate controller ↔ SQL authority</strong>
Independent gateway scaling; extra control process cost.</div></article></div>
<div class="boundary"><strong>Identical shared publication boundary:</strong> staging-only collector →
trusted adr-publisher → accepted SQL decision → real Delta materialization → SQL final receipt →
DELTA_COMMITTED → terminal RPC. DELTA_OPERATION is only an admitted operation identity.</div></section>
<section id="run-guide"><h2>How to read R1, R2, R3 and the run order</h2>
<p><strong>R = repetition; each row is a pair of full runs.</strong> R1, R2 and R3 repeat the same
56-input, nine-phase workload with the same binaries, two tables, batch sizes, concurrency and resource caps.
Each arm starts with fresh isolated SQL/Delta state. These are not three configuration variants.
Within each run, the tables and commit history accumulate across phases.</p>
<p><strong>Order = which complete design ran first, not packet or row ordering.</strong>
R1: Pingora then ADR; R2: ADR then Pingora; R3: Pingora then ADR. Runs are sequential on one host.
Host cache state, disk activity, scheduling and background contention can favor either position.
Reversing order reduces a fixed first/second-position bias; it does not prove that order caused a result
or eliminate it with only three pairs. Cold deployment state does not reset host caches.</p>
<h3>Same nine scenarios in every run, in this sequence</h3>
{table(("Step", "Phase", "Inputs", "Concurrency", "Rows/input", "What it exercises"), scenario_rows)}
<p class="note">One input = one fresh Flight client/RPC carrying one batch, half-closed before waiting for final completion.
Except elephant-table, inputs alternate between main and cas. Larger batches change rows/s independently of RPC latency.
These are finite workloads, not fixed-duration or persistent-stream capacity tests. Topology transitions occur
between workloads. Later phases also have longer commit histories, so their timings cannot isolate scaling effects.
Zombie/partition, lost-reply, source/target failure, controller restart and publication-fence proofs belong to
separate native fault qualification, not injected faults in these six performance runs.</p></section>
<section id="duration"><h2>How long, and how many rows per second?</h2>
{table(("Run", "Completed rows", "Workload seconds", "Workload rows/s", "First input → last scan seconds"), duration_rows)}
<p class="note"><strong>Workload rows/s = total completed rows ÷ sum of the nine phase timers</strong>, not the average
of phase rates and not rows divided by per-request latency. Timers include concurrent uploads, completion/reconciliation
and per-input standard Delta reader scans; they exclude topology transitions, setup and final audit/teardown.
The last column uses recorded first-input/last-scan timestamps and includes intervening transitions/control gaps,
but still excludes setup and final audit/teardown. Complete lifecycle wall time was not separately measured.</p>
<p><strong>{workload_total / 60:.1f} minutes of workload time across six sequential runs.</strong>
Each run completed {runs[0]["rows"]:,} rows; warm-only rates appear below.
These mixed-phase, same-host rates are observed achieved throughput, not sustainable service capacity.</p></section>
<section id="warm"><h2>Warm terminal RPC P50 — each repetition</h2>
<p class="note">12 inputs per arm/run; linear-interpolated sample median, never pooled percentiles.
Only successfully completed RPCs enter the terminal distribution; counts are retained per phase.</p>
<div class="chart">{''.join(charts)}</div>
<p class="note">P95/P99 unavailable as supported tail estimates. Exploratory P95 and all counts remain in the appendix.
{resolved_count} nonfinal RPCs resolved by exact operation identity without payload replay; these do not enter terminal-success latency.
Reader scans occur after final completion; they add matched generator load, not terminal latency.</p></section>
<section id="latency-layers"><h2>Routing vs. collection: what we actually measured</h2>
<div class="scope"><strong>Routing-only time: not isolated. Collector-only time: not isolated.</strong>
We measured end-to-end receipt boundaries, not per-service spans. Do not interpret the ~2-second
request latency as proxy overhead, or label all time before admission as routing.</div>
<div class="split latency-boundaries">
<div class="node"><strong>Before admission ID</strong><small>Client setup/upload → gateway routing/control →
collector decode, queue and SQL admission → identity reply. Mixed layers; NOT routing-only.</small></div>
<div class="node"><strong>Admission ID → final receipt</strong><small>Collector staging → publisher queue/work →
SQL authority + Delta publication + final receipt → reply transit. Shared ingestion path; NOT collector-only.</small></div>
</div>
<p><strong>{min(post_shares):.1f}–{max(post_shares):.1f}% of warm request time was after admission</strong>
(range of the six runs’ median per-request fractions). Most observed time is beyond initial routing/admission,
but SQL, filesystem, queueing and response transport costs are not separately timed.</p>
<div class="latency-legend"><span><i class="before_identity_ms"></i>Before admission ID (mixed)</span>
<span><i class="identity_to_final_ms"></i>Shared publication + reply</span>
<span><i class="final_to_terminal_ms"></i>Final reply → RPC end</span></div>
<div class="chart latency-chart">{''.join(latency_charts)}</div>
<p class="note">Warm phase only: 4,096 rows / 32 KiB logical Arrow data per input, concurrency 2.
Bars show mean milliseconds on one common scale. Per-request segments sum to terminal time; their means
are additive, unlike medians. Tiny final-to-terminal segments may be invisible.
<a href="#latency-details">Per-run medians and attribution limits</a> are in the appendix.
Nonfinal/identity-resolved RPCs are excluded from these bars, not counted as fast successes.</p></section>
<section><h2>Completion matters as much as latency</h2>
{table(("Pair", "Execution order", "Warm Pingora rows/s", "Warm ADR rows/s", "Hot-table completed Pingora", "Hot-table completed ADR", "Absent rows Pingora · ADR"), paired_summary)}
<p class="note"><strong>Previously “Elephant finals P · A”:</strong> P meant Pingora; A meant ADR.
An elephant is one disproportionately busy table: eight inputs × 65,536 rows = 524,288 rows, concurrency four,
all sent to the same owner. “8/8” means all eight eventually published with final receipts, including operation-ID
reconciliation after a nonfinal RPC; it does not mean all eight RPCs returned success.
The detailed table retains successful RPC counts. {history_note}</p></section>
<section id="insights"><h2>What this does—and does not—show</h2><ul class="insights">
<li><strong>No general performance winner established.</strong>ADR has the lower sample median in {adr_warm_wins}/3 pairs.
Only 12 warm inputs per arm/run; these observations do not establish a general speedup or supported tail estimate.</li>
<li><strong>Data-plane scale is not control-plane HA.</strong>{topology}
The primary Pingora router still couples its own data-plane and placement-controller lifecycle.</li>
<li><strong>Finite pressure exposes failures.</strong>Concurrency reaches four; elephant inputs are 65,536 rows.
The table above retains every final/absent input. Repair qualification isolated a collector mailbox/HTTP2
flow-control stall; bounded decoding now precedes publication serialization. Publisher deadline outcomes remain visible.
One hot table still has one owner/mailbox; no physical scale-out claim.</li>
<li><strong>Safety evidence stays separate.</strong>Prior native SQL/etcd fault qualification supplies fencing/recovery
evidence. Fresh performance retains naturally occurring errors and absent inputs; no injected-fault timing is pooled.</li></ul>
<div class="split"><div><h3>Adapted Pingora</h3><p><b>Pro:</b> routing and placement share a process.
<b>Con:</b> primary data/control lifecycle coupling, even with data-only replicas.</p></div>
<div><h3>ADR</h3><p><b>Pro:</b> independent gateway/controller topology.
<b>Con:</b> extra role and coordination; no automatic rebalancer or HA qualification.</p></div></div></section>
<section><h2>Next experiment — proposed, not measured</h2>
<p>Keep the shared fenced publication path. Test bounded incremental accepted-prefix recovery and
per-table authority access before another proxy swap: both designs currently pay shared prefix/recovery
and deployment-row serialization costs. Prefer separate placement control when lifecycle isolation is
the requirement; these finite timings alone do not justify a proxy winner.</p></section>"""
    phase_rows = []
    resource_rows = []
    transitions = []
    for run in runs:
        name = f'R{run["repetition"]} {run["arm"]}'
        for phase in run["phases"]:
            d = phase["terminal"]
            phase_rows.append((name, phase["phase"], "matched" if phase["comparable"] else "NOT matched",
                phase["completed_rows"], f'{d["n"]}/{phase["jobs"]}', fmt(phase["seconds"]), fmt(phase["rows_per_second"]),
                fmt(phase["logical_arrow_bytes_per_second"]), fmt(d["p50"]), fmt(d["exploratory_p95"]),
                "N/A", fmt(phase["admission_identity_observed"]["p50"]),
                f'{fmt(phase["final_receipt"]["p50"])} ({phase["final_receipt"]["n"]})',
                f'{fmt(phase["resolved_receipt"]["p50"])} ({phase["resolved_receipt"]["n"]})',
                f'{fmt(phase["reader_visibility_observed"]["p50"])} ({phase["reader_visibility_observed"]["n"]})'))
        for role, costs in run["roles"].items():
            resource_rows.append((name, role, fmt(costs["cpu_seconds"]), fmt(costs["memory_peak_bytes"] / 1048576), costs["samples"]))
        for transition in run["transitions"]:
            transitions.append((name, transition["phase"], transition["status"], fmt(transition["seconds"]),
                                transition.get("reason", "membership/assignment observed; quiescent transition")))
    historical = (directory / "historical-report.html").read_text()
    historical_body = re.search(r"<body[^>]*>(.*)</body>", historical, re.S).group(1)
    historical_body = re.sub(r'id="([^"]+)"', r'id="historical-\1"', historical_body)
    historical_body = re.sub(r'href="#([^"]+)"', r'href="#historical-\1"', historical_body)
    appendix = '<details class="appendix" id="appendix"><summary>Technical details, feature matrix, reproducibility and historical results</summary>'
    appendix += '<section id="latency-details"><h2>Receipt-boundary timing, not isolated layer timing</h2>'
    appendix += table(("Warm run", "Paired inputs", "Before ID P50 ms", "ID → final P50 ms",
                      "Final → RPC end P50 ms", "Post-ID share P50", "Terminal mean ms"), latency_rows)
    appendix += '<p>Every difference and fraction is calculated within the same input before aggregation. '
    appendix += 'The share is (final_receipt_ms − admission_identity_observed_ms) / terminal_ms. '
    appendix += 'Do not subtract independent percentile values or add median stage times. '
    appendix += 'All timestamps are client-observed elapsed monotonic time from before batch/client construction. '
    appendix += 'DELTA_OPERATION follows collector admission but precedes Parquet staging; it is not a durable ACK. '
    appendix += 'DELTA_COMMITTED follows publication and SQL final receipt. These boundaries do not isolate '
    appendix += 'gateway transit, placement/ownership calls, collector encoding, publisher waiting, SQL, '
    appendix += 'Delta materialization or reply-network duration. Pure routing and pure collector durations remain N/A. '
    appendix += 'Reader scans start after completion and are excluded. Identity-resolved completion includes polling '
    appendix += 'and remains a separate metric in the per-phase table below.</p>'
    appendix += '<h3>Why the shared path is expensive: code evidence, not a timed cost allocation</h3>'
    appendix += '<p><code>sql_publication.rs::materialize</code> revisits every accepted decision, verifies data, '
    appendix += 'checks/installs logs and repeats SQL receipt/authority operations before and after each new publication. '
    appendix += '<code>prepare</code> reconstructs a private Delta log for schema and data preparation, including '
    appendix += 'unchanged-schema probes, and decodes/re-encodes staged Parquet. '
    appendix += '<code>delta_backends.rs</code> uses fresh unpooled SQL connections and a single locked serialized '
    appendix += 'deployment-state row; collector/publisher per-table serialization adds queueing. '
    appendix += 'These verified implementation costs are shared by both arms, not proof of intrinsic Pingora, '
    appendix += 'Arrow Flight or Delta latency. Existing measurements cannot assign exact milliseconds to each cause.</p>'
    appendix += '<p>A future routing-versus-collector experiment needs correlated per-request spans for gateway '
    appendix += 'resolution/forwarding, collector decode/admission/queue/staging, publisher acceptance/materialization '
    appendix += 'and final receipt. Nested waits must not be double-counted. No new instrumentation or benchmark '
    appendix += 'was run for this report-only breakdown.</p></section>'
    appendix += f'<section><h2>Per-run metrics (ms unless labeled)</h2><p>{matched_pairs} matched phase pairs. '
    appendix += topology + '</p>'
    appendix += table(("Run", "Phase", "Pair scope", "Rows", "Successful RPCs/inputs", "Workload seconds", "Rows/s", "Arrow bytes/s", "Terminal P50",
                      "Exploratory P95", "P99", "Admission identity P50", "Streaming final P50 (n)",
                      "Identity-resolved final P50 (n)", "Reader observed P50 (n)"), phase_rows)
    appendix += '<p>Internal decision-acceptance and Delta-materialization durations, SQL operation counts/latency, '
    appendix += 'ADR cache hit/miss counts, exact assignment propagation, and publisher queue delay: unavailable. '
    appendix += 'Admission identity is client-observed after admission; it is not internal admission duration. '
    appendix += 'Reader observation is one scan after final completion (terminal or identity-resolved), not earliest visibility. '
    appendix += 'Unknown RPCs retain errors; exact-operation receipt polls run every 200 ms plus request time for at most 45 s. '
    appendix += 'Resolved-final latency stays separate from final streaming receipt latency. '
    appendix += 'Successful-RPC-only percentiles are censored where RPCs were nonfinal; do not treat those phases as complete latency distributions. '
    appendix += 'Missing-submitted rows are reported, not silently removed; every final-receipt marker and digest reconciles exactly. '
    appendix += 'Cold means new deployment/table/assignment, not an OS cache flush. Histories grow through the run; '
    appendix += 'after failures, equal offered bytes do not imply equal completed bytes or subsequent SQL history size. '
    appendix += 'Workload throughput includes client construction, post-terminal standard-reader work, and observer contention.</p></section>'
    appendix += '<section><h2>Role cgroup costs and finite bounds</h2>'
    appendix += table(("Run", "Role", "CPU seconds", "Sampled peak MiB", "Samples"), resource_rows)
    appendix += table(("Run", "Shared substrate CPU s", "All roles CPU s", "Pending sampled peak", "Active sampled peak",
                       "Preparation attempts", "SQL state UTF16 bytes", "Delta commits", "Parquet bytes"),
        [(f'R{r["repetition"]} {r["arm"]}', fmt(r["substrate_cpu_seconds"]), fmt(r["total_role_cpu_seconds"]),
          r["observed_pending_peak"], r["observed_active_streams_peak"], r["substrate"]["preparation_attempts"],
          r["substrate"]["sql_state_utf16_bytes"], r["substrate"]["delta_log_commits"], r["substrate"]["delta_parquet_bytes"]) for r in runs])
    appendix += '<p>CPU counter deltas span observation lifecycle through the last sample before final input completion, '
    appendix += 'including transitions/client scans but excluding startup and post-run audit. '
    appendix += 'Memory is sampled cgroup usage, not continuous peak or summed concurrent maximum. SQL is sampled separately. '
    appendix += 'Each native role: 1 CPU / 1 GiB; SQL: 2 CPUs / 3 GiB (2 GiB configured SQL memory). '
    appendix += 'Shared substrate = collectors + publisher + SQL. Generator CPU/RSS and host load averages are retained per phase; '
    appendix += 'reader, observer and generator share a host/process budget. No independent generator-isolation qualification.</p>'
    appendix += table(("Run", "Generator/reader CPU s", "Generator lifetime max RSS MiB", "Maximum host 1-minute load",
                       "Identity-resolved RPCs", "Unresolved/nonfinal inputs", "Absent submitted rows"),
        [(f'R{r["repetition"]} {r["arm"]}', fmt(sum(p["generator_cpu_seconds"] for p in r["phases"])),
          fmt(max(p["generator_peak_rss_kib"] for p in r["phases"]) / 1024),
          fmt(max(p["host_load_average"][0] for p in r["phases"])), r["identity_resolved_final"],
          r["nonfinal_inputs"], r["missing_submitted_rows"]) for r in runs])
    appendix += table(("Run", "Role", "Decoded bytes high-water", "Gateway input bytes high-water",
                       "Gateway result bytes high-water", "Mailbox waiters high-water", "Writer streams high-water"),
        [(f'R{r["repetition"]} {r["arm"]}', role, *[v["high_water"][g] for g in
          ("decoded_batch_bytes", "gateway_input_bytes", "gateway_result_bytes", "mailbox_waiters", "writer_streams")])
         for r in runs for role, v in r["native_observations"].items()])
    appendix += '<p>These native lifetime high-water counters include startup/transitions and are not phase-local. '
    appendix += 'ADR/collector cache snapshot/revision/refresh counters are retained in analysis.json; '
    appendix += 'Shared publisher mailbox wait time is unavailable.</p>'
    appendix += table(("Run", "Pingora role", "Observed route lookups", "Cache hits", "Cache misses", "Data-only lookups"),
        [(f'R{r["repetition"]} {r["arm"]}', role, v["lookups"], v["cache_hits"],
          v["cache_misses"], v["data_only_lookups"])
         for r in runs for role, v in r["routing_observations"].items()])
    appendix += '<p>Pingora route events are timestamp-filtered from first input start through last input completion; '
    appendix += 'these are observed lookups, not authority operations or an equivalent ADR cache metric.</p>'
    appendix += '<p>56 offered inputs × two preparations = at most 112/128 global private-workspace attempts, including no-op schema probes. '
    appendix += 'Actual preparations and commits vary when inputs fail; two initialized tables, all state/row/byte/backlog caps unchanged. '
    appendix += 'SQL deployment-row serialization and database-scoped db_owner credentials are explicit PoC limits. '
    appendix += 'No compaction, HA, exactly-once delivery or cloud/object-store proof.</p></section>'
    appendix += '<section><h2>Topology and migration</h2>' + table(("Run", "Transition", "Status", "Seconds", "Scope"), transitions) + '</section>'
    appendix += '<section id="feature-coverage"><h2>Feature guarantees versus orchestration policy</h2>' + table(
        ("Feature", "Status", "Owner/scope", "Qualification", "Evidence"), shared_delta(repaired)) + '</section>'
    appendix += '<section><h2>Reproduce and audit</h2><pre>' + esc(
        'export TMPDIR="$PWD/target/delta-runtime/scratch"\n'
        f"target/adr0001/venv/bin/python projects/loadtest/adr0001/delta_benchmark.py --cohort NEW-UNIQUE-NAME --subnet-start UNUSED-OCTET\n"
        f"target/adr0001/venv/bin/python projects/loadtest/adr0001/report.py --shared-delta {cohort}\n"
        f"target/adr0001/venv/bin/python projects/loadtest/adr0001/evidence.py --shared-delta {cohort}") + '</pre>'
    appendix += '<p>Evidence root: <code>' + esc(directory.relative_to(REPO)) + '</code>. '
    appendix += 'analysis.json preserves every run; attempts.json names raw inputs, observations, independent scans, SQL receipts, '
    appendix += 'scoped stopped inventories and transitions. source-binary-sha256.json freezes the actual measured binaries and sources. '
    appendix += 'Initial runtime tests: target/delta-runtime/closeout.json (108 distinct Rust tests). '
    if repaired:
        appendix += 'Repair qualification: target/delta-runtime/repair-20261026/closeout.json '
        appendix += '(109 distinct Rust tests, native SQL/etcd large-upload and data-only gateway gates). '
    appendix += 'Authenticated native fault evidence remains separate from performance.</p>'
    appendix += '<p>Historical baseline/custom-manifest ADR: instrumented-obs20261026final2, 11 phase pairs and 50,688 rows/design. '
    appendix += 'Warm terminal P95 per-run median was 767.6 ms baseline versus 1885.9 ms ADR, with different sinks/batching: '
    appendix += 'not the present causal comparison. Original source/report snapshot is in session files/router-benchmark/'
    appendix += 'shared-delta-provenance-20261026; earlier final-evidence.json and manifests remain unchanged. '
    appendix += 'Historical publication-gate executable bytes were overwritten by the runtime build and are unavailable; '
    appendix += 'historical source hashes/logs/closeout survive. The newly frozen executable is the runtime revision, not that old binary.</p></section>'
    appendix += '<section><h2>Earlier finite attempts (never selected for speed)</h2><p>'
    appendix += 'shared20261026a stopped on a nonfinal RPC whose rows were independently visible; '
    appendix += 'shared20261026b added identity-only resolution but stopped on four pre-identity elephant timeouts; '
    appendix += 'shared20261026c half-closed input before receiving final status and still reproduced those timeouts. '
    appendix += 'Their raw artifacts remain. The final harness reports failed inputs and independently verifies missing rows '
    appendix += 'instead of requiring error-free throughput. The first shared20261026d measurement was retained after '
    appendix += 'repairing read-only sqlcmd output formatting during reconciliation; it was not rerun. Exact per-arm harness '
    appendix += 'copies preserve that audit-only revision, with identical input/workload/topology methods verified at closeout. '
    appendix += 'That historical cohort did not relax native code, safety limits, workload sizes or concurrency.</p>'
    preceding_path = directory / "preceding-cohort.json"
    if preceding_path.exists():
        preceding = load(preceding_path)
        assert digest(directory / "preceding-shared-report.html") == preceding["report_sha256"]
        previous = load(REPO / preceding["closeout"])
        previous_missing = sum(r["missing_submitted_rows"] for r in previous["runs"])
        previous_nonfinal = sum(r["nonfinal_inputs"] for r in previous["runs"])
        appendix += '<p>Earlier shared-path cohort <code>' + esc(preceding["cohort"]) + '</code>: '
        appendix += f'{previous_nonfinal} nonfinal inputs / {previous_missing:,} absent submitted rows retained; '
        appendix += f'{previous["matched_phase_pairs"]} matched phase pairs. '
        appendix += 'Its exact report is archived as <code>preceding-shared-report.html</code> in this cohort directory. '
        appendix += 'Repair qualification records the collector scheduling fix and explicit data-only Pingora mode. '
        appendix += 'All six current arms use one repaired revision and unchanged workload/limits. '
        appendix += 'This is a new source cohort, not selection of faster historical attempts.</p>'
        appendix += table(("Historical run", "Completed rows", "Absent submitted rows", "Nonfinal RPCs"),
            [(f'R{r["repetition"]} {r["arm"]}', r["rows"], r["missing_submitted_rows"], r["nonfinal_rpcs"])
             for r in previous["runs"]])
    retained_path = directory / "retained-attempts.json"
    if retained_path.exists():
        appendix += table(("Excluded partial cohort", "Reason", "Completed phases", "Submitted inputs",
                           "Independently visible rows", "Absent rows"),
            [(r["cohort"], r["reason"], r["completed_phases"], r["submitted_inputs"],
              r["visible_rows"], r["absent_rows"]) for r in load(retained_path)])
        appendix += '<p>These incomplete attempts are retained, not pooled or counted as completed repetitions. '
        appendix += 'The unsupported Flight readiness call was replaced with a transport-listener check in both arms; '
        appendix += 'the measured phase then requires real native uploads through both gateways. '
        appendix += 'No native binary, workload, timeout or receipt policy was changed for that harness correction.</p>'
    appendix += '</section>'
    appendix += '<details><summary>Historical report snapshot — original labels, NOT shared-path measurements</summary>'
    appendix += historical_body + '</details></details>'
    document = '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
    document += '<title>Shared SQL / Delta routing comparison</title><style>' + EDITORIAL_CSS
    document += 'body{margin:auto}.scroll{overflow-x:auto}table{border-collapse:collapse;width:100%}th,td{padding:6px;text-align:left;border-bottom:1px solid #ddd}'
    document += (
        '.latency-boundaries{margin:18px 0}.latency-boundaries small{display:block;margin-top:8px}'
        '.latency-track{display:flex;height:24px;background:#e6e9eb;overflow:hidden}'
        '.latency-segment{display:block;flex-shrink:0;height:100%}'
        '.before_identity_ms{background:#8495ad}.identity_to_final_ms{background:#287b66}'
        '.final_to_terminal_ms{background:#aa5b30}'
        '.latency-legend{display:flex;flex-wrap:wrap;gap:12px;font-size:.82rem;margin-bottom:12px}'
        '.latency-legend i{display:inline-block;width:12px;height:12px;margin-right:5px}'
        '@media print{.latency-segment,.latency-legend i{print-color-adjust:exact;-webkit-print-color-adjust:exact}}')
    document += '</style></head><body><main>' + overview + appendix + '</main></body></html>'
    path = REPO / "projects/loadtest/adr0001/report.html"
    path.write_text(document)
    class Validator(HTMLParser):
        def __init__(self):
            super().__init__()
            self.ids, self.anchors, self.bars = [], [], []
        def handle_starttag(self, tag, attrs):
            attrs = dict(attrs)
            assert tag not in ("script", "iframe", "img"), "offline report must not load assets"
            if "id" in attrs:
                self.ids.append(attrs["id"])
            if tag == "a" and attrs.get("href", "").startswith("#"):
                self.anchors.append(attrs["href"][1:])
            if "data-value" in attrs:
                self.bars.append(attrs)
    validator = Validator()
    validator.feed(document)
    assert len(validator.ids) == len(set(validator.ids))
    assert set(validator.anchors) <= set(validator.ids)
    assert len(validator.bars) == 6
    for bar in validator.bars:
        width = float(re.search(r"width:([\d.]+)%", bar["style"])[1])
        assert abs(width - 100 * float(bar["data-value"]) / float(bar["data-max"])) < .000001
    save(directory / "report-validation.json", {"path": str(path.relative_to(REPO)), "sha256": digest(path),
        "runs": 6, "raw_phase_distributions_verified": 54, "main_chart_values_verified": 6,
        "matched_phase_pairs": matched_pairs, "unsupported_scale_pairs": len(PHASES) * 3 - matched_pairs, "external_assets": 0,
        "historical_snapshot_sha256": digest(directory / "historical-report.html")})
    save(directory / "feature-matrix.json", shared_delta(repaired))
    save(REPO / "target/delta-benchmark/selected-cohort.json", {"cohort": cohort})
    print(json.dumps({"report": str(path), "sha256": digest(path), "cohort": cohort}))
