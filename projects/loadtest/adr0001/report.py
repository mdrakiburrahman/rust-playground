"""Offline report, with displayed values derived directly from saved measured artifacts."""
import hashlib
import html
from html.parser import HTMLParser
import json
from pathlib import Path
import statistics
import re
import coverage as requirement_coverage
from analyze_observations import analyze

REPO = Path(__file__).resolve().parents[3]
EVIDENCE = REPO / "target/adr0001/evidence"


def esc(value):
    return html.escape(str(value))


def table(headers, rows):
    return ('<div class="scroll"><table><thead><tr>' +
            "".join(f"<th>{esc(v)}</th>" for v in headers) + "</tr></thead><tbody>" +
            "".join("<tr>" + "".join(f"<td>{esc(v)}</td>" for v in row) + "</tr>"
                    for row in rows) + "</tbody></table></div>")


FINAL_COHORT = "instrumented-obs20261026final2"

DOCUMENTATION = {
    "https://etcd.io/docs/v3.6/learning/api/": "etcd conditional transactions and revisioned watches",
    "https://etcd.io/docs/v3.6/op-guide/performance/": "etcd consensus, disk and network latency",
    "https://arrow.apache.org/docs/format/Flight.html": "Arrow Flight streaming and application metadata",
}

EDITORIAL_CSS = """
:root{--baseline:#2262ba;--adr:#7650b4;--proposed:#9b5b16;--ink:#192b3c;--muted:#526476}
*{box-sizing:border-box}html{scroll-behavior:smooth}body{max-width:1240px;padding:36px 28px;
font:16px/1.5 system-ui,sans-serif;background:#f6f5f1;color:var(--ink)}
main{min-width:0}h1{font-size:clamp(2.1rem,5vw,3.6rem);letter-spacing:-.045em;max-width:960px;margin:12px 0}
h2{font-size:1.65rem;letter-spacing:-.025em}h3{font-size:1.1rem}p{margin:.65em 0}
section{background:transparent;border:0;border-radius:0;padding:26px 0;margin:0;border-top:1px solid #cbd3d9}
.eyebrow,.kicker{font-size:.75rem;font-weight:750;letter-spacing:.1em;text-transform:uppercase;color:var(--muted)}
.deck{font-size:1.16rem;max-width:830px}.scope{border-left:4px solid #bd7730;padding:12px 18px;
background:#fff7e9;font-size:.91rem;margin:22px 0}.scope strong{color:#71400c}
nav{display:flex;flex-wrap:wrap;gap:8px 20px;font-size:.88rem;margin:22px 0}
a{color:#19559a;text-underline-offset:3px}a:focus-visible,summary:focus-visible{outline:3px solid #bd7730;outline-offset:4px}
.stats{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:0;margin:24px 0}
.stat{padding:8px 20px;border-left:1px solid #bbc8d3}.stat:first-child{padding-left:0;border:0}
.stat b{display:block;font-size:1.85rem;line-height:1.2;letter-spacing:-.035em}.stat span{font-size:.8rem;color:var(--muted)}
.split{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:24px}
.arch{background:#fff;padding:22px;border-top:4px solid var(--baseline);min-width:0}
.arch.adr{border-color:var(--adr)}.arch h3{font-size:1.3rem;margin:3px 0 14px}
.arch p,.flow p{font-size:.85rem}.node{padding:12px;border:1px solid #a8bed8;background:#f1f6fd;border-radius:5px;text-align:center}
.adr .node{background:#f6f2fb;border-color:#b6a3d0}.node strong,.node small{display:block}
.node small{font-size:.8rem;margin-top:3px}.route{text-align:center;font-size:.76rem;font-weight:650;padding:8px 0;color:var(--baseline)}
.adr .route{color:var(--adr)}.control{border:2px dashed #71808f;padding:12px;background:#f8fafb;margin-top:12px;font-size:.83rem}
.control strong{display:block}.control p{margin-bottom:0}.boundary{border:2px solid #7650b4;padding:12px;background:#f8f4fc;margin-top:10px;font-size:.83rem}
.boundaries{display:grid;grid-template-columns:1fr 1fr;gap:10px}.boundaries .node{background:#fff}
.legend{display:flex;flex-wrap:wrap;gap:8px 22px;font-size:.8rem;color:var(--muted);margin:12px 0}
.legend i{display:inline-block;width:22px;border-top:3px solid currentColor;vertical-align:middle;margin-right:6px}
.legend .dashed{border-top-style:dashed}.baseline-key{color:var(--baseline)}.adr-key{color:var(--adr)}
.muted,.note{color:var(--muted)}.note{font-size:.84rem}.chart{margin:20px 0}
.bar-row{display:grid;grid-template-columns:100px minmax(0,1fr) 90px;gap:12px;align-items:center;font-size:.84rem;margin:8px 0}
.bar-track{height:24px;background:#e6e9eb;border-left:1px solid #8b9ba9;position:relative}
.bar{height:100%;background:var(--baseline)}.bar.adr{background:var(--adr)}
.bar-row output{text-align:right;font-variant-numeric:tabular-nums}.axis{display:flex;justify-content:space-between;
margin:0 102px 10px 112px;font-size:.73rem;color:var(--muted)}
.repeat{font-size:.8rem;color:var(--muted);margin-top:16px}
.phase-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px 30px}
.phase{padding:10px 0;border-top:1px solid #d6dde3}.phase h3{font-size:.85rem;margin:0 0 7px}
.phase .bar-row{grid-template-columns:62px minmax(0,1fr) 75px;gap:8px;margin:5px 0;font-size:.75rem}
.phase .bar-track{height:12px}.correctness{background:#e8f2ee;border-left:4px solid #36715d;padding:12px 18px;font-size:.88rem}
.insights{counter-reset:insight;list-style:none;padding:0;display:grid;grid-template-columns:1fr 1fr;gap:20px 28px}
.insights li{border-top:1px solid #cbd3d9;padding-top:12px;font-size:.9rem}.insights strong{display:block;margin-bottom:4px}
.tradeoffs{padding:0;list-style:none;font-size:.9rem}.tradeoffs li{margin:10px 0}.tradeoffs b{font-size:.73rem;letter-spacing:.07em;text-transform:uppercase}
.proposal{background:#fffaf2;padding:22px;border:2px dashed var(--proposed)}
.proposal .node{background:#fff;border-color:#d4b58b}.proposal h3{color:#7e4b15;margin-top:0}
.proposal-flow{display:grid;grid-template-columns:1fr auto 1fr auto 1fr;gap:10px;align-items:center}
.proposal-flow .route{color:var(--proposed)}.experiment{font-size:.88rem}.experiment dt{font-weight:750;margin-top:14px}
.experiment dd{margin:3px 0}.appendix{margin:30px 0;background:#fff;border:1px solid #b7c5d0;padding:18px}
summary{cursor:pointer;font-weight:700}.appendix>summary{font-size:1.1rem}.appendix section{padding:22px 0}
.appendix h1{font-size:1.8rem}.appendix h2{font-size:1.35rem}.appendix table{font-size:.78rem}
.appendix .scroll{max-width:100%;overflow:auto}.appendix pre{white-space:pre-wrap;overflow-wrap:anywhere}
footer{font-size:.78rem;color:var(--muted);margin-top:16px}code{overflow-wrap:anywhere}
@media(max-width:700px){body{padding:24px 16px}.split,.phase-grid,.insights{grid-template-columns:1fr}
.stats{grid-template-columns:1fr 1fr;gap:20px}.stat:nth-child(3){padding-left:0;border:0}
.arch{padding:16px}.proposal-flow{grid-template-columns:1fr}.proposal-flow .route{padding:0}
.proposal-flow .route span{display:none}.proposal-flow .route:after{content:"↓"}section{padding:22px 0}
.bar-row{grid-template-columns:74px minmax(0,1fr) 82px;gap:8px}.axis{margin-left:82px;margin-right:90px}
.boundaries{grid-template-columns:1fr}.appendix{padding:12px}.appendix .diagram{grid-template-columns:1fr}}
@media print{body{max-width:none;padding:0;background:white;font-size:10pt}nav{display:none}
section{break-inside:auto}.arch,.phase,.proposal,.insights li{break-inside:avoid}
.split{grid-template-columns:1fr 1fr}.bar,.bar-track,.scope,.correctness{print-color-adjust:exact;-webkit-print-color-adjust:exact}
.appendix:not([open])>*:not(summary){display:none}.appendix[open] .scroll{overflow:auto}
h2,h3{break-after:avoid}a{color:inherit}}
"""


def architecture_diagrams():
    return """
<section id="architectures"><div class="kicker">01 / System boundaries</div>
<h2>Same routing invariant. Different commit paths.</h2>
<div class="legend"><span><i></i>Solid arrows: payload / publication / final receipt</span>
<span><i class="dashed"></i>Dashed boxes: control and assignment</span></div>
<div class="split">
<article class="arch" aria-labelledby="baseline-title">
<div class="eyebrow baseline-key">Baseline · measured</div><h3 id="baseline-title">Pingora + SQL + Delta</h3>
<div role="img" aria-label="Baseline architecture. Clients stream through Pingora to direct collectors. Router one alone controls disk assignments; router two caches assignments. SQL ownership and receipts fence writes separately from Delta publication with Hive. These are not one atomic boundary.">
<div class="node"><strong>Native Flight clients</strong><small>Bounded streams</small></div>
<div class="route">↓ Flight payload</div>
<div class="node"><strong>Pingora router 1 / router 2</strong><small>Forward to assigned collector</small></div>
<div class="control"><strong>⇢ Assignment control</strong>Router 1: sole disk-controller authority<br>
Router 2: data-only assignment cache</div>
<div class="route">↓ Direct collector stream</div>
<div class="node"><strong>Rust collectors</strong><small>One owner per table · buffered flush</small></div>
<div class="route">↓ Separate write-fence and publication paths</div>
<div class="boundaries">
<div class="node"><strong>SQL Server</strong><small>Ownership fence + receipt ledger</small></div>
<div class="node"><strong>Delta + Hive</strong><small>Payload files + table publication</small></div></div>
<p><strong>Two boundaries, not one atomic transaction.</strong> Final visibility is checked through SQL receipts and Delta reconciliation.</p>
</div></article>
<article class="arch adr" aria-labelledby="adr-title">
<div class="eyebrow adr-key">Fenced ADR · measured</div><h3 id="adr-title">Native gateway + atomic reader head</h3>
<div role="img" aria-label="ADR architecture. Retaining clients stream through native Rust tonic gateways to single-owner manifest collectors. Assignment snapshot and watch caches consult a combined controller backed by three same-host etcd members. Collectors persist immutable Parquet and manifests outside etcd. One conditional transaction validates ownership and publishes reader head. Only durable reachable data receives final TABLE_COMMITTED.">
<div class="node"><strong>Retaining Flight clients</strong><small>Keep unconfirmed units</small></div>
<div class="route">↓ Flight payload</div>
<div class="node"><strong>Native Rust / tonic gateways</strong><small>Assignment snapshot / watch cache</small></div>
<div class="control"><strong>⇢ Combined controller → 3-member etcd</strong>
Placement · ownership · admission · commit<br>Persisted reservations + migration budgets</div>
<div class="route">↓ Direct collector stream</div>
<div class="node"><strong>Manifest collectors</strong><small>One owner per table · local generation barrier</small></div>
<div class="route">↓ Stage and synchronize</div>
<div class="node"><strong>Immutable Parquet + manifests</strong><small>Payload files stay OUTSIDE etcd</small></div>
<div class="boundary"><strong>ONE conditional etcd transaction</strong><br>
Compare owner session / exact epoch / lifecycle / expected head; publish authoritative reader head.</div>
<div class="route">Head → reachable files · THEN ↑ TABLE_COMMITTED</div>
</div></article></div>
<p class="note">Neither design splits a hot table; equal schemas do not determine ownership.
No durable early ACK/WAL. ADR gateways persist no payload, receipt ledger or application replay.</p></section>
"""


def executive_view(final_summary, focused):
    root = EVIDENCE / FINAL_COHORT
    designs = ("baseline", "adr")
    raw = {d: {p["label"]: json.loads((root/d/p["label"]/"report.json").read_text())
               for p in final_summary["matched_pairs"]} for d in designs}
    labels = [p["label"] for p in final_summary["matched_pairs"]]
    warm_labels = sorted(label for label in labels if label.startswith("warm-"))
    assert len(labels) == 11 and len(warm_labels) == 3
    warm = {d: statistics.median(raw[d][label]["terminal_latency_ms"]["p95"]
                                 for label in warm_labels) for d in designs}
    counts = {d: {key: sum(r[key] for r in raw[d].values()) for key in
                  ("generated_rows", "visible_rows", "missing_rows", "unresolved_streams", "retries")}
              for d in designs}
    assert counts["baseline"] == counts["adr"]
    assert counts["baseline"]["generated_rows"] == counts["baseline"]["visible_rows"]
    assert all(counts[d][key] == 0 for d in designs
               for key in ("missing_rows", "unresolved_streams", "retries"))
    phase_max = max(r["terminal_latency_ms"]["p95"] for runs in raw.values() for r in runs.values())
    phase_scale = int(phase_max // 1000 + 1) * 1000
    warm_scale = int(max(warm.values()) // 500 + 1) * 500
    geometry = []

    def bar(design, value, maximum, label, samples):
        width = value / maximum * 100
        assert 0 <= width <= 100
        geometry.append({"design": design, "phase": label, "value_ms": value,
                         "axis_max_ms": maximum, "width_percent": width, "samples": samples})
        name = "Baseline" if design == "baseline" else "ADR"
        return (f'<div class="bar-row"><span class="{design}-key">{name}</span>'
                f'<div class="bar-track"><div class="bar {design}" style="width:{width:.8f}%" '
                f'data-value-ms="{value}" data-axis-max-ms="{maximum}" role="img" '
                f'aria-label="{name}, {esc(label)}: {value:.2f} milliseconds; {esc(samples)}"></div></div>'
                f'<output>{value:,.1f} ms</output></div>')

    phase_names = {"cold": "Cold placement", "pressure": "Pressure", "elephant": "Elephant table",
                   "after-move": "After owner move", "after-scale-in": "After scale-in",
                   "four-new": "Four collectors / new tables", "one-gateway": "One gateway",
                   "two-gateways": "Two gateways"}
    phase_order = warm_labels + [label for label in labels if label not in warm_labels]
    phases = []
    for label in phase_order:
        samples = {d: raw[d][label]["terminal_latency_ms"]["samples"] for d in designs}
        phases.append(f'<article class="phase"><h3>{esc(phase_names.get(label, label))}'
                      f' <span class="muted">· n={samples["baseline"]}/{samples["adr"]} streams</span></h3>' +
                      "".join(bar(d, raw[d][label]["terminal_latency_ms"]["p95"], phase_scale,
                                  label, f'{samples[d]} streams') for d in designs) + "</article>")
    rows = counts["baseline"]["visible_rows"]
    repeats = " · ".join(f'{esc(label)}: ' + " / ".join(
        f'{raw[d][label]["terminal_latency_ms"]["p95"]:,.1f}' for d in designs) for label in warm_labels)
    document = f"""
<header><div class="eyebrow">ADR 0001 / Architecture decision lab / October 26, 2026</div>
<h1>Keep the fence.<br>Test a cheaper commit path.</h1>
<p class="deck">Pingora’s complete baseline finished faster here. The fenced ADR makes publication safety explicit.
Next, test bounded per-table group-commit—not another proxy.</p>
<div class="scope"><strong>Read this as a same-host experiment, not a production verdict.</strong>
Different sinks, batching and receipt semantics confound the comparison. No production-capacity,
router-only winner or host-failure guarantee follows. Baseline HA remains uncertain after its preserved authority incident.</div>
<nav aria-label="Executive contents"><a href="#architectures">Systems</a><a href="#results">Results</a>
<a href="#insights">Insights</a><a href="#tradeoffs">Trade-offs</a><a href="#next">Next experiment</a>
<a href="#appendix">Evidence appendix</a></nav></header>
<div class="stats" aria-label="Latest cohort statistics">
<div class="stat"><b>{len(labels)}</b><span>matched phase pairs · final2 only</span></div>
<div class="stat"><b>{rows:,}</b><span>generated = visible rows / design</span></div>
<div class="stat"><b>{counts["baseline"]["missing_rows"]} / {counts["baseline"]["unresolved_streams"]} / {counts["baseline"]["retries"]}</b><span>missing / unresolved / retries · each design</span></div>
<div class="stat"><b>{warm["adr"]/warm["baseline"]:.2f}×</b><span>ADR / baseline warm-P95 median</span></div></div>
""" + architecture_diagrams() + f"""
<section id="results"><div class="kicker">02 / Latest matched cohort only</div>
<h2>End-to-end latency favors the baseline.</h2>
<p>Median of <strong>THREE phase P95s</strong>—not a pooled P95. Full-stream terminal latency; lower is better.</p>
<figure class="chart" aria-labelledby="warm-caption">
<figcaption id="warm-caption"><strong>Warm repetitions · milliseconds</strong></figcaption>
<div class="axis"><span>0</span><span>{warm_scale/2:,.0f}</span><span>{warm_scale:,.0f} ms</span></div>
{"".join(bar(d, warm[d], warm_scale, "median of three warm phase P95s", "3 phase P95s") for d in designs)}
<div class="repeat">Individual phase P95s, baseline / ADR (ms): {repeats}.</div></figure>
<h3>All {len(labels)} phases · one shared zero-based scale, 0–{phase_scale:,} ms</h3>
<p class="note">Each pair is its own workload/topology, not a pooled distribution.
Labels report baseline/ADR sample counts; bars encode milliseconds.</p>
<div class="phase-grid">{"".join(phases)}</div>
<p class="correctness"><strong>Reconciled:</strong> {rows:,}/{rows:,} rows per design; no missing rows,
unresolved streams or retries. Separate focused suite: {len(focused["scenarios"])} passed live proofs.
This does not establish exactly-once or production HA.</p></section>
<section id="insights"><div class="kicker">03 / What matters</div><h2>Four takeaways, without a proxy verdict.</h2>
<ol class="insights">
<li><strong>Speed is measured; cause is not.</strong> Per-batch staging, fsync, admission and CAS may add cost.
No stage breakdown proves the bottleneck.</li>
<li><strong>The safety gain is the commit boundary.</strong> ADR validates ownership and publishes reader head atomically;
a cached lease alone cannot authorize publication.</li>
<li><strong>More replicas do not split a hot table.</strong> Placement can move ownership, not parallelize one table.
Scaling phases are distinct workloads.</li>
<li><strong>Resource samples are not efficiency scores.</strong> Staggered cgroup sums and sparse CPU intervals
are not integrated CPU/byte or continuous peaks; lower memory alone proves no efficiency win.</li></ol></section>
<section id="tradeoffs"><div class="kicker">04 / Balanced design choices</div><h2>Performance result ≠ safety equivalence.</h2>
<div class="split"><article><h3 class="baseline-key">Baseline / Pingora + SQL + Delta</h3>
<ul class="tradeoffs"><li><b>Pros</b> Faster measured end-to-end path; existing Delta publication and buffered flush.</li>
<li><b>Cons</b> SQL/Hive dependencies; sole disk-controller authority; ownership/receipts and Delta publication are separate boundaries.
Automatic HA parity was not demonstrated.</li></ul></article>
<article><h3 class="adr-key">Fenced ADR / native + etcd</h3>
<ul class="tradeoffs"><li><b>Pros</b> Explicit atomic owner/head fence; exercised stale-owner/recovery proofs;
persisted migration reservations and local generation barriers.</li>
<li><b>Cons</b> Slower here; per-batch durable files and control coupling are plausible costs, not isolated causes.
No compaction/orphan GC or production HA proof.</li></ul></article></div></section>
<section id="next"><div class="kicker">05 / Recommended next experiment</div>
<h2>Bounded per-table group-commit on the fenced ADR.</h2>
<div class="proposal"><h3>UNTESTED · hypothesis, not a promised speedup</h3>
<div class="proposal-flow" role="img" aria-label="Proposed untested group commit. Keep gateways and routing unchanged. Admit compatible units for one immutable table and exact active writer epoch; stage and persist objects off the asynchronous executor. Publish several units with one expected-head conditional transaction. Final receipts only for durable reachable units.">
<div class="node"><strong>Same gateway + routing</strong><small>Retaining clients · no replay</small></div>
<div class="route"><span>→</span></div>
<div class="node"><strong>Bounded table / epoch group</strong><small>Fair admission · persist off async executor</small></div>
<div class="route"><span>→</span></div>
<div class="node"><strong>ONE fenced head publication</strong><small>Durable reachable units → final receipts</small></div></div>
<p>Amortize file/durability/consensus overhead while keeping controller, etcd, custom sink and
final TABLE_COMMITTED semantics fixed. Costs: dwell latency, retry amplification and fairness risk.</p></div>
<dl class="experiment">
<dt>Proposed knobs</dt><dd>Batch-1 control; dwell 0/2/5/10 ms × cap 64 KiB/256 KiB/1 MiB.
Fair per-table admission and memory caps. Group only already admitted units within one immutable table and exact active epoch;
separate-schema Parquet objects may share a manifest only under compatible table-schema rules—never unsafe concatenation.</dd>
<dt>Acceptance gates</dt><dd>Preserve client retention/drain barriers, exact session/epoch/lifecycle/expected-head CAS,
cutover fencing of all obsolete grouped proposals and unknown-outcome reconciliation by exact proposal identity.
No independent lease-check/blob-update, cached-lease authority or early success.</dd>
<dt>Measure before choosing</dt><dd>Same correctness/fault suite and workload; add larger payloads, 1,000+ schemas,
longer saturation and matched quotas. Require no missing durable-acknowledged rows, rejected stale heads,
and improved throughput without worse tails/timeouts or cap breaches versus batch-1.
Record stage timings, CAS/unit, bytes/s, CPU/byte and retained-memory/time.</dd>
</dl><p class="note">Proxy swaps come later with sink, ACK and batching identical. No untested Envoy/Pingora speed claim.
WAL is not the first optimization: it changes the durability contract.</p></section>
"""
    metrics = {"cohort": FINAL_COHORT, "warm_phase_labels": warm_labels,
               "warm_terminal_p95_median_ms": warm, "totals_by_design": counts,
               "chart_geometry": geometry, "phase_order": phase_order,
               "warm_aggregation": "median of three phase P95s; not pooled P95"}
    return document, metrics


def instrumented_section(cohort, title, include_matrix=False):
    summary = analyze(cohort)
    root = EVIDENCE / summary["cohort"]
    measured = []
    for pair in summary["matched_pairs"]:
        for design in ("baseline", "adr"):
            r = json.loads((root/design/pair["label"]/"report.json").read_text())
            if cohort == FINAL_COHORT:
                assert r["admission_latency_ms"]["samples"] == 0
                assert r["admission_semantics"].startswith("unavailable:")
                if design == "adr":
                    assert r["stream_open_latency_ms"]["samples"] == r["settings"]["streams"]
            measured.append((design, pair["label"], r["generated_rows"], r["visible_rows"],
                             r["missing_rows"], r["unresolved_streams"], r["retries"],
                             round(r["terminal_latency_ms"]["p95"], 2),
                             round(r["final_commit_observation_ms"]["p95"], 2),
                             round(r["visible_rows_per_second"], 1),
                             r.get("stream_open_latency_ms", {}).get("p95") or "unavailable",
                             r.get("admission_semantics", "unavailable: not timestamped")))
    intervals, components, clients, cadence = [], [], [], []
    for design, data in summary["designs"].items():
        cadence.append((design, data["samples"], data["sampling_errors"],
                        round(data["actual_start_cadence_ms"]["median"], 2),
                        round(data["actual_start_cadence_ms"]["maximum"], 2),
                        round(data["cycle_duration_ms"]["median"], 2)))
        for i in data["intervals"]:
            peak = i["cycle_memory_usage_sum_bytes"]["maximum"]
            intervals.append((design, i["label"], i["point_samples"],
                              i["incomplete_memory_cycles"], i["cycles_ending_after_interval"],
                              round(peak/1024**2, 2) if peak is not None else "unknown"))
            for name, v in i["components"].items():
                cpu = v["interior_cpu_one_core_percent"]
                def mib(value):
                    return round(value/1024**2, 2) if value is not None else "unknown"
                components.append((design, i["label"], v["service"], v["samples"], cpu["count"],
                                   round(cpu["median"], 2) if cpu["count"] else "n/a",
                                   round(cpu["maximum"], 2) if cpu["count"] else "n/a",
                                   mib(v["sampled_memory_usage_peak_bytes"]),
                                   mib(v["sampled_cgroup_rss_or_anon_peak_bytes"]),
                                   v["missing_memory_usage_samples"]))
            if "client_final" in i:
                v = i["client_final"]
                clients.append((design, i["label"], v["high_water"]["retained_bytes"],
                                v["high_water"]["in_flight_bytes"], v["high_water"]["active_units"],
                                v["retained_bytes"], v["unresolved_retained_bytes"]))
    gauges = []
    for service, values in summary["adr_metrics"]["service_gauges"].items():
        for key, value in values.items():
            if isinstance(value, dict) and "sampled_current_peak" in value:
                gauges.append((service, key, "bytes" if key.endswith("_bytes") else "count",
                               value["sampled_current_peak"], value["process_lifetime_high_water"],
                               value["last_sample_current"]))
    matrix = requirement_coverage.generate()
    section = f'<section id="{esc(cohort)}"><h2>{esc(title)}</h2>' + """
<p><strong>11 matched phase pairs × 4,608 = 50,688 generated and independently visible rows per design</strong>;
zero missing rows and unresolved streams in these pairs. These are separate from the legacy 13 pairs/11,520 rows.
The interrupted instrumented-obs20261026a contains four baseline reports, no matched ADR reports, and an interrupted
pressure attempt with sampling failures. It is retained as incomplete, excluded from all matched totals.</p>
<p>24 streams, three 64-row batches/stream, three workers, 100 ms pacing and matching Arrow bytes,
schemas, seeds and settings are checked offline. Order remains baseline then ADR. Final observation and
visible-throughput definitions have the same sink/polling confounders as the legacy measurements.</p>"""
    section += table(("Design", "Phase", "Generated", "Visible", "Missing", "Unresolved", "Retries",
                      "Terminal P95 ms", "Final obs P95 ms", "Visible rows/s",
                      "Stream-open P95 ms (not admission)", "Admission latency"), measured)
    section += "<h3>Sampling cadence and boundaries</h3>" + table(
        ("Design", "Cycles", "Sampling errors", "Median start cadence ms", "Max start cadence ms",
         "Median read-cycle duration ms"), cadence)
    section += "<ul>" + "".join(f"<li>{esc(v)}</li>" for v in summary["semantics"]) + """</ul>
<p>Observer waits 0.5 seconds <em>after</em> each read cycle; it is not a 2 Hz sampler.
CPU table uses only counter intervals entirely inside each recorded phase (one core = 100%).
Cgroup usage includes cache; anonymous/RSS cgroup fields are not per-process RSS.
MiB = 1,048,576 bytes. Scaling changes the included topology; no integrated CPU or capacity conclusion follows.
Missing component memory values remain unknown and their whole-cycle sums are excluded.
Very short non-workload intervals have as few as one point, not meaningful distributions.</p>"""
    section += table(("Design", "Interval", "Points", "Incomplete memory cycles", "Cycles ending outside interval",
                      "Max complete staggered stack sum MiB"), intervals)
    section += "<details><summary>Per-interval component CPU/cgroup measurements (SQL/Hive and all etcd members included)</summary>" + table(
        ("Design", "Interval", "Service", "Points", "Interior CPU intervals", "CPU median %",
         "CPU max %", "Usage max MiB", "Anon/RSS max MiB", "Missing usage reads"), components) + "</details>"
    section += "<h3>Client and application-owned backlog</h3><p>Client high-water values are exact for instrumented logical ownership per phase, not RSS. Baseline retention is not equivalent to the ADR retry-unit retention. All completed phase final queued/active/in-flight/retained counts are checked zero.</p>"
    section += table(("Design", "Phase", "Retained high-water bytes", "In-flight high-water bytes",
                      "Active high-water count", "Final retained bytes", "Final unconfirmed bytes"), clients)
    section += table(("Service", "Gauge", "Unit", "Sampled current max", "Lifetime high-water", "Last sampled current"), gauges)
    metrics = summary["adr_metrics"]
    section += "<p>" + esc(metrics["gauge_coverage_gap"]) + """ Last samples are not shutdown observations.
Lifetime high-water counters are not per-phase peaks. Authority pending units are sampled counts.
Cache resets include idle timeout/uncertainty, not just compaction. Signed asynchronous revision distance
can be negative because observations are staggered; it is not a time delay.
Watch distributions cover only matched successful CAS → watch applications, not snapshot/resolve,
all revisions, or an exact consensus commit timestamp. Bounds use controller RPC brackets and ±1 ms
timestamp precision on the same host; no cross-host clock guarantee.</p>
<details open><summary>Cache, watch propagation, CAS and pending-unit distributions (units in keys)</summary><pre>"""
    section += esc(json.dumps(metrics, indent=2)) + "</pre></details>"
    section += f"""<p>Sources: <code>{esc(cohort)}/run.json</code>,""" + """
<code>observations/{baseline,adr}/{samples.jsonl,intervals.json}</code>,
per-phase <code>report.json</code>, <code>adr-runtime.log</code>,
<code>watch-propagation.json</code>, <code>revision-observations.json</code> and
<code>observation-summary.json</code>, all relative to the evidence tree.
Run-time source hashes are preserved separately from final reporting-source hashes; old runtime snapshots
are compared to old validated binaries, never to the instrumented build.</p></section>"""
    if not include_matrix:
        return section, summary
    section += "<section><h2>Requirement-to-evidence matrix — implemented focused fixes and explicit remaining work</h2><p>Selected proof-of-concept boundaries were implemented and measured; the entire ADR is <strong>not complete</strong>. Status is descriptive, not an all-pass checklist. The focused fixes and proofs below are separate from historical cohort b.</p>"
    section += table(("Requirement", "Status", "Actual scope / gap", "Evidence"),
                     [(r["requirement"], r["status"], r["actual_scope_or_gap"], r["evidence"]) for r in matrix])
    return section + "</section>", summary


def generate(shared_delta=None):
    selected = REPO / "target/delta-benchmark/selected-cohort.json"
    if shared_delta is None and selected.exists():
        shared_delta = json.loads(selected.read_text())["cohort"]
    if shared_delta:
        from delta_report import generate as generate_shared
        return generate_shared(shared_delta)
    instrumented, instrumented_summary = instrumented_section(
        "instrumented-obs20261026b", "Historical instrumented cohort b — prior functional revision")
    final_section, final_summary = instrumented_section(
        FINAL_COHORT, "Final functional revision — fresh matched cohort final2", include_matrix=True)
    focused = json.loads((EVIDENCE/"focused-gaps20261026/manifest.json").read_text())
    assert focused["live_proof_passed"] and len(focused["scenarios"]) == 10
    final_section += "<section><h2>Focused functional fixes and ten passed live proofs</h2><p>Persisted global/per-target migration reservations, restart reconstruction and release; runtime retirement/encoding barriers and exact session/epoch fencing; retained in-process retry handles with primitive-only JSON metadata projection. Existing checks: seven Rust contracts and eight Python client tests, strict Clippy and formatting passed.</p>"
    final_section += table(("Passed proof",), [(name,) for name in focused["scenarios"]])
    final_section += "<h3>Residual unverified criteria</h3><ul>" + "".join(
        "<li>"+esc(value)+"</li>" for value in focused["residual_unverified_approved_criteria"]) + "</ul><p>ADR admission latency is unavailable by protocol: only final TABLE_COMMITTED is emitted. Stream-open latency is displayed separately and is not an admission proxy. Focused proofs: focused-gaps20261026/manifest.json and adr0001-focused20261026f.</p></section>"
    reports = []
    for path in sorted(EVIDENCE.glob("*/*/report.json")):
        value=json.loads(path.read_text())
        value["_artifact"]=str(path.relative_to(EVIDENCE))
        reports.append(value)
    matched = {}
    for report in reports:
        matched.setdefault(report["label"], {})[report["design"]] = report
    comparisons = []
    matched_rows = 0
    for label, designs in matched.items():
        if set(designs) != {"adr", "baseline"}:
            continue
        a, b = designs["baseline"], designs["adr"]
        for key in ("generated_rows", "arrow_bytes"):
            assert a[key] == b[key], (label, key)
        for key in ("run_id", "rows", "batches_per_stream", "streams", "workers", "seed", "schema_fingerprints"):
            assert a["settings"][key] == b["settings"][key], (label, key)
        for report in (a, b):
            assert report["reconciled"] and report["missing_rows"] == 0, label
            assert report["unresolved_streams"] == 0, label
            assert report["visible_rows"] == report["generated_rows"], label
        matched_rows += a["generated_rows"]
        comparisons.append(label)
    rows = []
    for report in reports:
        p95 = report["terminal_latency_ms"]["p95"]
        ack = report["final_commit_observation_ms"]["p95"]
        rows.append("<tr>" + "".join(f"<td>{esc(v)}</td>" for v in (
            report["design"], str(Path(report["_artifact"]).parent.name), report["generated_rows"], report["visible_rows"],
            report["missing_rows"], report["duplicates_or_unconfirmed"], report["unresolved_streams"],
            report["retries"], f"{p95:.2f}" if p95 is not None else "n/a",
            f"{ack:.2f}" if ack is not None else "n/a",
            f"{report['visible_rows_per_second']:.1f}", str(report["reconciled"]))) + "</tr>")
    faults = json.loads((EVIDENCE / "faults/events.json").read_text()) if (EVIDENCE / "faults/events.json").exists() else []
    faultrows = "".join(f"<tr><td>{esc(event['scenario'])}</td><td>assertions passed; saved authoritative evidence</td></tr>"
                        for event in faults)
    failures = [str(p.relative_to(EVIDENCE)) for p in EVIDENCE.rglob("*.log") if p.suffix == ".log"
                and ("Traceback" in p.read_text(errors="replace") or "error:" in p.read_text(errors="replace"))]
    manifest_counts = [sum(m["manifest_bytes"] for m in r["reconciliation"].get("manifests", []))
                       for r in reports if r["design"] == "adr"]
    metadata_max = max(manifest_counts, default=0)
    warm = {design: [r["terminal_latency_ms"]["p95"] for r in reports if r["design"] == design
                     and r["label"].startswith("warm-")] for design in ("baseline", "adr")}
    totals = {design: {"runs": sum(r["design"] == design for r in reports),
                      "generated_rows": sum(r["generated_rows"] for r in reports if r["design"] == design),
                      "visible_rows": sum(r["visible_rows"] for r in reports if r["design"] == design),
                      "missing_rows": sum(r["missing_rows"] for r in reports if r["design"] == design),
                      "unresolved_streams": sum(r["unresolved_streams"] for r in reports if r["design"] == design)}
              for design in ("baseline", "adr")}
    cas=[]
    for line in (EVIDENCE/"matched-runtime.log").read_text().splitlines():
        try:
            value=json.loads(line[line.index("{"):])
            if value.get("event")=="etcd_cas":cas.append(value["elapsed_us"])
        except (ValueError,KeyError):
            pass
    cas.sort()
    def memory(raw):
        number,unit=re.match(r"([0-9.]+)([A-Za-z]+)",raw.split(" / ")[0]).groups()
        return float(number)*{"B":1,"kB":1000,"KB":1000,"KiB":1024,"MB":1e6,"MiB":1024**2,
                              "GB":1e9,"GiB":1024**3}[unit]
    resource_totals={}
    for report in reports:
        if report["label"]=="warm-01":
            samples=[report["resources_before"],report["resources_after"]]
            resource_totals[report["design"]]=[{"containers":len(sample),
                "docker_cpu_percent_sum":sum(float(s["CPUPerc"].rstrip("%")) for s in sample),
                "docker_memory_mib_sum":sum(memory(s["MemUsage"]) for s in sample)/1024**2}
                for sample in samples]
    summary = {"totals": totals, "matched_labels": comparisons, "matched_rows_per_architecture": matched_rows,
               "totals_scope": "Legacy reports only; generated totals are not acknowledged/reconciled totals",
               "instrumented_completed": {"cohort": instrumented_summary["cohort"], "matched_pairs": 11,
                                         "rows_per_design": instrumented_summary["rows_per_design"]},
               "instrumented_incomplete": "instrumented-obs20261026a; excluded, not pooled",
               "final_completed": {"cohort": final_summary["cohort"], "matched_pairs": 11,
                                   "rows_per_design": final_summary["rows_per_design"]},
               "focused_checks": focused["checks_results"],
               "focused_live_proofs": len(focused["scenarios"]),
               "fault_events": len(faults),
               "maximum_reached_manifest_bytes": metadata_max,
               "warm_p95_median_ms": {d: statistics.median(v) if v else None for d, v in warm.items()},
               "preserved_failure_logs": failures,
               "matched_controller_etcd_cas_us":{"samples":len(cas),
                   "p50":cas[len(cas)//2] if cas else None,"p95":cas[min(len(cas)-1,int(len(cas)*.95))] if cas else None},
               "warm_01_whole_stack_sparse_resources":resource_totals}
    observations=f"""<section><h2>Measured observations, not a universal winner</h2>
<p><strong>{len(comparisons)} matched phase pairs reconciled</strong>: {matched_rows:,} generated/visible rows
per architecture, with zero missing rows or unresolved streams in those matched measurements.
Median of the three warm-run terminal P95 values: original {summary['warm_p95_median_ms']['baseline']:.2f} ms,
new {summary['warm_p95_median_ms']['adr']:.2f} ms. The new design was not faster on this short workload.</p>
<p>New matched controller/etcd transactions: {len(cas)} observations, P50 {summary['matched_controller_etcd_cas_us']['p50']} µs,
P95 {summary['matched_controller_etcd_cas_us']['p95']} µs (includes membership/placement/admission transactions,
not exclusively data commits). Resource samples and materially different sinks prevent proxy-only attribution.</p></section>"""
    document = """<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ADR 0001: measured, etcd-fenced Flight routing</title>
<style>
:root{color-scheme:light}body{font:17px/1.6 system-ui,sans-serif;margin:auto;max-width:1200px;padding:24px;color:#142534;background:#f5f8fb}
h1,h2,h3{line-height:1.2}h1{font-size:2.2rem}section{background:white;padding:24px;margin:22px 0;border:1px solid #c5d1df;border-radius:12px}
a{color:#005399}code{background:#edf2f7;padding:2px 5px;overflow-wrap:anywhere}table{border-collapse:collapse;width:100%;font-size:.88rem}
td,th{padding:8px;border:1px solid #abbccb;text-align:left}th{background:#e5edf6}caption{text-align:left;font-weight:bold;margin:10px 0}
.scroll{overflow:auto}.warning{border-left:6px solid #a75000;padding-left:18px}.diagram{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:16px}
.box{background:#edf3fb;border:2px solid #43678a;padding:14px;border-radius:8px}.small{font-size:.88rem}details pre{white-space:pre-wrap;overflow-wrap:anywhere}
@media print{body{padding:0;background:white}section{break-inside:avoid}.scroll{overflow:visible}}
</style><main><h1>Native Flight routing with an atomic ownership/head fence</h1>
<p>Fresh same-host experiment · October 26, 2026 · Complete architectures, not proxy-only causality.</p>
<nav aria-label="Contents"><a href="#architecture">Architecture</a> · <a href="#safety">Safety</a> ·
<a href="#measurements">Measurements</a> · <a href="#limits">Interpretation</a> · <a href="#reproduce">Evidence</a></nav>
<section class="warning"><h2>What was actually compared</h2>
<p>The original Pingora + disk-backed routing + SQL + Delta architecture versus an explicit native Rust Flight gateway,
combined placement/ownership/commit controller, no-WAL collector adjunct, and three dedicated etcd members.
These are different sinks, commit protocols, batching and dependency stacks. No result isolates proxy speed,
proves production capacity, or demonstrates host/disk/regional HA.</p>
<p>The ADR provenance is the user-supplied proposed text. The inaccessible GitHub PR head was not verified.
Private PRD/ADR material was not copied into this repository.</p></section>
""" + observations + """<section id="architecture"><h2>Architectures and technologies</h2><div class="diagram">
<div class="box"><h3>Original architecture</h3><p>Bounded native PyArrow clients → signed edge metadata →
Pingora gateway(s) → original disk-backed routing/direct Rust collectors → SQL ownership/receipts + Delta data.
SQL Server and Hive dependencies count in resource snapshots.</p></div>
<div class="box"><h3>Additional architecture</h3><p>Retaining native PyArrow clients → HMAC-authenticated Flight gateway(s) →
generation-bound manifest collector adjunct → immutable Parquet/parent manifests.
Gateway cache ← snapshot/revision/long-poll bridge ← combined Rust controller → three-member authenticated etcd.</p></div>
<div class="box"><h3>Authoritative boundary</h3><p>Parquet and manifest finalized first.
ONE etcd transaction checks live control + member identities/revisions + exact table record, epoch, lifecycle,
pending admission and old head, then changes reader-visible head. Only then TABLE_COMMITTED.</p></div></div>
<p>Arrow Flight/tonic stream forwarding uses raw messages and bounded channels, not array decoding in the gateway.
Collectors decode only their admitted streams and serialize each table through one local mailbox.
Rust ArrowWriter writes immutable Parquet. Readers traverse committed references, never glob staged objects.
Arrow schemas/table UUIDs come from the canonical catalog SDK. Equal schemas do not merge table ownership;
statically registered versions evolve under one immutable table ID; live catalog/DDL registration is absent.</p>
<p>Placement uses measured cgroup CPU, RSS, active streams and centralized assigned-owner counts.
The final revision accounts for persisted target reservations with global/per-target migration budgets,
reconstructs them on restart and releases them on completion/failure. Historical cohort b predates these fixes. Migration is
persisted DRAINING → higher-epoch RECOVERING → exact-head ACTIVE, with a bounded admission drain.
An elephant table still has one owner; there is no autoscaler or ingestion WAL.</p></section>
<section id="safety"><h2>Executed correctness and fault evidence</h2>
<p>The initial real stack used two collectors/two gateways. The stale-A proposal was staged after admission;
B activated at a greater epoch, and the resumed A proposal could not change the authoritative head.
Selected process deaths, coordination-only partition with storage still mounted, controller crash/restarts,
target failure, compaction, one-member loss and quorum loss were then tested on task-owned resources.</p>
<div class="scroll"><table><caption>Saved passing event assertions, not inferred production guarantees</caption>
<thead><tr><th>Scenario</th><th>Evidence classification</th></tr></thead><tbody>""" + faultrows + """</tbody></table></div>
<p>Live fixtures exercise two distinct tables sharing a schema, registered evolution under one owner,
schema/descriptor rejection, dictionary reconstruction and bounded client replay after a lost final-reply observation.
Focused Rust contracts and real local Flight client tests supplement, not replace, runtime checks.</p>
<p>The mailbox probe ran 18 concurrent streams/36 batches while an overlapping process attempted the same
collector identity: one unchanged generation, exactly 36 head transitions and 1,152 independently reconciled rows.
The bounded-stream probe held 16 schema-only streams, rejected stream 17, verified deadline/cancellation
released all active work, and rejected a 3,146,425-byte message against the 2,097,152-byte bound without changing head.</p>
<p>A client retains finite complete Arrow units. Failed calls are not replayed by the gateway.
Client retries can duplicate rows; at-least-once coverage and duplicates are accounted separately.
The lost-reply experiment discards an already received final observation at the application boundary;
it is not a packet-level network loss experiment.</p></section>
<section id="measurements"><h2>Legacy measured runs (old validated build)</h2><p>Matching schemas, generated Arrow bytes, fixed-length run markers,
seeds, 3 native worker threads, settings and phase names are checked by the report generator.
Warm and cold each have three matched repetitions; every cold replicate has its own dataset/project.
Architecture order was baseline then new,
not randomized/interleaved. Separate new datasets isolate correctness from matched cold placement.</p>
<p>Terminal P95 includes the complete stream. Baseline final observation is an SQL receipt polling/reconciliation
upper bound; new final observation is TABLE_COMMITTED receipt arrival. They are <strong>not equivalent latency definitions</strong>.
Visible rows/second includes independent authoritative scan/reconciliation time and is not sustained ingestion capacity.</p>
<p>Both send two 32-row batches/stream normally (elephant: two 64-row batches); baseline reads provisional
admission receipts then polls SQL publication and scans Delta. The new adapter stages one Parquet/manifest
per batch and synchronizes before atomic head publication; it does not share Delta's flush grouping.
Baseline global/per-table buffer caps are 128/32 MiB, and maximum Arrow flush memory is 32 MiB.
This different batching/publication contract is deliberate and materially confounds throughput and latency.</p>
<div class="scroll"><table><caption>Exact saved report.json values; milliseconds unless stated</caption><thead><tr>
<th>Design</th><th>Phase</th><th>Generated</th><th>Visible</th><th>Missing</th><th>Dup/unconfirmed</th><th>Unresolved</th>
<th>Retries</th><th>Terminal P95</th><th>Final obs. P95</th><th>Visible rows/s</th><th>Reconciled</th>
</tr></thead><tbody>""" + "".join(rows) + """</tbody></table></div>
<h3>Resource and metadata measurement boundaries</h3><p>Raw reports include every running project container's
Docker CPU/RSS/memory before and after each phase, including all three etcd members; generator peak RSS and
fixed thread count are separate. These sparse samples are not integrated CPU cost, working-set peaks or a
resource-normalized capacity comparison. Worker/gateway caps match, but total dependency caps differ:
baseline 7 CPU/8 GiB versus new 6.5 CPU/5.75 GiB at three collectors/one gateway.
Per-service allocations and dedicated idle samples are recorded in Compose/provenance.</p>
<p>Reached manifest metadata bytes, commit counts and Parquet sizes are recorded per authoritative scan.
No full-table rewrite is required per commit. Parent-chain recovery is linear and bounded at 100,000;
checkpoints, format compaction and orphan GC are not implemented. Controller logs record individual etcd CAS
latencies. Migration events retain operation/epoch/head and observed completion duration where measured.
Legacy runs lack the later interval resource/cache/backlog observations. See the separate instrumented cohort
for bounded samples, not exact continuous telemetry or per-phase restart timings.</p>
<details><summary>Machine-checked aggregate values</summary><pre>""" + esc(json.dumps(summary, indent=2)) + """</pre></details>
</section>""" + final_section + instrumented + """<section id="limits"><h2>Honest limitations and preserved failures</h2>
<ul><li>Same-host, non-Byzantine process model only. Shared storage is not malicious-writer WORM.
File/dir synchronization is not proof against host/disk loss. No backup restore, multi-host or regional test.</li>
<li>Three private etcd members authenticate clients but use HTTP on the scoped Docker network, not production TLS.
Only loopback/random Flight ingress is published. Control/etcd remain private; credentials stay ignored and scoped.</li>
<li>Collector is an explicit adjunct preserving baseline code paths; it is not the old BufferManager/Delta
publication path, generic Quiver WAL integration, or a production-ready replacement.</li>
<li>Single configured tenant/catalog identity is explicit. This is not arbitrary multi-tenant authorization.
Admission is not final success; no ingestion WAL means client process death can lose unacknowledged retained input.</li>
<li>Original unsupported automatic fault/recovery behavior is not fabricated as parity. The historical
uncertain-authority incident in the preserved baseline evidence/report remains relevant and is not overwritten.</li>
<li>A developmental post-quorum run had three unresolved streams/192 retained input rows; it remains visible
as fault-recovery-failed. Stale-owner errors were corrected from permanent precondition to transient unavailable,
and restart readiness now requires fresh incarnations plus ACTIVE ownership. The fresh full rerun recovered
192/192 rows and reconciled all 608 unique acknowledged client markers.</li>
<li>The earlier Playwright MCP navigation attempt timed out. This historical limitation is retained;
current report validation is recorded separately in report-validation.json and, when available,
report-browser-validation.json rather than inferred from that earlier attempt.</li>
<li>Host Cargo checks were used, not a freshly provisioned devcontainer/NX environment.
No commits, pushes, PRs, baseline source edits, global cleanup or volume deletion are authorized.</li></ul>
<details open><summary>Preserved failed commands/attempts</summary><pre>""" + esc("\n".join(failures)) + """</pre></details></section>
<section id="reproduce"><h2>Reproduction and provenance</h2><p>See README.md alongside this file. Exact source/binary/image hashes,
commands, check results, runtime totals and shutdown proof are in
<code>target/adr0001/evidence/final-evidence.json</code>. Raw per-stream attempts, independent scans and settings are
in the same evidence tree. Original source preservation is rechecked against all 45 overlay paths.
No external report assets, CDNs, fonts or scripts are needed.</p>
<p class="small">This report is generated from saved artifacts by projects/loadtest/adr0001/report.py;
report-metrics.json is independently reproducible. Failed evidence is retained, not silently replaced.</p></section></main></html>"""
    executive, executive_metrics = executive_view(final_summary, focused)
    summary["executive_final"] = executive_metrics
    (EVIDENCE / "report-metrics.json").write_text(json.dumps(summary, indent=2))
    retained = document.split("<main>", 1)[1].rsplit("</main>", 1)[0]
    retained = retained.replace("<details open>", "<details>")
    prior_css = document.split("<style>", 1)[1].split("</style>", 1)[0]
    references = "<section><h2>Primary references for the proposed experiment</h2><ul>" + "".join(
        f'<li><a href="{url}" rel="noopener noreferrer">{esc(title)}</a></li>'
        for url, title in DOCUMENTATION.items()) + """</ul>
<p>etcd documents conditional atomic transactions and revisioned watches. Its performance guidance describes
disk/network-sensitive consensus latency; it does not prove a speedup from this proposed grouping.
Flight provides streaming application metadata, while this application defines the final committed receipt.</p>
<p>The proposed dwell/byte sweep and acceptance gates are experimental design choices, not benchmark results
or a tested production recommendation. No external resources are fetched to render this report.</p></section>"""
    document = ('<!doctype html><html lang="en"><head><meta charset="utf-8">'
                '<meta name="viewport" content="width=device-width, initial-scale=1">'
                '<title>ADR 0001 · Keep the fence, test a cheaper commit path</title><style>' +
                prior_css + EDITORIAL_CSS + '</style></head><body><main>' + executive +
                '<details id="appendix" class="appendix"><summary>Evidence appendix — full measurements, '
                '28 requirements, historical cohorts, faults &amp; provenance</summary>' +
                '<p class="note">Latest headline charts use final2 only. The retained historical narrative '
                'below describes its own revisions; do not pool cohorts or attribute old runs to current binaries.</p>' +
                retained + references + '</details><footer>Offline, artifact-derived report · '
                'projects/loadtest/adr0001/report.py · Evidence: target/adr0001/evidence/</footer>'
                '</main></body></html>')
    path = REPO / "projects/loadtest/adr0001/report.html"
    path.write_text(document)
    class Validator(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=True)
            self.detail_depth = 0
            self.top_level_details = 0
            self.in_style = False
            self.visible_text = []
            self.prose_text = []
            self.content_stack = []
            self.charts = 0
            self.ids = set()
            self.anchors = []

        def handle_starttag(self, tag, attrs):
            values = dict(attrs)
            excluded = (tag in ("nav", "footer", "title") or values.get("role") == "img" or
                        bool(set(values.get("class", "").split()) &
                             {"stats", "chart", "phase-grid", "legend"}))
            if tag not in ("meta", "br", "hr", "img", "input", "link"):
                self.content_stack.append((tag, excluded))
            for key, value in attrs:
                if key in ("src", "href"):
                    if value.startswith(("http:", "https:", "//")):
                        assert tag == "a" and key == "href" and value in DOCUMENTATION, "external report asset"
                        assert self.detail_depth > 0, "documentation links belong in appendix"
                        assert values.get("rel") == "noopener noreferrer"
                assert not key.startswith("on"), "unexpected active report content"
            assert tag not in ("script", "iframe", "link", "object", "embed"), "unexpected active/external report content"
            if "id" in values:
                assert values["id"] not in self.ids, ("duplicate HTML ID", values["id"])
                self.ids.add(values["id"])
            if tag == "a" and values.get("href", "").startswith("#"):
                self.anchors.append(values["href"][1:])
            if tag == "details":
                assert "open" not in values, "evidence must initially be collapsed"
                if self.detail_depth == 0:
                    self.top_level_details += 1
                    assert values.get("id") == "appendix"
                self.detail_depth += 1
            if tag == "style":
                self.in_style = True
            if "data-value-ms" in values:
                width = float(re.fullmatch(r"width:([0-9.]+)%", values["style"])[1])
                expected = float(values["data-value-ms"]) / float(values["data-axis-max-ms"]) * 100
                assert abs(width - expected) < 1e-7
                assert values.get("aria-label") and 0 <= width <= 100
                self.charts += 1

        def handle_endtag(self, tag):
            assert self.content_stack and self.content_stack[-1][0] == tag, ("unbalanced HTML", tag)
            self.content_stack.pop()
            if tag == "details":
                self.detail_depth -= 1
                assert self.detail_depth >= 0
            if tag == "style":
                self.in_style = False

        def handle_data(self, data):
            if self.in_style:
                assert "@import" not in data and "url(" not in data, "external CSS resource"
            elif self.detail_depth == 0:
                self.visible_text.append(data)
                if not any(excluded for _, excluded in self.content_stack):
                    self.prose_text.append(data)

    validator = Validator()
    validator.feed(document)
    assert validator.top_level_details == 1 and validator.detail_depth == 0
    assert set(validator.anchors) <= validator.ids
    assert validator.charts == len(executive_metrics["chart_geometry"]) == 24
    visible_words = len(re.findall(r"\b[\w’-]+\b", " ".join(validator.visible_text)))
    prose_words = len(re.findall(r"\b[\w’-]+\b", " ".join(validator.prose_text)))
    assert prose_words <= 900, ("executive prose budget", prose_words)
    assert visible_words <= 1100, ("executive view including chart/diagram labels", visible_words)
    assert document.count("<table>") == document.count("</table>")
    assert document.count("<tr>") == document.count("</tr>")
    assert "entire ADR is <strong>not complete</strong>" in document
    (EVIDENCE / "report-validation.json").write_text(json.dumps({
        "static_html": True, "external_assets": 0, "raw_metric_comparisons_passed": comparisons,
        "reports": len(reports), "fault_events": len(faults),
        "instrumented_matched_pairs": 11, "instrumented_rows_per_design": 50688,
        "final_cohort": FINAL_COHORT, "final_matched_pairs": 11, "final_rows_per_design": 50688,
        "focused_live_proofs": 10, "focused_checks": focused["checks_results"],
        "requirements": len(requirement_coverage.generate()), "incomplete_cohort_excluded": True,
        "executive_visible_words_including_diagram_chart_labels": visible_words,
        "executive_prose_words_excluding_visual_labels_navigation_footer": prose_words,
        "top_level_details": validator.top_level_details, "initially_open_details": 0,
        "chart_geometry_checked": validator.charts,
        "executive_cohort": executive_metrics["cohort"],
        "external_documentation_links": list(DOCUMENTATION),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shared-delta", help="fresh paired cohort under target/delta-benchmark")
    generate(parser.parse_args().shared_delta)
