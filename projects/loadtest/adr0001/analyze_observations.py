"""Offline, bounded summaries of timestamped observations; no runtime mutations."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import statistics

REPO = Path(__file__).resolve().parents[3]


def distribution(values):
    values = sorted(values)
    return {"count": len(values), "minimum": values[0] if values else None,
            "median": statistics.median(values) if values else None,
            "p95": values[int((len(values)-1)*.95)] if values else None,
            "maximum": values[-1] if values else None}


def events(path):
    result = []
    for line in path.read_text().splitlines():
        try:
            value = json.loads(line[line.index("{"):])
            value["service"] = line.split("|", 1)[0].strip()
            result.append(value)
        except (ValueError, KeyError):
            continue
    return result


def analyze(cohort):
    root = REPO / "target/adr0001/evidence" / cohort
    run = json.loads((root/"run.json").read_text())
    reports = {design: {p.parent.name: json.loads(p.read_text())
                       for p in (root/design).glob("*/report.json")}
               for design in ("baseline", "adr")}
    assert set(reports["baseline"]) == set(reports["adr"])
    assert len(reports["adr"]) == 11
    rows = 0
    phase_pairs = []
    for label, a in sorted(reports["baseline"].items()):
        b = reports["adr"][label]
        for key in ("generated_rows", "arrow_bytes"):
            assert a[key] == b[key], (label, key)
        for key in ("run_id", "rows", "batches_per_stream", "streams", "workers", "seed",
                    "schema_fingerprints", "pacing_ms"):
            assert a["settings"][key] == b["settings"][key], (label, key)
        for value in (a, b):
            assert value["reconciled"] and value["unresolved_streams"] == value["missing_rows"] == 0
            assert value["generated_rows"] == value["visible_rows"] == 4608
        rows += a["generated_rows"]
        phase_pairs.append({"label": label, "rows_per_design": a["visible_rows"],
                            "arrow_bytes_per_design": a["arrow_bytes"],
                            "terminal_p95_ms": {d: reports[d][label]["terminal_latency_ms"]["p95"]
                                                for d in reports}})
    result = {"cohort": cohort, "matched_pairs": phase_pairs, "rows_per_design": rows,
              "provenance": "run.json source/binary digests; distinct from legacy comparisons",
              "semantics": [
                  "Cgroup memory is sampled usage/anonymous memory, not process RSS or continuous peak.",
                  "CPU percentages are counter deltas over exact Engine-read intervals, one core = 100%; not integrated phase CPU.",
                  "Cycles are selected by cycle start inside the interval; reads may finish outside it. Component sums combine staggered reads, not simultaneous observations. Missing memory is unknown, never zero; incomplete sums are excluded.",
                  "Client bytes are logical Arrow data, excluding IPC/native/allocator buffers. Exhausted bytes are unconfirmed regenerable input, not a resident-memory claim.",
                  "Gateway guards count application-owned queued/blocked-sender protobuf bytes; tonic/kernel buffers excluded.",
                  "Writer gauges count active streams, waiting mailbox calls and batch encoding/admit ownership, not all Parquet/native allocations.",
                  "Gauge high-water is exact for accounted ownership since process start; interval current peaks are sampled.",
                  "Revision distances have units of revisions, never seconds. RPC timestamps only bracket consensus commit time.",
                  "Snapshot/resolve application age is not watch propagation and is excluded from watch delay summaries."],
              "designs": {}}
    frames_by_design = {}
    for design in reports:
        directory = root/"observations"/design
        meta = json.loads((directory/"intervals.json").read_text())
        frames = [json.loads(line) for line in (directory/"samples.jsonl").read_text().splitlines()]
        assert 1 < len(frames) <= 1802 and meta["sampling_errors"] == 0
        assert all(not frame.get("error") and frame["project"] == Path(run["roots"][design]).name
                   for frame in frames)
        frames_by_design[design] = frames
        intervals = []
        for interval in meta["intervals"]:
            first, last = interval["started_ns"], interval["ended_ns"]
            assert frames[0]["started_ns"] <= first <= last <= frames[-1]["ended_ns"]
            chosen = [f for f in frames if first <= f["started_ns"] <= last]
            components = defaultdict(list)
            for frame in chosen:
                for component in frame["containers"]:
                    components[component["name"]].append(component)
            roles = {}
            for name, values in components.items():
                cpu = [v["cpu_interval"]["one_core_percent"] for v in values if v["cpu_interval"]
                       and v["engine_read_ns"]-v["cpu_interval"]["elapsed_ns"] >= first
                       and v["engine_read_ns"] <= last]
                usage = [v["memory_usage_bytes"] for v in values if v["memory_usage_bytes"] is not None]
                anon = [v["cgroup_rss_or_anon_bytes"] for v in values if v["cgroup_rss_or_anon_bytes"] is not None]
                roles[name] = {"service": values[0]["service"], "samples": len(values),
                    "missing_memory_usage_samples": len(values)-len(usage),
                    "missing_cgroup_rss_or_anon_samples": len(values)-len(anon),
                    "sampled_memory_usage_peak_bytes": max(usage, default=None),
                    "sampled_cgroup_rss_or_anon_peak_bytes": max(anon, default=None),
                    "interior_cpu_one_core_percent": distribution(cpu)}
            if interval["design"]:
                assert len(chosen) >= 2, (design, interval["label"], "insufficient workload samples")
                expected = ({"router", "sqlserver", "hive-metastore", "direct-1", "direct-2", "direct-3"}
                            if design == "baseline" else
                            {"controller", "gateway1", "collector1", "collector2", "collector3",
                             "etcd1", "etcd2", "etcd3"})
                assert expected <= {v["service"] for v in roles.values()}, (design, interval["label"])
                final = interval["client_final"]
                assert final["queued_jobs"] == final["active_units"] == final["in_flight_bytes"] == 0
                assert final["retained_bytes"] == final["unresolved_retained_bytes"] == 0
            intervals.append({**interval, "point_samples": len(chosen), "components": roles,
                              "cycles_ending_after_interval": sum(f["ended_ns"] > last for f in chosen),
                              "incomplete_memory_cycles": sum(any(c["memory_usage_bytes"] is None
                                  for c in f["containers"]) for f in chosen),
                              "cycle_memory_usage_sum_bytes": distribution([
                                  sum(c["memory_usage_bytes"] for c in f["containers"])
                                  for f in chosen if all(c["memory_usage_bytes"] is not None
                                                        for c in f["containers"])])})
        services = sorted({c["service"] for f in frames for c in f["containers"]})
        result["designs"][design] = {"project": meta["project"], "samples": len(frames),
            "nominal_wait_seconds": meta["cadence_seconds"], "sampling_errors": meta["sampling_errors"],
            "actual_start_cadence_ms": distribution([(b["started_ns"]-a["started_ns"])/1e6
                                                   for a, b in zip(frames, frames[1:])]),
            "cycle_duration_ms": distribution([(f["ended_ns"]-f["started_ns"])/1e6 for f in frames]),
            "services": services, "intervals": intervals}
    raw = events(root/"adr-runtime.log")
    cas = {v["revision"]: v for v in raw if v.get("event") == "etcd_cas" and v.get("succeeded")}
    gauges = ("gateway_input_bytes", "gateway_result_bytes", "mailbox_waiters", "writer_streams",
              "decoded_batch_bytes")
    flights = [v for v in raw if v.get("event") == "flight_observation"]
    gauge_summary = {}
    for service in sorted({v["service"] for v in flights}):
        values = [v for v in flights if v["service"] == service]
        gauge_summary[service] = {key: {
            "sampled_current_peak": max(v[key]["current"] for v in values),
            "last_sample_current": values[-1][key]["current"],
            "process_lifetime_high_water": max(v[key]["lifetime_high_water"] for v in values)}
            for key in gauges}
        caches = [v["cache"] for v in values if v["role"] == "gateway" and v["cache"]]
        if caches:
            gauge_summary[service]["last_gateway_cache"] = caches[-1]
            gauge_summary[service]["cache_samples_deferred"] = sum(v["cache_sample_deferred"] for v in values)
    watch = []
    application_counts = defaultdict(int)
    for value in raw:
        if value.get("event") != "directory_applied" or not value["service"].startswith("gateway"):
            continue
        application_counts[value["kind"]] += 1
        source = cas.get(value["assignment_revision"])
        if source is None or value["kind"] != "watch":
            continue
        after_start = value["observed_ms"] - source["started_ms"]
        assert after_start >= -1, "wall-clock/RPC observation ordering changed"
        watch.append({**value, "controller_rpc_started_ms": source["started_ms"],
            "controller_rpc_completed_ms": source["observed_ms"],
            "observed_after_rpc_completion_ms": value["observed_ms"]-source["observed_ms"],
            "propagation_lower_bound_ms": max(0, value["observed_ms"]-source["observed_ms"]-1),
            "propagation_upper_bound_ms": max(0, after_start+1)})
    assert watch and any(v["role"] == "gateway" and v["gateway_input_bytes"]["lifetime_high_water"] > 0 for v in flights)
    assert any(v["role"] == "collector" and v["mailbox_waiters"]["lifetime_high_water"] > 0 for v in flights)
    gaps = []
    gateways = defaultdict(list)
    for v in flights:
        if v["role"] == "gateway" and v["cache"]:
            gateways[v["service"]].append(v)
    for frame in frames_by_design["adr"]:
        control = frame.get("control_observation")
        if not control:
            continue
        t = control["observed_ns"]//1_000_000
        for service, values in gateways.items():
            earlier = [v for v in values if v["observed_ms"] <= t]
            if not earlier:
                continue
            last = earlier[-1]
            gaps.append({"service": service, "control_observed_ns": control["observed_ns"],
                "gateway_sample_ms": last["observed_ms"], "sample_age_ms": t-last["observed_ms"],
                "authority_directory_revision": control["directory_revision"],
                "gateway_applied_directory_revision": last["cache"]["applied_directory_revision"],
                "revision_distance": control["directory_revision"]-last["cache"]["applied_directory_revision"]})
    result["adr_metrics"] = {"service_gauges": gauge_summary,
        "gauge_service_coverage": sorted(gauge_summary),
        "gauge_coverage_gap": "Saved runtime log has no flight_observation from collector4 or gateway2; their application gauges/cache counters are unavailable, not zero. Docker samples include both.",
        "gateway_application_kinds": dict(application_counts),
        "watch_rpc_bracket_lower_ms": distribution([v["propagation_lower_bound_ms"] for v in watch]),
        "watch_rpc_bracket_upper_ms": distribution([v["propagation_upper_bound_ms"] for v in watch]),
        "watch_before_controller_rpc_completion": sum(v["observed_after_rpc_completion_ms"] < 0 for v in watch),
        "asynchronous_directory_revision_distance": distribution([v["revision_distance"] for v in gaps]),
        "cas_roundtrip_us": distribution([v["elapsed_us"] for v in cas.values()]),
        "authority_pending_units": distribution([v["pending_units"] for v in raw
                                                if v.get("event") == "authority_observation"])}
    (root/"watch-propagation.json").write_text(json.dumps(watch, indent=2))
    (root/"revision-observations.json").write_text(json.dumps(gaps, indent=2))
    (root/"observation-summary.json").write_text(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cohort", required=True)
    result = analyze(parser.parse_args().cohort)
    print(json.dumps({"cohort": result["cohort"], "pairs": len(result["matched_pairs"]),
                      "rows_per_design": result["rows_per_design"],
                      "samples": {d: v["samples"] for d, v in result["designs"].items()}}))
