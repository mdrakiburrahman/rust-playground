import argparse
import csv
import json
import math
import statistics as st
from pathlib import Path

CELLS = [f"t{t}-{arm}" for t in range(1, 6) for arm in ("a1", "b", "a2")]
SORT_ENV = "ARROWCOL_COLLECTOR_LAYOUT_SORT_EXECUTION"
IDENTITY = ("sha", "dirty", "host", "binaries", "maintenance_sha256", "allocator_env", "build_provenance",
            "config_sha256", "harness_sha256", "storage", "isolation_level", "duration", "warmup")
T_CRIT_DF4 = 2.776


def rows_of(path):
    return [json.loads(x) for x in Path(path).read_text(encoding="utf-8-sig").splitlines() if x.strip()]


def ok(r):
    v = r.get("committed_rps")
    return r.get("status") == "ok" and isinstance(v, (int, float)) and math.isfinite(v) and v > 0 and bool(r.get("physical"))


def payload(r):
    return tuple(c["preparation"]["payload_corpus_sha256"] for c in r["client"])


def sort_modes(r):
    found = set()

    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                if k == "sort_execution" and isinstance(v, str):
                    found.add(v)
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
    walk(r.get("effective_startup"))
    return sorted(found)


def preflight_modes(r):
    found = set()

    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                if k == "preflight_lookup_mode" and isinstance(v, str):
                    found.add(v)
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
    walk(r.get("effective_startup"))
    return sorted(found)


def same(rows, key):
    return len({json.dumps(r.get(key), sort_keys=True) for r in rows}) == 1


def headroom(path):
    rows = {r["cell"]: r for r in rows_of(path)}
    if sorted(rows) != ["l0", "l1", "l5"]:
        raise ValueError("Headroom needs exactly l0, l1, l5")
    base = rows["l5"]
    checks = {"all_status_ok": all(r["status"] == "ok" for r in rows.values()),
              "l5_committed_valid": ok(base),
              "identity_matched": all(same(list(rows.values()), k) for k in IDENTITY if k != "isolation_level"),
              "knobs_matched_except_isolation": all(
                  {k: v for k, v in r["knobs"].items() if k != "BWA_ISOLATION"} == base["knobs"] for r in rows.values()),
              "payload_matched": len({payload(r) for r in rows.values()}) == 1,
              "diagnostic_zero_commits": all(rows[l]["stats"]["rows"] == 0 and rows[l]["stats"]["commits"] == 0 for l in ("l0", "l1")),
              "l5_sort_execution_concatenate": sort_modes(base) == ["concatenate"]}
    out = {"threshold_ratio": 1.5, "semantics": "L0/L1 are diagnostic discards (no Delta commits); L5 is committed new ingestion",
           "l5_committed_rows_s": base["committed_rps"], "l5_bytes_per_row": base["bytes_per_row"],
           "l5_physical": base.get("physical"), "l5_sort_execution": sort_modes(base),
           "l5_preflight_lookup_mode": preflight_modes(base), "levels": {}}
    for level in ("l0", "l1"):
        r = rows[level]
        out["levels"][level] = {
            "diagnostic_rows_s": r["diagnostic_rps"], "first_attempt_offered_rows_s": r["first_attempt_offered_rps"],
            "retry_offered_rows_s": r["retry_offered_rps"],
            "diagnostic_over_l5": r["diagnostic_rps"] / base["committed_rps"],
            "offered_over_l5": r["first_attempt_offered_rps"] / base["committed_rps"],
            "status": r["status"], "window_s": r["duration"]}
    out["l5_first_attempt_offered_rows_s"] = base["first_attempt_offered_rps"]
    checks["headroom_over_1.5x"] = all(min(v["diagnostic_over_l5"], v["offered_over_l5"]) > 1.5 for v in out["levels"].values())
    out["checks"] = checks
    out["passed"] = all(checks.values())
    out["binaries"] = base["binaries"]
    return out


def aba(path):
    rows = rows_of(path)
    ids = [r["cell"] for r in rows]
    if ids != CELLS[:len(ids)] or len(ids) != len(set(ids)):
        raise ValueError(f"Unexpected cell order {ids}")
    complete = ids == CELLS
    by = {r["cell"]: r for r in rows}
    integrity = {"all_15_arms_present": complete}
    if rows:
        integrity.update({f"identity_{k}": same(rows, k) for k in IDENTITY})
        integrity["payload_matched"] = len({payload(r) for r in rows}) == 1 and all(payload(rows[0]))
        integrity["windows_ordered_non_overlapping"] = all(
            r["stats"]["end_observed_ms"] <= s["stats"]["start_observed_ms"] for r, s in zip(rows, rows[1:]))
        a_knobs = [r["knobs"] for r in rows if r["x"] != "b"]
        b_knobs = [r["knobs"] for r in rows if r["x"] == "b"]
        integrity["arm_knobs_consistent"] = (
            all(k == a_knobs[0] for k in a_knobs) and all(k == b_knobs[0] for k in b_knobs) and
            a_knobs[0].get(SORT_ENV) == "concatenate" and (not b_knobs or b_knobs[0].get(SORT_ENV) == "sort_runs") and
            (not b_knobs or {k: v for k, v in a_knobs[0].items() if k != SORT_ENV} ==
             {k: v for k, v in b_knobs[0].items() if k != SORT_ENV}))
        integrity["effective_sort_execution_matches_arm"] = all(
            sort_modes(r) == (["sort_runs"] if r["x"] == "b" else ["concatenate"]) for r in rows if r.get("effective_startup"))
        integrity["one_fresh_table_per_arm"] = all(
            len(r["stats"].get("table_logs_end", {})) == 1 for r in rows if r.get("stats"))
    invalid = [{"cell": r["cell"], "status": r["status"], "comment": r.get("comment")} for r in rows if not ok(r)]
    triplets = []
    for t in range(1, 6):
        a1, b, a2 = (by.get(f"t{t}-{arm}") for arm in ("a1", "b", "a2"))
        if not (a1 and b and a2):
            triplets.append({"triplet": t, "valid": False, "reason": "missing arm"})
            continue
        if not all(map(ok, (a1, b, a2))):
            triplets.append({"triplet": t, "valid": False, "reason": "invalid arm",
                             "statuses": [a1["status"], b["status"], a2["status"]]})
            continue
        mid = (a1["committed_rps"] + a2["committed_rps"]) / 2
        mid_bpr = (a1["bytes_per_row"] + a2["bytes_per_row"]) / 2
        aa = a2["committed_rps"] / a1["committed_rps"]
        triplets.append({"triplet": t, "valid": True, "A1": a1["committed_rps"], "B": b["committed_rps"],
                         "A2": a2["committed_rps"], "A_midpoint": mid, "ratio_B_over_A_midpoint": b["committed_rps"] / mid,
                         "AA_ratio": aa, "AA_drift_within_5pct": abs(aa - 1) <= 0.05,
                         "B_bytes_per_row": b["bytes_per_row"], "A_midpoint_bytes_per_row": mid_bpr,
                         "bytes_ratio": b["bytes_per_row"] / mid_bpr})
    valid = [x for x in triplets if x["valid"]]
    stats_out = None
    if len(valid) >= 2:
        logs = [math.log(x["ratio_B_over_A_midpoint"]) for x in valid]
        m, s = st.mean(logs), st.stdev(logs)
        t_stat = m / (s / math.sqrt(len(logs))) if s else (math.inf if m > 0 else 0.0)
        stats_out = {"n_triplets": len(valid), "degrees_freedom": len(valid) - 1,
                     "geometric_effect": math.exp(m), "paired_log_t": t_stat if math.isfinite(t_stat) else "positive_infinity",
                     "log_sd": s,
                     "bytes_ratio": st.mean(x["B_bytes_per_row"] for x in valid) / st.mean(x["A_midpoint_bytes_per_row"] for x in valid)}
    gates = None
    if stats_out:
        t_num = stats_out["paired_log_t"] if isinstance(stats_out["paired_log_t"], float) else math.inf
        gates = {"all_15_arms_valid": complete and not invalid,
                 "integrity_checks_passed": all(integrity.values()),
                 "every_triplet_AA_drift_within_5pct": len(valid) == 5 and all(x["AA_drift_within_5pct"] for x in valid),
                 "geomean_at_least_1.05": stats_out["geometric_effect"] >= 1.05,
                 "paired_log_t_at_least_2.776_df4": len(valid) == 5 and t_num >= T_CRIT_DF4,
                 "bytes_ratio_at_most_1.10": stats_out["bytes_ratio"] <= 1.10}
    if not gates or not (gates["all_15_arms_valid"] and gates["integrity_checks_passed"]):
        verdict, reason = "INCONCLUSIVE", "Invalid, missing or integrity-failed arm; no replacement triplets by pre-registration"
    elif not gates["every_triplet_AA_drift_within_5pct"]:
        verdict, reason = "INCONCLUSIVE", "At least one triplet failed the A/A 5% drift gate"
    elif all(gates[k] for k in ("geomean_at_least_1.05", "paired_log_t_at_least_2.776_df4", "bytes_ratio_at_most_1.10")):
        verdict, reason = "QUALIFIED", "All pre-registered gates passed"
    else:
        verdict, reason = "NOT_QUALIFIED", "Valid run; effect, t or bytes gate not met"
    arms = []
    for r in rows:
        c = r.get("collector") or {}
        s = r.get("stats") or {}
        rate = r.get("committed_rps")
        table, end = next(iter(s.get("table_logs_end", {"": {}}).items()))
        start = s.get("table_logs_start", {}).get(table, {})
        arms.append({
            "cell": r["cell"], "triplet": int(r["y"]), "arm": r["x"],
            "sort_execution": ",".join(sort_modes(r)), "preflight_lookup_mode": ",".join(preflight_modes(r)),
            "status": r["status"], "committed_rps": rate,
            "first_attempt_offered_rps": r.get("first_attempt_offered_rps"), "retry_offered_rps": r.get("retry_offered_rps"),
            "committed_over_offered": rate / r["first_attempt_offered_rps"] if rate and r.get("first_attempt_offered_rps") else None,
            "bytes_per_row": r.get("bytes_per_row"), "rows": s.get("rows"), "bytes": s.get("bytes"), "files": s.get("files"),
            "commits": s.get("commits"), "files_per_commit": s.get("files_per_commit"),
            "version_start": start.get("version"), "version_end": end.get("version"),
            "log_MB_end": end.get("bytes", 0) / 1e6 if end.get("bytes") is not None else None,
            "physical_files": (r.get("physical") or {}).get("files"), "physical_rows": (r.get("physical") or {}).get("rows"),
            "physical_bytes": (r.get("physical") or {}).get("bytes"),
            "physical_metadata_sha256": (r.get("physical") or {}).get("metadata_sha256"),
            "footer_reconciled": bool(r.get("physical")) and (r.get("physical") or {}).get("rows") == s.get("rows")
            and (r.get("physical") or {}).get("bytes") == s.get("bytes") and (r.get("physical") or {}).get("files") == s.get("files"),
            "user_cores": c.get("user_cores"), "system_cores": c.get("system_cores"),
            "cpu_s_per_Mrow": (c["user_cores"] + c["system_cores"]) / (rate / 1e6) if rate and c else None,
            "minor_faults_per_s": c["minor_faults"] / s["seconds"] if c.get("minor_faults") is not None and s.get("seconds") else None,
            "major_faults": c.get("major_faults"), "rss_end_GB": c["rss_end_bytes"] / 1e9 if c.get("rss_end_bytes") else None,
            "window_start_ms": s.get("start_observed_ms"), "window_end_ms": s.get("end_observed_ms"),
            "comment": r.get("comment"),
        })
    return {"verdict": verdict, "verdict_reason": reason, "gates": gates, "integrity": integrity,
            "primary": stats_out, "triplets": triplets, "invalid_arms": invalid,
            "primary_definition": "per triplet r = B / ((A1 + A2) / 2); geomean of r over five triplets; one-sample t on ln(r), df=4",
            "n_arms_observed": len(rows), "binaries": rows[0]["binaries"] if rows else None,
            "harness_sha256": rows[0].get("harness_sha256") if rows else None,
            "config_sha256": rows[0].get("config_sha256") if rows else None,
            "source_sha": rows[0].get("sha") if rows else None}, arms


def write_csv(path, rows):
    if not rows:
        return
    keys = list(dict.fromkeys(k for r in rows for k in r))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("action", choices=("headroom", "aba"))
    p.add_argument("rows")
    p.add_argument("output")
    a = p.parse_args()
    if a.action == "headroom":
        obj = headroom(a.rows)
    else:
        obj, arms = aba(a.rows)
        out = Path(a.output)
        write_csv(out.with_name("bc4r2-arms.csv"), arms)
        write_csv(out.with_name("bc4r2-triplets.csv"), obj["triplets"])
    Path(a.output).write_text(json.dumps(obj, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({k: obj[k] for k in obj if k in ("verdict", "verdict_reason", "passed", "checks", "primary", "levels", "l5_committed_rows_s")}, indent=2))
