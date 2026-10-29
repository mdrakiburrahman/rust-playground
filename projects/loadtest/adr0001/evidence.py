"""Final exact local provenance; never reads generated credentials or deletes runtime state."""
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import time


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def captured_binary(container, path):
    process=subprocess.Popen(["docker","--context","default","cp",f"{container}:{path}","-"],stdout=subprocess.PIPE)
    hashes={}
    with tarfile.open(fileobj=process.stdout,mode="r|") as archive:
        for entry in archive:
            if entry.isfile():
                h=hashlib.sha256()
                with archive.extractfile(entry) as stream:
                    for chunk in iter(lambda:stream.read(1024*1024),b""):h.update(chunk)
                hashes[entry.name]=h.hexdigest()
    assert process.wait()==0
    return hashes


def main():
    repo=Path(__file__).resolve().parents[3];evidence=repo/"target/adr0001/evidence"
    prior_path=evidence/"prior-cohort/final-evidence.json"
    prior=json.loads(prior_path.read_text())
    instrumented_root=evidence/"instrumented-obs20261026b"
    instrumented_run=json.loads((instrumented_root/"run.json").read_text())
    final_root=evidence/"instrumented-obs20261026final2"
    final_run=json.loads((final_root/"run.json").read_text())
    focused_path=evidence/"focused-gaps20261026/manifest.json"
    focused=json.loads(focused_path.read_text())
    assert focused["live_proof_passed"] and len(focused["scenarios"])==10
    for name,value in focused["current_source_sha256"].items():
        if name.startswith("crates/") or name=="Cargo.lock":
            assert digest(repo/name)==value, ("focused/current runtime source changed",name)
    for name,value in final_run["source_sha256"].items():
        if name.startswith("crates/") or name=="Cargo.lock":
            assert digest(repo/name)==value, ("final cohort runtime source changed",name)
            assert focused["current_source_sha256"][name]==value,name
    for name,value in final_run["binary_sha256"].items():
        assert digest(repo/"target/debug"/name)==value,name
        if name in focused["binary_sha256"]:
            assert focused["binary_sha256"][name]==value,name
    provenance=Path("/home/ravpate/.copilot/session-state/b46e239b-bb45-4f85-8f72-b2bfc58f8f68/files/router-benchmark/adr0001-provenance/baseline-overlay-manifest.json")
    original=json.loads(provenance.read_text())
    baseline=Path(original["source_worktree"])
    preservation=[dict(row,actual_sha256=digest(baseline/row["path"]),
                       matches=digest(baseline/row["path"])==row["sha256"]) for row in original["source_files"]]
    assert len(preservation)==45 and all(row["matches"] for row in preservation)
    overlay_changes=[row["path"] for row in original["source_files"]
                     if digest(repo/row["path"])!=row["sha256"]]
    assert overlay_changes==["Cargo.lock"],overlay_changes
    (evidence/"baseline-source-after.json").write_text(json.dumps(preservation,indent=2))
    branch=subprocess.check_output(["git","branch","--show-current"],cwd=repo,text=True).strip()
    head=subprocess.check_output(["git","rev-parse","HEAD"],cwd=repo,text=True).strip()
    assert branch=="feat/adr0001-routing-benchmarks" and head==original["base_commit"]
    containers=[]
    volumes={}
    unstarted_attempts={}
    projects=set(prior["preserved_volumes"])
    projects.update(p.name for p in (repo/"target/adr0001").glob("adr0001-*") if (p/"ownership.json").exists())
    for run_path in evidence.glob("instrumented-*/run.json"):
        projects.update(Path(root).name for root in json.loads(run_path.read_text())["roots"].values())
    images=set()
    for project in sorted(projects):
        volumes[project]=subprocess.check_output(["docker","--context","default","volume","ls",
            "--format","{{.Name}}","--filter","label=com.docker.compose.project="+project],text=True).split()
        expected_volumes=0 if project=="adr0001-focused20261026e" else (
            3 if project.startswith("adr0001-") else 2)
        assert len(volumes[project])==expected_volumes,project
        ids=subprocess.check_output(["docker","--context","default","ps","-aq","--filter",
            "label=com.docker.compose.project="+project],text=True).split()
        if not ids:
            assert project in ("adr0001-focused20261026d","adr0001-focused20261026e"),project
            unstarted_attempts[project]="preserved pre-container network allocation failure"
        for identifier in ids:
            record=json.loads(subprocess.check_output(["docker","--context","default","inspect",identifier],text=True))[0]
            assert not record["State"]["Running"],record["Name"]
            images.add(record["Image"])
            containers.append({"project":project,"id":identifier,"name":record["Name"],
                "image_id":record["Image"],"image_reference":record["Config"]["Image"],
                "state":record["State"]["Status"],"exit_code":record["State"]["ExitCode"],
                "nano_cpus":record["HostConfig"]["NanoCpus"],"memory_limit":record["HostConfig"]["Memory"]})
    image_records=[]
    for image in sorted(images):
        record=json.loads(subprocess.check_output(["docker","--context","default","image","inspect",image],text=True))[0]
        image_records.append({"id":record["Id"],"repo_digests":record.get("RepoDigests",[]),
                              "created":record["Created"]})
    sources=[repo/"Cargo.lock",repo/"Cargo.toml",*sorted((repo/"crates/arrowcol-authority").rglob("*")),
             *sorted((repo/"projects/loadtest/adr0001").glob("*"))]
    sources={str(p.relative_to(repo)):digest(p) for p in sources if p.is_file()}
    runtime={}
    focused_attempt_provenance={}
    for root in sorted((repo/"target/adr0001").glob("adr0001-*")):
        if (root/"bin").exists():
            runtime[root.name]={p.name:digest(p) for p in (root/"bin").iterdir() if p.is_file()}
    build_sources=json.loads((evidence/"build-source.json").read_text())
    assert build_sources==prior["build_crate_source_sha256"], "legacy build manifest changed"
    instrumented_sources=json.loads((evidence/"instrumented-checks/build-source.json").read_text())
    assert all(instrumented_run["source_sha256"][name]==value
               for name,value in instrumented_sources.items()), "historical b build/run sources differ"
    validated_binaries=prior["validated_binaries_sha256"]
    instrumented_binaries=instrumented_run["binary_sha256"]
    assert instrumented_binaries==instrumented_run["binary_sha256"]
    for project,binaries in runtime.items():
        if project in prior["runtime_binaries_sha256"]:
            assert binaries==prior["runtime_binaries_sha256"][project],project
        elif project==Path(instrumented_run["roots"]["adr"]).name:
            assert binaries=={name:instrumented_binaries[name] for name in ("adr-controller","adr-flight")},project
        else:
            assert project.startswith("adr0001-focused20261026") or project==Path(final_run["roots"]["adr"]).name,project
            snapshot_manifest=evidence/project/"manifest.json"
            expected=(json.loads(snapshot_manifest.read_text())["binary_sha256"]
                      if snapshot_manifest.exists() else focused["binary_sha256"])
            assert binaries==expected,project
            if project.startswith("adr0001-focused"):
                if snapshot_manifest.exists():
                    snapshot=json.loads(snapshot_manifest.read_text())
                    focused_attempt_provenance[project]={
                        "manifest":str(snapshot_manifest.relative_to(repo)),
                        "sha256":digest(snapshot_manifest),
                        "original_source_sha256":snapshot["source_sha256"],
                        "original_binary_sha256":snapshot["binary_sha256"]}
                else:
                    focused_attempt_provenance[project]={
                        "status":"unstarted network-allocation failure; no live proof",
                        "binary_matches_focused_current":True}
    baseline_binaries={}
    container="pingora-bench-adrfresh-20261026-router-1"
    for name in ("arrowcol-router-rust","arrowcol-rust","arrowcol-loadgen","arrowcol-sdk-gen"):
        baseline_binaries.update(captured_binary(container,"/usr/local/bin/"+name))
    prior_binary_path=baseline/"target/routing-benchmark/final-image-binary-provenance.json"
    prior_binary=json.loads(prior_binary_path.read_text())["least_pressure"]
    expected_binaries={Path(name).name:value for name,value in prior_binary["binary_sha256"].items()}
    assert baseline_binaries==expected_binaries,"reused baseline executables differ from audited provenance"
    fresh_router=next(c for c in containers if c["name"].endswith("pingora-bench-adrfresh-20261026-router-1"))
    prior_image=json.loads(subprocess.check_output(["docker","--context","default","image","inspect",
        prior_binary["image_id"]],text=True))[0]
    fresh_image=json.loads(subprocess.check_output(["docker","--context","default","image","inspect",
        fresh_router["image_id"]],text=True))[0]
    assert prior_image["RootFS"]==fresh_image["RootFS"]
    prior_config=dict(prior_image["Config"]);fresh_config=dict(fresh_image["Config"])
    prior_labels=dict(prior_config.pop("Labels"));fresh_labels=dict(fresh_config.pop("Labels"))
    image_project_labels={"prior":prior_labels.pop("com.docker.compose.project"),
                          "fresh":fresh_labels.pop("com.docker.compose.project")}
    assert prior_config==fresh_config and prior_labels==fresh_labels
    # Baseline observation projects reuse the audited filesystem, not current Cargo executables.
    for container_record in containers:
        if container_record["project"].startswith("pingora-bench-") and container_record["name"].endswith("-router-1"):
            image=json.loads(subprocess.check_output(["docker","--context","default","image","inspect",
                container_record["image_id"]],text=True))[0]
            assert image["RootFS"]==prior_image["RootFS"],container_record["name"]
    logs={}
    for p in sorted(evidence.rglob("*")):
        if p.is_file() and p.name!="final-evidence.json":
            logs[str(p.relative_to(repo))]={"sha256":digest(p),"bytes":p.stat().st_size}
    checks={"cargo_build":"build-sixth.log","cargo_contract_tests":"tests-final.log",
            "strict_clippy":"clippy-final.log","real_local_client_tests":"client-tests-final.log",
            "full_fault_matrix":"faults-third.log","mailbox_process_exclusion":"mailbox.log",
            "bounded_stream_cancellation":"limits.log","matched_new_phases":"adr-phases.log",
            "fresh_cold_replicates":"cold-repeats.log"}
    for name,filename in checks.items():
        assert (evidence/filename).exists(),name
    assert "test result: ok. 3 passed; 0 failed;" in (evidence/"tests-final.log").read_text()
    assert "Ran 4 tests" in (evidence/"client-tests-final.log").read_text()
    assert (evidence/"client-tests-final.log").read_text().strip().endswith("OK")
    for name in ("build-sixth.log","clippy-final.log"):
        assert "Finished `dev`" in (evidence/name).read_text()
        assert "error:" not in (evidence/name).read_text()
    for name in ("schema-proof","limits-proof","mailbox-proof","lost-reply-accounting","report-validation"):
        assert (evidence/(name+".json")).exists(),name
    fault_events=json.loads((evidence/"faults/events.json").read_text())
    assert len(fault_events)==17 and fault_events[-1]["evidence"]["missing_acknowledged_rows"]==0
    mailbox=json.loads((evidence/"mailbox-proof.json").read_text())
    assert mailbox["unchanged_live_generation"] and mailbox["authoritative_rows_reconciled"]==1152
    assert mailbox["after"]["head"]["sequence"]-mailbox["before"]["head"]["sequence"]==mailbox["batches"]==36
    limits=json.loads((evidence/"limits-proof.json").read_text())
    assert limits["head_before"]==limits["head_after"] and not limits["canceled_streams_published"]
    assert limits["simultaneous_schema_streams"]==16 and "stream capacity" in limits["excess_rejected"]
    assert "2097152" in limits["oversized_rejected"]
    schema=json.loads((evidence/"schema-proof.json").read_text())
    assert all(row["terminal"]=="ok" for row in schema["outcomes"])
    assert schema["acknowledged_markers_reconciled"]==32 and schema["dictionary_replay_duplicates"]==8
    assert all(schema[name]["terminal"]=="unresolved" and not schema[name]["receipts"]
               for name in ("unauthorized_table","schema_mismatch"))
    lost_reply=json.loads((evidence/"lost-reply-accounting.json").read_text())
    assert lost_reply["coverage"] and lost_reply["retained_bytes_at_end"]==0
    assert lost_reply["visible_rows"]-lost_reply["generated_rows"]==lost_reply["duplicates"]==32
    cold=json.loads((evidence/"cold-replicates.json").read_text())
    for design in ("baseline","adr"):
        assert len([row for row in cold if row["kind"]=="cold-"+design and row["value"]["report"]])==2
    metrics=json.loads((evidence/"report-metrics.json").read_text())
    report_validation=json.loads((evidence/"report-validation.json").read_text())
    assert report_validation["sha256"]==digest(repo/"projects/loadtest/adr0001/report.html")
    browser_path=evidence/"report-browser-validation.json"
    browser_validation=json.loads(browser_path.read_text()) if browser_path.exists() else None
    if browser_validation:
        assert browser_validation["html_sha256"]==report_validation["sha256"], "stale browser report proof"
        assert browser_validation["passed"], "browser presentation validation failed"
    assert len(metrics["matched_labels"])==13 and metrics["matched_rows_per_architecture"]==11520
    observation=json.loads((instrumented_root/"observation-summary.json").read_text())
    assert len(observation["matched_pairs"])==11 and observation["rows_per_design"]==50688
    instrumented_checks={}
    for name,filename,marker in (
        ("build","build.log","Finished `dev`"),
        ("rust_contracts","tests.log","test result: ok. 4 passed; 0 failed;"),
        ("strict_clippy","clippy.log","Finished `dev`"),
        ("client_tests","client-tests-closeout.log","Ran 6 tests")):
        text=(evidence/"instrumented-checks"/filename).read_text()
        assert marker in text and "error:" not in text,filename
        instrumented_checks[name]={"status":"passed","log":"instrumented-checks/"+filename,
                                  "verification":"historical b saved check; original build/run source hashes matched"}
    closeout=json.loads((evidence/"instrumented-checks/closeout-checks.json").read_text())
    assert closeout["cargo_fmt_exit"]==closeout["client_tests_exit"]==0
    final_observation=json.loads((final_root/"observation-summary.json").read_text())
    assert len(final_observation["matched_pairs"])==11 and final_observation["rows_per_design"]==50688
    assert metrics["final_completed"]["cohort"]==final_run["cohort"]
    assert {event["design"] for event in final_run["events"] if event["kind"]=="completed"}=={"baseline","adr"}
    assert len(final_run["explicit_subnets"])==len(set(final_run["explicit_subnets"]))==4
    for name,record in focused["checks"].items():
        assert digest(repo/record["path"])==record["sha256"],name
    assert digest(repo/focused["proof_manifest"])==focused["proof_manifest_sha256"]
    assert digest(repo/focused["events"])==focused["events_sha256"]
    assert "test result: ok. 7 passed; 0 failed;" in (evidence/"final-current-rust-tests.log").read_text()
    client_checks=(evidence/"final-current-client-tests.log").read_text()
    assert "Ran 8 tests" in client_checks and client_checks.strip().endswith("OK")
    final={"created_unix_ns":time.time_ns(),"repository":str(repo),"branch":branch,"head":head,
        "committed":False,"pushed":False,"pr_created":False,"source_sha256":sources,
        "final_source_manifest_scope":"Current authority crate, Cargo manifests/lock and ADR benchmark/report files; not a claim that legacy runs used current sources",
        "legacy_validated_provenance":{"manifest":"prior-cohort/final-evidence.json",
            "sha256":digest(prior_path),"created_unix_ns":prior["created_unix_ns"],
            "build_crate_source_sha256":build_sources,"validated_binaries_sha256":validated_binaries,
            "runtime_binaries_sha256":prior["runtime_binaries_sha256"],
            "source_sha256":prior["source_sha256"],"checks":prior["checks"]},
        "historical_b_closeout":{"manifest":"instrumented-b-closeout/final-evidence.json",
            "sha256":digest(evidence/"instrumented-b-closeout/final-evidence.json"),
            "report_html_sha256":digest(evidence/"instrumented-b-closeout/report.html")},
        "instrumented_build_provenance":{"build_crate_source_sha256":instrumented_sources,
            "validated_binaries_sha256":instrumented_binaries,"checks":instrumented_checks,
            "runtime_run_manifest":"instrumented-obs20261026b/run.json",
            "runtime_run_manifest_sha256":digest(instrumented_root/"run.json"),
            "runtime_source_sha256":instrumented_run["source_sha256"],
            "reporting_source_changes_since_run":[name for name,value in instrumented_run["source_sha256"].items()
                                                 if digest(repo/name)!=value]},
        "focused_current_build_provenance":{"manifest":str(focused_path.relative_to(repo)),
            "manifest_sha256":digest(focused_path),"validated_source_sha256":focused["current_source_sha256"],
            "validated_binaries_sha256":focused["binary_sha256"],"checks":focused["checks_results"],
            "live_proofs":focused["scenarios"],"residual_limits":focused["residual_unverified_approved_criteria"]},
        "final_cohort_provenance":{"cohort":final_run["cohort"],
            "run_manifest_sha256":digest(final_root/"run.json"),"source_sha256":final_run["source_sha256"],
            "binary_sha256":final_run["binary_sha256"],"explicit_subnets":final_run["explicit_subnets"],
            "matched_pairs":11,"rows_per_design":50688,
            "reporting_source_changes_since_run":[name for name,value in final_run["source_sha256"].items()
                                                 if digest(repo/name)!=value]},
        "new_overlay_paths_changed":overlay_changes,
        "baseline_overlay_manifest_sha256":digest(provenance),"original_baseline_files_verified":len(preservation),
        "baseline_source_unchanged":True,"baseline_image_binaries_sha256":baseline_binaries,
        "baseline_prior_binary_provenance_sha256":digest(prior_binary_path),
        "baseline_reused_filesystem_and_four_binaries_match_audited_prior_evidence":True,
        "baseline_prior_image_id":prior_binary["image_id"],
        "baseline_image_only_config_difference_compose_project_label":image_project_labels,
        "runtime_binaries_sha256":runtime,"images":image_records,"containers":containers,
        "unstarted_preserved_attempts":unstarted_attempts,
        "all_task_containers_stopped":True,"volumes_preserved":True,"preserved_volumes":volumes,"evidence_files":logs,
        "legacy_checks":{name:{"status":"passed","log":filename,"scope":"legacy cohort only"} for name,filename in checks.items()},
        "closeout_checks":closeout,
        "focused_tests":{"legacy_rust_contracts":3,"legacy_local_flight_client_tests":4,
                         "instrumented_rust_contracts":4,"instrumented_local_flight_client_tests":6},
        "current_focused_tests":focused["checks_results"],
        "final_current_test_rerun":{"rust_contracts":7,"python_client_tests":8,
            "rust_log_sha256":digest(evidence/"final-current-rust-tests.log"),
            "client_log_sha256":digest(evidence/"final-current-client-tests.log"),
            "validated_binary_restoration":"final-current-test-binary-restoration.json"},
        "focused_attempt_build_provenance":focused_attempt_provenance,
        "report_presentation_validation":{"static":report_validation,
            "browser":browser_validation,
            "browser_evidence_sha256":digest(browser_path) if browser_validation else None},
        "metrics":metrics,"tools":{"rustc":subprocess.check_output(["rustc","--version"],text=True).strip(),
            "cargo":subprocess.check_output(["cargo","--version"],text=True).strip(),
            "python_packages":subprocess.check_output([str(repo/"target/adr0001/venv/bin/pip"),"freeze"],text=True).splitlines(),
            "submodule":subprocess.check_output(["git","submodule","status","submodules/otel-arrow"],cwd=repo,text=True).strip()},
        "limits":["No host/disk/regional/production HA claim","Not proxy-only causality; different sinks/protocol/dependencies",
            "Worker and gateway limits matched; whole-stack dependency caps differ and are itemized",
            "Legacy sparse resource samples; separate instrumented cohort periodic cgroup/CPU deltas, not integrated CPU or continuous peaks",
            "Cache revision/time observations and accounted backlog gauges have bounded coverage; collector4/gateway2 application logs absent",
            "Two missing ADR memory reads remain unknown; incomplete cycle memory sums excluded",
            "Focused reservations/runtime fencing/retained retry fixes implemented; full-native hot replace, packet-level ambiguous etcd reply loss and exact consensus latency remain unverified; entire ADR is not complete",
            "Interrupted instrumented cohort a preserved as incomplete; never pooled with completed b or legacy rows",
            "No ingestion WAL, checkpoint compaction or orphan GC; bounded parent-chain recovery",
            "Private authenticated etcd HTTP, not production TLS; single explicit tenant mapping",
            "Host Cargo validation, not freshly provisioned devcontainer",
            ("Current report desktop/mobile/print verified in Playwright; earlier MCP timeout remains historical evidence"
             if browser_validation else "Current report statically validated; no current rendered-browser proof recorded"),
            "Initial failed post-quorum run retained with three unresolved streams; subsequent fresh full proof passed",
            "Legacy snapshots identify legacy validated binaries, not the later instrumented build; first proof remains developmental",
            "Baseline prior uncertain-authority incident remains in original artifacts"],
        "commands":{"build":"CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo build -p arrowcol-authority --bins",
            "tests":"CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p arrowcol-authority",
            "lint":"CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p arrowcol-authority --all-targets -- -D warnings",
            "client":"target/adr0001/venv/bin/python -m unittest discover -s projects/loadtest/adr0001 -p test_client.py",
            "faults":"target/adr0001/venv/bin/python projects/loadtest/adr0001/faults.py --root target/adr0001/adr0001-proof2-20261026 --baseline "+str(baseline/"target/routing-benchmark/pingora-bench-adrfresh-20261026"),
            "replicates":"target/adr0001/venv/bin/python projects/loadtest/adr0001/repeats.py"}}
    (evidence/"final-evidence.json").write_text(json.dumps(final,indent=2))
    print(json.dumps({"manifest":str(evidence/"final-evidence.json"),"containers_stopped":len(containers),
                      "baseline_preserved":len(preservation),"metrics":metrics}))


def shared_delta_closeout(cohort):
    """Verify new evidence without asserting historical mutable binaries survived."""
    from delta_report import analyze
    import ast
    repo = Path(__file__).resolve().parents[3]
    directory, runs = analyze(cohort)
    matched_pairs = sum(p["comparable"] for r in runs for p in r["phases"]) // 2
    scale_qualified = all(p["comparable"] and p["actual_gateways"] == 2
                          for r in runs for p in r["phases"] if p["phase"] == "gateways-two")
    measured = json.loads((directory / "source-binary-sha256.json").read_text())
    native = {p: sha for p, sha in measured.items() if p.startswith("crates/") or
              p.startswith("projects/arrowcol-router-rust/") or p in ("Cargo.toml", "Cargo.lock") or "/bin/" in p}
    assert all(digest(repo / p) == sha for p, sha in native.items()), "measured native revision changed"
    binary_names = ("adr-controller", "adr-flight", "adr-publisher", "arrowcol-router-rust",
                    "delta-contract", "adr-fingerprint", "publication-gate")
    inventory = []
    networks, volumes = set(), set()
    runtime_sources = None
    measurement_methods = None
    for run in runs:
        root = repo / run["evidence"]
        captured_sources = json.loads((root / "runtime-sources-at-start.json").read_text())
        assert all(digest(repo / p) == sha for p, sha in captured_sources.items())
        if runtime_sources is None:
            runtime_sources = captured_sources
        assert captured_sources == runtime_sources, "per-arm native source drift"
        tree = ast.parse((root / "measured-benchmark.py").read_text())
        methods = {node.name: ast.dump(node, include_attributes=False) for node in ast.walk(tree)
                   if isinstance(node, ast.FunctionDef) and node.name in
                   ("put", "measure", "change_topology", "setup_benchmark")}
        if measurement_methods is None:
            measurement_methods = methods
        assert methods == measurement_methods, "per-arm measurement-method drift"
        if scale_qualified:
            raw = json.loads((root / "benchmark-inputs.json").read_text())
            ports = [record["gateway_port"] for record in raw if record["phase"] == "gateways-two"]
            assert len(set(ports)) == 2 and all(ports.count(port) == 3 for port in set(ports))
            assert all(record["completion"] != "unresolved" for record in raw
                       if record["phase"] == "gateways-two"), "both gateways must complete real traffic"
            scoped = json.loads((root / "config/gateway2.json").read_text())
            assert not any(scoped[key] for key in
                           ("etcd", "etcd_user", "etcd_password", "operator_token", "collectors"))
        for name in binary_names:
            assert digest(root / "bin" / name) == digest(directory / "bin" / name)
        recorded = json.loads((root / "stopped-containers.json").read_text())
        ids = [c["Id"] for c in recorded]
        actual = json.loads(subprocess.check_output(["docker", "inspect", *ids], text=True))
        assert all(not c["State"]["Running"] for c in actual)
        assert all(c["Name"].lstrip("/").startswith(run["project"] + "-") for c in actual)
        networks.update(n for c in actual for n in c["NetworkSettings"]["Networks"])
        volumes.update(m["Name"] for c in actual for m in c["Mounts"] if m["Type"] == "volume")
        inventory += [{"id": c["Id"], "name": c["Name"].lstrip("/"), "state": c["State"]["Status"],
                       "exit_code": c["State"]["ExitCode"], "image": c["Image"],
                       "cpu_limit": c["HostConfig"]["NanoCpus"], "memory_limit": c["HostConfig"]["Memory"]}
                      for c in actual]
    preliminary = []
    retained_path = directory / "retained-attempts.json"
    retained_attempts = json.loads(retained_path.read_text()) if retained_path.exists() else []
    prior_cohorts = (("shared20261026a", "shared20261026b", "shared20261026c")
                    if cohort == "shared20261026d" else tuple(r["cohort"] for r in retained_attempts))
    for retained in retained_attempts:
        manifest = json.loads((repo / retained["manifest"]).read_text())
        assert all(digest(repo / p) == sha for p, sha in manifest.items()), "partial attempt changed"
    for prior in prior_cohorts:
        prior_directory = repo / "target/delta-benchmark" / prior
        if not prior_directory.exists():
            continue
        for attempt in json.loads((prior_directory / "attempts.json").read_text()):
            root = repo / attempt["evidence"]
            recorded = json.loads((root / "stopped-containers.json").read_text())
            actual = json.loads(subprocess.check_output(
                ["docker", "inspect", *[c["Id"] for c in recorded]], text=True))
            assert all(not c["State"]["Running"] and
                       c["Name"].lstrip("/").startswith(attempt["project"] + "-") for c in actual)
            networks.update(n for c in actual for n in c["NetworkSettings"]["Networks"])
            volumes.update(m["Name"] for c in actual for m in c["Mounts"] if m["Type"] == "volume")
            inventory += [{"id": c["Id"], "name": c["Name"].lstrip("/"),
                           "state": c["State"]["Status"], "exit_code": c["State"]["ExitCode"],
                           "image": c["Image"], "preliminary": prior} for c in actual]
            preliminary.append({**attempt, "cohort": prior, "containers_verified_stopped": len(actual)})
    assert len({c["id"] for c in inventory}) == len(inventory)
    projects = {r["project"] for r in [*runs, *preliminary]}
    listed = subprocess.check_output(["docker", "ps", "-a", "--format", "{{.Names}}"], text=True).splitlines()
    owned = {name for name in listed if any(name.startswith(p + "-") for p in projects)}
    assert owned == {c["name"] for c in inventory}, "unaccounted task-owned container"
    report = json.loads((directory / "report-validation.json").read_text())
    assert digest(repo / report["path"]) == report["sha256"]
    browser = json.loads((directory / "browser-validation.json").read_text())
    assert browser["sha256"] == report["sha256"] and browser["passed"]
    source_paths = list((repo / "projects/loadtest/adr0001").glob("*.py"))
    source_paths += list((repo / "projects/loadtest/adr0001").glob("*.md"))
    final_sources = {str(p.relative_to(repo)): digest(p) for p in source_paths}
    (directory / "final-report-sources-sha256.json").write_text(json.dumps(final_sources, indent=2))
    artifacts = []
    for run in [*runs, *preliminary]:
        root = repo / run["evidence"]
        artifacts += [p for p in root.glob("*.json") if "password" not in p.name]
        artifacts += list(root.glob("measured-benchmark.py"))
        artifacts += list((root / "observations").glob("*.jsonl"))
        artifacts += list((root / "logs").glob("*.log"))
    artifacts += [p for p in directory.glob("*.json")
                  if p.name not in ("closeout.json", "evidence-artifacts-sha256.json")]
    artifacts += list(directory.glob("*.png"))
    artifacts += list(directory.glob("*.html"))
    artifacts += [repo / r["manifest"] for r in retained_attempts]
    artifacts += list(directory.glob("mcp-*"))
    artifacts += [p for p in (repo / "target/delta-benchmark/checks").glob("*.log")
                  if p.name != f"{cohort}-closeout.log"]
    artifacts += list((repo / "target/delta-benchmark/checks").glob("*.json"))
    artifact_manifest = {str(p.relative_to(repo)): digest(p) for p in sorted(set(artifacts))}
    (directory / "evidence-artifacts-sha256.json").write_text(json.dumps(artifact_manifest, indent=2))
    closeout = {
        "status": "Three paired finite runs per design and report verified; both gateway topologies exercised"
                  if scale_qualified else
                  "Three paired finite runs per design and report verified; Pingora gateway scale-out blocked",
        "cohort": cohort, "repetitions": 3, "matched_phase_pairs": matched_pairs,
        "same_host_gateway_topology_qualified": scale_qualified,
        "physical_scale_out_qualified": False,
        "blocker": None if scale_qualified else "Second Pingora router fails live-controller contention.",
        "evidence": str(directory.relative_to(repo)), "report": report,
        "native_source_and_binary_entries_verified": len(native),
        "all_crates_runtime_source_entries_verified_each_run": len(runtime_sources),
        "same_measurement_methods_all_arms": sorted(measurement_methods),
        "read_only_audit_fix": "First arm retained; sqlcmd output corrected after measurement."
                              if cohort == "shared20261026d" else
                              "Previously repaired audit code used unchanged in all six fresh arms.",
        "source_binary_manifest": str((directory / "source-binary-sha256.json").relative_to(repo)),
        "current_report_sources_manifest": str((directory / "final-report-sources-sha256.json").relative_to(repo)),
        "evidence_artifacts_manifest": str((directory / "evidence-artifacts-sha256.json").relative_to(repo)),
        "evidence_artifact_count": len(artifact_manifest),
        "browser": browser,
        "validation": {
            "focused_tests": "PYTHONPATH=projects/loadtest/adr0001 target/adr0001/venv/bin/python -m unittest "
                             "test_client test_delta_benchmark (10 passed)",
            "test_log": f"target/delta-benchmark/checks/{cohort}-focused-tests.log",
            "browser_command": "target/adr0001/venv/bin/python projects/loadtest/adr0001/delta_browser.py --cohort " + cohort,
            "browser_log": f"target/delta-benchmark/checks/{cohort}-browser.log",
            "source_verification": str((directory / "parent-verification.json").relative_to(repo)),
            "native_rebuild": False,
            "qualification_manifest_note": "Repaired runtime qualification is separately attributed to "
                                           "target/delta-runtime/repair-20261026/closeout.json; "
                                           "its frozen binaries precede all six fresh measurements.",
            "helper_cleanup": "Isolated browser and its loopback server closed; task containers stopped and state retained.",
        },
        "runs": [{"repetition": r["repetition"], "arm": r["arm"], "evidence": r["evidence"],
                  "rows": r["rows"], "matched_rows": r["matched_rows"],
                  "matched_offered_rows": r["matched_offered_rows"],
                  "nonfinal_inputs": r["nonfinal_inputs"], "missing_submitted_rows": r["missing_submitted_rows"],
                  "nonfinal_rpcs": r["nonfinal_rpcs"],
                  "warm": next(p for p in r["phases"] if p["phase"] == "warm"),
                  "identity_resolved_final": r["identity_resolved_final"],
                  "substrate_cpu_seconds": r["substrate_cpu_seconds"]} for r in runs],
        "stopped_containers": inventory,
        "preliminary_attempts": preliminary,
        "retained_partial_summaries": retained_attempts,
        "retained_networks": sorted(networks),
        "retained_sql_volumes": sorted(volumes),
        "retained": "All attempt files, frozen binaries, SQL volumes and networks; no commit/push/PR/removal",
        "historical_provenance_gap": "Old publication-gate executable bytes unavailable after runtime cargo build; "
                                     "original source/logs/closeout retained. Current frozen binary is not historical gate.",
        "limitations": ["Finite 56-input runs with 112 global preparations including no-op schema work",
                        "P95 exploratory only; P99 unavailable; per-run rather than pooled percentiles",
                        "Internal acceptance/materialization latency and SQL operation latency unavailable",
                        "Same-host cgroup roles, generator/reader/observer contention; no physical scale-out claim",
                        "SQL deployment-row serialization, db_owner credentials, local-filesystem-only publication proof",
                        "No HA, exactly-once delivery or autonomous rebalancing qualification"],
    }
    (directory / "closeout.json").write_text(json.dumps(closeout, indent=2))
    print(json.dumps({"closeout": str(directory / "closeout.json"), "stopped": len(inventory),
                      "report_sha256": report["sha256"]}))


if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shared-delta")
    args = parser.parse_args()
    shared_delta_closeout(args.shared_delta) if args.shared_delta else main()
