"""Explicit catalog-bound identity/schema fixtures, kept out of matched benchmark inputs."""
import copy
import json
from pathlib import Path
import subprocess

import pyarrow as pa
import benchmark as b


def extra(baseline):
    root = b.REPO / "target/adr0001/fixtures"
    root.mkdir(parents=True, exist_ok=True)
    def fingerprint(name, schema):
        path = root / (name + ".arrow")
        with pa.OSFile(str(path), "wb") as output, pa.ipc.new_stream(output, schema):
            pass
        return subprocess.check_output([str(b.REPO / "target/debug/adr-fingerprint"), str(path)], text=True).strip()
    def manifest(name, key, fp, version=1):
        return {"table": {"catalog": "telemetry", "schema": "benchmark", "table": name,
                          "table_id": key, "fingerprint": fp},
                "flight": {"descriptor_path": ["telemetry", "benchmark", name, f"v{version}"]}}
    dictionary_schema = pa.schema([pa.field("sql_server_instance_name", pa.string(), nullable=False),
                                  pa.field("tag", pa.dictionary(pa.int8(), pa.string()), nullable=False)])
    first = pa.schema([pa.field("sql_server_instance_name", pa.string(), nullable=False)])
    evolved = first.append(pa.field("added", pa.int64(), nullable=True))
    fpd = fingerprint("dictionary", dictionary_schema)
    fp1 = fingerprint("evolution-v1", first)
    fp2 = fingerprint("evolution-v2", evolved)
    same, same_schema = b.sdks(baseline)[0]
    same = copy.deepcopy(same)
    same["table"]["table_id"] = "same-schema-different-table"
    same["table"]["table"] = "same_schema_other_table"
    same["flight"]["descriptor_path"][2] = "same_schema_other_table"
    values = [(same, same_schema),
              (manifest("dictionary", "dictionary-table", fpd), dictionary_schema),
              (manifest("evolution", "evolution-table", fp1), first),
              (manifest("evolution", "evolution-table", fp2, 2), evolved)]
    entries = [{"key": b.table_key(m), "path": m["flight"]["descriptor_path"][:3],
                "fingerprints": [m["table"]["fingerprint"]]} for m, _ in values[:2]]
    entries.append({"key": "evolution-table", "path": ["telemetry", "benchmark", "evolution"], "fingerprints": [fp1, fp2]})
    return entries, values


def prove(root, baseline):
    root = Path(root)
    config = json.loads((root / "secrets/controller.json").read_text())
    entries, values = extra(baseline)
    outcomes = []
    endpoint = b.adr.endpoint(root)
    settings = {"run_id": "schema-proof", "rows": 8, "batches_per_stream": 1, "max_attempts": 3}
    # Shared fingerprints must create different immutable owners; evolution must keep one record/epoch.
    for ordinal, sdk in enumerate(values):
        result = b.retained_upload(endpoint, config["edge_secret"].encode(), sdk, settings, ordinal,
                                  lose_first_reply=ordinal == 1)
        assert result["terminal"] == "ok", result
        outcomes.append(result)
    view = b.adr.control(root, {"op": "view"})
    records = {a["record"]["key"]: a["record"] for a in view["assignments"]}
    assert "same-schema-different-table" in records and b.table_key(b.sdks(baseline)[0][0]) in records
    assert records["evolution-table"]["epoch"] == 1
    assert records["evolution-table"]["head"]["sequence"] == 2
    wrong = copy.deepcopy(values[0][0])
    wrong["flight"]["descriptor_path"][2] = "unknown_table"
    rejected = b.retained_upload(endpoint, config["edge_secret"].encode(), (wrong, values[0][1]), settings, 99)
    assert rejected["terminal"] == "unresolved" and len(rejected["attempts"]) == 1
    wrong_schema = b.retained_upload(endpoint, config["edge_secret"].encode(), (values[2][0], values[3][1]), settings, 100)
    assert wrong_schema["terminal"] == "unresolved" and len(wrong_schema["attempts"]) == 1
    actual, manifests = b.visible(root)
    expected = {marker for outcome in outcomes for marker in outcome["markers"]}
    observed = {marker for markers in actual.values() for marker in markers}
    assert expected <= observed
    evidence = {"outcomes": outcomes, "unauthorized_table": rejected, "schema_mismatch": wrong_schema,
                "records": records, "acknowledged_markers_reconciled": len(expected),
                "dictionary_replay_duplicates": len(actual["dictionary-table"]) - 8}
    path = b.REPO / "target/adr0001/evidence/schema-proof.json"
    path.write_text(json.dumps(evidence, indent=2))
    return evidence
