"""Strict, non-sensitive benchmark exchange validation; imports never authorize actions."""
import argparse
import json
import math
from pathlib import Path

from jsonschema import Draft202012Validator

CONTRACT = Path(__file__).parent / "contracts"
MAX_BYTES = 32 * 1024 * 1024


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def validate(value):
    if len(json.dumps(value, allow_nan=False).encode()) > MAX_BYTES:
        raise ValueError("document size limit")
    def bounded(item, depth=0):
        if depth > 16:
            raise ValueError("document nesting limit")
        if isinstance(item, float) and not math.isfinite(item):
            raise ValueError("nonfinite number")
        if isinstance(item, dict):
            for child in item.values():
                bounded(child, depth + 1)
        elif isinstance(item, list):
            for child in item:
                bounded(child, depth + 1)
    bounded(value)
    schema = json.loads((CONTRACT / "benchmark-v1.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(value)
    runs = {run["id"] for run in value["runs"]}
    records = {record["id"]: record for record in value["records"]}
    if len(runs) != len(value["runs"]) or len(records) != len(value["records"]) or runs & records.keys():
        raise ValueError("duplicate ID")
    parents = {}
    derived = set()
    for record in records.values():
        if record["run_id"] not in runs:
            raise ValueError("unknown run")
        for field, kinds in (("trace_id", ("request", "attempt")), ("table_id", ("table",))):
            if field in record:
                target = records.get(record[field])
                if target is None or target["run_id"] != record["run_id"] or target["kind"] not in kinds:
                    raise ValueError("invalid record reference")
    for link in value["links"]:
        a, b = records.get(link["from"]), records.get(link["to"])
        if a is None or b is None or a["run_id"] != b["run_id"]:
            raise ValueError("missing or cross-run link")
        if link["relation"] == "derived-from":
            if a["id"] == b["id"] or a["evidence"] != "derived":
                raise ValueError("invalid derivation")
            derived.add(a["id"])
        if link["relation"] == "parent":
            if a["kind"] != "span" or b["kind"] != "span" or b["id"] in parents:
                raise ValueError("invalid span parent")
            if a["clock_id"] != b["clock_id"] or a["process_id"] != b["process_id"]:
                raise ValueError("cross-clock parent")
            start, end = int(a["offset_ns"]), int(a["offset_ns"]) + int(a["duration_ns"])
            if not start <= int(b["offset_ns"]) <= int(b["offset_ns"]) + int(b["duration_ns"]) <= end:
                raise ValueError("parent does not enclose child")
            parents[b["id"]] = a["id"]
    visited = set()
    for key in parents:
        seen = set()
        while key in parents and key not in visited:
            if key in seen:
                raise ValueError("parent cycle")
            seen.add(key)
            key = parents[key]
        visited.update(seen)
    if any(r["evidence"] == "derived" and r["id"] not in derived for r in records.values()):
        raise ValueError("derived record lacks sources")
    coverage = value["coverage"]
    if coverage["complete"] and (coverage["dropped_events"] or coverage["incomplete_attempts"]):
        raise ValueError("false completeness")
    return value


def load(path):
    with Path(path).open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError("document size limit")
    return validate(json.loads(raw, object_pairs_hook=unique_object))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path", nargs="?", type=Path, default=CONTRACT / "benchmark-v1.fixture.json")
    load(parser.parse_args().path)
    print("valid benchmark exchange")
