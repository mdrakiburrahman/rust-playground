"""Independent acceptance checks against the implemented exchange validator."""

import argparse
import copy
import hashlib
import importlib.util
import json
import tempfile
from pathlib import Path

from jsonschema.exceptions import ValidationError


def changed(document, mutation):
    candidate = copy.deepcopy(document)
    mutation(candidate)
    return candidate


def record(document, kind):
    return next(item for item in document["records"] if item["kind"] == kind)


def second_span(document, relation="parent"):
    original = record(document, "span")
    child = copy.deepcopy(original)
    child["id"] = "acceptance-child"
    document["records"].append(child)
    document["links"].append(
        {"from": original["id"], "to": child["id"], "relation": relation}
    )
    return child


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("validator", type=Path)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--corpus", type=Path)
    args = parser.parse_args()
    inputs = [
        args.validator,
        args.fixture,
        args.fixture.with_name("benchmark-v1.schema.json"),
    ]
    hashes = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in inputs}
    spec = importlib.util.spec_from_file_location("benchmark_exchange", args.validator)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fixture = json.loads(args.fixture.read_text(encoding="utf-8"))
    results = []
    corpus = []

    def accepts(name, candidate):
        corpus.append({"name": name, "accept": True, "document": candidate})
        try:
            module.validate(candidate)
            results.append({"case": name, "passed": True})
        except Exception as error:
            results.append(
                {"case": name, "passed": False, "error_type": type(error).__name__}
            )

    def rejects(name, mutation):
        candidate = changed(fixture, mutation)
        corpus.append({"name": name, "accept": False, "document": candidate})
        try:
            module.validate(candidate)
        except (ValueError, ValidationError) as error:
            results.append(
                {"case": name, "passed": True, "rejection_type": type(error).__name__}
            )
        except Exception as error:
            results.append(
                {"case": name, "passed": False, "error_type": type(error).__name__}
            )
        else:
            results.append({"case": name, "passed": False, "error": "accepted-invalid"})

    accepts("canonical-fixture", fixture)
    rejects("unsupported-version", lambda d: d.update(schema_version=999))
    rejects("sensitive-unknown-field", lambda d: d.update(authorization="test-only"))
    rejects("duplicate-record-id", lambda d: d["records"].append(copy.deepcopy(d["records"][0])))
    rejects("duplicate-run-id", lambda d: d["runs"].append(copy.deepcopy(d["runs"][0])))
    rejects("unknown-record-run", lambda d: d["records"][0].update(run_id="absent-run"))
    rejects("unknown-link-target", lambda d: d["links"][0].update(to="absent-record"))
    rejects("unknown-trace", lambda d: record(d, "span").update(trace_id="absent-trace"))
    rejects("negative-duration", lambda d: record(d, "span").update(duration_ns="-1"))
    rejects("false-completeness", lambda d: d["coverage"].update(complete=True, dropped_events=1))

    def cross_run_trace(d):
        run = copy.deepcopy(d["runs"][0])
        run["id"] = "acceptance-other-run"
        d["runs"].append(run)
        attempt = copy.deepcopy(record(d, "attempt"))
        attempt.update(id="acceptance-other-attempt", run_id=run["id"])
        d["records"].append(attempt)
        record(d, "span")["trace_id"] = attempt["id"]

    rejects("cross-run-trace-root", cross_run_trace)
    rejects("non-root-trace-target", lambda d: record(d, "span").update(trace_id=record(d, "batch")["id"]))

    def cross_clock_parent(d):
        second_span(d)["clock_id"] = "other-clock"

    def outliving_child(d):
        child = second_span(d)
        child["duration_ns"] = str(int(child["duration_ns"]) + 1)

    def parent_cycle(d):
        child = second_span(d)
        original = record(d, "span")
        d["links"].append({"from": child["id"], "to": original["id"], "relation": "parent"})

    rejects("cross-clock-parent", cross_clock_parent)
    rejects("child-outlives-parent", outliving_child)
    rejects("cyclic-parentage", parent_cycle)

    def derived_without_source(d):
        item = record(d, "span")
        item.update(evidence="derived", method="union-of-intervals")
        item.pop("assumption", None)

    rejects("derived-without-source", derived_without_source)

    def asynchronous_completion(d):
        child = second_span(d, "async")
        child.update(clock_id="other-clock", process_id="other-process")
        child["offset_ns"] = str(int(child["offset_ns"]) + 100)
        child["duration_ns"] = str(int(child["duration_ns"]) + 100)

    accepts("asynchronous-outliving-work", changed(fixture, asynchronous_completion))

    def reject_file(name, write_input, expected_error):
        with tempfile.NamedTemporaryFile(prefix="arrowcol-acceptance-", suffix=".json") as handle:
            write_input(handle)
            handle.flush()
            try:
                module.load(handle.name)
            except ValueError as error:
                results.append(
                    {
                        "case": name,
                        "passed": str(error) == expected_error,
                        "rejection_type": type(error).__name__,
                    }
                )
            except Exception as error:
                results.append(
                    {"case": name, "passed": False, "error_type": type(error).__name__}
                )
            else:
                results.append({"case": name, "passed": False, "error": "accepted-invalid"})

    reject_file(
        "duplicate-json-key",
        lambda handle: handle.write(b'{"format":"arrowcol-benchmark","format":"arrowcol-benchmark"}'),
        "duplicate JSON key",
    )

    def oversized_file(handle):
        handle.seek(module.MAX_BYTES)
        handle.write(b"0")

    reject_file("oversized-file", oversized_file, "document size limit")
    unchanged = all(
        hashlib.sha256(path.read_bytes()).hexdigest() == hashes[path.name]
        for path in inputs
    )
    result = {
        "passed": unchanged and all(item["passed"] for item in results),
        "inputs_unchanged": unchanged,
        "sha256": hashes,
        "cases": results,
    }
    text = json.dumps(result, indent=2)
    if args.output:
        args.output.write_text(text + "\n", encoding="utf-8")
    if args.corpus:
        args.corpus.write_text(
            json.dumps(
                {
                    "format": "arrowcol-contract-acceptance",
                    "schema_sha256": hashes["benchmark-v1.schema.json"],
                    "fixture_sha256": hashes[args.fixture.name],
                    "cases": corpus,
                },
                indent=2,
            ) + "\n",
            encoding="utf-8",
        )
    print(text)
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
