"""Test-only driver for the real infra/azure/bootstrap.py parent around an offline initializer.

`child-env` prints the exact environment the parent hands the initializer child. `reconcile`
runs the parent's manifest/Delta-log validation against a local store and stops at the first
ARM call, before any Kusto command. No Azure endpoint, token or credential is used.
"""

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "infra" / "azure"))

import bootstrap as b  # noqa: E402


class StopBeforeKusto(Exception):
    pass


class OfflineAzure:
    def __init__(self, c, store):
        self.c = c
        self.store = Path(store)
        self.reads = 0
        self.kql_calls = 0

    def blob(self, method, container, name="", missing_ok=False, **kwargs):
        if method != "GET" or container != self.c.data or kwargs.get("query"):
            raise AssertionError("unexpected storage request")
        path = self.store / name
        if not path.is_file():
            if missing_ok:
                return None, None
            raise AssertionError("missing blob without missing_ok")
        self.reads += 1
        return path.read_text(), None

    def arm(self, *args, **kwargs):
        raise StopBeforeKusto()

    def kql(self, *args, **kwargs):
        self.kql_calls += 1
        raise AssertionError("Kusto attempted")


def main():
    mode, values = sys.argv[1], json.loads(Path(sys.argv[2]).read_text())
    c = b.Config(values)
    if mode == "child-env":
        print(json.dumps(b.initializer_env(values, c, Path(sys.argv[3]), sys.argv[4])))
        return
    if mode != "reconcile":
        raise SystemExit("unknown mode")
    azure = OfflineAzure(c, sys.argv[4])
    try:
        b.reconcile(azure, Path(sys.argv[3]))
        outcome = {"outcome": "completed"}
    except StopBeforeKusto:
        outcome = {"outcome": "stopped-before-kusto"}
    except b.NotReady as exc:
        outcome = {"outcome": "not-ready", "detail": str(exc)}
    outcome.update(validated=azure.reads, kql=azure.kql_calls)
    print(json.dumps(outcome))


if __name__ == "__main__":
    main()
