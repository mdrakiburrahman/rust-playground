"""Bounded read-only SQL acceptance, final receipt and physical Delta projection."""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from urllib.parse import unquote, urlsplit

from exchange import unique_object


MAX_DATA = 8 * 1024 * 1024
MAX_VERIFY_BYTES = 32 * 1024 * 1024
MAX_ACTIONS = 256 * 1024
IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,100}")
DIGEST = re.compile(r"[a-f0-9]{64}")
QUERY = (
    "SET NOCOUNT ON; SET LOCK_TIMEOUT 1000; SET XACT_ABORT ON; "
    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION; "
    "DECLARE @state nvarchar(max); SELECT @state=payload FROM delta_authority.state WHERE id=1; "
    "IF @state IS NULL OR DATALENGTH(@state)>1572864 THROW 51000,'bounded state unavailable',1; "
    "DECLARE @receipts nvarchar(max); "
    "IF EXISTS(SELECT 1 FROM delta_authority.receipts) "
    "SELECT @receipts=(SELECT TOP(385) table_id,operation_id,version,digest "
    "FROM delta_authority.receipts ORDER BY table_id,version FOR JSON PATH); "
    "ELSE SET @receipts=N'[]'; "
    "IF @receipts IS NULL OR ISJSON(@receipts)<>1 THROW 51000,'receipt projection unavailable',1; "
    "SELECT N'{\"state\":'+@state+N',\"receipts\":'+@receipts+N'}'; COMMIT TRANSACTION;"
)
READER = (
    "import json,sys;from deltalake import DeltaTable;"
    "d=DeltaTable(sys.argv[1],version=int(sys.argv[2]));"
    "print(json.dumps({'version':d.version(),'files':d.file_uris()}))"
)


def bounded_file(path, limit):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("receipt evidence is not a regular file")
        raw = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
    if len(raw) > limit or signature(before) != signature(after):
        raise ValueError("receipt evidence changed or exceeded its bound")
    return raw, signature(after)


def signature(value):
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def sql_projection(runtime):
    password, _ = bounded_file(runtime.root / "config/runtime-sql-password", 128)
    if password.count(b"\n") != 1 or not password.endswith(b"\n"):
        raise ValueError("invalid private SQL credential file")
    result = subprocess.run(
        ["docker", "exec", "-i", runtime.owned_container(runtime.sql_name), "bash", "-c",
         "IFS= read -r SQLCMDPASSWORD; export SQLCMDPASSWORD; "
         "exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U delta_runtime -d delta_runtime "
         "-C -b -l 1 -t 1 -w 65535 -y 0 -f 65001 -Q '" + QUERY.replace("'", "'\\''") + "'"],
        input=password, capture_output=True, timeout=3,
    )
    if result.returncode != 0 or len(result.stdout) > 2 * 1024 * 1024:
        raise RuntimeError("bounded read-only SQL receipt observation unavailable")
    # sqlcmd adds physical newlines between FOR JSON chunks, not JSON string escapes.
    raw = result.stdout.replace(b"\r", b"").replace(b"\n", b"").strip()
    return json.loads(raw, object_pairs_hook=unique_object)


def ordinary_reader(root, version):
    result = subprocess.run(
        [sys.executable, "-B", "-c", READER, str(root), str(version)],
        capture_output=True, timeout=1.5,
        env={**os.environ, "TOKIO_WORKER_THREADS": "1", "RAYON_NUM_THREADS": "1"},
    )
    if result.returncode != 0 or len(result.stdout) > 65536:
        raise RuntimeError("bounded ordinary Delta reader unavailable")
    value = json.loads(result.stdout, object_pairs_hook=unique_object)
    if value["version"] != version or len(value["files"]) > 128:
        raise ValueError("ordinary Delta snapshot identity mismatch")
    files = set()
    for uri in value["files"]:
        parsed = urlsplit(uri)
        if parsed.scheme not in ("", "file") or parsed.netloc not in ("", "localhost") or parsed.query or parsed.fragment:
            raise ValueError("unexpected ordinary Delta file URI")
        path = Path(unquote(parsed.path))
        if path.parent != root or path.name in files:
            raise ValueError("ordinary Delta file outside owned table")
        files.add(path.name)
    return files


def proposal_digest(proposal):
    fields = ("table", "operation", "identity", "predecessor", "version", "actions", "data")
    identity = ("controller", "controller_generation", "membership", "process", "local_generation", "epoch")
    if set(proposal) != set(fields) or set(proposal["identity"]) != set(identity):
        raise ValueError("unexpected accepted proposal shape")
    value = {name: proposal[name] for name in fields}
    value["identity"] = {name: proposal["identity"][name] for name in identity}
    value["data"] = [{name: reference[name] for name in ("name", "sha256", "bytes")}
                     for reference in proposal["data"]]
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


class ReceiptObserver:
    def __init__(self, runtime, sql=None, reader=None):
        self.runtime = runtime
        self.sql = sql or (lambda: sql_projection(runtime))
        self.reader = reader or ordinary_reader
        self.files = {}
        self.readers = {}
        self.remaining = 0

    def verify_file(self, path, limit, expected=None, digest=None):
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > limit:
            raise ValueError("invalid physical receipt evidence")
        observed = signature(metadata)
        key = str(path)
        cached = self.files.get(key)
        if cached is not None and cached == (observed, expected, digest):
            return observed
        if metadata.st_size > self.remaining:
            raise RuntimeError("receipt verification byte budget exhausted")
        raw, observed = bounded_file(path, limit)
        self.remaining -= len(raw)
        if expected is not None and raw != expected or digest is not None and hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError("physical receipt digest mismatch")
        self.files[key] = (observed, expected, digest)
        return observed

    def sample(self):
        observation = self.sql()
        if (not isinstance(observation, dict) or set(observation) != {"state", "receipts"}
                or not isinstance(observation["state"], dict)
                or not isinstance(observation["state"].get("tables"), dict)
                or not isinstance(observation["receipts"], list)):
            raise ValueError("SQL receipt projection requires explicit state, tables and receipts")
        tables = observation["state"]["tables"]
        allowed = {table["key"] for table in self.runtime.config["tables"]}
        if (not set(tables) <= allowed or len(tables) > 3 or len(observation["receipts"]) > 384
                or any(not IDENTIFIER.fullmatch(key) for key in allowed)):
            raise ValueError("receipt observation exceeded dedicated catalog bounds")
        final = {}
        for receipt in observation["receipts"]:
            key = receipt["table_id"], receipt["operation_id"]
            if (key in final or key[0] not in allowed or not IDENTIFIER.fullmatch(key[1])
                    or type(receipt["version"]) is not int or receipt["version"] < 0
                    or not DIGEST.fullmatch(receipt["digest"])):
                raise ValueError("invalid SQL receipt identity")
            final[key] = receipt
        self.remaining = MAX_VERIFY_BYTES
        output, missing, dropped, accepted = [], set(), 0, set()
        file_keys, reader_keys = set(), set()
        for table, state in sorted(tables.items()):
            history = state["decisions"]
            if len(history) > 128:
                raise ValueError("accepted receipt prefix exceeds bound")
            root = self.runtime.root / "delta-private/tables" / table
            if root.is_symlink() or (root / "_delta_log").is_symlink():
                raise ValueError("physical receipt table is a symlink")
            prefix, names, complete = [], set(), True
            table_output = []
            last_final = -1
            for version, proposal in enumerate(history):
                if (proposal["table"] != table or type(proposal["version"]) is not int
                        or proposal["version"] != version or type(proposal["predecessor"]) is not int
                        or proposal["predecessor"] != version - 1
                        or not IDENTIFIER.fullmatch(proposal["operation"]) or len(proposal["data"]) > 1
                        or not isinstance(proposal["actions"], str) or not proposal["actions"].endswith("\n")
                        or len(proposal["actions"].encode()) > MAX_ACTIONS):
                    raise ValueError("invalid accepted receipt prefix")
                digest = proposal_digest(proposal)
                key = table, proposal["operation"]
                if key in accepted:
                    raise ValueError("duplicate accepted operation")
                accepted.add(key)
                receipt = final.get(key)
                record = {
                    "id": "receipt-" + hashlib.sha256((table + "\0" + proposal["operation"]).encode()).hexdigest(),
                    "table_id": table, "operation_id": proposal["operation"], "version": str(version),
                    "completion": "accepted", "digest": digest,
                }
                if receipt is not None:
                    if receipt["version"] != version or receipt["digest"] != digest:
                        raise ValueError("SQL receipt does not match exact accepted decision")
                    last_final = version
                    try:
                        log = root / "_delta_log" / f"{version:020}.json"
                        file_keys.add(str(log))
                        prefix.append(self.verify_file(log, MAX_ACTIONS, expected=proposal["actions"].encode()))
                        for reference in proposal["data"]:
                            name = reference["name"]
                            if (not re.fullmatch(r"[A-Za-z0-9_.-]+[.]parquet", name)
                                    or type(reference["bytes"]) is not int
                                    or not 1 <= reference["bytes"] <= MAX_DATA
                                    or not DIGEST.fullmatch(reference["sha256"]) or name in names):
                                raise ValueError("invalid accepted data reference")
                            path = root / name
                            file_keys.add(str(path))
                            prefix.append(self.verify_file(path, MAX_DATA, digest=reference["sha256"]))
                            if path.stat().st_size != reference["bytes"]:
                                raise ValueError("accepted physical data length mismatch")
                            names.add(name)
                        if not complete:
                            raise ValueError("earlier accepted prefix is not physically complete")
                        record["completion"] = "completed"
                    except (OSError, ValueError, RuntimeError):
                        complete = False
                        record["completion"] = "unknown"
                        missing.add("receipt-physical-unverified")
                        dropped += 1
                else:
                    complete = False
                table_output.append(record)
            final_prefix_complete = all(record["completion"] == "completed"
                                        for record in table_output[:last_final + 1])
            if last_final >= 0 and final_prefix_complete:
                cache_key = table, last_final
                reader_keys.add(cache_key)
                try:
                    verified = tuple(prefix), frozenset(names)
                    if self.readers.get(cache_key) != verified:
                        if self.reader(root, last_final) != names:
                            raise ValueError("ordinary Delta reader file set mismatch")
                        self.readers[cache_key] = verified
                except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.TimeoutExpired):
                    missing.add("receipt-reader-unavailable")
                    dropped += 1
                    for record in table_output:
                        if record["completion"] == "completed":
                            record["completion"] = "unknown"
            elif any(record["completion"] == "completed" for record in table_output):
                for record in table_output:
                    if record["completion"] == "completed":
                        record["completion"] = "unknown"
                missing.add("receipt-prefix-unverified")
            output.append(table_output)
        if set(final) - accepted:
            raise ValueError("SQL final receipt has no accepted decision")
        self.files = {key: value for key, value in self.files.items() if key in file_keys}
        self.readers = {key: value for key, value in self.readers.items() if key in reader_keys}
        selected = []
        for offset in range(128):
            for records in output:
                if offset < len(records) and len(selected) < 128:
                    selected.append(records[-offset - 1])
        omitted = sum(len(records) for records in output) - len(selected)
        if omitted:
            missing.add("receipt-history-truncated")
            dropped += omitted
        return sorted(selected, key=lambda record: (record["table_id"], int(record["version"]))), sorted(missing), dropped
