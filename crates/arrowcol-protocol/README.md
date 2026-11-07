# arrowcol-protocol

Versioned, storage-independent contracts for Arrow routers and dataflow collectors.

| Surface | Contract |
| --- | --- |
| Routing metadata | Required bounded ASCII protocol version (`1`), pool (`dataflow`), catalog, schema, table, and claimed fingerprint |
| Identity | Validated portable `catalog.schema.table` components, up to 128 bytes each |
| Schema | Canonical SHA-256 fingerprints for the Arrow 58.3 compatibility line |
| Authentication | Validated principals, scopes, methods, and expiry |
| Coordination | Instance lifecycle, table leases, publication fencing, and bounded pressure measurements |
| Errors | Validated messages, stable categories/codes, explicit retryability; no receipt IDs or receipt status API |
| Capabilities | Delta publication, lease fencing, pressure reporting, and durable payload storage |
| Validation | `npx --no-install nx run arrowcol-protocol:verify` |

Metadata parsing rejects missing, malformed, oversized, control-bearing,
non-ASCII, retired `direct` pool, and unsupported-version values before routing.
Version/pool values are capped at 16 bytes and fingerprints at 71 bytes.

`ClaimedSchemaFingerprint` is a client claim, not schema proof or authorization.
Collectors compute the received schema fingerprint and compare it with
authoritative catalog state. Fingerprints include nested Arrow types, field
order/name/nullability and sorted metadata; transport-local IPC dictionary IDs
are excluded.

Acknowledgement and publication are distinct: durable payload storage does not
promise Delta publication. Retrying a delivery never promises deduplication or
exactly-once behavior. Unknown publication outcomes remain non-retryable until
storage is reconciled.

JSON uses integer protocol versions and snake-case enum values. Fixtures under
`tests/fixtures/` cover the retained contracts. Deserialization revalidates
bounds and cross-field invariants.
