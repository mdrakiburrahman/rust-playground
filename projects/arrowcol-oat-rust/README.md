# ArrowCol OAT server

Runs benchmark matrices through an unauthenticated HTTP control API. The launcher
uses the default loopback listener; it does not need a remote listener.

| Command | Expected result |
| --- | --- |
| `npx --no-install arrowcol-oat --mode local` | Runs the local matrix and serves its viewer. |
| `cargo run --locked -p arrowcol-oat-rust -- --listen 127.0.0.1:8088` | Starts a loopback-only control API. |
| `cargo run --locked -p arrowcol-oat-rust -- --validate-config CONFIG.json` | Validates without starting a listener or running a matrix. |
| `npx --no-install nx run arrowcol-oat-rust:verify` | Builds, formats, lints and tests the crate. |

Non-loopback listeners are refused unless `--allow-insecure-non-loopback` is
explicitly supplied. **That flag exposes an unauthenticated API capable of
starting privileged orchestration and outbound requests. Use it only on a trusted
development network behind separately enforced access controls.** It does not
enable authentication or TLS.

`qap_endpoint` must be a credential-free HTTP(S) URL with a host and no query or
fragment. This is URL-shape validation, not a destination allowlist: local Kusto
and private AKS QAP endpoints are intentionally supported. Keep run submission
restricted to trusted operators.
