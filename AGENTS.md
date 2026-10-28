# jobq contributor guide

jobq is a general-purpose macOS and Linux job scheduler for cooperating AI agents.
Keep examples and fixtures self-contained. Do not add personal paths, internal
project names, real job records, exported environments, or private test output.
Use synthetic subprocesses for integration tests.

Build with `cargo build --release --locked`; run `cargo test --locked` and
`cargo clippy --locked --all-targets -- -D warnings`. Native daemon tests require
exclusive use of the account's singleton: run them on a disposable runner
or a separate user account. Never interrupt unrelated jobs to obtain the lock.
See the README for live-safe checks and maintenance procedures.

Preserve direct argument boundaries, stable submission tokens, durable acceptance,
process identity checks, cleanup accounting, and CLI/MCP compatibility. A wait
ending or an adapter disconnecting must never cancel its jobs.

Do not commit runtime state, credentials, build output, or validation receipts.
Keep changes focused and document any new public behavior.
