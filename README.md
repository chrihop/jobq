# jobq

Durable, resource-aware FIFO job queues for cooperating AI agents on **macOS**.
Let multiple agents share one computer without launching every heavy command
at once. Jobs wait in durable FIFO queues until runner slots and declared memory
reservations are available. Agents can submit work, inspect results, stream logs,
wait for one or many jobs, and cancel their work through the CLI or MCP.

One Rust executable provides the daemon, CLI client, and MCP stdio adapter.
Version 0.1 targets macOS and cooperating agents under one user account.
Linux and Windows are not supported. See [LICENSE](LICENSE) for the MIT license.

## Build and start

Requires macOS, a stable Rust toolchain, and Xcode Command Line Tools. The
integration scripts additionally need Python 3.11 or later. GitHub Actions runs
the unit, lint, release-build and native integration checks on a clean macOS
runner; local checks must respect any existing daemon and jobs.

```sh
git clone https://github.com/chrihop/jobq.git
cd jobq
cargo build --release --locked
./target/release/jobq server --config config.example.toml
```

Run the server in its own terminal. Omitting `--config` uses built-in defaults;
an explicitly supplied configuration must exist. The default state directory is
`~/Library/Application Support/jobq`; `server --state-dir PATH` changes it.
SIGINT/SIGTERM stop admission, interrupt active jobs, clean up their tracked trees, and
leave waiting jobs durable for the next start. No background service is installed.

All submitting agents must run under the daemon's macOS account. The fixed lock
`/tmp/jobq-<uid>/daemon.lock` prevents a second daemon even with different socket,
state, or config paths. Never delete a live lock file. The default socket is
`/tmp/jobq-<uid>/socket`; global `--socket PATH` selects another endpoint without
bypassing the singleton. Its parent must already be an owner-only directory.

```sh
# Optional installation; not needed to use target/release/jobq.
cargo install --path . --locked

jobq submit --timeout 3s --max-memory 2GiB -- cargo test
jobq submit --shell 'make build && ./benchmark'
jobq submit --emergency --timeout 20s -- ./urgent-task
jobq submit --wait -- /usr/bin/true
jobq status JOB_ID
jobq jobs                 # unfinished jobs
jobq jobs --all           # includes retained results
jobq queues
jobq wait JOB_ID           # blocks until execution and cleanup finish
jobq logs JOB_ID --follow # stdout/stderr remain separate
jobq cancel JOB_ID
jobq reload               # reread the startup TOML file
jobq drain --wait         # close admission, hold queued jobs, finish active work
jobq drain --all --wait   # close admission, finish all already-accepted work
jobq maintenance          # mode, active/queued counts, ready_to_restart
jobq resume              # reopen admission and dispatch
```

Use `--json` with CLI client commands for machine-readable output. `submit`
returns an accepted job immediately, including its ID and estimates. `wait`
and `submit --wait` return the final status and use these exit codes:

| Result | Exit code |
| --- | --- |
| Success | 0 |
| Command failure | command's nonzero exit code, or 1 for a signal |
| Timeout | 124 |
| Memory exceeded | 125 |
| Monitoring failure | 126 |
| Spawn failure | 127 |
| Cancelled | 130 |
| Interrupted | 131 |
| Client/API error | 2 |

JSON preserves the distinct result even if the command itself exits with one of
these numbers. Stopping a waiting client leaves its job running; withdraw it with
`cancel`. Cancelling waiting work removes it immediately. Running cancellation
shows `cancelling` until process cleanup completes. Repeated cancellation is safe;
finished results are unchanged.

## Waiting for a set of jobs

```sh
jobq --json wait --any ID1 ID2 ID3
jobq --json wait --all ID1 ID2 ID3
jobq --json wait --all --wait-ms 10000 ID1 ID2 ID3
```

```json
{"name":"jobq_wait_many","arguments":{"ids":["ID1","ID2"],"mode":"any","block":true}}
{"name":"jobq_wait_many","arguments":{"ids":["ID1","ID2"],"mode":"all","wait_ms":10000}}
```

`any` returns once at least one selected job is terminal; `all` waits for every
selected job, including process cleanup. Already-terminal jobs count immediately.
Failures, cancellations and timeouts count as completion; inspect individual
outcomes. Neither mode cancels pending jobs. Continue successive `any` waits with
`pending_ids`; stop when that list is empty. Reusing completed IDs returns those
same jobs immediately, rather than waiting for another completion.

CLI set waits block by default; `--wait-ms` bounds the total wait to 1–30000ms.
MCP defaults to a 10000ms bounded wait; `block: true` removes jobq's wait deadline
and cannot be combined with `wait_ms`. Client tool deadlines still apply.
On expiry, `condition_met` is false and `wait_expired` is true. Set-wait CLI exit
codes are 0 when the condition is met and all observed completed jobs succeeded,
1 when it is met with any observed completed failure, and 124 on wait expiry.
The existing `jobq wait ID` output and job-specific exit codes are unchanged.
Multiple CLI IDs require `--any`, `--all`, or `--wait-ms` (which implies `all`).

Empty, duplicate, blank or overlong IDs and sets over 100 are rejected. Unknown
or unavailable retained IDs fail the whole call, even when another ID is already
terminal; jobs are never modified. Status reads run concurrently with one overall
wait deadline. If the initial complete observation cannot be obtained before
expiry, the call returns `daemon_timeout`; later expiry returns the latest full
set of observations. Other daemon/transport errors return promptly.

Each job carries its own `snapshot_at_ms`; `observed_at_ms` is the time the set
result was assembled, not an atomic daemon snapshot. Terminal observations are
cached for the lifetime of the wait, so subsequent record pruning cannot remove
an already-observed result. Cancelling/disconnecting a wait only drops its status
requests. Other MCP calls remain available while waiting. Paused queued jobs may
wait indefinitely until an operator resumes dispatch.

These are client-side operations over the existing status protocol, so upgrading
CLI/MCP requires no daemon restart. Reconnect MCP adapters to discover the eighth
tool. Test without stopping production:

```sh
python3 tests/wait_many.py /absolute/jobq
# Also submit small test-owned jobs to the existing live daemon:
python3 tests/wait_many.py /absolute/jobq --live --long
```

## Maintenance and launchd scheduling

`drain` commits maintenance state before acknowledging. New submissions receive
the retryable `maintenance` error, while existing submission tokens still return
their original jobs. Status, waits, logs, and cancellation remain available.
The default drain holds queued jobs in their existing FIFO order while active
jobs finish normally. `drain --all` also runs the queued work to completion.
`--wait` blocks until supervisor cleanup is done and `ready_to_restart` is true.
Interrupting the wait does not cancel jobs or resume the service.

Maintenance is durable in SQLite and survives restart and TOML reload. Start
with `server --maintenance` to persist paused mode before the first dispatch,
then use `jobq resume` explicitly after verification. Queued-job estimates are
unavailable while dispatch is paused. Administrative drain/resume operations
remain in the CLI; MCP queue/job/status responses include maintenance state.

For launchd, configure the service with:

```xml
<key>ProcessType</key>
<string>Interactive</string>
```

The default/Standard service policy can impose a Utility QoS ceiling inherited
by every worker. `Interactive` removes that service ceiling; it does not pin
cores or give every worker User-Interactive QoS. Ordinary workers inherit the
service's default QoS. macOS chooses core placement. Keep existing singleton,
permissions, crash-restart, log, and process-cleanup settings.

`jobq --json queues` reports CPU performance levels and requested daemon QoS in
`scheduling`. Requested QoS alone is not proof of effective worker priority.
For effective CPU-time accounting and sampled execution locations on Apple
silicon, build and queue the diagnostic:

```sh
clang -O2 -Wall -Wextra -pthread scripts/scheduling_probe.c -o /absolute/probe
jobq submit --wait --timeout 20s --max-memory 64MiB -- /absolute/probe 3 6
```

The probe samples CPU IDs in its own threads using XNU's arm64 userspace CPU
register encoding. Map IDs to `logical-cpu-id`/`cluster-type` in IODeviceTree
and validate against `sysctl hw.perflevel0 hw.perflevel1`; do not assume numbering
across Mac models. Samples prove execution at those points, not time-weighted
residency percentages. This diagnostic uses a private OS encoding and should be
revalidated after OS changes; jobq scheduling itself does not depend on it.

For a safe upgrade, serialize administrative operations, close admission with
`drain --all --wait`, verify readiness, back up SQLite using its backup API
(including WAL contents), and only then unload/reload launchd. SIGTERM and
`launchctl kickstart -k` still interrupt active executions; never use them as a
drain mechanism. Preserve the live database on rollback rather than restoring
an older backup over accepted jobs. The additive maintenance table is compatible
with the previous database layout, but older binaries ignore maintenance: close
admission externally before reverting to one.

The one-time `scripts/legacy_maintenance_gate.py` bridges daemons predating drain:
it gates their public socket, preserves committed retries, and lets all accepted
work finish. Its private upstream is an operator endpoint, not a security
boundary. It must stay alive until handover; follow its `--help` recovery
instructions. Native `tests/maintenance.py` requires exclusive daemon use;
never stop a busy production daemon to run it.

### Blocking completion for agents

The CLI already waits without an overall client deadline:

```sh
jobq --json wait JOB_ID
jobq --json submit --wait --timeout 300s --max-memory 2GiB -- make check
```

For MCP, submit first to retain the durable ID, then make **one** tool call:

```json
{"name":"jobq_wait","arguments":{"id":"JOB_ID","block":true}}
```

Omit `wait_ms` with `block: true`. The call stays pending while the job is queued,
running, or cancelling, and returns the final snapshot with `wait_expired: false`.
Job failure is still a successful wait response; inspect `state` and `outcome`.
Other MCP calls, including cancellation, remain available on the same adapter.
Disconnecting or cancelling the wait does not terminate the job. A daemon error
ends the wait with an error; reconnect and wait on the same ID without resubmitting.

Blocking mode removes jobq's overall wait deadline, **not the MCP client's tool
timeout**. Set the client's timeout to cover queue waiting plus execution and
cleanup. For example, in Codex's existing `[mcp_servers.jobq]` section,
`tool_timeout_sec = 86400` allows up to 24 hours per call. Configure the equivalent
timeout in other clients; some hosts impose their own maximum. After updating the
binary, reconnect the MCP adapter to refresh its tool schema. The daemon does not
need restarting for this change.

The CLI and MCP adapter check the daemon internally every 250ms; the agent does
not need to poll. Each daemon RPC retains its I/O deadline, so an unavailable
daemon does not leave the wait hanging forever. Existing bounded MCP waits
remain unchanged: omit `block` and use `wait_ms` (default 10000, maximum 30000).

Reproduce the live CLI/MCP wait checks without restarting the daemon:
`python3 tests/wait_live.py /absolute/path/to/jobq /absolute/new-evidence-dir`.
The suite uses small owned jobs, includes a 35-second wait, and never cancels
other agents' jobs.

## MCP access for agents

Start the daemon as above, then configure your agent application's MCP client to
launch `jobq mcp`. Every adapter connects to the same daemon through the private
Unix socket. The adapter uses the official [Rust MCP SDK](https://github.com/modelcontextprotocol/rust-sdk)
with stdio transport. An async runtime is also used by CLI set waits; ordinary
server scheduling runs in its own small event loop.
It exposes eight tools and can initialize/list them while the daemon is offline.
Tool calls explain how to start or reconnect to an unavailable daemon.

The submit schema uses a plain object without root `oneOf`/`anyOf`/`allOf` for
Claude Code and Codex compatibility. Exactly one nonempty `argv` or `shell` is
enforced by the adapter at runtime. After upgrading the binary, reconnect the
client's MCP adapter to refresh its cached tool schema. This schema-only update
does not require restarting a compatible running daemon or interrupting jobs.
To check submission compatibility against an existing daemon without restarting
it, run `python3 tests/mcp_submit_live.py /absolute/path/to/jobq /absolute/new-evidence-dir`.
This checks the schema, invalid inputs, argv/shell execution, explicit context and
limits, and idempotent retries using short printf jobs. Actual client acceptance
also requires a fresh authenticated Claude Code or Codex session.

Client configuration pattern (adapt the enclosing syntax to your application):

```json
{
  "mcpServers": {
    "jobq": {
      "command": "/absolute/path/to/jobq",
      "args": ["mcp"]
    }
  }
}
```

For a custom socket use `"args": ["--socket", "/absolute/private/socket", "mcp"]`.
The client owns the adapter process and stdin/stdout pipes. Stdout carries only
MCP messages; diagnostics go to stderr. `--json` is rejected with `mcp`. Use a
compatible daemon and adapter. If upgrading a daemon is necessary, drain it
first as described above; reconnecting an adapter alone does not stop jobs.

| Tool | Arguments and result |
| --- | --- |
| `jobq_submit` | Required `cwd`, `submission_token`, exactly one of `argv` or `shell`. Optional `shell_executable`, `timeout`, `max_memory`, `emergency`, `env`. Returns the durable job snapshot immediately. |
| `jobq_status` | `id`. Full status, limits, resource samples, outcome, log availability, both estimates, and `snapshot_at_ms`. |
| `jobq_jobs` | Optional `all` (false), `limit` (25, maximum 100), `cursor`. Compact summaries in submission order, `snapshot_at_ms`, and `next_cursor` (null at the end). |
| `jobq_queues` | No arguments. Queues, runner usage, memory reservations, and estimates. |
| `jobq_logs` | `id`; optional `stream` (`stdout` default or `stderr`), `offset` (0), `max_bytes` (4096, maximum 16384). Text, byte offsets, availability, and truncation flags. |
| `jobq_cancel` | `id`. Returns the cancellation snapshot; `cancelling` means cleanup is still underway. Repeated calls preserve completed results. |
| `jobq_wait` | `id`; `block: true` waits until terminal in one call (omit `wait_ms`). Otherwise optional `wait_ms` (10000, range 1..30000) returns on completion or with `wait_expired: true`. Client tool timeouts still apply. |
| `jobq_wait_many` | `ids` (1–100 unique IDs), `mode: "any"` or `"all"`; the same `block`/`wait_ms` choices. Returns ordered job snapshots, `completed_ids`, `pending_ids`, `condition_met`, and `wait_expired`. |

All tool results contain `structuredContent` plus the same JSON in a text block.
Tool failures use `isError: true` and `error.code` / `error.message`; daemon error
codes are preserved. Invalid MCP arguments can produce JSON-RPC validation errors.
A failed **job** is a normal status/wait result: inspect `state` and `outcome`.
Normal daemon RPCs have a ten-second deadline. A bounded wait's deadline covers
its entire operation, including socket I/O. If it expires after receiving a snapshot, inspect
`snapshot_at_ms` for that snapshot's freshness; if no snapshot was received, the
tool reports `daemon_timeout`. Concurrent status/cancel calls remain available.

Example `tools/call` parameters for a submission:

```json
{
  "name": "jobq_submit",
  "arguments": {
    "cwd": "/absolute/path/to/project",
    "submission_token": "agent-task-123-build-attempt-1",
    "argv": ["cargo", "test", "--release"],
    "timeout": "300s",
    "max_memory": "4GiB",
    "env": {"OMP_NUM_THREADS": "1"}
  }
}
```

Use the returned `id` with `jobq_wait` (`wait_ms: 10000`) or `jobq_status`.
Read output with `jobq_logs` and continue at its `next_offset`. `has_more` means
more captured bytes were available at that read; it does not predict future
output. `terminal` describes job completion. `truncated` reports the daemon's
storage cap, including the latest supervisor sample while a job is running.
`decoding_lossy` identifies invalid UTF-8 or a code point split across chunk
boundaries. Offsets and `bytes_read` always count original bytes; the CLI/raw
socket API remains available for exact binary output.

For job listings, pass `next_cursor` with the same `all` filter. Cursors are
submission sequence numbers represented as strings. Each page is live: jobs
may complete or expire, and new jobs may appear, between requests.

Supply an **absolute working directory** for each job. Exported environment and
umask are captured when the adapter launches. Environment precedence is adapter
environment, then server TOML overrides, then the submission's `env` overrides.
Shell execution uses `shell_executable`, otherwise the adapter's `$SHELL`, then
`/bin/sh`. Changing `env.SHELL` does not select a different shell executable.
An application-launched adapter does not automatically acquire your terminal's
current `direnv`, virtual environment, aliases, or later exports. Configure its
launch environment, submit a wrapper such as `direnv exec /absolute/project ...`,
or use the CLI from the prepared terminal to capture that terminal's context.

Keep the same `submission_token` when retrying after an uncertain response.
Submission tool errors include the token and `outcome_unknown`; a reused token
returns the original retained job even if the new payload differs. Use a new
token only for an intentional new execution. The adapter never resubmits on its
own. Accepted jobs survive MCP disconnection, adapter shutdown, and cancellation
of an MCP wait request. Stop jobs explicitly with `jobq_cancel`. All agents under
the shared account can inspect or cancel those jobs. Configuration reload remains
the CLI's `jobq reload` operation.

## Configuration and scheduling

See [config.example.toml](config.example.toml). Defaults are:

| Setting | Default |
| --- | --- |
| Queues | 1s, 5s, 10s, 20s, 60s, 300s, 600s |
| Unspecified timeout | 10s |
| Per-job memory | 4GiB |
| Runners | logical CPU count |
| Total admission memory | 75% of physical RAM |
| Aging threshold | 30s |
| Memory monitoring | every 100ms while active |
| Each stdout/stderr file | first 16MiB; drain and discard the rest |
| Result/log retention | seven days after completion |
| Maximum waiting jobs | 10,000 |

Durations accept values such as `0.5s`, `100ms`, and `2h` (1ms through 365days).
Memory sizes use integers: `4GiB`, `500MiB`, `4GB`, or plain bytes. `GiB` is binary;
`GB` is decimal. Queue limits must be distinct. At most 128 ordinary queues are
supported. Monitoring intervals must be at most one second.

Requested time selects the smallest covering bucket: `0.5s → 1s`, `2s → 5s`,
`22s → 60s`. **The bucket limit is the actual execution deadline.** Queue waiting
does not consume it. Deadlines and `elapsed_ms` use `CLOCK_MONOTONIC`, which
keeps counting while the Mac sleeps, so a job running across a sleep can time out
on wake. All `*_at_ms` timestamps are Unix wall-clock milliseconds. Requests above the largest bucket and job memory caps above
the admission budget are rejected. Runners above CPU count or a budget above
physical RAM produce warnings and are accepted.

Each running/starting job reserves its full declared memory cap. Reservations
remain held during cancellation and cleanup. This budget applies to job memory;
daemon/supervisor overhead and other applications are additional. A runner is one
job, not one CPU: multithreaded commands can consume several cores.

Only FIFO heads are eligible. An emergency FIFO head wins first; otherwise the
shortest fitting ordinary head wins. Emergency jobs keep their routed timeout
and memory cap. They never terminate existing work. An emergency head waiting
for memory blocks younger ordinary dispatches until capacity becomes available.

After `aging_after`, the oldest aged head across all queues wins. If it cannot
fit, dispatch pauses while existing jobs drain. FIFO successors cannot bypass a
blocked head. This prevents starvation under continuous short/emergency traffic,
assuming running jobs terminate and resources become available.

Edit TOML and run `reload` to add or remove queues without restarting. Added
queues affect new submissions. Removing a bucket reroutes its waiting jobs using
their original requested durations; destination FIFOs merge by original sequence.
Running limits and accepted execution contexts stay frozen. Reload is atomic:
an unroutable waiting job or one above the new budget rejects the entire change.
Lower runner/budget settings take effect as existing work drains. Startup applies
the same reconciliation to recovered waiting jobs; fix incompatible TOML and
restart if startup rejects it.

## Execution context, monitoring, and recovery

Direct commands after `--` preserve argv exactly. `--shell` uses the client's
`$SHELL -c`, falling back to `/bin/sh`. The client captures its absolute cwd,
exported environment, and umask. Environment precedence is client exports,
then TOML `[env]`, then explicit `--env NAME=value` flags. Context is persisted
at acceptance. Raw API submissions without a umask use 022. Inputs must be UTF-8.
There is no interactive terminal or stdin, and no transfer of aliases,
unexported variables, open files, or live shell state.

An internal supervisor owns each active process tree. Two launch gates ensure
daemon and command identities are persisted before user code can run. A private
pipe reports daemon death and cancellation. Helpers use native polling, not
repeated `ps`/`time` subprocesses. Deadlines are independent of daemon database
work. stdout/stderr draining is bounded per iteration so output floods cannot
monopolize the monitor loop. Descendants are discovered through libproc parent
relationships as well as the original group. Their kernel start identities are
retained across `setsid`, group changes and reparenting, and persisted for crash
recovery. Cancellation freezes the observed tree, confirms suspension, discovers
remaining children and kills only matching identities. Reservations stay held
until all tracked live descendants are gone. Normal command exit also cleans up
observed background descendants, including detached wrapper guardians.

The memory limit is the **sum of tracked process-tree physical footprints**, sampled
through Apple's libproc. Current and peak sampled RSS/footprint are reported
separately. RSS may count shared pages repeatedly. Samples can miss brief spikes.
An observed detached descendant remains included, but a process that forks and
reparents entirely between censuses can still escape discovery. This is cooperative
workload management, not a strict sandbox or kernel-enforced memory ceiling.
Scheduling delays and machine sleep can delay observations and cleanup.

Short-lived setuid helpers such as macOS `/bin/ps` temporarily deny task queries.
The monitor retries those queries for at most 100ms and uses public BSD process
status to confirm protected zombies. A sustained denial is resolved through the
unprivileged `KERN_PROC_PID` view: each tracked identity records its wall-clock
start time and real uid at enrollment (`processes.json` version 2), so a PID
reused by another user's process (for example a root-owned daemon) is recognised
as a different process and is never signalled. A denied process whose start time
and uid still match is treated as live; its memory cannot be sampled, which is
an explicit monitoring failure, never zero usage. Version-1 inventories, which
lack a recorded start, only accept replacement by another user's process that
provably started after the original could have; anything undecidable stays
unresolved.

Command CPU time and maximum RSS come from `wait4`; this includes descendants
accounted to the waited command, not necessarily every orphaned descendant.
Sampled tree peaks are not exact lifetime peaks. Live statistics are reported
at most twice per second; `sampled_at_ms` shows freshness.

SQLite uses WAL, `synchronous=FULL`, and macOS full-fsync. Acceptance is committed
before acknowledgment. Contexts live on disk rather than in the queue's memory
records. Supervisors atomically write final results before exiting. Restart first
waits for old supervisors to clean up, recovers durable results, marks ambiguous
executions interrupted, and resumes waiting work. Executions that might have
started are **never automatically rerun**. Cleanup is bounded: a supervisor that
cannot confirm its tree gone within 10s publishes `monitoring_failed` naming the
unresolved identities, reaps the command and exits. During restart or reap, a job
whose tree cannot be confirmed gone (or whose old supervisor is still running)
becomes terminal `interrupted`/`monitoring_failed` with a "cleanup unresolved"
message and its memory reservation **quarantined**: still counted against the
budget (`quarantined_jobs` in queue status), excluded from pruning, and released
only when a later cleanup attempt (about once a minute) confirms the tree gone.
Other jobs and startup continue. Pre-tree historical jobs retain their original
group recovery.

Provide `submit --token TOKEN ...` when retrying after a lost connection. Tokens
are unique submission identities: reusing one returns the original job even if
the new payload differs. Generated tokens are included in client error messages
when the submission outcome might be unknown. Deduplication lasts until the job
record is pruned. Pruning runs at startup and roughly once a minute; it excludes
unfinished jobs. Retained metadata consumes memory proportional to retained job
count. Per-file caps and retention bound individual logs and their lifetime, not
total disk usage; size the state volume for the workload.

State directories are 0700; socket, database, SQLite sidecars, snapshots, and logs
are owner-only. Existing non-private directories are rejected. **Exported
credentials are persisted with the execution context while a job is queued or
running**: in the database and in the job's `spec.json`. When the job becomes
terminal (finished, cancelled before running, failed to launch, or settled by
recovery), environment values in both are replaced with `<redacted>`; variable
names, command, cwd, and umask stay for the retention period. Startup also redacts
jobs finished by older daemons. The supervisor holds the environment in memory
from launch and the command inherits it, so redaction never changes what a job
runs with. SQLite `secure_delete` zeroes the replaced rows and the WAL is truncated
at startup and about once a minute, so redacted values are not left in database
or WAL file free space. Database backups, filesystem snapshots, and unlinked file
blocks taken or left before redaction are outside jobq's control. Context values
are not returned by status/queue APIs. Use a local filesystem. SQLite persistence errors
stop the daemon and close supervisor control channels; later startup reconciles
durable work. Job log/preparation errors produce explicit failures.

## Estimates and JSON protocol

Job status includes state, sequence, queue, requested/effective limits, timestamps,
elapsed time, cancellation progress, log availability, resource samples, outcome,
and an `estimate` for unfinished work. Queue status includes per-queue projected
next start and drain times, counts, runner use, and reserved memory.

Both estimates simulate the actual scheduling policy and memory reservations:

- `historical` uses the median of the most recent 100 successful/timed-out
  durations per timeout bucket. Running jobs use durations conditional on already
  having lasted this long; no applicable samples fall back to remaining budget.
- `budget` uses full limits for waiting jobs and remaining limits for running work.
- Fewer than five samples give `confidence: "low"`; otherwise it is
  `"queue_history"`. Queue history can mix unrelated commands and is not a
  statistical accuracy guarantee.

Projections assume no new submissions, cancellation, config changes, or cleanup
delays. Emergency arrivals can invalidate estimates. They are predictions, not
completion promises. Times and durations in JSON use epoch milliseconds and
integer milliseconds respectively; memory uses bytes.

The Unix socket accepts one newline-delimited JSON request per connection (4MiB
maximum), returning one response before closing. Peers must have the server's UID;
128 simultaneous connections and a ten-second connection lifetime bound inputs.

```json
{"version":1,"op":"submit","token":"unique-submission-token","context":{"command":{"kind":"exec","argv":["/bin/echo","hello"]},"cwd":"/tmp","env":{"PATH":"/usr/bin:/bin"},"umask":18},"timeout_ms":2000,"max_memory_bytes":67108864,"emergency":false,"env_overrides":{}}
```

Shell command form: `{"kind":"shell","shell":"/bin/sh","command":"echo hi | cat"}`.
`timeout_ms` and `max_memory_bytes` may be omitted/null for server defaults.
`emergency` defaults false, `env_overrides` defaults empty, and `umask` is optional.
The umask above is decimal 18, equivalent to octal 022.

| Operation | Fields beyond `version` and `op` |
| --- | --- |
| `status`, `cancel` | `id` |
| `jobs` | optional `all` (default false), `limit` (1..100), `cursor` (sequence string) |
| `queues`, `reload`, `resume`, `maintenance` | none |
| `drain` | optional `all` (default false; hold queued jobs) |
| `logs` | `id`, `stream` (`stdout`/`stderr`), optional `offset`, `max_bytes` (1..65536) |

Omitting both `limit` and `cursor` retains the original unbounded `jobs` response
used by the CLI. Paged requests return `next_cursor`; a cursor without a limit
uses 25 rows. Pagination uses live submission order.

`logs` defaults to up to 64KiB as `data_base64`, `next_offset`, `available_bytes`,
`has_more`, `terminal`, and `truncated`; `max_bytes` requests a smaller chunk.
Follow by reading both streams at their next offsets. CLI `logs
--json --follow` emits one JSON object per chunk/query. CLI `wait` polls status
every 250ms. CLI JSON success output is the response's `data`; failures are
structured error envelopes. Socket response envelopes are:

```json
{"version":1,"ok":true,"data":{}}
{"version":1,"ok":false,"error":{"code":"request_failed","message":"..."}}
```

Unknown fields on operation/context objects and unsupported versions are rejected.
Public states are `queued`, `starting`, `running`, `cancelling`, `succeeded`,
`failed`, `timed_out`, `memory_exceeded`, `cancelled`, `spawn_failed`,
`monitoring_failed`, and `interrupted`. Terminal outcomes preserve command exit
code/signal, a diagnostic, duration, accounting, and truncation flags. Inspect
state rather than treating every nonzero exit as a timeout.

## Validation and measurements

```sh
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo build --release --locked
python3 tests/smoke.py
python3 tests/mcp.py
python3 tests/process_tree.py
python3 tests/maintenance.py
python3 tests/legacy_gate.py
python3 tests/wait_many.py target/release/jobq
python3 tests/benchmark.py > benchmark-results.json
python3 tests/mcp_benchmark.py > mcp-benchmark-results.json
```

If a minimal shell exposes `cargo`/`rustc` but cannot find `rustdoc` or Clippy,
add the active toolchain utilities to that shell's PATH first:

```sh
export PATH="$(rustc --print sysroot)/bin:$PATH"
```

The `smoke`, `mcp`, `process_tree`, `maintenance`, and benchmark scripts require
no other daemon for this account. Never stop a busy daemon just to run them.
Use a separate macOS user or a disposable CI runner when necessary. They use temporary state
and sockets, and stop their own daemon. The smoke suite executes real processes:
argument/env/cwd inheritance, shell pipelines, routing, timeout, child memory,
output flooding, multi-runner reservations, singleton, withdrawal, duplicate and
disconnected submissions, queue reassignment, failed reload, emergency/aging,
daemon and supervisor crashes, durable recovery, failed job preparation, privacy,
retention, and estimates. Unit tests cover shared policy and SQLite transaction
rollback/schema refusal, and environment redaction of terminal jobs (including
that no redacted value survives in the database or WAL files).

The process-tree suite covers detached sessions, reparented descendants, memory,
cancellation, timeout, normal root exit, protected `ps` helpers/zombies, persisted
supervisor and daemon recovery, unrelated-process isolation and missing-inventory
failure. Integration workloads use synthetic programs, shell commands,
and Python fixtures.

The MCP suite is an independent stdio wire client. It covers handshake/discovery,
schemas, argv/env/cwd/umask, daemon defaults, input/API errors, simultaneous calls,
bounded waits and wait cancellation, cross-adapter idempotency, pagination,
UTF-8/log truncation, adapter death, and a live adapter across daemon restart.
Unit tests additionally simulate lost responses and blocked daemon I/O. The
native suites and benchmarks must run sequentially because of the fixed lock.

Pre-MCP release snapshot measured on 2026-10-12 at 23:04 UTC+8, on macOS 27.0.1 arm64
with 18 logical CPUs and 48GiB RAM (100ms sampling):

| Measurement | Result |
| --- | --- |
| Idle daemon RSS / CPU | 3.64MiB / below 0.001% of one core |
| Daemon + 16 supervisors RSS / CPU | 40.08MiB / 0.067% of one core |
| Direct spawn through exit, p50 / p95 | 7.59ms / 12.46ms |
| Supervised command elapsed, p50 / p95 | 12ms / 21ms |
| Eight concurrent clients, admission p50 / p95 | 358ms / 397ms |
| Submission through launch, p50 / p95 | 160.5ms / 267ms |

The idle and active CPU windows are only three seconds. Launch/admission results
use 64 jobs, with eight simultaneous clients; the direct baseline uses 40 jobs.
Command elapsed excludes time waiting for admission and launch. Durable state
flushes and process launch are substantial costs for tiny commands; the intended
workload is long CPU/memory-heavy commands. These results are observations from
one host, without a latency or resource-use guarantee.

See [benchmark-results.json](benchmark-results.json) for the measured release
snapshot, sample sizes, CPU/RAM, and raw values. CPU percentages refer to **one
core**. Short samples measure daemon/supervisor overhead while jobs sleep; they
exclude the jobs' own resource usage and do not predict overhead for huge process
trees. Concurrent admission includes durable flushes. Compare the raw direct
spawn baseline separately from queue wait and supervised execution.

### MCP adapter measurements

Measured on 2026-10-12 at 23:41 UTC+8 on the same macOS host, using the release
binary with `rmcp` 3.5.1. These measurements are separate from the pre-MCP table.
The MCP fixture uses one runner, a 192MiB admission budget, 64MiB job reservations,
and 20ms monitoring. The release executable is 5.42MiB.

| Measurement | Result |
| --- | --- |
| Adapter launch → initialize + tool discovery, p50 / p95 | 4.75ms / 5.26ms |
| One idle adapter RSS / physical footprint | 4.67MiB / 2.05MiB |
| Eight idle adapters, summed RSS / footprint | 37.11MiB / 16.00MiB |
| Idle adapter CPU | no CPU time observed during the three-second samples |
| Idle daemon RSS with this binary | 5.03MiB |
| Sequential MCP queue query, p50 / p95 | 0.104ms / 0.208ms |
| Sequential direct socket queue query, p50 / p95 | 0.030ms / 0.043ms |
| Eight concurrent MCP query callers, p50 / p95 | 0.299ms / 0.793ms |
| Eight concurrent MCP submit callers, admission p50 / p95 | 65.0ms / 68.9ms |

Startup uses 20 launches; sequential queries use 40 calls per path; concurrent
queries use 80 calls; submissions use 64 jobs across eight adapters. Query
measurements use an empty daemon. Submission latency includes durable admission
and is not the added MCP transport cost. The different runner configuration and
host conditions prevent direct comparison with the older admission benchmark.
Summed RSS can count shared pages more than once. Short idle samples do not prove
zero CPU usage over longer periods, and none of these values is a performance
guarantee. Raw values, host/config metadata, and sample sizes are in
[mcp-benchmark-results.json](mcp-benchmark-results.json); reproduce them with
`python3 tests/mcp_benchmark.py > mcp-benchmark-results.json`.
