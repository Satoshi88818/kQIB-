
# kQIB Fleet v3.1

kQIB is a three-tier system for coordinating a fleet of robots in
real time: a bare-metal microkernel on each robot, an edge proxy bridging
that robot to the cloud, and a cloud backend that maintains a per-robot
digital twin and issues safety commands over Kafka.

This README documents the system as it stands after a full round of
review, fixes, and validation — every item below has been compiled, run,
and tested against something real (a real simulated Kafka broker, real
concurrent OS threads, a real bound HTTP socket, and a genuinely separate
bare-metal execution context in QEMU), not just written to look correct.

**96/96 tests pass** across the workspace. Both deployable binaries build
cleanly with zero warnings.

---

## Table of contents

- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [Building and testing](#building-and-testing)
- [Configuration reference](#configuration-reference)
- [Key management and rotation](#key-management-and-rotation)
- [Twin-state persistence](#twin-state-persistence)
- [Observability](#observability)
- [Testing strategy](#testing-strategy)
- [Known limitations and deferred work](#known-limitations-and-deferred-work)
- [Fix history](#fix-history)

---

## Architecture

```
┌─────────────────┐   shared memory    ┌──────────────┐    Kafka     ┌──────────────────┐
│   qib-muk        │◄──────────────────►│ qib-edge-proxy│◄────────────►│ qib-cloud-backend │
│ (bare-metal,     │  SharedRingBuffer   │  (async,      │              │  (async, one      │
│  no_std,         │  IPC, 1ms tick)     │   per-robot)  │              │   worker per      │
│  microkernel)    │                     │               │              │   Kafka partition)│
└─────────────────┘                     └──────────────┘              └──────────────────┘
        │                                       │                              │
   hardware safety                        Ed25519 verify,                per-robot digital
   trips (velocity,                       TTL/epoch/rate                 twin, breaker,
   acceleration,                          checks, then                  command issuance,
   watchdog)                              pushes to IPC ring             Prometheus metrics
```

- **`qib-muk`** — the microkernel side. Runs a 1ms isochronous scheduler
  tick that reads hardware sensors, applies hard safety trips (velocity,
  jerk/acceleration, a watchdog deadline), processes incoming commands
  from the edge proxy (with replay protection via epoch + monotonic
  sequence checks), and emits telemetry — all through a lock-free,
  wait-free SPSC ring buffer (`SharedRingBuffer`) designed to be safely
  shared across a real process/address-space boundary.
- **`qib-edge-proxy`** — bridges the microkernel's shared-memory IPC to
  Kafka. Verifies Ed25519 command signatures (supporting dual-key
  rotation), checks TTL against a clock-drift-corrected deadline, checks
  epoch fencing and per-robot rate limits, then pushes verified commands
  into the microkernel's IPC ring — and pulls telemetry back out the other
  direction, publishing it to Kafka.
- **`qib-cloud-backend`** — one Kafka partition per worker task, each
  with exclusive ownership of a subset of robots (partitioned by
  `robot_id`). Maintains a Bayesian digital twin (EWMA mean/variance) per
  robot, evaluates a Brier-score circuit breaker, issues signed
  cool-down/halt commands when a robot's telemetry diverges from its
  twin's prediction, and persists twin state so a worker restart doesn't
  silently reset breaker/sequence state.
- **`qib-schema`** — shared wire types (`TelemetryEvent`, `CommandEvent`,
  `TwinStateSnapshot`) and the binary (postcard) serialization helpers
  used by both the proxy and backend. `no_std`-compatible so `qib-muk` can
  depend on it without pulling in `std`.

### Kafka topics

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `robot-telemetry-control` | edge proxy | cloud backend | Per-robot telemetry, keyed by `robot_id` |
| `robot-commands` | cloud backend | edge proxy | Signed cool-down/halt commands |
| `robot-commands-dlq` | both | (monitoring) | Rejected/failed commands and breaker-tripped telemetry |
| `robot-twin-state` | cloud backend | cloud backend (on restart) | Twin snapshots, published every tick and replayed on worker startup |

`robot-telemetry-control` and `robot-twin-state` must have the **same
partition count** — both are keyed by `robot_id`, so matching partition
counts is what guarantees partition N holds the same robot subset on both
topics, which is what makes twin-state replay line up correctly with
partition ownership. `main()` validates this at startup and degrades
gracefully (cold start, not a crash) if they don't match.

---

## Repository layout

```
kqib/
├── qib-schema/              Shared wire types + postcard helpers (no_std-compatible)
├── qib-muk/                 Microkernel: scheduler tick, safety trips, SharedRingBuffer
├── qib-edge-proxy/          Async proxy: signature verification, TTL/epoch/rate checks, IPC
├── qib-cloud-backend/       Async backend: digital twin, breaker, command issuance, metrics
│   ├── src/
│   │   ├── partition.rs     Static Kafka partition assignment + compaction check
│   │   ├── warmup.rs        Twin-state replay on startup
│   │   ├── twin.rs          Bayesian digital twin + circuit breaker
│   │   ├── key_source.rs    Pluggable signing-key sources + dual-key rotation
│   │   ├── metrics.rs       Prometheus metric definitions + text-format rendering
│   │   └── metrics_server.rs Real /metrics + /healthz HTTP endpoints (axum)
│   └── tests/               Unit-adjacent + MockCluster-backed integration tests
└── tools/                   Validation tooling that isn't part of the deployed system
    ├── layout-probe/        Prints SharedRingBuffer's real, empirically-measured layout
    ├── qemu-ipc-validation/       Bare-metal QEMU guest, BIOS-disk-I/O transport
    └── qemu-ivshmem-validation/   Bare-metal QEMU guest, genuine memory-mapped transport
```

---

## Building and testing

### Toolchain requirements

- Rust (this workspace was validated against 1.75; `Cargo.lock` pins a
  handful of transitive dependencies — `fixed`, `az`, `half`,
  `ed25519-dalek`, `uuid`, `zeroize`, `indexmap`, `proc-macro-crate`,
  `base64ct`, `proptest` — to versions compatible with that MSRV. On a
  newer toolchain (1.85+) these pins are unnecessary.)
- A C toolchain (`gcc`, `make`) and `cmake` — `rdkafka`'s `cmake-build`
  feature statically compiles librdkafka, including the mock broker used
  by several integration tests.
- Standard `binutils` (`as`, `ld`, `objcopy`) and `qemu-system-x86` — only
  needed to build/run the two crates under `tools/qemu-*-validation`;
  every other workspace member is unaffected if these aren't installed.

### Common commands

```bash
# Build everything
cargo build --workspace

# Run the full test suite (96 tests)
cargo test --workspace

# Run just the bare-metal QEMU validation (each ~150ms, not flaky)
cargo test -p qemu-ipc-validation
cargo test -p qemu-ivshmem-validation

# Run the two deployable binaries
cargo run -p qib-cloud-backend
cargo run -p qib-edge-proxy
```

---

## Configuration reference

All configuration is via environment variables; there are no config files
for the deployed binaries (the key/trust *sources* can point at files —
see below — but which source to use is itself chosen via env var).

### `qib-cloud-backend`

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_PARTITIONS` | `6` | Expected partition count for `robot-telemetry-control`; validated against the broker's actual count at startup (fatal on mismatch) |
| `METRICS_ADDR` | `0.0.0.0:9090` | Bind address for the `/metrics` and `/healthz` HTTP endpoints |
| `SIGNING_KEY_FILE` | — | Path to a file containing a 32-byte hex-encoded Ed25519 seed; re-read on every load, so an external process can rotate it without a restart. Takes priority over `SIGNING_KEY_SEED_HEX` |
| `SIGNING_KEY_SEED_HEX` | — | A 32-byte hex-encoded Ed25519 seed, read once at startup |
| *(neither of the above)* | — | Falls back to a freshly generated **ephemeral** key (logged loudly) — fine for local dev, wrong for production |
| `SIGNING_KEY_PREVIOUS_FILE` | — | Path to the **previous** signing key's seed, for a dual-key rotation transition window (see below). Takes priority over `SIGNING_KEY_PREVIOUS_SEED_HEX` |
| `SIGNING_KEY_PREVIOUS_SEED_HEX` | — | The previous signing key's seed, inline |
| *(neither of the above)* | — | No rotation in progress — single-key signing, identical to before this feature existed |

### `qib-edge-proxy`

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap servers |
| `ROBOT_EPOCH` | `1` | This robot's current epoch, checked against each command's `min_epoch`/`max_epoch` |
| `TRUSTED_PUBKEYS_FILE` | — | Path to a file with one 32-byte hex-encoded Ed25519 public key per line (`#` comments and blank lines ignored). Background-reloaded every `TRUSTED_PUBKEYS_RELOAD_SECS`, so rotating the file rotates trust without a restart. Takes priority over `TRUSTED_PUBKEYS` |
| `TRUSTED_PUBKEYS` | — | Comma-separated hex-encoded public keys, static for the process lifetime |
| *(neither of the above)* | — | Command signature verification is **disabled** (logged loudly — a deliberate, visible opt-out, not a silent bypass) |
| `TRUSTED_PUBKEYS_RELOAD_SECS` | `30` | Poll interval for `TRUSTED_PUBKEYS_FILE`'s background reload |
| `IPC_UNLINK_ON_SHUTDOWN` | `false` | If `true`/`1`, removes this proxy's POSIX shared-memory segments on graceful shutdown. Defaults off because unlinking is only safe when the microkernel side isn't expected to reattach across a proxy restart — see the comment at the shutdown call site for the full tradeoff |

If a variable in either binary isn't set and there's no documented
fallback, expect a startup error naming exactly what's missing — none of
these fail silently.

---

## Key management and rotation

### Static configuration

The simplest setup: set `SIGNING_KEY_SEED_HEX` on the backend, and the
corresponding public key (hex-encoded) in `TRUSTED_PUBKEYS` on every edge
proxy. No rotation support, no file-watching — appropriate for a small
fixed fleet or local development.

### File-based, hot-reloadable configuration

For anything larger: use `SIGNING_KEY_FILE` (backend) and
`TRUSTED_PUBKEYS_FILE` (edge proxy) instead. An external process — a
Vault agent's file sink, a Kubernetes Secret volume refresh, a deployment
runbook — can then update either file, and:
- The backend re-reads `SIGNING_KEY_FILE` on its next signing operation
  (the backend currently loads its key once per process lifetime, so this
  means "picked up on next restart," not truly live — see [Known
  limitations](#known-limitations-and-deferred-work)).
- The edge proxy's `TRUSTED_PUBKEYS_FILE` is re-read by a background task
  every `TRUSTED_PUBKEYS_RELOAD_SECS` — genuinely live, no restart needed.

### Zero-downtime key rotation

Rotating the backend's signing key without any edge proxy losing
connectivity mid-rotation requires the **dual-key** mechanism, which
removes the ordering dependency between "the backend starts signing with
a new key" and "every edge proxy has learned to trust it":

```
1. Generate the new keypair.
2. Add the new PUBLIC key to every edge proxy's TRUSTED_PUBKEYS(_FILE),
   alongside the existing old key.
3. Wait for that to propagate (automatic within TRUSTED_PUBKEYS_RELOAD_SECS
   if using the file-based source).
4. Switch the backend: SIGNING_KEY_* = new key,
   SIGNING_KEY_PREVIOUS_* = old key. Every command is now signed with
   BOTH keys (as two separate Kafka headers, "x-signature" and
   "x-signature-prev"). Every edge proxy — regardless of whether it has
   picked up the new trusted key yet — keeps accepting commands, because
   verification checks every attached signature against every trusted
   key and accepts if any pair matches.
5. Once confident every proxy has the new key trusted, remove
   SIGNING_KEY_PREVIOUS_* from the backend. Back to single-key signing,
   now on the new key.
6. Eventually remove the old key from every edge proxy's trusted set.
```

A proxy that skips step 2/3 and is still only trusting the old key by the
time step 5 happens will correctly start rejecting commands — that's the
required order, not a bug, and it's covered by a dedicated test
(`after_rotation_completes_a_lagging_proxy_correctly_loses_access`).

### What's deliberately not implemented

Real secrets-manager integration (Vault, AWS/GCP Secrets Manager) isn't
implemented — this environment has no network access or credentials to
validate such an integration against a real service, and shipping
untested integration code seemed worse than not having it. Both
`SigningKeySource` (backend) and the equivalent structure on the edge
proxy side are the extension points: a new source type slots in without
touching any call site.

---

## Twin-state persistence

Each `PartitionWorker`'s per-robot digital twin (EWMA mean/variance,
circuit-breaker state, and — most importantly — the monotonic command
sequence counter) is published to `robot-twin-state` on every telemetry
tick and **replayed from that topic on worker startup**, so a restart
doesn't silently reset every owned robot to a fresh, cold twin.

The sequence counter is the sharp edge here: the microkernel rejects any
command whose sequence number is behind what it's already seen. If the
backend restarted and began issuing sequence numbers from zero again while
the microkernel had already observed higher numbers, every subsequent
command for that robot would be silently dropped. Replay specifically
preserves `next_sequence` to prevent this.

Replay doesn't require or assume Kafka log compaction on
`robot-twin-state` — it dedupes to the latest snapshot per robot itself,
using offset order — but **without compaction the topic grows unbounded
and replay gets slower every deployment cycle**. `main()` checks
`cleanup.policy` at startup via the Kafka Admin API and logs a clear
warning (with the exact `kafka-configs` command to fix it) if compaction
isn't enabled; this is non-fatal, since it's a durability/performance
concern, not a safety one.

If `robot-twin-state`'s partition count doesn't match
`robot-telemetry-control`'s, every worker starts cold rather than
replaying against a mismatched partition-to-robot mapping — logged as a
warning, not a crash.

---

## Observability

`qib-cloud-backend` serves Prometheus metrics at `GET /metrics`
(`METRICS_ADDR`, default `0.0.0.0:9090`) and a plain liveness probe at
`GET /healthz`. Metrics exposed:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `qib_command_latency_seconds` | Histogram | `robot_id` | End-to-end latency from telemetry receipt to command send |
| `qib_breaker_trips_total` | Counter | `robot_id`, `reason` | Circuit breaker trip count |
| `qib_breaker_open` | Gauge | `robot_id` | 1 if commands are currently halted for this robot |
| `qib_twin_temp_variance` | Gauge | `robot_id` | Current EWMA temperature variance |
| `qib_twin_zscore` | Gauge | `robot_id` | Most recent prediction-error z-score |
| `qib_commands_issued_total` | Counter | `robot_id`, `action` | Commands issued, by type |

Standard Prometheus `process_*` metrics are also present (auto-registered
by the `prometheus` crate).

---

## Testing strategy

Tests are organized in layers, from pure/fast to genuinely end-to-end:

1. **Pure unit tests** — logic that doesn't touch a broker, socket, or
   filesystem: partition-assignment math, twin variance/breaker
   evaluation, signature parsing, clock-drift bounding, key-source
   resolution priority.
2. **Real-broker integration tests** — `rdkafka::mocking::MockCluster`
   spins up an in-process, real (simulated) Kafka broker via statically
   linked librdkafka, with no Docker or external service needed. Used to
   validate partition assignment (including a regression-guard test that
   reproduces the original `subscribe()` bug against a live broker to
   prove it stays fixed), twin-state replay, and the compaction check —
   including one test that **honestly documents a real limitation**: the
   mock broker's admin `describe_configs` isn't actually implemented
   (confirmed by direct probing, not assumed), so that test asserts the
   caller's non-fatal-warning path is exercised rather than asserting a
   specific config value.
3. **Real socket / real filesystem tests** — the metrics server test
   binds an actual ephemeral TCP port and makes a real HTTP request over
   loopback; the shared-memory IPC tests use real POSIX `shm_open`/`mmap`
   across independent mappings (not just in-process structs) to prove the
   cross-process contract holds, including a genuine two-OS-thread
   producer/consumer test moving 50,000 messages with zero loss.
4. **Bare-metal QEMU validation** (`tools/qemu-*-validation`) — the
   deepest layer: a hand-written, real machine-code guest (no OS, no Rust
   runtime) running as its own CPU context inside QEMU, fed by the actual
   production `SharedRingBuffer::push()`. Two variants exist:
   - `qemu-ipc-validation` — BIOS disk I/O as the transport (a substitute
     adopted after a documented, reproducible QEMU environment quirk with
     overriding main RAM via `memory-backend-file`).
   - `qemu-ivshmem-validation` — the properly-scoped fix: a real
     `ivshmem-plain` PCI device as genuine memory-mapped shared memory,
     with the guest performing real PCI configuration-space enumeration
     and a 32-bit protected-mode transition to reach it.

   Both are fully automated (`cargo test`), fast (~150ms each), and
   confirmed non-flaky across repeated runs.

Run `cargo test --workspace` for everything, or scope to a single crate
(`cargo test -p qib-cloud-backend`) or test file
(`cargo test -p qib-cloud-backend --test mock_cluster_tests`) as needed.

---

## Known limitations and deferred work

Documented here rather than silently left out, so nothing is assumed
finished that isn't:

- **No real secrets-manager integration** (Vault, AWS/GCP Secrets
  Manager) — see [Key management](#key-management-and-rotation).
- **Backend key rotation isn't truly live** — `SIGNING_KEY_FILE` is
  re-read on next process restart, not on a background timer the way the
  edge proxy's `TRUSTED_PUBKEYS_FILE` is. The dual-key mechanism is what
  actually makes *rotation* zero-downtime; hot-reloading the backend's
  own key file without a restart would be a further improvement.
- **QEMU, not real hardware** — both bare-metal validation crates prove
  correctness against QEMU's TCG/KVM execution, which is
  instruction-accurate but not a substitute for a real board's memory
  controller, cache coherency, or bootloader.
- **`TRUSTED_PUBKEYS_FILE` reload is polling-based**, not event-driven
  (inotify) — a deliberate simplicity tradeoff; rotation propagation delay
  is bounded by `TRUSTED_PUBKEYS_RELOAD_SECS` (default 30s), which seemed
  reasonable for a security-relevant but non-hot-path operation.
- **No staging/production Kafka run** — every integration test here uses
  `rdkafka::mocking::MockCluster`, a real but in-process simulated broker.
  A run against an actual multi-broker Kafka deployment (real network
  partitions, real consumer-group rebalancing under load, real disk-backed
  log compaction) hasn't been done from this environment.

---

## Fix history

This codebase went through several rounds of review and remediation. In
brief, in order:

1. **Static Kafka partition assignment** — `PartitionWorker` used to call
   `subscribe()` with a unique `group.id` per worker, which handed every
   worker every partition via Kafka's dynamic group-rebalance protocol
   (since each was the lone member of its own group) — defeating the
   "exclusive per-partition ownership, no locks needed" design entirely.
   Fixed with `consumer.assign()` for static ownership.
2. **A tautological safety test, and two silent no-op stubs** — the
   acceleration-trip test asserted `... || { true }`, always passing
   regardless of the real values; `sign_command()` existed but nothing
   called it; the IPC ring push/pop were commented-out stubs. All three
   fixed, the last one requiring a latent `&mut self` soundness bug in the
   shared-memory ring buffer to be fixed first (switched to `UnsafeCell` +
   `&self` with a documented single-producer/single-consumer contract).
3. **Twin-state persistence** — see [above](#twin-state-persistence).
4. **A real Prometheus `/metrics` endpoint** — replacing a stub that only
   logged a message claiming one existed.
5. **Key management scoping, plus three smaller fixes** — pluggable,
   file-based key/trust sources; a clock-sync EWMA overflow bound; opt-in
   shared-memory cleanup on shutdown; the twin-state compaction check.
6. **Bare-metal QEMU validation**, in two rounds — first over a BIOS-disk-
   I/O substitute transport after a documented QEMU environment quirk,
   then properly over `ivshmem`, genuine memory-mapped shared memory.
7. **Dual-key signing rotation** — closing the zero-downtime rotation gap
   explicitly flagged as deferred in step 5.

Six additional pre-existing defects, unrelated to any of the above, were
found and fixed purely by actually compiling and running the suite rather
than reading the code: a circular self-dependency in a `Cargo.toml`, an
invalid `impl Trait` usage on a concrete struct, a Kafka client
type-inference gap, an inline-const syntax needing a newer compiler than
this toolchain, a missing crate dependency, and `no_std`-unsafe code that
only broke when a crate was built in isolation from the rest of the
workspace.
