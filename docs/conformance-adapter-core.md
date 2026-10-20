# adapter.core conformance profile

The adapter.core conformance profile is the executable restatement of what a
Criteria host expects from an adapter built on this SDK. It lives in
[`adapterhost/profile_test.go`](../adapterhost/profile_test.go) as
`TestProfileAdapterCore` and runs against this SDK's own implementation — which
is the reference the other two adapter SDKs port — so the profile doubles as
their porting checklist (KB-181, split KB-177c).

The suite is a PR gate: it must be green on every PR (see the blocking
`conformance` job in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)).

## Running it

```
make conformance
# or directly:
go test ./adapterhost -run 'TestProfileAdapterCore|TestProfileMutationChecks'
```

The suite is sequential by design: the heartbeat cases swap the
`logHeartbeatRunner` test seam, which is package state.

## Cases

The reference adapter used by the suite is `profileReferenceSpec` — a
full-surface spec object (metadata, capabilities, platforms, environments,
all three schemas, secrets, permissions, examples, every lifecycle verb, no
Log handler) served through the real gRPC bridge. Its schemas deliberately use
the manifest-publishable type vocabulary
(`string|number|boolean|object|array`).

| Case | Pins |
| --- | --- |
| `handshake_and_info_round_trip_vs_spec` | A real go-plugin handshake (the test binary re-serves itself in a helper subprocess via `ServeSpec`), wire protocol version 2 negotiated, then a field-by-field Info round-trip vs the spec object — every declared surface survives the wire unchanged. |
| `manifest_emit_parses_under_pinned_host_parser` | The `--emit-manifest` bytes parsed and validated by `internal/strictmanifest` (the pinned restatement of the host's manifest parser, host main tip 70959d34): schema_version, platform `os/arch` shapes, capabilities, schemas, secrets (`required: true`), permissions, and empty container image stays unset. |
| `execute_chunks_reassemble_in_arrival_order` | Results larger than the spec's `MaxChunkBytes` are delivered as ordered chunks with correct framing (strict `seq == arrival index`, totals agree, `final` only on the last fragment), and join to the exact logical result. |
| `log_heartbeat_liveness_and_cadence` | The SDK-owned log stream for an idle adapter session carries heartbeats for the full stream lifetime at the declared cadence — first beat prompt, no silence gaps, minimum count met. This is the case the documented mutation check kills (below). |
| `concurrent_execute_spec_tier` / `concurrent_execute_tier2_reference` | `RunConcurrentExecuteConformance` — one session, N concurrent calls, per-call result correctness under the real gRPC bridge, both the declarative spec tier (unchunked and chunked) and the tier-2 reference `Service`. |
| `secrets_resolve_once` | Session secrets travel the wire once, at `OpenSession`; per-call the spec tier delivers the wire `SecretInputs` verbatim — never re-resolved, never taken from the process env (an env fallback is armed before the calls and its value must never appear). The `Secrets` helper composition (`WithStepSecrets`, `Get`, `SpawnEnv`, undeclared-refusal) is asserted alongside. |
| `permissions_per_wire` | Permissionless adapters answering `allow` with the matching request id (the `UnimplementedPermissions` default) vs an implemented deny — both answered on the bidi permissions stream. |

## Mutation check (documented once)

The acceptance gate for the profile itself is a mutation check: break a
host-facing behavior the profile guards and prove the suite fails. The one
documented mutation is the **log-stream heartbeat cadence**:
`TestProfileMutationChecks` swaps the heartbeat runner for one that never
sends and asserts `log_heartbeat_liveness_and_cadence` fails with
`no log heartbeats`.

Why this mutation: the log stream's heartbeats are what feed the host's 90s
stall detector for an idle adapter session. A regression that silences them
means idle sessions are falsely declared crashed — and without the check the
rest of the suite is blind to it, because an idle adapter emits no log lines
at all. See `driveProfileHeartbeatWindow` in
[`adapterhost/profile_test.go`](../adapterhost/profile_test.go).