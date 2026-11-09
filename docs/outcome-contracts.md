# Outcome contracts on the Execute surface

Since `criteria-adapter-proto` v0.7.0 a host can drive an adapter step in
**contract mode**: every allowed outcome carries a contract — a payload
schema, a `require_comment` flag, and a fallback marker — and the engine
validates the finalized result against the matched contract after finalize
(the *backstop*). When a result loses its contract the host rejects it and
re-executes the step with a rejection context attached to the next Execute
(*repair mode*). This SDK leg makes the same surface first-class for adapter
authors: negotiate the mode, hand the typed values to the handler, and let
the adapter validate its own result **pre-flight**. The pinned evaluator —
`criteria/v2.EvaluateOutcomeContracts` — remains the single validation
algorithm; nothing here re-derives it.

## Hand-off

Tier 1 ([`Spec.Execute`](../adapterhost/spec.go)): the handler's
`ExecuteInput` carries the step's contracts and any repair context as plain
mirrors:

```go
in.OutcomeContracts   // []OutcomeContract — name, schema JSON, fallback, require_comment
in.OutcomeContracts.For("completed")     // first-entry-wins lookup
in.Rejection              // *Rejection — attempt, outcome, newline-joined issues; nil otherwise
```

Tier 2 ([`Service.Execute`](../adapterhost/service.go)): the raw request
carries the typed wire values (`req.GetOutcomeContracts()`,
`req.GetRejection()`). An empty contract list is *legacy mode*
(pre-v0.7.0 behavior — results ride the wire untouched, name-only
`allowed_outcomes` on the host); `in.Rejection == nil` means the step is not
a repair re-execution.

## Pre-flight enforcement

Adapters validate a candidate result BEFORE pushing it across the Execute
stream. The helpers live on [`adapterhost/outcome.go`](../adapterhost/outcome.go):

| Helper | Consumers | Pins |
| --- | --- | --- |
| `ValidateOutcomeResult(contracts, result) []string` | tier 2 | nil issues = push verbatim; non-empty = the pinned issue list the host would reject with. Legacy mode enforces nothing. |
| `OutcomeContracts.ValidateResult(result)` | tier 1 | same, for native `ExecuteResult` values (serializes `Outputs` onto `outputs_json`; returns the serialization error when the result cannot ride the wire at all). |
| `ContractFor(contracts, outcome)` / `OutcomeContracts.For(outcome)` | both | first-entry-wins contract lookup for the matched-schema pattern. |
| `NewExecutionRejection(outcome, issues, prior)` | tier 2 | the host repair framing: attempt increments the prior attempt (fresh steps start at 1), issues joined with `\n`. |

```go
// tier-1 pre-flight, inside a Spec.Execute handler
candidate := &ExecuteResult{Outcome: "completed", Comment: "summarized", Outputs: outputs}
issues, err := in.OutcomeContracts.ValidateResult(candidate)
if err != nil {
	return nil, err // Outputs cannot ride the wire at all
}
if len(issues) > 0 {
	return nil, fmt.Errorf("refusing contract-invalid result: %s", strings.Join(issues, "; "))
}
return candidate, nil
```

A failed pre-flight must produce **no** `ExecuteResult` on the stream: the
step fails loudly with the pinned issues, one round-trip before the host's
post-finalize validation would have rejected the same payload. A handler
that skips pre-flight is still safe — the SDK pushes the result verbatim and
the host backstop rejects it, so no contract check can be silently lost; the
SDK neither rewrites results nor re-enforces them after the handler.

`ValidateOutcomeResult` deliberately does **not** take the request's
`allowed_outcomes`: name-only membership stays the host's attempt-loop
concern (per the pinned evaluator's compat matrix), so the helper never
emits `outcome_not_allowed`. Everything else the pinned evaluator emits
(`empty_outcome`, `outcome_uncontracted`, `missing_comment`, `no_result`,
`payload_schema: …`) arrives byte-identically.

## Negotiation

An adapter declares its contract posture with the ordinary capabilities
surface — [`Spec.Capabilities` / `InfoResponse.capabilities`](../adapterhost/spec.go),
the same mechanism as `adapter_tools` — with the value
`CapabilityOutcomeContracts` (`"outcome_contracts"`). No new wire surface is
introduced; unknown capabilities are ignored by hosts that predate the mode,
so the declaration is safe against every host.

## Conformance

The SDK consumes the embedded shared vectors
(`github.com/brokenbots/criteria-adapter-proto/conformance`) — the same
byte-identical fixtures the engine suite and the other language SDKs run —
plus loopback bridge vectors in
[`adapterhost/outcome_e2e_test.go`](../adapterhost/outcome_e2e_test.go):
typed hand-off, verbatim push, pre-flight refusal with zero results, and the
full repair framing round-trip, on both tiers. Run them with the usual suite:

```
go test ./adapterhost -run 'TestOutcomeContract|TestSpecTierContract|TestTier2Contract' -count=1
```

## References

- Pinned evaluator + wire fields: `criteria/v2/outcome_contract.go` and the
  ExecuteRequest contract fields in the pinned
  `criteria-adapter-proto` (documented exception in
  [dependency-policy.md](dependency-policy.md); the pin carries the full
  v0.7.0 contract surface).
- Host-side split (in-session `outcome.payload_invalid` vs post-finalize
  rejection): `docs/adapters.md` in the pinned proto module.
