// The SDK-side outcome-contract surface: typed hand-off, negotiation, and
// pre-flight enforcement helpers for the per-outcome contracts the host pins
// onto every Execute since criteria-adapter-proto v0.7.0 (conformance
// README). The pinned proto evaluator
// ([v2.EvaluateOutcomeContracts]) remains the single validation algorithm;
// these helpers wrap it so adapter authors never hand-roll payload-schema
// checks, contract lookup, or rejection framing (the re-derivation the
// engine-side ruling calls out). The host's post-finalize validation stays
// the backstop: a result that was not pre-flighted (tier-1 handlers that
// skip [OutcomeContracts.ValidateResult]) is still checked after finalize.
package adapterhost

import (
	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// CapabilityOutcomeContracts is the InfoResponse.capabilities value an
// adapter declares when it speaks the outcome-contract mode (contract
// hand-off, repair framing, pre-flight enforcement). Capability strings are
// free-form on the wire and unknown ones are ignored by hosts that predate
// the surface, so declaring it is safe against every host; hosts that honor
// it use the declaration as the negotiation point for driving the step in
// contract mode instead of name-only allowed_outcomes.
const CapabilityOutcomeContracts = "outcome_contracts"

// OutcomeContract is the spec-tier mirror of the wire outcome contract. For
// tier-1 adapters the contracts a step is executed under arrive populated on
// [ExecuteInput.OutcomeContracts] with exactly these fields.
type OutcomeContract struct {
	// Name is the outcome the contract governs; it must be one of the
	// step's declared (allowed_outcomes) names.
	Name string
	// SchemaJSON is the payload schema for the outcome's outputs as a
	// serialized JSON Schema document (the pinned evaluator understands the
	// required-array/presence-before-type subset; see
	// [v2.ValidatePayloadSchema]). Empty means the outcome carries no
	// payload constraint.
	SchemaJSON string
	// Fallback marks the outcome the host finalizes a zero-result step
	// with (no payload, no comment). At most one contract per step may set
	// it; which entry wins is the host's compile-time concern.
	Fallback bool
	// RequireComment requires a non-empty finalize comment for the
	// outcome.
	RequireComment bool
}

// OutcomeContracts is one step's ordered outcome-contract set.
type OutcomeContracts []OutcomeContract

// InContractMode reports whether the step carries outcome contracts. Empty
// (the zero value, and every pre-v0.7.0 host) means legacy mode: results
// ride the wire untouched and only the host's name-only allowed_outcomes
// check applies.
func (cs OutcomeContracts) InContractMode() bool { return len(cs) > 0 }

// For returns the first contract governing the named outcome, or nil when
// no entry matches an outcome the handler intends to produce. Lookup is
// first-entry-wins, mirroring the pinned evaluator's [v2.contractFor]
// semantics — duplicate names in one step's set are a host compile error.
func (cs OutcomeContracts) For(outcome string) *OutcomeContract {
	for i := range cs {
		if cs[i].Name == outcome {
			return &cs[i]
		}
	}
	return nil
}

// ValidateResult pre-flights a candidate [ExecuteResult] against the
// step's outcome contracts (contract mode only; legacy mode always
// validates clean). Adapters call it BEFORE handing the result back — the
// engine will otherwise run the same pinned evaluator post-finalize, reject,
// and re-execute with a repair rejection, so a known-bad payload can be
// caught one round-trip earlier. A non-empty issue list means the result
// as-is will be rejected; each string is from the pinned issue vocabulary
// (see the pinned evaluator's doc). The serialization error is non-nil only
// when the result's Outputs cannot ride the wire's typed outputs_json
// channel at all (that failure is outside the pinned contract vocabulary).
//
// A nil result exercises the host's zero-result path: acceptance when the
// step declares a fallback outcome, rejection ("no_result") otherwise.
func (cs OutcomeContracts) ValidateResult(res *ExecuteResult) ([]string, error) {
	if !cs.InContractMode() {
		return nil, nil
	}
	var wire *v2.ExecuteResult
	if res != nil {
		outputsJSON, err := v2.MarshalOutputs(res.Outputs)
		if err != nil {
			return nil, err
		}
		wire = &v2.ExecuteResult{Outcome: res.Outcome, Comment: res.Comment, OutputsJson: outputsJSON}
	}
	return ValidateOutcomeResult(cs.wire(), wire), nil
}

// Rejection is the spec-tier mirror of the host's post-rejection repair
// framing. Its fields are the rejection the host attaches to the NEXT
// Execute of the same step after a contract-invalid finalization: the
// attempt counter (1-based repair count), the outcome that produced the
// rejected result, and Issues — the joined issue text adapters quote into
// repair prompts but must never parse.
type Rejection struct {
	// Attempt is the 1-based repair counter for the step; it increments
	// every re-execute that follows a contract rejection.
	Attempt uint32
	// Outcome is the outcome the rejected result finalized with.
	Outcome string
	// Issues is the newline-joined pinned issue list describing why the
	// prior result was rejected.
	Issues string
}

// wire converts the plain mirror to the wire type the pinned helpers and a
// tier-2 hand-off consume.
func (r *Rejection) wire() *v2.ExecutionRejection {
	if r == nil {
		return nil
	}
	return &v2.ExecutionRejection{Outcome: r.Outcome, Issues: r.Issues, Attempt: r.Attempt}
}

// wire converts the plain mirrors to the wire contracts
// [ValidateOutcomeResult] and the tier-2 Execute request consume.
func (cs OutcomeContracts) wire() []*v2.OutcomeContract {
	wire := make([]*v2.OutcomeContract, 0, len(cs))
	for _, c := range cs {
		wire = append(wire, &v2.OutcomeContract{
			Name:           c.Name,
			SchemaJson:     []byte(c.SchemaJSON),
			Fallback:       c.Fallback,
			RequireComment: c.RequireComment,
		})
	}
	return wire
}

// ValidateOutcomeResult pre-flights one finalized wire result against the
// step's outcome contracts: a nil issue list means the result is safe to
// push across the Execute stream as-is, a non-empty one is the pinned,
// ordered issue list the host would reject the result with after finalize.
// It is the SDK wrapper around the pinned evaluator
// ([v2.EvaluateOutcomeContracts]): contract mode (non-empty contracts)
// enforces the result through that evaluator, legacy mode (empty or nil
// contracts) rejects nothing — byte-for-byte pre-v0.7.0 behavior. The
// result's outputs_json is expected in final wire form; tier-1 authors use
// [OutcomeContracts.ValidateResult] to keep native Outputs maps.
//
// Name-only allowed_outcomes membership stays the host's attempt-loop
// concern and is NOT re-enforced here (the synthetic request carries no
// list), so a result never produces the host's outcome_not_allowed issue
// via this helper. A nil result exercises the host's zero-result path:
// acceptance under the step's fallback contract, rejection ("no_result")
// without one.
func ValidateOutcomeResult(contracts []*v2.OutcomeContract, res *v2.ExecuteResult) []string {
	if len(contracts) == 0 {
		// Legacy mode: nothing to enforce.
		return nil
	}
	req := &v2.ExecuteRequest{OutcomeContracts: contracts}
	var results []*v2.ExecuteResult
	if res != nil {
		results = []*v2.ExecuteResult{res}
	}
	_, issues := v2.EvaluateOutcomeContracts(req, results)
	return issues
}

// ContractFor returns the first contract on the wire set governing the
// named outcome, or nil when the step covers none — the wire twin of
// [OutcomeContracts.For], for tier-2 authors matching their candidate
// result against the request's contracts.
func ContractFor(contracts []*v2.OutcomeContract, outcome string) *v2.OutcomeContract {
	for _, c := range contracts {
		if c.GetName() == outcome {
			return c
		}
	}
	return nil
}

// NewExecutionRejection builds the ExecutionRejection the host attaches to
// the NEXT Execute of a step after a contract-invalid result: it wraps
// [v2.NewExecutionRejection] (attempt increments the prior rejection's
// attempt — a fresh step with a nil prior starts at 1 — and the issues are
// joined with "\n" into the diagnostic text). Tier-1 authors frame repairs
// themselves only when simulating the host loop; the plain mirror arrives
// on [ExecuteInput.Rejection] already.
func NewExecutionRejection(outcome string, issues []string, prior *v2.ExecutionRejection) *v2.ExecutionRejection {
	return v2.NewExecutionRejection(outcome, issues, prior)
}

// outcomeContractsFromWire converts the request's wire contracts to the
// spec-tier mirrors. The plain mirrors are freshly built values, decoupled
// from any later mutation of the request struct by the bridging tiers.
func outcomeContractsFromWire(wire []*v2.OutcomeContract) OutcomeContracts {
	if len(wire) == 0 {
		return nil
	}
	cs := make(OutcomeContracts, 0, len(wire))
	for _, c := range wire {
		cs = append(cs, OutcomeContract{
			Name:           c.GetName(),
			SchemaJSON:     string(c.GetSchemaJson()),
			Fallback:       c.GetFallback(),
			RequireComment: c.GetRequireComment(),
		})
	}
	return cs
}

// rejectionFromWire converts the request's repair framing to the plain
// mirror; nil stays nil (legacy and non-repair executes).
func rejectionFromWire(wire *v2.ExecutionRejection) *Rejection {
	if wire == nil {
		return nil
	}
	return &Rejection{Attempt: wire.GetAttempt(), Outcome: wire.GetOutcome(), Issues: wire.GetIssues()}
}
