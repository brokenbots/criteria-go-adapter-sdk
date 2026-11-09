// End-to-end contract-mode executes over the real loopback gRPC bridge:
// typed contracts and rejection contexts survive the tier-1 and tier-2
// hand-offs, pre-flight enforcement keeps contract-losing results off the
// wire, and — deliberately — a contract-losing result an author did NOT
// pre-flight still rides verbatim (the host's post-finalize validation is
// the backstop; the SDK neither rewrites nor silently rejects).
package adapterhost

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// contractModeSpec builds a summarize-shaped spec whose Execute handler
// calls execute with the 1-based call index (the sequential loopback bridge
// serves one Execute at a time), capturing every handler input it saw.
func contractModeSpec(execute func(call int, in *ExecuteInput) (*ExecuteResult, error)) (*Spec, *[]*ExecuteInput) {
	handoffs := &[]*ExecuteInput{}
	call := 0
	spec := &Spec{
		Name:         "summarize",
		Version:      "1.0.0",
		InputSchema:  Schema{Field("path", SchemaTypeString, true, "what to summarize")},
		Capabilities: []string{CapabilityOutcomeContracts},
		Execute: func(_ context.Context, in *ExecuteInput) (*ExecuteResult, error) {
			call++
			*handoffs = append(*handoffs, in)
			return execute(call, in)
		},
	}
	return spec, handoffs
}

// contractModeWire is the host-shaped request carrying the pinned summarize
// contract set (require_comment included, like the shared vectors).
func contractModeWire() *v2.ExecuteRequest {
	return &v2.ExecuteRequest{
		SessionId: "sess-contract",
		StepName:  "summarize",
		Input:     map[string]string{"path": "src"},
		OutcomeContracts: []*v2.OutcomeContract{
			{Name: "completed", SchemaJson: []byte(summarizeSchema), RequireComment: true},
		},
	}
}

func validOutputs() map[string]any {
	return map[string]any{"summary": "3 files linted", "exit_code": 0}
}
func invalidOutputs() map[string]any { return map[string]any{"summary": 3, "exit_code": "0"} }

// TestSpecTierContractModeAcceptedOverWire proves the full tier-1 hand-off:
// the handler sees the step's contracts typed as plain mirrors, the
// negotiation capability rides Info over the bridge, and a contract-valid
// result is pushed byte-identically (verbatim, no rewriting).
func TestSpecTierContractModeAcceptedOverWire(t *testing.T) {
	spec, handoffs := contractModeSpec(func(_ int, in *ExecuteInput) (*ExecuteResult, error) {
		if !in.OutcomeContracts.InContractMode() || in.Rejection != nil {
			return nil, fmt.Errorf("fixture expects bare contract mode, got contracts=%v rejection=%+v",
				in.OutcomeContracts.InContractMode(), in.Rejection)
		}
		contract := in.OutcomeContracts.For("completed")
		if contract == nil || !contract.RequireComment || contract.SchemaJSON != summarizeSchema {
			return nil, fmt.Errorf("typed hand-off mismatched: %+v", contract)
		}
		return &ExecuteResult{Outcome: "completed", Comment: "summarized 3 files", Outputs: validOutputs()}, nil
	})
	client := specServerForTest(t, spec)

	info, err := client.Info(context.Background(), &v2.InfoRequest{})
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if !slicesContains(info.GetCapabilities(), CapabilityOutcomeContracts) {
		t.Errorf("Info.Capabilities = %v; want %q negotiated", info.GetCapabilities(), CapabilityOutcomeContracts)
	}

	frags, res, err := driveSpecExecute(context.Background(), client, contractModeWire(), 2*time.Second)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.GetOutcome() != "completed" {
		t.Errorf("result = %+v", res)
	}
	// The test driver's joiner reads the chunked channel (outcome +
	// outputs_json); the comment rides the single base fragment.
	if len(frags) != 1 || frags[0].GetComment() != "summarized 3 files" {
		t.Errorf("result fragments = %+v; want one with the finalize comment", frags)
	}
	want, err := v2.MarshalOutputs(validOutputs())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(res.GetOutputsJson(), want) {
		t.Errorf("outputs_json = %s; want the handler's own serialization %s", res.GetOutputsJson(), want)
	}
	if len(*handoffs) != 1 {
		t.Fatalf("handler saw %d calls; want 1", len(*handoffs))
	}
}

// TestSpecTierContractModePushesInvalidResultVerbatim pins the deliberate
// non-behavior: an author (or pre-v0.7.0 host semantics) whose Execute
// returns a contract-losing result WITHOUT pre-flighting still gets that
// result across the bridge untouched — the host's post-finalize evaluator
// is the rejection authority, so this is exactly the shape it backstops.
func TestSpecTierContractModePushesInvalidResultVerbatim(t *testing.T) {
	spec, _ := contractModeSpec(func(_ int, _ *ExecuteInput) (*ExecuteResult, error) {
		// Mirrors the shared 02_contract_payload_invalid payload.
		return &ExecuteResult{Outcome: "completed", Comment: "oops", Outputs: invalidOutputs()}, nil
	})
	client := specServerForTest(t, spec)

	frags, res, err := driveSpecExecute(context.Background(), client, contractModeWire(), 2*time.Second)
	if err != nil {
		t.Fatalf("Execute: %v; want the contract-losing result delivered, not rejected", err)
	}
	if res.GetOutcome() != "completed" || len(frags) != 1 || frags[0].GetComment() != "oops" {
		t.Errorf("result = %v / fragments = %+v", res, frags)
	}
	want, err := v2.MarshalOutputs(invalidOutputs())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(res.GetOutputsJson(), want) {
		t.Errorf("outputs_json = %s; want the contract-losing payload verbatim %s", res.GetOutputsJson(), want)
	}
	if len(frags) != 1 {
		t.Errorf("delivered %d fragments; want one untouched result", len(frags))
	}
}

// TestSpecTierContractModePreflightRejection pins the pre-flight lane: the
// handler validates its candidate through the typed hand-off and refuses to
// return a contract-losing result — zero results ride the stream and the
// step fails loudly, one round-trip before the host's post-finalize
// backstop would have rejected it.
func TestSpecTierContractModePreflightRejection(t *testing.T) {
	spec, _ := contractModeSpec(func(_ int, in *ExecuteInput) (*ExecuteResult, error) {
		candidate := &ExecuteResult{Outcome: "completed", Comment: "oops", Outputs: invalidOutputs()}
		issues, err := in.OutcomeContracts.ValidateResult(candidate)
		if err != nil {
			return nil, err
		}
		if len(issues) > 0 {
			return nil, fmt.Errorf("refusing contract-invalid result: %s", strings.Join(issues, "; "))
		}
		return candidate, nil
	})
	client := specServerForTest(t, spec)

	frags, _, err := driveSpecExecute(context.Background(), client, contractModeWire(), 2*time.Second)
	if err == nil || !strings.Contains(err.Error(), `payload_schema: property "summary": expected "string", got "number"`) {
		t.Fatalf("Execute err = %v; want the pinned pre-flight issue named", err)
	}
	if len(frags) != 0 {
		t.Errorf("stream delivered %d results; want none from a refused pre-flight", len(frags))
	}
}

// TestSpecTierContractModeRepairHandoff closes the repair loop end to end:
// phase 1 delivers (verbatim) the payload that loses its contract; the
// host-shaped second execute frames the rejection with the SDK builder from
// the pinned issues of that payload; phase 2 delivers it typed on the
// next ExecuteInput and the repaired payload rides verbatim.
func TestSpecTierContractModeRepairHandoff(t *testing.T) {
	spec, handoffs := contractModeSpec(func(call int, in *ExecuteInput) (*ExecuteResult, error) {
		if call == 1 {
			return &ExecuteResult{Outcome: "completed", Comment: "oops", Outputs: invalidOutputs()}, nil
		}
		return &ExecuteResult{Outcome: "completed", Comment: "repaired payload", Outputs: validOutputs()}, nil
	})
	client := specServerForTest(t, spec)

	_, _, err := driveSpecExecute(context.Background(), client, contractModeWire(), 2*time.Second)
	if err != nil {
		t.Fatalf("phase-1 Execute: %v", err)
	}

	// Host framing: the pinned evaluator's issues for the delivered payload,
	// framed exactly like the host would frame the next attempt.
	issueIssues := ValidateOutcomeResult(contractModeWire().GetOutcomeContracts(),
		&v2.ExecuteResult{Outcome: "completed", Comment: "oops",
			OutputsJson: mustMarshalOutputs(t, invalidOutputs())})
	rejection := NewExecutionRejection("completed", issueIssues, nil)

	req := contractModeWire()
	req.Rejection = rejection
	_, res, err := driveSpecExecute(context.Background(), client, req, 2*time.Second)
	if err != nil {
		t.Fatalf("phase-2 Execute: %v", err)
	}
	if len(*handoffs) != 2 {
		t.Fatalf("handler saw %d calls; want 2", len(*handoffs))
	}
	in := (*handoffs)[1]
	if in.Rejection == nil {
		t.Fatal("repair execute delivered no typed rejection on the hand-off")
	}
	if in.Rejection.Attempt != rejection.GetAttempt() || in.Rejection.Outcome != rejection.GetOutcome() ||
		in.Rejection.Issues != rejection.GetIssues() {
		t.Errorf("typed rejection = %+v; want the host framing %+v", in.Rejection, rejection)
	}
	if len(issueIssues) == 0 {
		t.Error("test bookkeeping broken: phase-1 payload carried no pinned issues")
	}
	want, err := v2.MarshalOutputs(validOutputs())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(res.GetOutputsJson(), want) {
		t.Errorf("repaired outputs_json = %s; want %s", res.GetOutputsJson(), want)
	}
}

// TestTier2ContractModeHelperUse proves the tier-2 surface: the raw request
// carries the typed wire contracts, and ValidateOutcomeResult /
// NewExecutionRejection enforce pre-flight and frame repairs without the
// custom Service re-deriving any evaluator logic.
func TestTier2ContractModeHelperUse(t *testing.T) {
	svc := &tier2ContractAdapter{}
	client := serveAdapterOnLoopbackForTest(t, svc)

	// Accepted: a contract-valid result pushes verbatim.
	req := contractModeWire()
	ev, err := v2.NewExecuteResultEvent("completed", validOutputs())
	if err != nil {
		t.Fatal(err)
	}
	svc.next = func(req *v2.ExecuteRequest, sender ExecuteEventSender) error { return sender.Send(ev) }
	if _, res, err := driveSpecExecute(context.Background(), client, req, 2*time.Second); err != nil || res.GetOutcome() != "completed" {
		t.Fatalf("tier-2 accepted execute = (%+v, %v)", res, err)
	}

	// Pre-flight: the issue list drives both a refusal and the same framing
	// the host would ride the next Execute.
	issueIssues := ValidateOutcomeResult(req.GetOutcomeContracts(),
		&v2.ExecuteResult{Outcome: "completed", Comment: "oops", OutputsJson: mustMarshalOutputs(t, invalidOutputs())})
	if len(issueIssues) == 0 {
		t.Fatal("bookkeeping broken: the contract-losing candidate produced no issues")
	}
	svc.next = func(req *v2.ExecuteRequest, sender ExecuteEventSender) error {
		candidate := &v2.ExecuteResult{Outcome: "completed", Comment: "oops", OutputsJson: mustMarshalOutputs(t, invalidOutputs())}
		if issues := ValidateOutcomeResult(req.GetOutcomeContracts(), candidate); len(issues) > 0 {
			return fmt.Errorf("tier-2 pre-flight refused: %s", strings.Join(issues, "; "))
		}
		return sender.Send(&v2.ExecuteEvent{Event: &v2.ExecuteEvent_Result{Result: candidate}})
	}
	if _, res, err := driveSpecExecute(context.Background(), client, req, 2*time.Second); err == nil || res != nil {
		t.Fatalf("tier-2 pre-flight = (%v, %v); want refusal with zero results", res, err)
	} else if !strings.Contains(err.Error(), `payload_schema: property "exit_code": expected "number", got "string"`) {
		t.Fatalf("tier-2 pre-flight err = %v; want the pinned issue named", err)
	}

	// Repair framing: the rejection context the host would send next arrives
	// on the raw request, and the typed mirror matches.
	req.Rejection = NewExecutionRejection("completed", issueIssues, nil)
	svc.next = func(req *v2.ExecuteRequest, sender ExecuteEventSender) error {
		got := rejectionFromWire(req.GetRejection())
		if got == nil || got.Attempt != req.GetRejection().GetAttempt() || got.Issues != req.GetRejection().GetIssues() {
			return fmt.Errorf("hand-off mismatch: %+v vs %v", got, req.GetRejection())
		}
		return sender.Send(&v2.ExecuteEvent{Event: &v2.ExecuteEvent_Result{Result: &v2.ExecuteResult{
			Outcome: "completed", Comment: "repaired payload", OutputsJson: mustMarshalOutputs(t, validOutputs()),
		}}})
	}
	if _, res, err := driveSpecExecute(context.Background(), client, req, 2*time.Second); err != nil || res.GetOutcome() != "completed" {
		t.Fatalf("tier-2 rejection hand-off = (%v, %v)", res, err)
	}
	if len(svc.requests) < 3 {
		t.Fatalf("tier-2 service saw %d executes; want 3", len(svc.requests))
	}
}

func mustMarshalOutputs(t *testing.T, outputs map[string]any) []byte {
	t.Helper()
	raw, err := v2.MarshalOutputs(outputs)
	if err != nil {
		t.Fatalf("MarshalOutputs: %v", err)
	}
	return raw
}

// serveAdapterOnLoopbackForTest serves a custom-Service adapter on the
// conformance loopback harness.
func serveAdapterOnLoopbackForTest(t *testing.T, svc Service) v2.AdapterServiceClient {
	t.Helper()
	client, stop, err := serveAdapterOnLoopback(svc)
	if err != nil {
		t.Fatalf("serveAdapterOnLoopback: %v", err)
	}
	t.Cleanup(stop)
	return client
}

func slicesContains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// tier2ContractAdapter is the custom-Service fixture (the conformance base
// plus a scripted Execute that records each request).
type tier2ContractAdapter struct {
	conformanceBaseService
	mu       sync.Mutex
	requests []*v2.ExecuteRequest
	next     func(req *v2.ExecuteRequest, sender ExecuteEventSender) error
}

func (a *tier2ContractAdapter) Info(context.Context, *v2.InfoRequest) (*v2.InfoResponse, error) {
	return &v2.InfoResponse{Name: "contract-tier2", Version: "1.0.0", SdkProtocolVersion: "2",
		Capabilities: []string{CapabilityOutcomeContracts}}, nil
}

func (a *tier2ContractAdapter) Execute(ctx context.Context, req *v2.ExecuteRequest, sender ExecuteEventSender) error {
	a.mu.Lock()
	a.requests = append(a.requests, req)
	next := a.next
	a.mu.Unlock()
	return next(req, sender)
}
