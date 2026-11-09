package adapterhost

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/brokenbots/criteria-adapter-proto/conformance"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// summarizeSchema is the pinned payload schema from the shared conformance
// vectors: summary must be a string, exit_code a number, both required.
const summarizeSchema = `{"type":"object","required":["summary","exit_code"],"properties":{"summary":{"type":"string"},"exit_code":{"type":"number"}}}`

// summarizeContracts is the wire contract set for the schema above.
func summarizeContracts() []*v2.OutcomeContract {
	return []*v2.OutcomeContract{{Name: "completed", SchemaJson: []byte(summarizeSchema)}}
}

func summarizeResult(summary any, exitCode any) *v2.ExecuteResult {
	outputs := map[string]any{"summary": summary, "exit_code": exitCode}
	raw, err := json.Marshal(outputs)
	if err != nil {
		panic(err)
	}
	return &v2.ExecuteResult{Outcome: "completed", Comment: "summarized 3 files", OutputsJson: raw}
}

// TestValidateOutcomeResultLegacyMode pins the mode gate: no contracts on
// the request means byte-for-byte pre-v0.7.0 behavior — nothing is
// enforced, whatever the result contains.
func TestValidateOutcomeResultLegacyMode(t *testing.T) {
	for _, contracts := range [][]*v2.OutcomeContract{nil, {}} {
		issues := ValidateOutcomeResult(contracts, &v2.ExecuteResult{})
		if issues != nil {
			t.Errorf("contracts %v: legacy mode must enforce nothing, got issues %q", contracts, issues)
		}
	}
}

// TestValidateOutcomeResultContractMode walks the pinned issue lanes of the
// evaluator through the SDK wrapper.
func TestValidateOutcomeResultContractMode(t *testing.T) {
	t.Run("valid payload accepted", func(t *testing.T) {
		if issues := ValidateOutcomeResult(summarizeContracts(), summarizeResult("3 files linted", 0)); issues != nil {
			t.Errorf("valid result rejected: %q", issues)
		}
	})
	t.Run("payload schema invalid", func(t *testing.T) {
		// Mirrors the shared 02_contract_payload_invalid vector byte-for-byte.
		got := ValidateOutcomeResult(summarizeContracts(), summarizeResult(3, "0"))
		want := []string{
			`payload_schema: property "summary": expected "string", got "number"`,
			`payload_schema: property "exit_code": expected "number", got "string"`,
		}
		if !slices.Equal(got, want) {
			t.Errorf("issues = %q; want %q", got, want)
		}
	})
	t.Run("missing required payload keys", func(t *testing.T) {
		raw, err := json.Marshal(map[string]any{"summary": "3 files"})
		if err != nil {
			t.Fatal(err)
		}
		got := ValidateOutcomeResult(summarizeContracts(), &v2.ExecuteResult{Outcome: "completed", Comment: "c", OutputsJson: raw})
		want := []string{
			"payload_schema: property \"exit_code\": required property is missing",
		}
		if !slices.Equal(got, want) {
			t.Errorf("issues = %q; want %q", got, want)
		}
	})
	t.Run("require_comment", func(t *testing.T) {
		// Mirrors the shared 03_require_comment_missing vector byte-for-byte.
		contracts := []*v2.OutcomeContract{{Name: "completed", RequireComment: true}}
		got := ValidateOutcomeResult(contracts, &v2.ExecuteResult{Outcome: "completed"})
		want := []string{`missing_comment: outcome "completed" requires a comment (require_comment)`}
		if !slices.Equal(got, want) {
			t.Errorf("issues = %q; want %q", got, want)
		}
		if issues := ValidateOutcomeResult(contracts, &v2.ExecuteResult{Outcome: "completed", Comment: "done"}); issues != nil {
			t.Errorf("commented result rejected: %q", issues)
		}
	})
	t.Run("empty outcome", func(t *testing.T) {
		got := ValidateOutcomeResult(summarizeContracts(), &v2.ExecuteResult{Comment: "no outcome"})
		want := []string{`empty_outcome: result has no outcome`}
		if !slices.Equal(got, want) {
			t.Errorf("issues = %q; want %q", got, want)
		}
	})
	t.Run("outcome uncontracted", func(t *testing.T) {
		got := ValidateOutcomeResult(summarizeContracts(), &v2.ExecuteResult{Outcome: "failed"})
		want := []string{`outcome_uncontracted: outcome "failed" has no outcome_contracts entry`}
		if !slices.Equal(got, want) {
			t.Errorf("issues = %q; want %q", got, want)
		}
	})
	t.Run("fallback fires with no result", func(t *testing.T) {
		// Mirrors the shared 04_fallback_fires vector: a zero-result step
		// finalizes with the declared fallback — no issues to surface.
		contracts := []*v2.OutcomeContract{{Name: "completed", Fallback: true}}
		if issues := ValidateOutcomeResult(contracts, nil); issues != nil {
			t.Errorf("fallback step rejected: %q", issues)
		}
	})
	t.Run("no result and no fallback", func(t *testing.T) {
		got := ValidateOutcomeResult(summarizeContracts(), nil)
		want := []string{"no_result: step ended without a finalized result"}
		if !slices.Equal(got, want) {
			t.Errorf("issues = %q; want %q", got, want)
		}
	})
}

// TestContractFor pins first-entry-wins lookup on the wire surface, the
// semantics the pinned evaluator applies internally.
func TestContractFor(t *testing.T) {
	contracts := []*v2.OutcomeContract{
		{Name: "failed", RequireComment: true},
		{Name: "completed", SchemaJson: []byte(summarizeSchema)},
		{Name: "completed", Fallback: true},
	}
	for _, tc := range []struct {
		outcome string
		want    *v2.OutcomeContract
	}{
		{"completed", contracts[1]},
		{"failed", contracts[0]},
		{"unknown", nil},
	} {
		got := ContractFor(contracts, tc.outcome)
		if got != tc.want && (got == nil || tc.want == nil || !proto.Equal(got, tc.want)) {
			t.Errorf("ContractFor(%q) = %v; want %v", tc.outcome, got, tc.want)
		}
	}
	if ContractFor(nil, "completed") != nil {
		t.Error("ContractFor on empty set must return nil")
	}
}

// TestNewExecutionRejection pins the host repair framing: attempt increments
// the prior rejection's counter (fresh steps start at 1) and issues are
// joined with newlines.
func TestNewExecutionRejection(t *testing.T) {
	issues := []string{
		`payload_schema: property "summary": expected "string", got "boolean"`,
		"missing_comment: outcome \"completed\" requires a comment (require_comment)",
	}
	fresh := NewExecutionRejection("completed", issues, nil)
	if fresh.GetAttempt() != 1 || fresh.GetOutcome() != "completed" {
		t.Errorf("fresh rejection = %+v", fresh)
	}
	if fresh.GetIssues() != strings.Join(issues, "\n") {
		t.Errorf("fresh issues = %q", fresh.GetIssues())
	}
	repaired := NewExecutionRejection("failed", []string{"no_result: step ended without a finalized result"}, fresh)
	if repaired.GetAttempt() != 2 {
		t.Errorf("repair attempt = %d; want 2", repaired.GetAttempt())
	}
}

// TestOutcomeContractsPlainMirrors pins the spec-tier conversion and
// lookup surface.
func TestOutcomeContractsPlainMirrors(t *testing.T) {
	plain := OutcomeContracts{
		{Name: "completed", SchemaJSON: summarizeSchema, RequireComment: true},
		{Name: "failed", Fallback: true},
	}
	if !plain.InContractMode() {
		t.Error("non-empty set must be contract mode")
	}
	if got := plain.For("completed"); got != &plain[0] {
		t.Errorf("For(completed) = %v; want the first entry", got)
	}
	if got := plain.For("unknown"); got != nil {
		t.Errorf("For(unknown) = %v; want nil", got)
	}
	if (OutcomeContracts{}).InContractMode() {
		t.Error("empty set must be legacy mode")
	}
	if got := (OutcomeContracts{}).For("completed"); got != nil {
		t.Errorf("empty For = %v; want nil", got)
	}

	wire := plain.wire()
	if len(wire) != 2 || wire[0].GetName() != "completed" || !wire[0].GetRequireComment() {
		t.Fatalf("wire contracts = %+v", wire)
	}
	if string(wire[0].GetSchemaJson()) != summarizeSchema || !wire[1].GetFallback() || wire[1].GetRequireComment() {
		t.Errorf("wire contracts = %+v / %+v", wire[0], wire[1])
	}
	if got := outcomeContractsFromWire(wire); !reflect.DeepEqual(got, plain) {
		t.Errorf("wire→plain round trip = %+v; want %+v", got, plain)
	}
	if got := outcomeContractsFromWire(nil); got != nil {
		t.Errorf("wire→plain of nil = %v; want nil", got)
	}

	rej := rejectionFromWire(&v2.ExecutionRejection{Outcome: "completed", Issues: "issue-a\nissue-b", Attempt: 3})
	if rej == nil || rej.Attempt != 3 || rej.Outcome != "completed" || rej.Issues != "issue-a\nissue-b" {
		t.Errorf("rejectionFromWire = %+v", rej)
	}
	var nilRej *Rejection
	if nilRej.wire() != nil {
		t.Error("nil rejection wire conversion must stay nil")
	}
	if !proto.Equal(rej.wire(), &v2.ExecutionRejection{Outcome: "completed", Issues: "issue-a\nissue-b", Attempt: 3}) {
		t.Errorf("rejection wire round trip = %+v", rej.wire())
	}
	if rejectionFromWire(nil) != nil {
		t.Error("nil wire rejection conversion must stay nil")
	}
}

// TestSpecTierValidateResult pins the tier-1 pre-flight helper: native
// outputs maps are serialized and validated through the same pinned
// evaluator, a wire-hostile outputs value surfaces as the serialization
// error, and legacy mode stays silent.
func TestSpecTierValidateResult(t *testing.T) {
	contracts := OutcomeContracts{{Name: "completed", SchemaJSON: summarizeSchema}}
	res := &ExecuteResult{Outcome: "completed", Comment: "summarized 3 files",
		Outputs: map[string]any{"summary": "3 files linted", "exit_code": 0}}
	if issues, err := contracts.ValidateResult(res); issues != nil || err != nil {
		t.Errorf("valid result pre-flight = %q, %v", issues, err)
	}
	res.Outputs["summary"] = 3
	issues, err := contracts.ValidateResult(res)
	if err != nil {
		t.Fatalf("pre-flight error: %v", err)
	}
	want := []string{`payload_schema: property "summary": expected "string", got "number"`}
	if !slices.Equal(issues, want) {
		t.Errorf("issues = %q; want %q", issues, want)
	}
	res.Outputs["summary"] = "3 files linted"
	res.Outputs["poison"] = make(chan int)
	if issues, err := contracts.ValidateResult(res); err == nil || issues != nil {
		t.Errorf("wire-hostile outputs = %q, %v; want a serialization error", issues, err)
	}

	// A nil result exercises the zero-result path.
	if issues, err := contracts.ValidateResult(nil); issues == nil || err != nil {
		t.Errorf("nil result = %q, %v; want the no_result issue", issues, err)
	}

	legacy := OutcomeContracts{}
	if issues, err := legacy.ValidateResult(res); issues != nil || err != nil {
		t.Errorf("legacy mode pre-flight = %q, %v", issues, err)
	}
}

// TestCapabilityOutcomeContractsNegotiation pins the negotiation surface:
// the contract capability is an ordinary capabilities entry — it rides Info
// verbatim and publishes under the manifest capabilities list.
func TestCapabilityOutcomeContractsNegotiation(t *testing.T) {
	spec := echoSpec(func(s *Spec) {
		s.Capabilities = []string{CapabilityOutcomeContracts}
	})
	info, err := deriveInfo(spec)
	if err != nil {
		t.Fatalf("deriveInfo: %v", err)
	}
	if !slices.Contains(info.GetCapabilities(), CapabilityOutcomeContracts) {
		t.Errorf("Info.Capabilities = %v; want %q declared", info.GetCapabilities(), CapabilityOutcomeContracts)
	}
	doc := buildManifestDoc(info)
	if !slices.Contains(doc.Capabilities, CapabilityOutcomeContracts) {
		t.Errorf("manifest capabilities = %v; want %q published", doc.Capabilities, CapabilityOutcomeContracts)
	}
}

// contractVector is the shared vector document shape
// (criteria-adapter-proto/conformance).
type contractVector struct {
	Name   string                `json:"name"`
	Phases []contractVectorPhase `json:"phases"`
}

type contractVectorPhase struct {
	Request json.RawMessage      `json:"request"`
	Results []json.RawMessage    `json:"results"`
	Expect  contractVectorExpect `json:"expect"`
}

type contractVectorExpect struct {
	Accepted  bool                     `json:"accepted"`
	Issues    []string                 `json:"issues"`
	Rejection *contractVectorRejection `json:"rejection"`
}

type contractVectorRejection struct {
	Attempt int    `json:"attempt"`
	Issues  string `json:"issues"`
	Outcome string `json:"outcome"`
}

// TestOutcomeContractSharedVectors runs every embedded shared conformance
// vector against the SDK helpers: byte-identical fixtures at the pinned
// release (the same files the engine suite and the other language SDKs
// consume), so SDK enforcement can never drift from the pinned evaluator.
func TestOutcomeContractSharedVectors(t *testing.T) {
	names, err := conformance.VectorNames()
	if err != nil {
		t.Fatalf("VectorNames: %v", err)
	}
	if len(names) == 0 {
		t.Fatal("no shared conformance vectors found at the pinned release")
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			raw, err := conformance.ReadVector(name)
			if err != nil {
				t.Fatalf("ReadVector(%s): %v", name, err)
			}
			var vec contractVector
			if err := json.Unmarshal(raw, &vec); err != nil {
				t.Fatalf("parse vector: %v", err)
			}
			if len(vec.Phases) == 0 {
				t.Fatalf("vector %s has no phases", name)
			}
			for pi, phase := range vec.Phases {
				req := &v2.ExecuteRequest{}
				if err := protojson.Unmarshal(phase.Request, req); err != nil {
					t.Fatalf("phase %d: decode request: %v", pi, err)
				}
				var res *v2.ExecuteResult
				if len(phase.Results) > 1 {
					t.Fatalf("phase %d: the SDK helper enforces the single terminal result", pi)
				}
				if len(phase.Results) == 1 {
					res = &v2.ExecuteResult{}
					if err := protojson.Unmarshal(phase.Results[0], res); err != nil {
						t.Fatalf("phase %d: decode result: %v", pi, err)
					}
				}

				got := ValidateOutcomeResult(req.GetOutcomeContracts(), res)
				if phase.Expect.Accepted {
					if got != nil {
						t.Errorf("phase %d: accepted expected, got issues %q", pi, got)
					}
				} else {
					if len(phase.Expect.Issues) == 0 {
						t.Fatalf("phase %d: rejected expectation carries no issues", pi)
					}
					if !slices.Equal(got, phase.Expect.Issues) {
						t.Errorf("phase %d: issues = %q; want %q", pi, got, phase.Expect.Issues)
					}
				}

				if rej := phase.Expect.Rejection; rej != nil {
					framed := NewExecutionRejection(rej.Outcome, got, nil)
					if framed.GetAttempt() != uint32(rej.Attempt) || framed.GetIssues() != rej.Issues || framed.GetOutcome() != rej.Outcome {
						t.Errorf("phase %d: rejection = {attempt:%d outcome:%q issues:%q}; want {attempt:%d outcome:%q issues:%q}",
							pi, framed.GetAttempt(), framed.GetOutcome(), framed.GetIssues(), rej.Attempt, rej.Outcome, rej.Issues)
					}
				}
				if wantRej := req.GetRejection(); wantRej != nil {
					// Repair phases: the shared vector's rejection context
					// must arrive as the typed plain mirror on tier 1.
					mirrored := rejectionFromWire(wantRej)
					if mirrored == nil || mirrored.Attempt != wantRej.GetAttempt() || mirrored.Issues != wantRej.GetIssues() || mirrored.Outcome != wantRej.GetOutcome() {
						t.Errorf("phase %d: repair mirror = %+v; want %+v", pi, mirrored, wantRej)
					}
				}
			}
		})
	}
}
