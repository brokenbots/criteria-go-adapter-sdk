package adapterhost

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// conformanceBaseService carries the methods the concurrent-Execute case does
// not exercise. Conformance fixture Service implementations embed it and
// define Execute themselves.
type conformanceBaseService struct {
	UnimplementedPermissions
}

func (conformanceBaseService) Info(context.Context, *v2.InfoRequest) (*v2.InfoResponse, error) {
	return &v2.InfoResponse{Name: "conformance-fixture", Version: "0.0.0", SdkProtocolVersion: "2"}, nil
}

func (conformanceBaseService) OpenSession(context.Context, *v2.OpenSessionRequest) (*v2.OpenSessionResponse, error) {
	return &v2.OpenSessionResponse{}, nil
}

func (conformanceBaseService) Log(context.Context, *v2.LogRequest, LogEventSender) error { return nil }

func (conformanceBaseService) CloseSession(context.Context, *v2.CloseSessionRequest) (*v2.CloseSessionResponse, error) {
	return &v2.CloseSessionResponse{}, nil
}

func (conformanceBaseService) Execute(context.Context, *v2.ExecuteRequest, ExecuteEventSender) error {
	return nil
}

// conformanceCallID is the per-call identity every fixture echoes back in its
// ExecuteResult outputs; the case's expected results are pairwise distinct
// only because of it.
func conformanceCallID(i int) string { return fmt.Sprintf("call-%d", i) }

// conformanceScript builds the case script used by the in-repo fixtures: call
// i sends input {call_id, delay_ms} and expects its own call_id echoed back in
// the result outputs. delays[i] (may be zero) is honored by the well-behaved
// fixtures to bias completion order — the assertions themselves never depend
// on timing.
func conformanceScript(delays []time.Duration) ConcurrentExecuteScript {
	return func(i int) (*v2.ExecuteRequest, *v2.ExecuteResult) {
		req := &v2.ExecuteRequest{
			StepName: fmt.Sprintf("step-%d", i),
			Input: map[string]string{
				"call_id":  conformanceCallID(i),
				"delay_ms": strconv.FormatInt(delays[i].Milliseconds(), 10),
			},
		}
		want := &v2.ExecuteResult{
			Outcome:     "succeeded",
			OutputsJson: []byte(fmt.Sprintf(`{"call_id":%q}`, conformanceCallID(i))),
		}
		return req, want
	}
}

// conformanceReferenceService is the well-behaved reference Service fixture:
// each Execute call computes its own result, holds no cross-call state, and
// sends it on its own stream. It also guards the ONE-session property by
// failing if it is ever invoked with two distinct session ids.
type conformanceReferenceService struct {
	conformanceBaseService
	ignoreDelays bool

	mu      sync.Mutex
	session map[string]int
}

func (s *conformanceReferenceService) Execute(ctx context.Context, req *v2.ExecuteRequest, sender ExecuteEventSender) error {
	s.mu.Lock()
	if s.session == nil {
		s.session = make(map[string]int)
	}
	s.session[req.GetSessionId()]++
	distinct := len(s.session)
	s.mu.Unlock()
	if distinct > 1 {
		return fmt.Errorf("reference impl: %d distinct session ids seen; concurrent Executes share ONE session", distinct)
	}

	if ms, err := strconv.Atoi(req.GetInput()["delay_ms"]); err == nil && ms > 0 && !s.ignoreDelays {
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	ev, err := v2.NewExecuteResultEvent("succeeded", map[string]any{"call_id": req.GetInput()["call_id"]})
	if err != nil {
		return err
	}
	return sender.Send(ev)
}

// sharedResultStateService is the deliberately-broken baseline: ONE shared
// result slot holds every concurrent call's computed result — the TS-SDK-style
// shared result state this case exists to detect. Each call computes its own
// result, stores it in the shared slot, and only after every call has
// computed does it send FROM THE SHARED SLOT, so every stream deterministically
// receives whichever call computed last. An adapter with this defect must fail
// the case; the barrier exists only to make the clobber deterministic instead
// of a race the test could win by luck.
type sharedResultStateService struct {
	conformanceBaseService

	mu          sync.Mutex
	shared      *v2.ExecuteResult
	remaining   int
	allComputed chan struct{}
	closed      sync.Once
}

func newSharedResultStateService(calls int) *sharedResultStateService {
	return &sharedResultStateService{remaining: calls, allComputed: make(chan struct{})}
}

func (s *sharedResultStateService) Execute(ctx context.Context, req *v2.ExecuteRequest, sender ExecuteEventSender) error {
	res, err := v2.NewExecuteResult("succeeded", map[string]any{"call_id": req.GetInput()["call_id"]})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.shared = res // the defect: clobbers any sibling's still-unsent result
	s.remaining--
	last := s.remaining == 0
	s.mu.Unlock()
	if last {
		s.closed.Do(func() { close(s.allComputed) })
	}
	select {
	case <-s.allComputed:
	case <-ctx.Done():
		return ctx.Err()
	}
	// Send from the shared slot, not the local res: the slot now holds the
	// last call's result, which is what every stream will receive.
	return sender.Send(&v2.ExecuteEvent{Event: &v2.ExecuteEvent_Result{Result: s.shared}})
}

// silentExecuteService drops every Execute result: it streams nothing and
// returns nil, so each call's stream ends without a result.
type silentExecuteService struct {
	conformanceBaseService
}

// chunkedInOrderService delivers its result the way a conforming chunking
// adapter may: as ExecuteResult fragments sent in strict seq order. Fragment 0
// carries the outcome and later fragments leave it empty by default — the host
// records the outcome from fragment 0 (seq 0) and ignores it on every later
// fragment, so both shapes are part of the contract.
type chunkedInOrderService struct {
	conformanceBaseService
	// outcomeOnEveryFragment keeps the outcome on all fragments instead of
	// only fragment 0 when set.
	outcomeOnEveryFragment bool
}

func (s chunkedInOrderService) Execute(_ context.Context, req *v2.ExecuteRequest, sender ExecuteEventSender) error {
	res, err := v2.NewExecuteResult("succeeded", map[string]any{"call_id": req.GetInput()["call_id"]})
	if err != nil {
		return err
	}
	fragments := v2.ChunkExecuteResultOutputs(res, res.GetOutputsJson(), 8)
	if len(fragments) < 2 {
		return fmt.Errorf("fixture bug: expected a multi-fragment delivery, got %d fragments", len(fragments))
	}
	if !s.outcomeOnEveryFragment {
		for _, frag := range fragments[1:] {
			frag.Outcome = ""
		}
	}
	for _, frag := range fragments {
		if err := sender.Send(&v2.ExecuteEvent{Event: &v2.ExecuteEvent_Result{Result: frag}}); err != nil {
			return err
		}
	}
	return nil
}

// chunkedEmptySeq0OutcomeService is the deliberately-broken chunking variant:
// its fragment 0 carries no outcome. The host records the outcome from seq 0
// only, so the outcome is unrecoverable from later fragments and the case
// must fail it.
type chunkedEmptySeq0OutcomeService struct {
	conformanceBaseService
}

func (chunkedEmptySeq0OutcomeService) Execute(_ context.Context, req *v2.ExecuteRequest, sender ExecuteEventSender) error {
	fragments, err := chunkedFragments(req)
	if err != nil {
		return err
	}
	fragments[0].Outcome = ""
	for _, frag := range fragments {
		if err := sender.Send(&v2.ExecuteEvent{Event: &v2.ExecuteEvent_Result{Result: frag}}); err != nil {
			return err
		}
	}
	return nil
}

// chunkedOutOfOrderService is the deliberately-broken chunking variant: it
// sends its fragments in reverse Chunk.Seq order. A real host rejects this
// with "execute result chunk out-of-order" the moment fragment 0's seq is not
// the arrival index, so the case must fail it too.
type chunkedOutOfOrderService struct {
	conformanceBaseService
}

func (chunkedOutOfOrderService) Execute(_ context.Context, req *v2.ExecuteRequest, sender ExecuteEventSender) error {
	fragments, err := chunkedFragments(req)
	if err != nil {
		return err
	}
	for i := len(fragments) - 1; i >= 0; i-- {
		if err := sender.Send(&v2.ExecuteEvent{Event: &v2.ExecuteEvent_Result{Result: fragments[i]}}); err != nil {
			return err
		}
	}
	return nil
}

// chunkedFragments computes call req's logical result the way the other
// fixtures do and splits it into a multi-fragment delivery, so the chunk
// ordering rules — not output size — are what the case exercises. The guard
// keeps the chunk-ordering tests meaningful: if the fixture ever degrades to
// a single fragment they fail loudly instead of passing vacuously.
func chunkedFragments(req *v2.ExecuteRequest) ([]*v2.ExecuteResult, error) {
	res, err := v2.NewExecuteResult("succeeded", map[string]any{"call_id": req.GetInput()["call_id"]})
	if err != nil {
		return nil, err
	}
	fragments := v2.ChunkExecuteResultOutputs(res, res.GetOutputsJson(), 8)
	if len(fragments) < 2 {
		return nil, fmt.Errorf("fixture bug: expected a multi-fragment delivery, got %d fragments", len(fragments))
	}
	return fragments, nil
}

// TestConcurrentExecuteConformanceReference runs the case against the
// reference Service fixture: one open session, N concurrent Executes, every
// call's stream receiving exactly its own result. The out-of-order subtest
// makes call 0 the late finisher (longest delay) so a sibling's completion
// cannot be allowed to corrupt it; assertions stay per-call, never
// wall-clock.
func TestConcurrentExecuteConformanceReference(t *testing.T) {
	t.Run("out_of_order_completions", func(t *testing.T) {
		const n = 4
		delays := []time.Duration{40 * time.Millisecond, 30 * time.Millisecond, 20 * time.Millisecond, 10 * time.Millisecond}
		err := RunConcurrentExecuteConformance(&conformanceReferenceService{}, conformanceScript(delays), WithConcurrentExecuteCalls(n))
		if err != nil {
			t.Fatalf("reference implementation failed the concurrent-execute case: %v", err)
		}
	})
	t.Run("immediate_completions", func(t *testing.T) {
		delays := make([]time.Duration, 8)
		err := RunConcurrentExecuteConformance(&conformanceReferenceService{ignoreDelays: true}, conformanceScript(delays), WithConcurrentExecuteCalls(8))
		if err != nil {
			t.Fatalf("reference implementation failed the concurrent-execute case: %v", err)
		}
	})
}

// TestConcurrentExecuteConformanceDetectsSharedResultState proves the case
// detects the defect class it exists for: a Service impl with TS-SDK-style
// shared result state cross-delivers results and must fail. All N streams
// receive the same shared result, so exactly N-1 calls are reported wrong.
func TestConcurrentExecuteConformanceDetectsSharedResultState(t *testing.T) {
	const n = 4
	delays := make([]time.Duration, n)
	err := RunConcurrentExecuteConformance(newSharedResultStateService(n), conformanceScript(delays), WithConcurrentExecuteCalls(n))
	if err == nil {
		t.Fatal("shared-result-state implementation passed the concurrent-execute case; the case failed to detect the defect")
	}

	failures := 0
	for _, line := range strings.Split(err.Error(), "\n") {
		if strings.HasPrefix(line, "call ") && strings.Contains(line, "result mismatch") {
			failures++
		}
	}
	if failures < 2 {
		t.Fatalf("expected at least two cross-delivered calls in the failure, got %d in:\n%s", failures, err)
	}
	if !strings.Contains(err.Error(), `{"call_id":"call-`) {
		t.Errorf("failure does not show the correlated call ids:\n%s", err)
	}
}

// TestConcurrentExecuteConformanceDetectsLostResult proves the case fails
// when a call's result never arrives on its stream: every call must be
// reported as lost.
func TestConcurrentExecuteConformanceDetectsLostResult(t *testing.T) {
	const n = 3
	delays := make([]time.Duration, n)
	err := RunConcurrentExecuteConformance(silentExecuteService{}, conformanceScript(delays), WithConcurrentExecuteCalls(n))
	if err == nil {
		t.Fatal("silent implementation passed the concurrent-execute case; the case failed to detect the lost result")
	}
	for i := 0; i < n; i++ {
		if !strings.Contains(err.Error(), fmt.Sprintf("call %d: Execute(step-%d): stream ended with no ExecuteResult: the result was lost", i, i)) {
			t.Errorf("call %d loss not reported:\n%s", i, err)
		}
	}
}

// TestConcurrentExecuteConformanceDetectsOutOfOrderChunkDelivery proves the
// case rejects a chunked delivery whose fragments arrive out of Chunk.Seq
// order: a real host fails such an adapter with "execute result chunk
// out-of-order" the moment a fragment's seq is not the arrival index, so the
// case must fail it too — sorting fragments by seq would hide the defect.
func TestConcurrentExecuteConformanceDetectsOutOfOrderChunkDelivery(t *testing.T) {
	const n = 3
	delays := make([]time.Duration, n)
	err := RunConcurrentExecuteConformance(chunkedOutOfOrderService{}, conformanceScript(delays), WithConcurrentExecuteCalls(n))
	if err == nil {
		t.Fatal("out-of-order chunk sender passed the concurrent-execute case; the case failed to detect the defect")
	}
	if !strings.Contains(err.Error(), "chunk seq gap: got seq 2, expected 0") {
		t.Errorf("failure does not report the out-of-order fragment against arrival order:\n%s", err)
	}
}

// TestConcurrentExecuteConformanceDetectsMissingSeq0Outcome proves the case
// rejects a chunked delivery whose fragment 0 carries no outcome: the host
// records the outcome from seq 0 only, so it is unrecoverable from later
// fragments and the case must fail it.
func TestConcurrentExecuteConformanceDetectsMissingSeq0Outcome(t *testing.T) {
	const n = 3
	delays := make([]time.Duration, n)
	err := RunConcurrentExecuteConformance(chunkedEmptySeq0OutcomeService{}, conformanceScript(delays), WithConcurrentExecuteCalls(n))
	if err == nil {
		t.Fatal("chunk sender with empty fragment-0 outcome passed the concurrent-execute case; the case failed to detect the defect")
	}
	if !strings.Contains(err.Error(), "fragment[0] outcome is empty") {
		t.Errorf("failure does not report the missing fragment-0 outcome:\n%s", err)
	}
}

// TestConcurrentExecuteConformanceChunkedOutputs proves the case reads the
// result contract the way the host does: a chunked delivery sent in seq order
// reassembles before comparison, and the outcome comes from fragment 0 — the
// host ignores the outcome on every later fragment, so an adapter that sets
// it only on fragment 0 still passes.
func TestConcurrentExecuteConformanceChunkedOutputs(t *testing.T) {
	t.Run("outcome_on_fragment_0_only", func(t *testing.T) {
		const n = 3
		delays := make([]time.Duration, n)
		err := RunConcurrentExecuteConformance(chunkedInOrderService{}, conformanceScript(delays), WithConcurrentExecuteCalls(n))
		if err != nil {
			t.Fatalf("chunking implementation failed the concurrent-execute case: %v", err)
		}
	})
	t.Run("outcome_on_every_fragment", func(t *testing.T) {
		const n = 3
		delays := make([]time.Duration, n)
		err := RunConcurrentExecuteConformance(chunkedInOrderService{outcomeOnEveryFragment: true}, conformanceScript(delays), WithConcurrentExecuteCalls(n))
		if err != nil {
			t.Fatalf("chunking implementation failed the concurrent-execute case: %v", err)
		}
	})
}

// TestRunConcurrentExecuteConformanceValidation covers the runner's input
// contract; none of these may open a session or start a server.
func TestRunConcurrentExecuteConformanceValidation(t *testing.T) {
	noop := conformanceBaseService{}
	singleWant := &v2.ExecuteResult{Outcome: "succeeded", OutputsJson: []byte(`{"call_id":"call-0"}`)}

	for _, tc := range []struct {
		name string
		svc  Service
		opts []ConcurrentExecuteOption
		run  func(t *testing.T) error
		want string
	}{
		{
			name: "nil service",
			run: func(t *testing.T) error {
				return RunConcurrentExecuteConformance(nil, conformanceScript(nil))
			},
			want: "svc is nil",
		},
		{
			name: "nil script",
			run: func(t *testing.T) error {
				return RunConcurrentExecuteConformance(noop, nil)
			},
			want: "script is nil",
		},
		{
			name: "too few calls",
			opts: []ConcurrentExecuteOption{WithConcurrentExecuteCalls(2)},
			run: func(t *testing.T) error {
				return RunConcurrentExecuteConformance(noop, conformanceScript(nil), WithConcurrentExecuteCalls(2))
			},
			want: "at least 3",
		},
		{
			name: "zero call timeout",
			run: func(t *testing.T) error {
				return RunConcurrentExecuteConformance(noop, conformanceScript(nil), WithConcurrentExecuteCallTimeout(0))
			},
			want: "CallTimeout must be > 0",
		},
		{
			name: "script nil request",
			run: func(t *testing.T) error {
				return RunConcurrentExecuteConformance(noop, func(i int) (*v2.ExecuteRequest, *v2.ExecuteResult) {
					return nil, singleWant
				})
			},
			want: "nil request for call 0",
		},
		{
			name: "script nil expected result",
			run: func(t *testing.T) error {
				return RunConcurrentExecuteConformance(noop, func(i int) (*v2.ExecuteRequest, *v2.ExecuteResult) {
					return &v2.ExecuteRequest{StepName: "s"}, nil
				})
			},
			want: "nil expected result for call 0",
		},
		{
			name: "chunked expected result",
			run: func(t *testing.T) error {
				return RunConcurrentExecuteConformance(noop, func(i int) (*v2.ExecuteRequest, *v2.ExecuteResult) {
					return &v2.ExecuteRequest{StepName: "s"}, &v2.ExecuteResult{Outcome: "succeeded", Chunk: &v2.Chunk{Seq: 0, Total: 1, Final: true}}
				})
			},
			want: "logical, non-chunked result",
		},
		{
			name: "ambiguous expected results",
			run: func(t *testing.T) error {
				return RunConcurrentExecuteConformance(noop, func(i int) (*v2.ExecuteRequest, *v2.ExecuteResult) {
					return &v2.ExecuteRequest{StepName: "s"}, singleWant
				})
			},
			want: "pairwise distinct",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run(t)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

// TestRunConcurrentExecuteConformanceSessionForced proves the case drives all
// calls onto the ONE session it opened, even when the script sets a different
// session id: the reference fixture fails if it ever sees two distinct ids.
func TestRunConcurrentExecuteConformanceSessionForced(t *testing.T) {
	const n = 4
	delays := make([]time.Duration, n)
	script := func(i int) (*v2.ExecuteRequest, *v2.ExecuteResult) {
		req, want := conformanceScript(delays)(i)
		req.SessionId = fmt.Sprintf("script-side-session-%d", i) // must be overridden
		return req, want
	}
	err := RunConcurrentExecuteConformance(&conformanceReferenceService{}, script, WithConcurrentExecuteCalls(n))
	if err != nil {
		t.Fatalf("forced-session case failed: %v", err)
	}
}

// TestServeAdapterOnLoopbackCleanup verifies the loopback server helper stops
// cleanly: after stop, the listener is gone, so a second start gets a fresh
// server and a fresh connection.
func TestServeAdapterOnLoopbackCleanup(t *testing.T) {
	impl := &conformanceReferenceService{}
	client, stop, err := serveAdapterOnLoopback(impl)
	if err != nil {
		t.Fatalf("serveAdapterOnLoopback: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.OpenSession(ctx, &v2.OpenSessionRequest{SessionId: "probe"}); err != nil {
		t.Fatalf("OpenSession before stop: %v", err)
	}
	stop()
	if _, err := client.OpenSession(context.Background(), &v2.OpenSessionRequest{SessionId: "probe"}); err == nil {
		t.Error("OpenSession after stop unexpectedly succeeded; server was not torn down")
	}
}
