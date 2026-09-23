package adapterhost

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// Concurrent-execute conformance defaults. The case is defined for N >= 3
// concurrent Execute calls against one open session; four calls with distinct
// completions are enough to catch per-call result corruption while staying
// fast in CI.
const (
	defaultConcurrentExecuteCalls       = 4
	minConcurrentExecuteCalls           = 3
	defaultConcurrentExecuteCallTimeout = 10 * time.Second
)

// conformanceSessionID is the single open session every concurrent Execute
// call is driven against. The runner is fresh per run, so a fixed id is
// collision-free and keeps failure messages reproducible.
const conformanceSessionID = "concurrent-execute-conformance"

// ConcurrentExecuteScript maps a call index i (0-based) to the ExecuteRequest
// the conformance case sends for that call and the ExecuteResult that must
// arrive on that call's own stream. The returned request's SessionId is
// ignored — the case opens ONE session and forces every call onto it. The
// returned result must be the logical, non-chunked result (Chunk nil); if the
// adapter under test chunks its outputs, the case reassembles the fragments
// before comparing. Scripts run on the caller's goroutine before the calls
// start and must not block.
type ConcurrentExecuteScript func(i int) (req *v2.ExecuteRequest, want *v2.ExecuteResult)

// ConcurrentExecuteOptions tunes [RunConcurrentExecuteConformance].
type ConcurrentExecuteOptions struct {
	// Calls is the number of concurrent Execute calls driven against the one
	// open session. Must be >= 3: fewer calls cannot demonstrate per-call
	// correlation. Defaults to 4.
	Calls int
	// CallTimeout bounds each individual Execute call, including the session
	// open. Must be > 0. Defaults to 10s.
	CallTimeout time.Duration
}

// ConcurrentExecuteOption modifies [ConcurrentExecuteOptions].
type ConcurrentExecuteOption func(*ConcurrentExecuteOptions)

// WithConcurrentExecuteCalls sets the number of concurrent Execute calls the
// case drives. Values below 3 make [RunConcurrentExecuteConformance] fail
// before any call is made.
func WithConcurrentExecuteCalls(n int) ConcurrentExecuteOption {
	return func(o *ConcurrentExecuteOptions) { o.Calls = n }
}

// WithConcurrentExecuteCallTimeout sets the deadline of each individual
// Execute call driven by [RunConcurrentExecuteConformance].
func WithConcurrentExecuteCallTimeout(d time.Duration) ConcurrentExecuteOption {
	return func(o *ConcurrentExecuteOptions) { o.CallTimeout = d }
}

// RunConcurrentExecuteConformance verifies that svc delivers each concurrent
// Execute call's result to that call's own stream — the property a host's
// parallel fan-out relies on when it declares an adapter "parallel_safe" and
// runs concurrent iterations against one wire session.
//
// The case opens ONE session, then drives N concurrent Execute calls over a
// real gRPC bridge (the same serving path a Criteria host uses)
// and requires every call's stream to receive exactly its own ExecuteResult:
// the result is present, exactly once, and equals the result the script
// expects for that call. Nothing is lost, nothing is delivered to a sibling's
// stream, and calls that finish later than their siblings still get their own
// result. The contract is per-call RESULT CORRECTNESS, not simultaneous
// execution: an adapter that serializes concurrent Executes satisfies it as
// long as each call still gets its own result on its own stream, so never
// assert wall-clock overlap — correlate per call.
//
// script tells the case what to send and what to expect: for call i it
// returns the [v2.ExecuteRequest] to send and the logical [v2.ExecuteResult]
// that must arrive on that call's stream (Chunk must be nil; the case joins
// chunked deliveries before comparing). Expected results must be pairwise
// distinct, otherwise per-call correlation cannot be proven.
//
// Usage, from an adapter author's test:
//
//	err := adapterhost.RunConcurrentExecuteConformance(myAdapter,
//		func(i int) (*v2.ExecuteRequest, *v2.ExecuteResult) {
//			return &v2.ExecuteRequest{
//					StepName: fmt.Sprintf("step-%d", i),
//					Input:    map[string]string{"call_id": fmt.Sprintf("call-%d", i)},
//				},
//				// myAdapter echoes call_id back in its outputs.
//				&v2.ExecuteResult{
//					Outcome:     "succeeded",
//					OutputsJson: []byte(fmt.Sprintf(`{"call_id":"call-%d"}`, i)),
//				}
//		})
//
// This SDK's own reference Service implementation is exercised by the same
// case in conformance_test.go; the deliberately-broken variant there
// (TS-SDK-style shared result state) fails it, proving the case detects the
// defect class.
func RunConcurrentExecuteConformance(svc Service, script ConcurrentExecuteScript, opts ...ConcurrentExecuteOption) error {
	if svc == nil {
		return errors.New("adapterhost: RunConcurrentExecuteConformance: svc is nil")
	}
	if script == nil {
		return errors.New("adapterhost: RunConcurrentExecuteConformance: script is nil")
	}
	o := &ConcurrentExecuteOptions{
		Calls:       defaultConcurrentExecuteCalls,
		CallTimeout: defaultConcurrentExecuteCallTimeout,
	}
	for _, opt := range opts {
		opt(o)
	}
	if o.Calls < minConcurrentExecuteCalls {
		return fmt.Errorf("adapterhost: RunConcurrentExecuteConformance: %d concurrent calls; the case needs at least %d", o.Calls, minConcurrentExecuteCalls)
	}
	if o.CallTimeout <= 0 {
		return fmt.Errorf("adapterhost: RunConcurrentExecuteConformance: CallTimeout must be > 0, got %v", o.CallTimeout)
	}

	reqs := make([]*v2.ExecuteRequest, o.Calls)
	wants := make([]*v2.ExecuteResult, o.Calls)
	for i := range reqs {
		req, want := script(i)
		if req == nil {
			return fmt.Errorf("adapterhost: RunConcurrentExecuteConformance: script returned nil request for call %d", i)
		}
		if want == nil {
			return fmt.Errorf("adapterhost: RunConcurrentExecuteConformance: script returned nil expected result for call %d", i)
		}
		if want.GetChunk() != nil {
			return fmt.Errorf("adapterhost: RunConcurrentExecuteConformance: expected result for call %d is chunked; the script must return the logical, non-chunked result", i)
		}
		// The case owns the session: every call is forced onto the one open
		// session regardless of what the script set.
		req.SessionId = conformanceSessionID
		reqs[i], wants[i] = req, want
	}
	if err := verifyDistinctResults(wants); err != nil {
		return fmt.Errorf("adapterhost: RunConcurrentExecuteConformance: %w", err)
	}

	client, stop, err := serveAdapterOnLoopback(svc)
	if err != nil {
		return fmt.Errorf("adapterhost: start adapter under test: %w", err)
	}
	defer stop()

	// ONE open session backs every concurrent call, matching the host's
	// parallel fan-out (one wire session per adapter ref).
	openCtx, cancelOpen := context.WithTimeout(context.Background(), o.CallTimeout)
	defer cancelOpen()
	if _, err := client.OpenSession(openCtx, &v2.OpenSessionRequest{SessionId: conformanceSessionID}); err != nil {
		return fmt.Errorf("adapterhost: OpenSession(%q): %w", conformanceSessionID, err)
	}

	type callOutcome struct {
		got *v2.ExecuteResult
		err error
	}
	outcomes := make([]callOutcome, o.Calls)
	var wg sync.WaitGroup
	for i := 0; i < o.Calls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, err := driveConformanceExecute(context.Background(), client, reqs[i], o.CallTimeout)
			outcomes[i] = callOutcome{got: got, err: err}
		}(i)
	}
	wg.Wait()

	var failures []string
	for i, oc := range outcomes {
		switch {
		case oc.err != nil:
			failures = append(failures, fmt.Sprintf("call %d: %v", i, oc.err))
		case oc.got == nil:
			failures = append(failures, fmt.Sprintf("call %d: internal error: no result and no error", i))
		case !sameExecuteResult(oc.got, wants[i]):
			failures = append(failures, fmt.Sprintf(
				"call %d: result mismatch: got outcome %q outputs %s; want outcome %q outputs %s",
				i, oc.got.GetOutcome(), string(oc.got.GetOutputsJson()),
				wants[i].GetOutcome(), string(wants[i].GetOutputsJson())))
		}
	}
	if len(failures) > 0 {
		sort.Strings(failures)
		return fmt.Errorf("adapterhost: concurrent Execute conformance failed:\n%s", strings.Join(failures, "\n"))
	}
	return nil
}

// driveConformanceExecute runs one Execute call against client and returns
// the logical ExecuteResult it received on its own stream. The stream is
// drained fully so extra results are detected, not just the first one.
func driveConformanceExecute(ctx context.Context, client v2.AdapterServiceClient, req *v2.ExecuteRequest, timeout time.Duration) (*v2.ExecuteResult, error) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	stream, err := client.Execute(callCtx, req)
	if err != nil {
		return nil, fmt.Errorf("Execute(%s): %w", req.GetStepName(), err)
	}

	var fragments []*v2.ExecuteResult
	for {
		ev, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // normal stream end after the adapter's Execute returned nil
			}
			return nil, fmt.Errorf("Execute(%s): recv: %w", req.GetStepName(), err)
		}
		// AdapterEvent, ToolInvocation, and Heartbeat are valid on an Execute
		// stream and not this case's subject; only ExecuteResults are checked.
		if res := ev.GetResult(); res != nil {
			fragments = append(fragments, res)
		}
	}

	res, err := joinExecuteResultFragments(fragments)
	if err != nil {
		return nil, fmt.Errorf("Execute(%s): %w", req.GetStepName(), err)
	}
	return res, nil
}

// joinExecuteResultFragments reduces one stream's ExecuteResult messages to
// the logical result the host would consume: a single non-chunked message
// passes through; a chunked delivery is reassembled like the host does —
// fragments are consumed in strict arrival order (the i-th fragment received
// must carry Chunk.Seq == i, matching the host's resultChunkNextSeq check,
// which rejects out-of-order fragments), the totals must agree, the final
// flag only on the last fragment, and the outcome is read from fragment 0,
// which the host records at seq 0 and ignores on every later fragment.
func joinExecuteResultFragments(fragments []*v2.ExecuteResult) (*v2.ExecuteResult, error) {
	switch {
	case len(fragments) == 0:
		return nil, errors.New("stream ended with no ExecuteResult: the result was lost")
	case len(fragments) == 1 && fragments[0].GetChunk() == nil:
		first := fragments[0]
		return &v2.ExecuteResult{Outcome: first.GetOutcome(), OutputsJson: first.GetOutputsJson()}, nil
	}

	// More than one message is only valid as one chunked delivery: whole
	// results must arrive as exactly one message each.
	for _, frag := range fragments {
		if frag.GetChunk() == nil {
			return nil, fmt.Errorf("stream received %d ExecuteResult messages but only one non-chunked result is allowed", len(fragments))
		}
	}

	// The host consumes fragments in strict arrival order: the i-th fragment
	// received must carry Chunk.Seq == i or emitResult fails the adapter with
	// "execute result chunk out-of-order". Sorting by seq here would let an
	// out-of-order sender pass a case the host rejects, so validate directly.
	var total uint32
	for i, frag := range fragments {
		if i == 0 {
			total = frag.GetChunk().GetTotal()
			if total == 0 {
				return nil, errors.New("fragment[0] declares total 0")
			}
			if total != uint32(len(fragments)) {
				return nil, fmt.Errorf("result declares total %d chunks but %d were received", total, len(fragments))
			}
		} else if frag.GetChunk().GetTotal() != total {
			return nil, fmt.Errorf("fragment[%d] declares total %d, expected %d", i, frag.GetChunk().GetTotal(), total)
		}
		if frag.GetChunk().GetSeq() != uint32(i) {
			return nil, fmt.Errorf("chunk seq gap: got seq %d, expected %d", frag.GetChunk().GetSeq(), i)
		}
	}

	final := fragments[total-1]
	if !final.GetChunk().GetFinal() {
		return nil, fmt.Errorf("final chunk (seq %d) missing final flag", total-1)
	}
	for i, frag := range fragments[:total-1] {
		if frag.GetChunk().GetFinal() {
			return nil, fmt.Errorf("fragment[%d] (seq %d) sets final flag early", i, frag.GetChunk().GetSeq())
		}
	}

	// The host records the outcome from fragment 0 (seq 0) and ignores the
	// outcome on every later fragment, so read it there: taking it from the
	// final fragment false-fails adapters that set it only on fragment 0.
	outcome := fragments[0].GetOutcome()
	if outcome == "" {
		return nil, errors.New("fragment[0] outcome is empty")
	}
	for i, frag := range fragments[1:] {
		if out := frag.GetOutcome(); out != "" && out != outcome {
			return nil, fmt.Errorf("fragment[%d] outcome %q differs from fragment[0] outcome %q", i+1, out, outcome)
		}
	}

	outputs, err := v2.JoinExecuteResultOutputs(fragments)
	if err != nil {
		return nil, fmt.Errorf("reassemble outputs: %w", err)
	}
	return &v2.ExecuteResult{Outcome: outcome, OutputsJson: outputs}, nil
}

// verifyDistinctResults rejects scripts whose expected results are ambiguous:
// if two calls expect the same logical result, a cross-delivery could not be
// detected, so the case would prove nothing about per-call correlation.
func verifyDistinctResults(wants []*v2.ExecuteResult) error {
	for i := 0; i < len(wants); i++ {
		for j := i + 1; j < len(wants); j++ {
			if sameExecuteResult(wants[i], wants[j]) {
				return fmt.Errorf("calls %d and %d expect identical results; expected results must be pairwise distinct so cross-delivery is detectable", i, j)
			}
		}
	}
	return nil
}

// sameExecuteResult compares the logical (outcome, outputs) of two results.
func sameExecuteResult(a, b *v2.ExecuteResult) bool {
	return a.GetOutcome() == b.GetOutcome() && bytes.Equal(a.GetOutputsJson(), b.GetOutputsJson())
}

// serveAdapterOnLoopback hosts svc behind the SDK's real gRPC bridge
// (grpcAdapterServer) on a loopback listener and returns a connected client
// plus a stop func that tears the server and connection down. The conformance
// case and the in-repo tests share it.
func serveAdapterOnLoopback(svc Service) (v2.AdapterServiceClient, func(), error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, fmt.Errorf("listen: %w", err)
	}

	s := grpc.NewServer()
	v2.RegisterAdapterServiceServer(s, &grpcAdapterServer{impl: svc})
	go func() { _ = s.Serve(lis) }()

	cc, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		s.Stop()
		_ = lis.Close()
		return nil, nil, fmt.Errorf("grpc client: %w", err)
	}

	stop := func() {
		_ = cc.Close()
		s.Stop()
		_ = lis.Close()
	}
	return v2.NewAdapterServiceClient(cc), stop, nil
}
