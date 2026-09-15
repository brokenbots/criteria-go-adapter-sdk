package adapterhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// toolCallTestHost is the fake host for the CallAdapterTool tests. It mirrors
// the sdk test style in serve_test.go: in-memory channels between the
// adapter-side helper under test and a scripted host goroutine.
type toolCallTestHost struct {
	t *testing.T

	// onRequest is the test's scripted reply: given the request_id and payload
	// of a permission.request AdapterEvent, it returns the PermissionEvents the
	// host emits on the Permissions stream.
	onRequest func(requestID string, payload *structpb.Struct) []*v2.PermissionEvent

	executeC  chan *v2.ExecuteEvent
	permC     chan *v2.PermissionEvent
	stopC     chan struct{}
	loopDone  chan struct{}
	permDone  chan struct{}
	ctx       context.Context
	cancelCtx context.CancelFunc

	mu   sync.Mutex
	sent []*structpb.Struct
	acks []*v2.PermissionDecision
}

func newToolCallTestHost(t *testing.T) *toolCallTestHost {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &toolCallTestHost{
		t:         t,
		executeC:  make(chan *v2.ExecuteEvent, 16),
		permC:     make(chan *v2.PermissionEvent, 32),
		stopC:     make(chan struct{}),
		loopDone:  make(chan struct{}),
		permDone:  make(chan struct{}),
		ctx:       ctx,
		cancelCtx: cancel,
	}
}

// start launches the fake host: one goroutine consuming Execute events (the
// CallAdapterTool side) and one running the bridge's Permissions dispatch loop.
func (h *toolCallTestHost) start(bridge *ToolCallBridge) {
	go func() {
		defer close(h.loopDone)
		for {
			select {
			case ev := <-h.executeC:
				payload := ev.GetAdapter().GetPayload()
				fields := payload.GetFields()
				requestID := fields["request_id"].GetStringValue()
				h.mu.Lock()
				h.sent = append(h.sent, payload)
				h.mu.Unlock()
				if h.onRequest != nil {
					for _, pe := range h.onRequest(requestID, payload) {
						select {
						case h.permC <- pe:
						case <-h.stopC:
							return
						}
					}
				}
			case <-h.stopC:
				return
			}
		}
	}()
	go func() {
		defer close(h.permDone)
		_ = bridge.Permissions(h.ctx, chanPermissionsStream{h: h})
	}()
}

// stop shuts both host goroutines down and fails the test if they do not exit,
// so a hang in the helper surfaces as a test failure rather than a leaked
// goroutine.
func (h *toolCallTestHost) stop() {
	close(h.stopC)
	h.cancelCtx()
	for name, done := range map[string]<-chan struct{}{
		"execute loop":    h.loopDone,
		"permission loop": h.permDone,
	} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			h.t.Fatalf("fake host %s did not stop", name)
		}
	}
}

func (h *toolCallTestHost) sink() ExecuteEventSender { return chanExecuteSink{h: h} }

// sentPayloads returns the payloads of every permission.request the bridge sent.
func (h *toolCallTestHost) sentPayloads() []*structpb.Struct {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*structpb.Struct(nil), h.sent...)
}

func (h *toolCallTestHost) acksSent() []*v2.PermissionDecision {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*v2.PermissionDecision(nil), h.acks...)
}

type chanExecuteSink struct{ h *toolCallTestHost }

func (s chanExecuteSink) Send(ev *v2.ExecuteEvent) error {
	select {
	case s.h.executeC <- ev:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("test: fake host did not consume the execute event")
	}
}

type chanPermissionsStream struct{ h *toolCallTestHost }

func (s chanPermissionsStream) Recv() (*v2.PermissionEvent, error) {
	select {
	case ev, ok := <-s.h.permC:
		if !ok {
			return nil, io.EOF
		}
		return ev, nil
	case <-s.h.ctx.Done():
		return nil, s.h.ctx.Err()
	}
}

func (s chanPermissionsStream) Send(d *v2.PermissionDecision) error {
	s.h.mu.Lock()
	defer s.h.mu.Unlock()
	s.h.acks = append(s.h.acks, d)
	return nil
}

func (s chanPermissionsStream) Context() context.Context { return s.h.ctx }

func grantEvent(requestID string) *v2.PermissionEvent {
	return &v2.PermissionEvent{
		Event: &v2.PermissionEvent_Request{Request: &v2.PermissionRequest{RequestId: requestID}},
	}
}

func cancelEvent(requestID, reason string) *v2.PermissionEvent {
	return &v2.PermissionEvent{
		Event: &v2.PermissionEvent_Cancel{Cancel: &v2.PermissionCancel{RequestId: requestID, Reason: reason}},
	}
}

func resultEvent(requestID string, res *v2.ToolCallResult) *v2.PermissionEvent {
	// The host echoes the caller's payload request_id into every reply.
	res.RequestId = requestID
	return &v2.PermissionEvent{
		Event: &v2.PermissionEvent_ToolCallResult{ToolCallResult: res},
	}
}

func testToolCall() AdapterToolCall {
	return AdapterToolCall{
		SessionID: "sess-1",
		Target:    FormatAdapterToolTarget("shell", "worker", "git_status"),
		Tool:      "git_status",
		Args:      map[string]any{"path": ".", "depth": 2},
	}
}

// TestCallAdapterToolResultPath covers the success path: grant ACKed, single
// tool_call_result correlated, outcome and outputs returned, and the CRI-152
// payload contract on the wire (kind, request_id, target, tool, args,
// args_digest, args_preview).
func TestCallAdapterToolResultPath(t *testing.T) {
	host := newToolCallTestHost(t)
	bridge := NewToolCallBridge()
	host.onRequest = func(requestID string, payload *structpb.Struct) []*v2.PermissionEvent {
		return []*v2.PermissionEvent{
			grantEvent(requestID),
			resultEvent(requestID, &v2.ToolCallResult{
				Outcome:     "succeeded",
				OutputsJson: []byte(`{"files":["main.go"],"count":2}`),
			}),
		}
	}
	host.start(bridge)
	defer host.stop()

	call := testToolCall()
	call.Preview = `{"path":"."}`
	outcome, outputs, err := bridge.CallAdapterTool(context.Background(), host.sink(), call)
	if err != nil {
		t.Fatalf("CallAdapterTool() error = %v; want success", err)
	}
	if outcome != "succeeded" {
		t.Errorf("outcome = %q; want %q", outcome, "succeeded")
	}
	wantOutputs := map[string]any{"files": []any{"main.go"}, "count": float64(2)}
	if diff := jsonDiff(outputs, wantOutputs); diff != "" {
		t.Errorf("outputs mismatch: %s", diff)
	}

	// Payload contract.
	sent := host.sentPayloads()
	if len(sent) != 1 {
		t.Fatalf("sent %d permission.request payloads; want 1", len(sent))
	}
	fields := sent[0].GetFields()
	if got := fields["kind"].GetStringValue(); got != PayloadKindAdapterTool {
		t.Errorf("payload kind = %q; want %q", got, PayloadKindAdapterTool)
	}
	requestID := fields["request_id"].GetStringValue()
	if requestID == "" {
		t.Fatal("payload request_id is empty; the host cannot correlate the reply")
	}
	if got := fields["target"].GetStringValue(); got != call.Target {
		t.Errorf("payload target = %q; want %q", got, call.Target)
	}
	if got := fields["tool"].GetStringValue(); got != call.Tool {
		t.Errorf("payload tool = %q; want %q", got, call.Tool)
	}
	if got := fields["args_preview"].GetStringValue(); got != call.Preview {
		t.Errorf("payload args_preview = %q; want %q", got, call.Preview)
	}
	// args round-trips as a JSON object on the wire.
	argsMap := fields["args"].GetStructValue().AsMap()
	wantArgs := map[string]any{"path": ".", "depth": float64(2)}
	if diff := jsonDiff(argsMap, wantArgs); diff != "" {
		t.Errorf("payload args mismatch: %s", diff)
	}
	// args_digest is over the canonical form of the normalized args.
	wantDigest, err := v2.ArgsDigest(wantArgs)
	if err != nil {
		t.Fatalf("ArgsDigest(wantArgs) error = %v", err)
	}
	if got := fields["args_digest"].GetStringValue(); got != wantDigest {
		t.Errorf("payload args_digest = %q; want %q", got, wantDigest)
	}

	// The grant was ACKed with an allow decision for the same request_id.
	acks := host.acksSent()
	if len(acks) != 1 {
		t.Fatalf("sent %d PermissionDecision ACKs; want 1", len(acks))
	}
	if acks[0].GetRequestId() != requestID || acks[0].GetDecision() != decisionAllow {
		t.Errorf("ACK = {request_id:%q decision:%q}; want {request_id:%q decision:%q}",
			acks[0].GetRequestId(), acks[0].GetDecision(), requestID, decisionAllow)
	}
}

// TestCallAdapterToolEmptyOutputs verifies a successful result that carries no
// outputs decodes to a nil outputs map.
func TestCallAdapterToolEmptyOutputs(t *testing.T) {
	host := newToolCallTestHost(t)
	bridge := NewToolCallBridge()
	host.onRequest = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		return []*v2.PermissionEvent{
			grantEvent(requestID),
			resultEvent(requestID, &v2.ToolCallResult{Outcome: "succeeded"}),
		}
	}
	host.start(bridge)
	defer host.stop()

	outcome, outputs, err := bridge.CallAdapterTool(context.Background(), host.sink(), testToolCall())
	if err != nil {
		t.Fatalf("CallAdapterTool() error = %v; want success", err)
	}
	if outcome != "succeeded" {
		t.Errorf("outcome = %q; want %q", outcome, "succeeded")
	}
	if outputs != nil {
		t.Errorf("outputs = %v; want nil", outputs)
	}
}

// TestCallAdapterToolChunkedPath covers chunked outputs reassembled out of
// order: fragments arrive seq 2,0,1 and must be joined in Chunk.seq order.
func TestCallAdapterToolChunkedPath(t *testing.T) {
	const body = `{"files":["a.go","b.go","c.go","d.go"],"summary":"4 files"}`
	chunks, payloads := v2.SplitChunks([]byte(body), 8)
	if len(chunks) < 2 {
		t.Fatalf("SplitChunks produced %d chunks; want >1 for the test to exercise reassembly", len(chunks))
	}
	host := newToolCallTestHost(t)
	bridge := NewToolCallBridge()
	host.onRequest = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		var events []*v2.PermissionEvent
		events = append(events, grantEvent(requestID))
		// Emit the non-final fragments out of order and close with the
		// fragment carrying the final flag, since reassembly only completes
		// on the final flag.
		for i := len(chunks) - 2; i >= 0; i-- {
			events = append(events, resultEvent(requestID, &v2.ToolCallResult{
				Outcome:     "succeeded",
				Chunk:       chunks[i],
				OutputsJson: payloads[i],
			}))
		}
		events = append(events, resultEvent(requestID, &v2.ToolCallResult{
			Outcome:     "succeeded",
			Chunk:       chunks[len(chunks)-1],
			OutputsJson: payloads[len(chunks)-1],
		}))
		return events
	}
	host.start(bridge)
	defer host.stop()

	outcome, outputs, err := bridge.CallAdapterTool(context.Background(), host.sink(), testToolCall())
	if err != nil {
		t.Fatalf("CallAdapterTool() error = %v; want success", err)
	}
	if outcome != "succeeded" {
		t.Errorf("outcome = %q; want %q", outcome, "succeeded")
	}
	var want map[string]any
	if err := json.Unmarshal([]byte(body), &want); err != nil {
		t.Fatalf("unmarshal fixture body: %v", err)
	}
	if diff := jsonDiff(outputs, want); diff != "" {
		t.Errorf("reassembled outputs mismatch: %s", diff)
	}
}

// TestCallAdapterToolChunkReassemblyError verifies malformed chunk framing —
// a final flag on a non-closing fragment — surfaces as a typed reassembly
// error instead of returning partial outputs.
func TestCallAdapterToolChunkReassemblyError(t *testing.T) {
	host := newToolCallTestHost(t)
	bridge := NewToolCallBridge()
	host.onRequest = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		return []*v2.PermissionEvent{
			grantEvent(requestID),
			// Malformed: declares total 2 but sets the final flag on the first
			// fragment, so the pending call resolves with a non-reassemblable
			// fragment set.
			resultEvent(requestID, &v2.ToolCallResult{
				Outcome:     "succeeded",
				Chunk:       &v2.Chunk{Seq: 0, Total: 2, Final: true},
				OutputsJson: []byte(`{"half":`),
			}),
			resultEvent(requestID, &v2.ToolCallResult{
				Outcome:     "succeeded",
				Chunk:       &v2.Chunk{Seq: 1, Total: 2},
				OutputsJson: []byte(`true}`),
			}),
		}
	}
	host.start(bridge)
	defer host.stop()

	_, _, err := bridge.CallAdapterTool(context.Background(), host.sink(), testToolCall())
	if err == nil {
		t.Fatal("CallAdapterTool() error = nil; want chunk reassembly failure")
	}
	if !strings.Contains(err.Error(), "reassemble outputs") {
		t.Errorf("error %v does not mention output reassembly", err)
	}
}

// TestCallAdapterToolDenyPath covers PermissionEvent.cancel: typed ErrDenied
// with the host's reason carried in the error text.
func TestCallAdapterToolDenyPath(t *testing.T) {
	host := newToolCallTestHost(t)
	bridge := NewToolCallBridge()
	host.onRequest = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		return []*v2.PermissionEvent{cancelEvent(requestID, "policy denies git tools")}
	}
	host.start(bridge)
	defer host.stop()

	outcome, outputs, err := bridge.CallAdapterTool(context.Background(), host.sink(), testToolCall())
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("CallAdapterTool() error = %v; want errors.Is(err, ErrDenied)", err)
	}
	if !strings.Contains(err.Error(), "policy denies git tools") {
		t.Errorf("error %v does not carry the deny reason", err)
	}
	if outcome != "" || outputs != nil {
		t.Errorf("outcome/outputs = %q, %v; want zero values on deny", outcome, outputs)
	}
}

// TestCallAdapterToolTypedCallError covers a typed call_error reply: the code
// round-trips through ToolCallError, with the host_unsupported code matching
// the ErrHostUnsupported sentinel via errors.Is.
func TestCallAdapterToolTypedCallError(t *testing.T) {
	host := newToolCallTestHost(t)
	bridge := NewToolCallBridge()
	host.onRequest = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		return []*v2.PermissionEvent{
			grantEvent(requestID),
			resultEvent(requestID, &v2.ToolCallResult{CallError: CallErrUnknownTool}),
		}
	}
	host.start(bridge)
	defer host.stop()

	_, _, err := bridge.CallAdapterTool(context.Background(), host.sink(), testToolCall())
	var callErr *ToolCallError
	if !errors.As(err, &callErr) {
		t.Fatalf("CallAdapterTool() error = %T(%v); want *ToolCallError", err, err)
	}
	if callErr.Code != CallErrUnknownTool {
		t.Errorf("Code = %q; want %q", callErr.Code, CallErrUnknownTool)
	}
	if errors.Is(err, ErrDenied) || errors.Is(err, ErrHostUnsupported) {
		t.Errorf("error %v must not match deny or host_unsupported sentinels", err)
	}

	// A host-sent host_unsupported call_error matches the sentinel too.
	host2 := newToolCallTestHost(t)
	bridge2 := NewToolCallBridge()
	host2.onRequest = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		return []*v2.PermissionEvent{
			grantEvent(requestID),
			resultEvent(requestID, &v2.ToolCallResult{CallError: CallErrHostUnsupported}),
		}
	}
	host2.start(bridge2)
	defer host2.stop()

	_, _, err = bridge2.CallAdapterTool(context.Background(), host2.sink(), testToolCall())
	if !errors.Is(err, ErrHostUnsupported) {
		t.Fatalf("host-sent host_unsupported error %v; want errors.Is(err, ErrHostUnsupported)", err)
	}
}

// TestCallAdapterToolBareGrantHostUnsupported covers the old-host signature:
// a bare allow-grant with no tool_call_result within the call deadline
// resolves as typed host_unsupported, caches the session so the next call on
// it fails fast without sending, and leaves other sessions unaffected.
func TestCallAdapterToolBareGrantHostUnsupported(t *testing.T) {
	host := newToolCallTestHost(t)
	bridge := NewToolCallBridge()
	host.onRequest = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		// Old host: grants the call but never sends a tool_call_result.
		return []*v2.PermissionEvent{grantEvent(requestID)}
	}
	host.start(bridge)
	defer host.stop()

	// The call carries no ctx deadline, so the helper's Timeout field governs.
	call := testToolCall()
	call.Timeout = 150 * time.Millisecond
	start := time.Now()
	outcome, outputs, err := bridge.CallAdapterTool(context.Background(), host.sink(), call)
	elapsed := time.Since(start)
	var callErr *ToolCallError
	if !errors.As(err, &callErr) || callErr.Code != CallErrHostUnsupported {
		t.Fatalf("CallAdapterTool() error = %T(%v); want ToolCallError{host_unsupported}", err, err)
	}
	if !errors.Is(err, ErrHostUnsupported) {
		t.Errorf("error %v; want errors.Is(err, ErrHostUnsupported)", err)
	}
	if outcome != "" || outputs != nil {
		t.Errorf("outcome/outputs = %q, %v; want zero values", outcome, outputs)
	}
	if elapsed > time.Second {
		t.Errorf("call took %s; want bounded by the call Timeout", elapsed)
	}

	// Same session: cached, fails fast without sending.
	before := len(host.sentPayloads())
	_, _, err = bridge.CallAdapterTool(context.Background(), host.sink(), call)
	if !errors.Is(err, ErrHostUnsupported) {
		t.Fatalf("cached call error = %v; want ErrHostUnsupported", err)
	}
	if got := len(host.sentPayloads()); got != before {
		t.Errorf("cached call sent %d new payloads; want 0", got-before)
	}

	// A different session is not poisoned and still sends (and still learns
	// the same signature).
	other := call
	other.SessionID = "sess-2"
	_, _, err = bridge.CallAdapterTool(context.Background(), host.sink(), other)
	if !errors.Is(err, ErrHostUnsupported) {
		t.Fatalf("other-session call error = %v; want ErrHostUnsupported", err)
	}
	if got := len(host.sentPayloads()); got != before+1 {
		t.Errorf("other-session call sent %d new payloads; want 1", len(host.sentPayloads())-before)
	}
}

// TestCallAdapterToolTimeoutPath covers a call whose correlated reply never
// arrives: the caller's ctx deadline resolves as a typed error wrapping
// context.DeadlineExceeded, and the timeout does not poison the session cache.
func TestCallAdapterToolTimeoutPath(t *testing.T) {
	host := newToolCallTestHost(t)
	bridge := NewToolCallBridge()
	host.onRequest = nil // the host never replies
	host.start(bridge)
	defer host.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, _, err := bridge.CallAdapterTool(ctx, host.sink(), testToolCall())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CallAdapterTool() error = %v; want wrap of context.DeadlineExceeded", err)
	}

	// A deadline timeout without a grant must not cache the session.
	before := len(host.sentPayloads())
	retry := testToolCall()
	retry.Timeout = 100 * time.Millisecond
	_, _, err = bridge.CallAdapterTool(context.Background(), host.sink(), retry)
	if err == nil {
		t.Fatal("subsequent call succeeded; want timeout again")
	}
	if got := len(host.sentPayloads()); got != before+1 {
		t.Errorf("subsequent call sent %d new payloads; want 1 (session not cached)", len(host.sentPayloads())-before)
	}
}

// TestCallAdapterToolCallerCancel covers caller cancellation mid-call: the
// typed error wraps context.Canceled and does not poison the session cache.
func TestCallAdapterToolCallerCancel(t *testing.T) {
	host := newToolCallTestHost(t)
	bridge := NewToolCallBridge()
	host.onRequest = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		return []*v2.PermissionEvent{grantEvent(requestID)}
	}
	host.start(bridge)
	defer host.stop()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Cancel after the grant has been delivered.
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, _, err := bridge.CallAdapterTool(ctx, host.sink(), testToolCall())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CallAdapterTool() error = %v; want wrap of context.Canceled", err)
	}

	// Cancellation must not cache the session as unsupported.
	before := len(host.sentPayloads())
	retry := testToolCall()
	retry.Timeout = 100 * time.Millisecond
	_, _, err = bridge.CallAdapterTool(context.Background(), host.sink(), retry)
	if err == nil {
		t.Fatal("subsequent call succeeded; want timeout (host never sends a result)")
	}
	if got := len(host.sentPayloads()); got != before+1 {
		t.Errorf("subsequent call sent %d new payloads; want 1 (session not cached)", len(host.sentPayloads())-before)
	}
}

// TestCallAdapterToolStreamClosed covers the Permissions stream ending while a
// call is in flight: every pending call resolves as ErrPermissionsClosed
// instead of hanging.
func TestCallAdapterToolStreamClosed(t *testing.T) {
	host := newToolCallTestHost(t)
	bridge := NewToolCallBridge()
	host.onRequest = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		// Host dies right after receiving the request: the Permissions stream
		// closes, so no reply can ever arrive.
		close(host.permC)
		return nil
	}
	host.start(bridge)
	defer host.stop()

	_, _, err := bridge.CallAdapterTool(context.Background(), host.sink(), testToolCall())
	if !errors.Is(err, ErrPermissionsClosed) {
		t.Fatalf("CallAdapterTool() error = %v; want errors.Is(err, ErrPermissionsClosed)", err)
	}
}

// TestCallAdapterToolIgnoresUnknownRequestID verifies the dispatch loop drops
// events correlated to no in-flight call and still completes the real one.
func TestCallAdapterToolIgnoresUnknownRequestID(t *testing.T) {
	host := newToolCallTestHost(t)
	bridge := NewToolCallBridge()
	host.onRequest = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		return []*v2.PermissionEvent{
			grantEvent("adapter-tool-not-mine"),
			resultEvent("adapter-tool-not-mine", &v2.ToolCallResult{
				Outcome:     "succeeded",
				OutputsJson: []byte(`{"late":true}`),
			}),
			grantEvent(requestID),
			resultEvent(requestID, &v2.ToolCallResult{
				Outcome:     "succeeded",
				OutputsJson: []byte(`{"ok":true}`),
			}),
		}
	}
	host.start(bridge)
	defer host.stop()

	_, outputs, err := bridge.CallAdapterTool(context.Background(), host.sink(), testToolCall())
	if err != nil {
		t.Fatalf("CallAdapterTool() error = %v; want success", err)
	}
	if diff := jsonDiff(outputs, map[string]any{"ok": true}); diff != "" {
		t.Errorf("outputs mismatch: %s", diff)
	}
}

// TestCallAdapterToolValidation covers argument validation: missing fields and
// non-object args fail before anything is sent.
func TestCallAdapterToolValidation(t *testing.T) {
	cases := []struct {
		name string
		call AdapterToolCall
	}{
		{"missing session", AdapterToolCall{Target: "adapter.shell.a.tools.t", Tool: "t"}},
		{"missing target", AdapterToolCall{SessionID: "s", Tool: "t"}},
		{"missing tool", AdapterToolCall{SessionID: "s", Target: "adapter.shell.a.tools.t"}},
		{"non-object args", AdapterToolCall{
			SessionID: "s", Target: "adapter.shell.a.tools.t", Tool: "t",
			Args: []string{"not", "an", "object"},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			host := newToolCallTestHost(t)
			bridge := NewToolCallBridge()
			host.start(bridge)
			defer host.stop()

			_, _, err := bridge.CallAdapterTool(context.Background(), host.sink(), tc.call)
			if err == nil {
				t.Fatal("CallAdapterTool() error = nil; want validation failure")
			}
			if got := len(host.sentPayloads()); got != 0 {
				t.Errorf("sent %d payloads; want 0 before validation passes", got)
			}
		})
	}

	// nil args are normalized to an empty object and digested as such.
	host := newToolCallTestHost(t)
	bridge := NewToolCallBridge()
	host.onRequest = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		return []*v2.PermissionEvent{
			grantEvent(requestID),
			resultEvent(requestID, &v2.ToolCallResult{Outcome: "succeeded"}),
		}
	}
	host.start(bridge)
	defer host.stop()

	call := AdapterToolCall{SessionID: "s", Target: FormatAdapterToolTarget("shell", "a", "t"), Tool: "t"}
	_, _, err := bridge.CallAdapterTool(context.Background(), host.sink(), call)
	if err != nil {
		t.Fatalf("CallAdapterTool() with nil args error = %v; want success", err)
	}
	sent := host.sentPayloads()
	if len(sent) != 1 {
		t.Fatalf("sent %d payloads; want 1", len(sent))
	}
	wantDigest, err := v2.ArgsDigest(map[string]any{})
	if err != nil {
		t.Fatalf("ArgsDigest(empty) error = %v", err)
	}
	if got := sent[0].GetFields()["args_digest"].GetStringValue(); got != wantDigest {
		t.Errorf("nil-args digest = %q; want %q", got, wantDigest)
	}
}

// TestCallAdapterToolConcurrent verifies concurrent calls are correlated by
// request_id: each caller receives its own callee result.
func TestCallAdapterToolConcurrent(t *testing.T) {
	host := newToolCallTestHost(t)
	bridge := NewToolCallBridge()
	host.onRequest = func(requestID string, payload *structpb.Struct) []*v2.PermissionEvent {
		// Echo the caller's own tool name back so each call can assert its
		// correlation.
		tool := payload.GetFields()["tool"].GetStringValue()
		body, _ := json.Marshal(map[string]any{"tool": tool})
		return []*v2.PermissionEvent{
			grantEvent(requestID),
			resultEvent(requestID, &v2.ToolCallResult{
				Outcome:     "succeeded",
				OutputsJson: body,
			}),
		}
	}
	host.start(bridge)
	defer host.stop()

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tool := "tool-" + string(rune('a'+i))
			call := testToolCall()
			call.Tool = tool
			call.Target = FormatAdapterToolTarget("shell", "worker", tool)
			_, outputs, err := bridge.CallAdapterTool(context.Background(), host.sink(), call)
			if err != nil {
				errs[i] = err
				return
			}
			if got := outputs["tool"]; got != tool {
				errs[i] = fmt.Errorf("outputs mismatch: got %v, want %s", got, tool)
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("call %d failed: %v", i, err)
		}
	}
}

func jsonDiff(got, want any) string {
	gotJSON, err := json.Marshal(got)
	if err != nil {
		return "marshal got: " + err.Error()
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		return "marshal want: " + err.Error()
	}
	if string(gotJSON) != string(wantJSON) {
		return "got " + string(gotJSON) + "; want " + string(wantJSON)
	}
	return ""
}
