package adapterhost

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// Wire constants for the adapter tool-call flow (ADR-0004 §8, CRI-152).
const (
	// EventKindPermissionRequest is the AdapterEvent event_kind that carries
	// both plain permission requests and adapter tool calls. The payload kind
	// (PayloadKindAdapterTool for tool calls) distinguishes the two.
	EventKindPermissionRequest = "permission.request"

	// PayloadKindAdapterTool is the `kind` value of a permission.request
	// AdapterEvent payload that makes the request an adapter tool call.
	PayloadKindAdapterTool = "adapter_tool"

	// CapabilityAdapterTools is the InfoResponse.capabilities string an adapter
	// declares when it speaks the tool-call flow (ADR-0004 §9, CRI-153).
	CapabilityAdapterTools = "adapter_tools"
)

// Well-known ToolCallResult.call_error codes (CRI-152). The registry is
// free-form: hosts may emit values not listed here, and they round-trip
// unchanged through [ToolCallError].
const (
	CallErrUnknownAdapter    = "unknown_adapter"
	CallErrUnknownTool       = "unknown_tool"
	CallErrCapabilityMissing = "capability_missing"
	CallErrHostUnsupported   = "host_unsupported"
	CallErrDepthExceeded     = "depth_exceeded"
	CallErrCycleDetected     = "cycle_detected"
	CallErrCalleeCrash       = "callee_crash"
	CallErrCalleeTimeout     = "callee_timeout"
	CallErrCanceled          = "canceled"
	CallErrNotYetSupported   = "not_yet_supported"
	CallErrSelfCall          = "self_call"
)

// DefaultToolCallTimeout is the deadline a tool call runs under when the
// caller's context carries no deadline. The bound is what makes an old host
// (one that predates adapter tools and only ever answers with a bare
// allow-grant) resolve deterministically instead of hanging a call forever
// (ADR-0004 §9, CRI-153 versioning matrix).
const DefaultToolCallTimeout = 60 * time.Second

// Typed errors for the tool-call flow.
var (
	// ErrDenied is returned when the host denies the tool call
	// (PermissionEvent.cancel). The host's reason is carried in the error text.
	ErrDenied = errors.New("adapterhost: tool call denied")

	// ErrHostUnsupported is returned when the host predates adapter tools: the
	// granted call came back as a bare allow-grant with no
	// PermissionEvent.tool_call_result within the call deadline (ADR-0004 §9).
	// The detection is cached per session by [ToolCallBridge], so later calls
	// on the same session fail fast without sending. A
	// [*ToolCallError] with Code CallErrHostUnsupported — whether detected
	// locally from this signature or returned by the host as a call_error —
	// satisfies errors.Is(err, ErrHostUnsupported).
	ErrHostUnsupported = errors.New("adapterhost: host does not support adapter tool calls")

	// ErrPermissionsClosed is returned when the Permissions stream ends while
	// a tool call is still waiting for its correlated reply.
	ErrPermissionsClosed = errors.New("adapterhost: permissions stream closed")
)

// ToolCallError is the typed failure of an adapter tool call, carrying the
// host's ToolCallResult.call_error code (CRI-152). Code is a free-form
// registry value: well-known codes are the CallErr* constants; unknown values
// round-trip unchanged.
type ToolCallError struct {
	// Code is the host's call_error code.
	Code string
}

// Error implements the error interface.
func (e *ToolCallError) Error() string {
	return fmt.Sprintf("adapterhost: adapter tool call failed: %s", e.Code)
}

// Is reports whether the error matches a sentinel for its code. The
// host_unsupported code maps to [ErrHostUnsupported] so both detection paths
// (host-sent call_error and the local bare allow-grant signature) are
// matched by the same sentinel.
func (e *ToolCallError) Is(target error) bool {
	return target == ErrHostUnsupported && e.Code == CallErrHostUnsupported
}

// AdapterToolCall describes one adapter tool call.
type AdapterToolCall struct {
	// SessionID is the Execute session the call is issued from. It keys the
	// per-session host_unsupported cache and is required.
	SessionID string
	// Target is the full tool target string, "adapter.<type>.<name>.tools[.<tool>]"
	// (ADR-0004 §2). Required. Use [FormatAdapterToolTarget] to build it.
	Target string
	// Tool is the callee tool name only (no adapter prefix). Required.
	Tool string
	// Args are the call arguments; any JSON-round-trippable value is accepted,
	// and the wire form is a JSON object. nil is sent as an empty object.
	Args any
	// Preview is an optional short printable preview of the args carried as
	// the payload's args_preview key.
	Preview string
	// Timeout overrides [DefaultToolCallTimeout] for calls issued on a context
	// that carries no deadline. It has no effect when ctx already has a
	// deadline.
	Timeout time.Duration
}

func (c AdapterToolCall) validate() error {
	if c.SessionID == "" {
		return errors.New("adapterhost: tool call requires SessionID")
	}
	if c.Target == "" {
		return errors.New("adapterhost: tool call requires Target")
	}
	if c.Tool == "" {
		return errors.New("adapterhost: tool call requires Tool")
	}
	return nil
}

// FormatAdapterToolTarget builds the full tool target string
// "adapter.<type>.<name>.tools[.<tool>]" (ADR-0004 §2). An empty tool yields
// the bare surface form "adapter.<type>.<name>.tools".
func FormatAdapterToolTarget(adapterType, instance, tool string) string {
	target := "adapter." + adapterType + "." + instance + ".tools"
	if tool != "" {
		target += "." + tool
	}
	return target
}

// decisionAllow is the PermissionDecision value ACKing an allow-grant,
// mirroring the mcp bridge (cmd/criteria-adapter-mcp/bridge.go).
const decisionAllow = "allow"

// pendingToolCall is the in-flight state for one issued tool call, keyed by
// request_id. The Permissions dispatch loop writes correlated events into it;
// the blocking call waits on done.
type pendingToolCall struct {
	done chan struct{}
	// grant records that a bare allow-grant (PermissionEvent.request) was seen
	// for this call — the old-host signature.
	grant     bool
	fragments []*v2.ToolCallResult
	cancelled *v2.PermissionCancel
	err       error
}

// ToolCallBridge coordinates blocking adapter tool calls for one adapter
// process, mirroring the mcp bridge's awaitPermission pending-map pattern
// (cmd/criteria-adapter-mcp/bridge.go): CallAdapterTool sends the tool call as
// a permission.request AdapterEvent on the Execute stream and blocks on the
// Permissions stream for the correlated reply, while Permissions runs the
// host-to-adapter dispatch loop that routes every PermissionEvent into the
// pending call it correlates with (allow-grants are ACKed with a
// PermissionDecision allow, mirroring [UnimplementedPermissions]).
//
// An adapter wires it up by delegating both halves of [Service] to one bridge:
//
//	type myAdapter struct {
//		adapterhost.UnimplementedPermissions
//		bridge *adapterhost.ToolCallBridge
//	}
//
//	func (a *myAdapter) Execute(ctx context.Context, req *v2.ExecuteRequest, sink adapterhost.ExecuteEventSender) error {
//		outcome, outputs, err := a.bridge.CallAdapterTool(ctx, sink, adapterhost.AdapterToolCall{
//			SessionID: req.GetSessionId(),
//			Target:    adapterhost.FormatAdapterToolTarget("shell", "worker", "git_status"),
//			Tool:      "git_status",
//			Args:      map[string]any{"path": "."},
//		})
//		// ...
//	}
//
//	func (a *myAdapter) Permissions(ctx context.Context, stream adapterhost.PermissionsStream) error {
//		return a.bridge.Permissions(ctx, stream)
//	}
//
// # Capability gate (CRI-153 versioning matrix)
//
// The current wire protocol has no host-to-adapter capability announcement at
// handshake, so whether the host supports adapter tools can only be learned
// from the reply signature — the gate here is therefore reply-signature based:
//
//   - PermissionEvent.tool_call_result with matching request_id → success
//     (outcome, outputs) or typed [*ToolCallError] failure (call_error).
//   - PermissionEvent.cancel → typed [ErrDenied]; the host's reason is
//     carried in the error text.
//   - A bare allow-grant (PermissionEvent.request) with no tool_call_result
//     within the call deadline means the host predates adapter tools: the
//     call resolves to the typed host_unsupported failure and the session is
//     cached, so later calls on the same session fail fast without sending.
//   - The caller's ctx deadline or cancellation surfaces as a typed error
//     wrapping the context error (errors.Is against context.DeadlineExceeded
//     or context.Canceled).
//
// The bare allow-grant signature is only observable after a call deadline has
// elapsed, which is why a call issued on a context without a deadline runs
// under [DefaultToolCallTimeout] (or the call's Timeout): an old host can
// degrade a granted call deterministically but never hang it (ADR-0004 §9).
// Callers with slow callees should set a ctx deadline (or AdapterToolCall
// Timeout) covering the callee's expected runtime; a result that misses the
// deadline after a grant is indistinguishable from an old host and resolves
// as host_unsupported.
//
// The dispatch loop must be running for calls to complete: wire
// [Service.Permissions] to [ToolCallBridge.Permissions]. Without it, calls
// resolve only via their deadline as a typed timeout error.
//
// # Non-blocking variant
//
// A non-blocking variant is explicitly deferred: the pending-map plumbing
// here resolves entries for a blocking caller (deadline handling, the
// host_unsupported session cache, and result reassembly all complete on the
// caller's goroutine), so an async surface would need its own completion and
// timeout-ownership contract rather than falling out of this plumbing.
type ToolCallBridge struct {
	mu      sync.Mutex
	pending map[string]*pendingToolCall
	// unsupportedSessions holds the sessions whose host revealed the bare
	// allow-grant no-result signature (ADR-0004 §9).
	unsupportedSessions map[string]struct{}
}

// NewToolCallBridge returns an empty bridge. One bridge serves the whole
// adapter process; concurrent tool calls are correlated by request_id.
func NewToolCallBridge() *ToolCallBridge {
	return &ToolCallBridge{
		pending:             make(map[string]*pendingToolCall),
		unsupportedSessions: make(map[string]struct{}),
	}
}

// CallAdapterTool sends the tool call as a permission.request AdapterEvent
// (payload kind adapter_tool, per the CRI-152 contract) on the Execute stream
// via sink, blocks for the correlated reply on the Permissions stream, and
// returns the callee's (outcome, outputs, error). See [ToolCallBridge] for the
// reply interpretation, the capability gate, and the deferred non-blocking
// variant.
func (b *ToolCallBridge) CallAdapterTool(ctx context.Context, sink ExecuteEventSender, call AdapterToolCall) (string, map[string]any, error) {
	if ctx == nil {
		return "", nil, errors.New("adapterhost: tool call requires a context")
	}
	if sink == nil {
		return "", nil, errors.New("adapterhost: tool call requires an ExecuteEventSender")
	}
	if err := call.validate(); err != nil {
		return "", nil, err
	}

	if b.sessionUnsupported(call.SessionID) {
		return "", nil, &ToolCallError{Code: CallErrHostUnsupported}
	}

	requestID := newToolCallRequestID()
	payload, err := buildToolCallPayload(call, requestID)
	if err != nil {
		return "", nil, err
	}
	entry := b.register(requestID)

	if err := sink.Send(&v2.ExecuteEvent{
		Event: &v2.ExecuteEvent_Adapter{
			Adapter: &v2.AdapterEvent{
				EventKind: EventKindPermissionRequest,
				Payload:   payload,
				EmittedAt: timestamppb.Now(),
			},
		},
	}); err != nil {
		b.remove(requestID)
		return "", nil, fmt.Errorf("adapterhost: send permission.request for tool call %q: %w", call.Tool, err)
	}

	// A call on a context without a deadline runs under the SDK-imposed bound;
	// a caller-provided deadline governs as-is.
	callCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		wait := call.Timeout
		if wait <= 0 {
			wait = DefaultToolCallTimeout
		}
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, wait)
		defer cancel()
	}

	select {
	case <-entry.done:
		return finishToolCall(call, entry)
	case <-callCtx.Done():
		granted := b.takeGrant(requestID)
		if granted && callCtx.Err() == context.DeadlineExceeded {
			// Bare allow-grant with no result within the call deadline: the
			// host predates adapter tools (ADR-0004 §9). Cache the session so
			// later calls fail fast without sending.
			b.cacheUnsupported(call.SessionID)
			return "", nil, &ToolCallError{Code: CallErrHostUnsupported}
		}
		b.remove(requestID)
		return "", nil, fmt.Errorf("adapterhost: tool call %q to %s: %w", call.Tool, call.Target, callCtx.Err())
	}
}

// buildToolCallPayload renders the CRI-152 permission.request payload for an
// adapter tool call. The canonical-JSON bytes of args are the single source
// for both args_digest and the wire args object, so the two cannot diverge.
func buildToolCallPayload(call AdapterToolCall, requestID string) (*structpb.Struct, error) {
	canonical, err := v2.CanonicalJSON(call.Args)
	if err != nil {
		return nil, fmt.Errorf("adapterhost: tool call %q args: %w", call.Tool, err)
	}
	var args map[string]any
	if err := json.Unmarshal(canonical, &args); err != nil {
		return nil, fmt.Errorf("adapterhost: tool call %q args must be a JSON object: %w", call.Tool, err)
	}
	if args == nil {
		args = map[string]any{}
	}
	digest, err := v2.ArgsDigest(args)
	if err != nil {
		return nil, fmt.Errorf("adapterhost: tool call %q args digest: %w", call.Tool, err)
	}
	argsStruct, err := structpb.NewStruct(args)
	if err != nil {
		return nil, fmt.Errorf("adapterhost: tool call %q args struct: %w", call.Tool, err)
	}

	fields := map[string]any{
		"kind":        PayloadKindAdapterTool,
		"request_id":  requestID,
		"target":      call.Target,
		"tool":        call.Tool,
		"args":        argsStruct.AsMap(),
		"args_digest": digest,
	}
	if call.Preview != "" {
		fields["args_preview"] = call.Preview
	}
	return structpb.NewStruct(fields)
}

// newToolCallRequestID mints a correlation id for one tool call.
func newToolCallRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand failing is unrecoverable; fall back to a process-unique
		// counter so concurrent calls still cannot collide.
		return fmt.Sprintf("adapter-tool-fallback-%d", requestIDFallback.Add(1))
	}
	return "adapter-tool-" + hex.EncodeToString(buf[:])
}

// requestIDFallback keeps minted ids unique if crypto/rand ever fails.
var requestIDFallback atomic.Uint64

// register installs a pending entry for requestID.
func (b *ToolCallBridge) register(requestID string) *pendingToolCall {
	entry := &pendingToolCall{done: make(chan struct{})}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending[requestID] = entry
	return entry
}

// remove drops requestID's pending entry (idempotent).
func (b *ToolCallBridge) remove(requestID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.pending, requestID)
}

// takeGrant reports whether a bare allow-grant was observed for requestID and
// drops the entry.
func (b *ToolCallBridge) takeGrant(requestID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	entry, ok := b.pending[requestID]
	delete(b.pending, requestID)
	return ok && entry.grant
}

// sessionUnsupported reports whether the session is known to predate adapter
// tools.
func (b *ToolCallBridge) sessionUnsupported(sessionID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.unsupportedSessions[sessionID]
	return ok
}

// cacheUnsupported records the session as host-unsupported.
func (b *ToolCallBridge) cacheUnsupported(sessionID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.unsupportedSessions[sessionID] = struct{}{}
}

// Permissions runs the host-to-adapter dispatch loop for the Permissions bidi
// stream. It ACKs every allow-grant with a PermissionDecision (the host
// consumes ACKs; tool-call results never ride PermissionDecision), routes
// correlated events into pending tool calls, and resolves every pending call
// when the stream ends so a dying stream can never hang a caller. Wire
// [Service.Permissions] to this method.
func (b *ToolCallBridge) Permissions(ctx context.Context, stream PermissionsStream) error {
	defer b.closeAll()
	for {
		ev, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled || status.Code(err) == codes.OK {
				return nil
			}
			return err
		}
		switch {
		case ev.GetRequest() != nil:
			id := ev.GetRequest().GetRequestId()
			if id == "" {
				continue
			}
			// ACK the allow-grant; the host drains PermissionDecision, so an
			// ack for an unknown (e.g. already timed out) call is harmless.
			if err := stream.Send(&v2.PermissionDecision{RequestId: id, Decision: decisionAllow}); err != nil {
				return err
			}
			b.markGranted(id)
		case ev.GetCancel() != nil:
			b.routeCancel(ev.GetCancel())
		case ev.GetToolCallResult() != nil:
			b.routeResult(ev.GetToolCallResult())
		}
	}
}

// markGranted records the bare allow-grant signature on the pending call.
func (b *ToolCallBridge) markGranted(requestID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if entry, ok := b.pending[requestID]; ok {
		entry.grant = true
	}
}

// routeCancel resolves the pending call a deny correlates with. A cancel for
// an unknown id (e.g. a late denial after the caller timed out) is dropped.
func (b *ToolCallBridge) routeCancel(c *v2.PermissionCancel) {
	b.mu.Lock()
	defer b.mu.Unlock()
	entry, ok := b.pending[c.GetRequestId()]
	if !ok {
		return
	}
	delete(b.pending, c.GetRequestId())
	entry.cancelled = c
	close(entry.done)
}

// routeResult accumulates one tool_call_result fragment and resolves the call
// on the final fragment (or on a typed failure, which is never chunked).
func (b *ToolCallBridge) routeResult(res *v2.ToolCallResult) {
	b.mu.Lock()
	defer b.mu.Unlock()
	entry, ok := b.pending[res.GetRequestId()]
	if !ok {
		return
	}
	entry.fragments = append(entry.fragments, res)
	if res.GetCallError() != "" || res.GetChunk() == nil || res.GetChunk().GetFinal() {
		delete(b.pending, res.GetRequestId())
		close(entry.done)
	}
}

// closeAll resolves every pending call; the Permissions stream is gone, so no
// reply can ever arrive.
func (b *ToolCallBridge) closeAll() {
	b.mu.Lock()
	entries := make([]*pendingToolCall, 0, len(b.pending))
	for id, entry := range b.pending {
		entries = append(entries, entry)
		delete(b.pending, id)
	}
	b.mu.Unlock()
	for _, entry := range entries {
		entry.err = ErrPermissionsClosed
		close(entry.done)
	}
}

// finishToolCall interprets a resolved pending call into the helper's result.
func finishToolCall(call AdapterToolCall, entry *pendingToolCall) (string, map[string]any, error) {
	if entry.err != nil {
		return "", nil, fmt.Errorf("adapterhost: tool call %q to %s: %w", call.Tool, call.Target, entry.err)
	}
	if c := entry.cancelled; c != nil {
		denied := fmt.Errorf("adapterhost: tool call %q to %s: %w", call.Tool, call.Target, ErrDenied)
		if reason := c.GetReason(); reason != "" {
			denied = fmt.Errorf("%w: %s", denied, reason)
		}
		return "", nil, denied
	}

	// The dispatch loop only routes fragments carrying the call's request_id,
	// so every fragment here is correlated.
	first := entry.fragments[0]
	if code := first.GetCallError(); code != "" {
		return "", nil, &ToolCallError{Code: code}
	}

	var outputsJSON []byte
	if len(entry.fragments) == 1 && first.GetChunk() == nil {
		// Single non-chunked message: outputs_json is the whole object.
		outputsJSON = first.GetOutputsJson()
	} else {
		joined, err := v2.JoinToolCallResultOutputs(entry.fragments)
		if err != nil {
			return "", nil, fmt.Errorf("adapterhost: reassemble outputs of tool call %q: %w", call.Tool, err)
		}
		outputsJSON = joined
	}

	outputs, err := decodeToolCallOutputs(outputsJSON)
	if err != nil {
		return "", nil, fmt.Errorf("adapterhost: decode outputs of tool call %q: %w", call.Tool, err)
	}
	return first.GetOutcome(), outputs, nil
}

// decodeToolCallOutputs decodes the callee's typed outputs object; empty
// outputs_json means the callee emitted none.
func decodeToolCallOutputs(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var outputs map[string]any
	if err := json.Unmarshal(raw, &outputs); err != nil {
		return nil, fmt.Errorf("outputs_json: %w", err)
	}
	return outputs, nil
}
