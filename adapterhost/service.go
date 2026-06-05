package adapterhost

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// AdapterName is the dispenser key shared between the host and every adapter
// process. Adapter authors do not need to reference this constant directly;
// [Serve] registers it automatically.
const AdapterName = "adapter"

// Service is the contract an out-of-process adapter must implement.
// The Criteria host creates one subprocess per adapter binary and calls these
// methods over a local gRPC transport managed by hashicorp/go-plugin.
//
// All methods receive a context that is cancelled when the host initiates
// teardown. Implementations must respect context cancellation.
//
// Lifecycle methods (Pause, Resume, Snapshot, Restore, Inspect) are NOT part
// of this interface and are not wired through the WS03 gRPC bridge.
type Service interface {
	Info(context.Context, *v2.InfoRequest) (*v2.InfoResponse, error)
	OpenSession(context.Context, *v2.OpenSessionRequest) (*v2.OpenSessionResponse, error)
	// Execute streams semantic events back to the host via [ExecuteEventSender].
	// It must send exactly one [v2.ExecuteResult] inside a [v2.ExecuteEvent] before
	// returning nil, or return a non-nil error. Log lines are NOT sent here;
	// they flow through [Log].
	Execute(context.Context, *v2.ExecuteRequest, ExecuteEventSender) error
	// Log streams log lines for the step to the host via [LogEventSender].
	// The host drives this concurrently with [Execute].
	Log(context.Context, *v2.LogRequest, LogEventSender) error
	// Permissions is the bidi permission stream. The host sends PermissionEvent
	// messages; the adapter responds with PermissionDecision messages. Embed
	// [UnimplementedPermissions] if the adapter does not need to block tool
	// execution on host decisions — the host applies post-hoc enforcement
	// (overriding success to needs_review) when any permission was denied.
	// Adapters that must prevent tool execution on denial should implement
	// Permissions themselves (see cmd/criteria-adapter-copilot for a blocking
	// example).
	Permissions(context.Context, PermissionsStream) error
	CloseSession(context.Context, *v2.CloseSessionRequest) (*v2.CloseSessionResponse, error)
}

// ExecuteEventSender pushes Execute stream events from the adapter to the host.
// Only AdapterEvent, ToolInvocation, ExecuteResult, and Heartbeat events are
// valid on this stream. Send must be safe for concurrent use.
type ExecuteEventSender interface {
	Send(*v2.ExecuteEvent) error
}

// LogEventSender pushes LogEvent messages from the adapter to the host.
// Send must be safe for concurrent use.
type LogEventSender interface {
	Send(*v2.LogEvent) error
}

// PermissionsStream is the bidi permission stream from the adapter's perspective.
// The adapter receives PermissionEvent messages from the host and sends
// PermissionDecision messages back.
type PermissionsStream interface {
	Recv() (*v2.PermissionEvent, error)
	Send(*v2.PermissionDecision) error
	Context() context.Context
}

// UnimplementedPermissions satisfies the Permissions method of [Service] with
// fail-closed semantics: unexpected stream errors are propagated rather than
// swallowed. Embed this in your adapter when the adapter does not need to block
// tool execution on host decisions; the host applies post-hoc enforcement
// (overriding a success outcome to needs_review) when any permission was denied.
// Adapters that must prevent tool execution on denial should implement Permissions
// themselves (see cmd/criteria-adapter-copilot for a blocking example).
type UnimplementedPermissions struct{}

func (UnimplementedPermissions) Permissions(_ context.Context, stream PermissionsStream) error {
	for {
		ev, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				return nil // normal stream end
			}
			return err // fail closed on unexpected stream error
		}
		req := ev.GetRequest()
		if req == nil {
			continue // Cancel or unknown event; no acknowledgment needed
		}
		if err := stream.Send(&v2.PermissionDecision{
			RequestId: req.GetRequestId(),
			Decision:  "allow",
		}); err != nil {
			return err // fail closed on send failure
		}
	}
}
