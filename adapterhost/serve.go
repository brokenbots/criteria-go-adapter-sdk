package adapterhost

import (
	"context"
	"errors"
	"sync"

	hplugin "github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// logHeartbeatRunner starts the per-session log-stream heartbeat. It is
// unexported because it is a test seam: production uses v2.RunHeartbeat with
// the standard 30s interval, while tests substitute a faster ticker so the
// regression suite does not wait for real-time heartbeats.
var logHeartbeatRunner = func(ctx context.Context, sender LogEventSender) {
	go func() {
		_ = v2.RunHeartbeat(ctx, "log", func(hb *v2.Heartbeat) error {
			return sender.Send(&v2.LogEvent{Heartbeat: hb})
		})
	}()
}

// Serve starts the adapter process using the shared [HandshakeConfig].
// Call this from your adapter's main() function.
//
// When the process is invoked with --emit-manifest, Serve writes the adapter's
// adapter.yaml (derived from its Info response) to stdout and exits, instead of
// starting the plugin server. The build pipeline and `criteria adapter publish`
// use this to extract the manifest.
func Serve(impl Service) {
	if runEmitManifestIfRequested(impl) {
		return
	}
	hplugin.Serve(&hplugin.ServeConfig{
		HandshakeConfig: HandshakeConfig,
		Plugins: map[string]hplugin.Plugin{
			AdapterName: &grpcAdapter{Impl: impl},
		},
		GRPCServer: hplugin.DefaultGRPCServer,
	})
}

// grpcAdapter adapts a Service implementation to hashicorp/go-plugin on the
// adapter (server) side. It is intentionally unexported: callers use [Serve].
type grpcAdapter struct {
	hplugin.NetRPCUnsupportedPlugin
	Impl Service
}

func (p *grpcAdapter) GRPCServer(_ *hplugin.GRPCBroker, s *grpc.Server) error {
	if p.Impl == nil {
		return errors.New("adapter implementation is nil")
	}
	v2.RegisterAdapterServiceServer(s, &grpcAdapterServer{impl: p.Impl})
	return nil
}

// GRPCClient is not used in the adapter process; the host-side client lives in
// internal/adapterhost. This stub satisfies the hplugin.GRPCPlugin interface.
func (p *grpcAdapter) GRPCClient(_ context.Context, _ *hplugin.GRPCBroker, _ *grpc.ClientConn) (interface{}, error) {
	return nil, errors.New("GRPCClient is not implemented in the adapter process")
}

// grpcAdapterServer bridges the generated AdapterServiceServer interface to Service.
// Only the RPCs in Service (Info, OpenSession, Execute, Log, Permissions,
// CloseSession) are explicitly bridged; lifecycle RPCs (Pause, Resume,
// Snapshot, Restore, Inspect) fall back to the generated Unimplemented stubs
// via the embedded v2.UnimplementedAdapterServiceServer.
type grpcAdapterServer struct {
	v2.UnimplementedAdapterServiceServer
	impl Service
}

func (s *grpcAdapterServer) Info(ctx context.Context, req *v2.InfoRequest) (*v2.InfoResponse, error) {
	return s.impl.Info(ctx, req)
}

func (s *grpcAdapterServer) OpenSession(ctx context.Context, req *v2.OpenSessionRequest) (*v2.OpenSessionResponse, error) {
	return s.impl.OpenSession(ctx, req)
}

// Execute adapts the generated server-streaming signature to ExecuteEventSender.
func (s *grpcAdapterServer) Execute(req *v2.ExecuteRequest, stream v2.AdapterService_ExecuteServer) error {
	return s.impl.Execute(stream.Context(), req, &grpcExecuteEventServer{stream: stream})
}

// Log adapts the generated server-streaming signature to LogEventSender.
//
// The SDK owns the log stream and its heartbeat for the entire lifetime of the
// stream context, even if the adapter's Log implementation returns early. The
// host's heartbeat-stall detector is fed solely by the per-session Log stream,
// and it declares a session crashed after 90s of silence. An idle adapter
// session (e.g. a reviewer waiting behind a long developer or CI step on another
// session) emits no log lines on its own, so without these heartbeats it is
// falsely declared crashed.
//
// If the adapter's Log returns a non-nil error, that error is propagated and
// the stream ends. A nil return simply means the adapter has nothing further
// to log; the SDK keeps the stream alive and heartbeats running until the host
// tears the stream down.
//
// Sends are serialised by grpcLogEventServer's mutex, so the heartbeat
// goroutine is safe alongside any log lines the adapter's Log implementation
// emits.
func (s *grpcAdapterServer) Log(req *v2.LogRequest, stream v2.AdapterService_LogServer) error {
	sender := &grpcLogEventServer{stream: stream}

	// Run the heartbeat on the stream context so it survives an early nil
	// return from the adapter's Log implementation.
	logHeartbeatRunner(stream.Context(), sender)

	// Run the adapter's Log implementation concurrently so that an early nil
	// return does not tear down the heartbeat ticker.
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.impl.Log(stream.Context(), req, sender)
	}()

	// Propagate real errors immediately.
	err := <-errCh
	if err != nil {
		return err
	}

	// The adapter has finished logging. Keep the stream (and therefore the
	// heartbeat) open until the host cancels the stream context.
	<-stream.Context().Done()
	return nil
}

// Permissions adapts the generated bidi-streaming signature to PermissionsStream.
func (s *grpcAdapterServer) Permissions(stream v2.AdapterService_PermissionsServer) error {
	return s.impl.Permissions(stream.Context(), &grpcPermissionsServer{stream: stream})
}

func (s *grpcAdapterServer) CloseSession(ctx context.Context, req *v2.CloseSessionRequest) (*v2.CloseSessionResponse, error) {
	return s.impl.CloseSession(ctx, req)
}

// grpcExecuteEventServer wraps AdapterService_ExecuteServer to satisfy ExecuteEventSender.
// The mutex serialises all Send calls because grpc.ServerStream.Send is not goroutine-safe.
type grpcExecuteEventServer struct {
	mu     sync.Mutex
	stream v2.AdapterService_ExecuteServer
}

func (s *grpcExecuteEventServer) Send(evt *v2.ExecuteEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stream.Send(evt)
}

// grpcLogEventServer wraps AdapterService_LogServer to satisfy LogEventSender.
type grpcLogEventServer struct {
	mu     sync.Mutex
	stream v2.AdapterService_LogServer
}

func (s *grpcLogEventServer) Send(evt *v2.LogEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stream.Send(evt)
}

// grpcPermissionsServer wraps AdapterService_PermissionsServer to satisfy PermissionsStream.
type grpcPermissionsServer struct {
	stream v2.AdapterService_PermissionsServer
}

func (s *grpcPermissionsServer) Recv() (*v2.PermissionEvent, error) {
	return s.stream.Recv()
}

func (s *grpcPermissionsServer) Send(dec *v2.PermissionDecision) error {
	return s.stream.Send(dec)
}

func (s *grpcPermissionsServer) Context() context.Context {
	return s.stream.Context()
}
