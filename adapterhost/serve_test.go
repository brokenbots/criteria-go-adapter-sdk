package adapterhost

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// TestAdapterWireNames verifies that the v2 service descriptor has the expected
// methods. A mismatch causes host/adapter negotiation to fail at runtime.
func TestAdapterWireNames(t *testing.T) {
	svc := v2.File_criteria_v2_adapter_proto.Services().ByName("AdapterService")
	if svc == nil {
		t.Fatal("AdapterService not found in v2 proto descriptor")
	}

	wantService := string(svc.FullName())
	const wantServiceName = "criteria.v2.AdapterService"
	if wantService != wantServiceName {
		t.Errorf("service full name = %q; want %q", wantService, wantServiceName)
	}

	for _, tc := range []struct {
		name   string
		method string
	}{
		{"Info", "Info"},
		{"OpenSession", "OpenSession"},
		{"Execute", "Execute"},
		{"Log", "Log"},
		{"Permissions", "Permissions"},
		{"Pause", "Pause"},
		{"Resume", "Resume"},
		{"Snapshot", "Snapshot"},
		{"Restore", "Restore"},
		{"Inspect", "Inspect"},
		{"CloseSession", "CloseSession"},
	} {
		var found bool
		for i := 0; i < svc.Methods().Len(); i++ {
			if string(svc.Methods().Get(i).Name()) == tc.method {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("method %q not found in v2 proto descriptor", tc.method)
		}
	}
}

// TestHandshakeConfigValues confirms that the magic cookie constants are
// consistent with the HandshakeConfig. An accidental edit to one without
// updating the other would break the host/adapter handshake.
func TestHandshakeConfigValues(t *testing.T) {
	if HandshakeConfig.MagicCookieKey != MagicCookieKey {
		t.Errorf("HandshakeConfig.MagicCookieKey = %q; want %q", HandshakeConfig.MagicCookieKey, MagicCookieKey)
	}
	if HandshakeConfig.MagicCookieValue != MagicCookieValue {
		t.Errorf("HandshakeConfig.MagicCookieValue = %q; want %q", HandshakeConfig.MagicCookieValue, MagicCookieValue)
	}
	if HandshakeConfig.ProtocolVersion != 2 {
		t.Errorf("HandshakeConfig.ProtocolVersion = %d; want 2", HandshakeConfig.ProtocolVersion)
	}
}

// TestGRPCServerNilImpl confirms that calling GRPCServer with a nil Impl
// returns an error rather than panicking. This guard prevents a subtle
// misconfigured-adapter failure mode.
func TestGRPCServerNilImpl(t *testing.T) {
	p := &grpcAdapter{Impl: nil}
	err := p.GRPCServer(nil, nil)
	if err == nil {
		t.Fatal("expected non-nil error from GRPCServer with nil Impl, got nil")
	}
}

// logFixtureService is a minimal Service used by the log-stream lifetime tests.
// Only Log is exercised; the rest are stubs.
type logFixtureService struct {
	UnimplementedPermissions
	logFn func(context.Context, *v2.LogRequest, LogEventSender) error
}

func (logFixtureService) Info(context.Context, *v2.InfoRequest) (*v2.InfoResponse, error) {
	return &v2.InfoResponse{}, nil
}

func (logFixtureService) OpenSession(context.Context, *v2.OpenSessionRequest) (*v2.OpenSessionResponse, error) {
	return &v2.OpenSessionResponse{}, nil
}

func (logFixtureService) Execute(context.Context, *v2.ExecuteRequest, ExecuteEventSender) error {
	return nil
}

func (s *logFixtureService) Log(ctx context.Context, req *v2.LogRequest, sender LogEventSender) error {
	if s.logFn != nil {
		return s.logFn(ctx, req, sender)
	}
	return nil
}

func (logFixtureService) CloseSession(context.Context, *v2.CloseSessionRequest) (*v2.CloseSessionResponse, error) {
	return &v2.CloseSessionResponse{}, nil
}

// withFastLogHeartbeat swaps the production 30s heartbeat ticker for a fast
// test ticker and restores it on test cleanup.
func withFastLogHeartbeat(t *testing.T, interval time.Duration) {
	t.Helper()
	orig := logHeartbeatRunner
	logHeartbeatRunner = func(ctx context.Context, sender LogEventSender) {
		go func() {
			_ = v2.RunHeartbeatWithInterval(ctx, "log", func(hb *v2.Heartbeat) error {
				return sender.Send(&v2.LogEvent{Heartbeat: hb})
			}, interval)
		}()
	}
	t.Cleanup(func() { logHeartbeatRunner = orig })
}

// startLogTestServer starts a gRPC server hosting grpcAdapterServer for svc
// and returns a client connected to it. Cleanup stops the server and closes
// the connection.
func startLogTestServer(t *testing.T, svc Service) v2.AdapterServiceClient {
	t.Helper()
	client, stop, err := serveAdapterOnLoopback(svc)
	if err != nil {
		t.Fatalf("serve adapter on loopback: %v", err)
	}
	t.Cleanup(stop)
	return client
}

// TestLogEarlyReturnKeepsHeartbeats verifies that an adapter whose Log returns
// nil immediately keeps the log stream (and its heartbeats) alive until the
// host cancels the stream context. Without the fix, the handler returned when
// impl.Log returned and no further heartbeats were sent.
func TestLogEarlyReturnKeepsHeartbeats(t *testing.T) {
	withFastLogHeartbeat(t, 10*time.Millisecond)
	client := startLogTestServer(t, &logFixtureService{
		logFn: func(context.Context, *v2.LogRequest, LogEventSender) error { return nil },
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.Log(ctx, &v2.LogRequest{SessionId: "s1", StepName: "log-test"})
	if err != nil {
		t.Fatalf("Log: %v", err)
	}

	// Cancel the stream after long enough to collect several heartbeats.
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	count := 0
	for {
		ev, err := stream.Recv()
		if err != nil {
			break
		}
		if ev.GetHeartbeat() != nil {
			count++
		}
	}

	if count < 5 {
		t.Fatalf("expected at least 5 heartbeats after early nil return, got %d", count)
	}
}

// TestLogBlockingReturnUnchanged verifies that an adapter whose Log blocks on
// context cancellation continues to work exactly as before.
func TestLogBlockingReturnUnchanged(t *testing.T) {
	withFastLogHeartbeat(t, 10*time.Millisecond)
	client := startLogTestServer(t, &logFixtureService{
		logFn: func(ctx context.Context, _ *v2.LogRequest, _ LogEventSender) error {
			<-ctx.Done()
			return nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.Log(ctx, &v2.LogRequest{SessionId: "s1", StepName: "log-test"})
	if err != nil {
		t.Fatalf("Log: %v", err)
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	count := 0
	for {
		ev, err := stream.Recv()
		if err != nil {
			break
		}
		if ev.GetHeartbeat() != nil {
			count++
		}
	}

	if count < 5 {
		t.Fatalf("expected at least 5 heartbeats while Log blocked on context, got %d", count)
	}
}

// TestLogErrorPropagated verifies that a non-nil error from the adapter's Log
// implementation is still returned to the host.
func TestLogErrorPropagated(t *testing.T) {
	// Use the production heartbeat runner so no 10ms ticks race with the
	// immediate error.
	client := startLogTestServer(t, &logFixtureService{
		logFn: func(context.Context, *v2.LogRequest, LogEventSender) error {
			return errors.New("adapter boom")
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Log(ctx, &v2.LogRequest{SessionId: "s1", StepName: "log-test"})
	if err != nil {
		// Server may return the error before the client reads any messages.
		if !strings.Contains(err.Error(), "adapter boom") {
			t.Fatalf("Log initial error does not contain adapter message: %v", err)
		}
		return
	}

	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected error from Recv, got nil")
	}
	if !strings.Contains(err.Error(), "adapter boom") {
		t.Fatalf("Recv error does not contain adapter message: %v", err)
	}
}

// TestLogHeartbeatsStopOnCancel verifies that heartbeats stop when the stream
// context is cancelled, so the host's stall detector can still catch genuine
// crashes and hangs.
func TestLogHeartbeatsStopOnCancel(t *testing.T) {
	withFastLogHeartbeat(t, 10*time.Millisecond)
	client := startLogTestServer(t, &logFixtureService{
		logFn: func(context.Context, *v2.LogRequest, LogEventSender) error { return nil },
	})

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.Log(ctx, &v2.LogRequest{SessionId: "s1", StepName: "log-test"})
	if err != nil {
		t.Fatalf("Log: %v", err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	count := 0
	for {
		ev, err := stream.Recv()
		if err != nil {
			break
		}
		if ev.GetHeartbeat() != nil {
			count++
		}
	}

	if count < 2 {
		t.Fatalf("expected at least 2 heartbeats before cancellation, got %d", count)
	}
	firstCount := count

	// Give any rogue heartbeat goroutine time to send another message if it
	// failed to observe cancellation. Because the stream is closed, Recv must
	// not block forever and no additional messages should arrive.
	time.Sleep(50 * time.Millisecond)
	for {
		_, err := stream.Recv()
		if err != nil {
			break
		}
		count++
	}

	if count != firstCount {
		t.Fatalf("heartbeats continued after cancellation: firstCount=%d final=%d", firstCount, count)
	}
}
