package adapterhost

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// remoteTestService is a minimal Service used only by ServeRemote tests. Only
// Info is exercised; the rest satisfy the interface.
type remoteTestService struct {
	UnimplementedPermissions
}

func (remoteTestService) Info(context.Context, *v2.InfoRequest) (*v2.InfoResponse, error) {
	return &v2.InfoResponse{Name: "remote-test", Version: "9.9.9"}, nil
}

func (remoteTestService) OpenSession(context.Context, *v2.OpenSessionRequest) (*v2.OpenSessionResponse, error) {
	return &v2.OpenSessionResponse{}, nil
}

func (remoteTestService) Execute(context.Context, *v2.ExecuteRequest, ExecuteEventSender) error {
	return nil
}

func (remoteTestService) Log(context.Context, *v2.LogRequest, LogEventSender) error {
	return nil
}

func (remoteTestService) CloseSession(context.Context, *v2.CloseSessionRequest) (*v2.CloseSessionResponse, error) {
	return &v2.CloseSessionResponse{}, nil
}

func TestServeRemoteValidation(t *testing.T) {
	if err := ServeRemote(remoteTestService{}, nil); err == nil {
		t.Error("expected error for nil opts")
	}
	if err := ServeRemote(remoteTestService{}, &ServeRemoteOptions{}); err == nil {
		t.Error("expected error for empty Host")
	}
}

func TestSendHandshakeFrame(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	go func() {
		_ = sendHandshake(c1, RemoteIdentity{Name: "n", Version: "1.0.0", Digest: "sha256:abc"}, "tok-123")
	}()

	line, err := bufio.NewReader(c2).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read handshake: %v", err)
	}
	var hs remoteHandshake
	if err := json.Unmarshal(line, &hs); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if hs.Name != "n" || hs.Version != "1.0.0" || hs.Digest != "sha256:abc" {
		t.Errorf("unexpected identity fields: %+v", hs)
	}
	if hs.Token != "tok-123" {
		t.Errorf("token = %q; want tok-123", hs.Token)
	}
	if hs.SDKProtocolVersion != 2 {
		t.Errorf("sdk_protocol_version = %d; want 2", hs.SDKProtocolVersion)
	}
}

// TestServeRemoteEndToEnd dials a fake host (the listener), reads the identity
// handshake, then speaks gRPC back over the held connection to call Info() —
// exercising the full phone-home path the way the host shim does.
func TestServeRemoteEndToEnd(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	type accepted struct {
		hs   remoteHandshake
		conn net.Conn
		err  error
	}
	acceptCh := make(chan accepted, 1)
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			acceptCh <- accepted{err: err}
			return
		}
		// Read the handshake line byte-by-byte so no gRPC bytes are buffered.
		var raw []byte
		buf := make([]byte, 1)
		for {
			if _, err := conn.Read(buf); err != nil {
				acceptCh <- accepted{err: err}
				return
			}
			if buf[0] == '\n' {
				break
			}
			raw = append(raw, buf[0])
		}
		var hs remoteHandshake
		_ = json.Unmarshal(raw, &hs)
		acceptCh <- accepted{hs: hs, conn: conn}
	}()

	go func() {
		_ = ServeRemote(remoteTestService{}, &ServeRemoteOptions{
			Host:        lis.Addr().String(),
			Identity:    RemoteIdentity{Name: "remote-test", Version: "9.9.9", Digest: "sha256:abc"},
			AcceptToken: "tok",
		})
	}()

	var got accepted
	select {
	case got = <-acceptCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for adapter to connect")
	}
	if got.err != nil {
		t.Fatalf("accept: %v", got.err)
	}
	if got.hs.Digest != "sha256:abc" || got.hs.Token != "tok" {
		t.Errorf("unexpected handshake: %+v", got.hs)
	}
	defer got.conn.Close()

	// Speak gRPC as a client over the held connection (the shim's role).
	cc, err := grpc.NewClient(
		"passthrough:///remote",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return got.conn, nil }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc client: %v", err)
	}
	defer cc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := v2.NewAdapterServiceClient(cc).Info(ctx, &v2.InfoRequest{})
	if err != nil {
		t.Fatalf("Info over held connection: %v", err)
	}
	if resp.GetName() != "remote-test" || resp.GetVersion() != "9.9.9" {
		t.Errorf("Info = %q/%q; want remote-test/9.9.9", resp.GetName(), resp.GetVersion())
	}
}
