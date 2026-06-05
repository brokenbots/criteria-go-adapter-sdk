package adapterhost

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brokenbots/criteria/internal/adapter/environment/remote"
	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// fakeRemoteAdapter is a minimal Service implementation for tests.
type fakeRemoteAdapter struct {
	infoName    string
	infoVersion string
}

func (a *fakeRemoteAdapter) Info(_ context.Context, _ *v2.InfoRequest) (*v2.InfoResponse, error) {
	return &v2.InfoResponse{
		Name:    a.infoName,
		Version: a.infoVersion,
	}, nil
}

func (a *fakeRemoteAdapter) OpenSession(_ context.Context, _ *v2.OpenSessionRequest) (*v2.OpenSessionResponse, error) {
	return &v2.OpenSessionResponse{}, nil
}

func (a *fakeRemoteAdapter) Execute(_ context.Context, _ *v2.ExecuteRequest, _ ExecuteEventSender) error {
	return errors.New("not implemented")
}

func (a *fakeRemoteAdapter) Log(_ context.Context, _ *v2.LogRequest, _ LogEventSender) error {
	return nil
}

func (a *fakeRemoteAdapter) Permissions(_ context.Context, _ PermissionsStream) error {
	return nil
}

func (a *fakeRemoteAdapter) CloseSession(_ context.Context, _ *v2.CloseSessionRequest) (*v2.CloseSessionResponse, error) {
	return &v2.CloseSessionResponse{}, nil
}

// TestServeRemote_Info verifies that ServeRemote can dial the WS20 shim,
// complete the handshake, and serve Info() successfully.
func TestServeRemote_Info(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Use a reserved TCP port so we know the address before starting the shim.
	tmpLis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := tmpLis.Addr().String()
	require.NoError(t, tmpLis.Close())

	shim, err := remote.NewShim(&remote.Config{ListenAddress: addr},
		nil, // no digest verifier
	)
	require.NoError(t, err)

	err = shim.Start(ctx)
	require.NoError(t, err)
	defer func() { _ = shim.Stop(ctx) }()

	adapter := &fakeRemoteAdapter{
		infoName:    "test-adapter",
		infoVersion: "1.2.3",
	}

	// Run the adapter in the background.
	adapterErr := make(chan error, 1)
	go func() {
		adapterErr <- ServeRemote(adapter, &ServeRemoteOptions{
			Host: addr,
			Identity: RemoteIdentity{
				Name:    "test-adapter",
				Version: "1.2.3",
				Digest:  "sha256:deadbeef",
			},
		})
	}()

	// Give the adapter time to connect and the shim to process it.
	time.Sleep(200 * time.Millisecond)

	// Wait for the shim to surface the adapter handle.
	handle, err := shim.WaitForHandle(ctx, "test-adapter")
	if err != nil {
		select {
		case aerr := <-adapterErr:
			t.Logf("adapter error: %v", aerr)
		default:
		}
		require.NoError(t, err)
	}
	defer handle.Kill()

	info, err := handle.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, "test-adapter", info.Name)
	assert.Equal(t, "1.2.3", info.Version)

	// Clean up the handle; the adapter should then exit.
	handle.Kill()

	select {
	case err := <-adapterErr:
		// The adapter may return an error because the connection was closed,
		// or nil if it shut down cleanly. We only care that it returns.
		_ = err
	case <-ctx.Done():
		t.Fatal("adapter did not exit in time")
	}
}

// TestServeRemote_MissingHost verifies that ServeRemote rejects an empty Host.
func TestServeRemote_MissingHost(t *testing.T) {
	adapter := &fakeRemoteAdapter{}
	err := ServeRemote(adapter, &ServeRemoteOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Host is required")
}

// TestServeRemote_NilOpts verifies that ServeRemote rejects a nil opts pointer.
func TestServeRemote_NilOpts(t *testing.T) {
	adapter := &fakeRemoteAdapter{}
	err := ServeRemote(adapter, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "opts is required")
}

// TestRemoteHandshakeRoundTrip verifies handshake JSON encoding/decoding.
func TestRemoteHandshakeRoundTrip(t *testing.T) {
	h := remoteHandshake{
		Name:               "my-adapter",
		Version:            "2.0.0",
		Digest:             "sha256:abc123",
		Token:              "secret-token",
		SDKProtocolVersion: 2,
	}
	data, err := json.Marshal(h)
	require.NoError(t, err)

	var decoded remoteHandshake
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)
	assert.Equal(t, h, decoded)
}

// TestRemoteHandshake_OmitsEmptyToken verifies omitempty on token.
func TestRemoteHandshake_OmitsEmptyToken(t *testing.T) {
	h := remoteHandshake{
		Name:               "adapter",
		Version:            "1.0.0",
		Digest:             "sha256:0000",
		SDKProtocolVersion: 2,
	}
	data, err := json.Marshal(h)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "token")
}
