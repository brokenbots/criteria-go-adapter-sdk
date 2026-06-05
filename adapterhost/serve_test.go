package adapterhost

import (
	"testing"

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
