package adapterhost

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// baseSpec returns a minimal valid Spec for tests to modify.
func baseSpec() *Spec {
	return &Spec{
		Name:    "test-adapter",
		Version: "1.2.3",
		Execute: func(context.Context, *ExecuteInput) (*ExecuteResult, error) {
			return &ExecuteResult{Outcome: "success"}, nil
		},
	}
}

// fullSpec returns a spec exercising every declarative field.
func fullSpec() *Spec {
	spec := &Spec{
		Name:                   "greeter",
		Version:                "0.3.0",
		Description:            "greets politely",
		SourceURL:              "https://example.test/greeter",
		Capabilities:           []string{CapabilityAdapterTools},
		Platforms:              []string{"linux/amd64"},
		CompatibleEnvironments: []string{"docker"},
		ContainerImage:         "ghcr.io/test/greeter:0.3.0",
		MaxChunkBytes:          1024,
		Secrets:                []Secret{{Name: "api_key", Description: "upstream auth"}},
		Permissions:            []string{"tool.call.example"},
		Examples:               []Example{{Name: "greeter", Description: "hello world", HCL: "workflow \"example\" { }"}},
		ConfigSchema: Schema{
			Field("mode", SchemaTypeString, true, "greeter mode"),
			{Name: "threshold", Type: SchemaTypeNumber, Default: "3", Sensitive: true},
		},
		InputSchema:  Schema{Field("name", SchemaTypeString, true, "who to greet")},
		OutputSchema: Schema{Field("greeting", SchemaTypeString, true, "the greeting")},
		OpenSession:  func(context.Context, *SessionStart) error { return nil },
		Execute: func(context.Context, *ExecuteInput) (*ExecuteResult, error) {
			return &ExecuteResult{Outcome: "success"}, nil
		},
		Log:          func(context.Context, *LogInput, LogWriter) error { return nil },
		CloseSession: func(context.Context, string) error { return nil },
		PermissionsFunc: func(context.Context, PermissionsStream) error {
			return errors.New("not called in deriveInfo tests")
		},
		Pause:    func(context.Context, string) error { return nil },
		Resume:   func(context.Context, string) error { return nil },
		Snapshot: func(context.Context, string) (SnapshotState, error) { return SnapshotState{}, nil },
		Restore:  func(context.Context, *RestoreInput) error { return nil },
		Inspect:  func(context.Context, string) (InspectInfo, error) { return InspectInfo{}, nil },
	}
	return spec
}

// TestDeriveInfoFullSpec is the tier-1 Info contract: every declarative spec
// field lands on the InfoResponse, with the proto schema map built only here
// (D2) and the SDK protocol version declared as "2".
func TestDeriveInfoFullSpec(t *testing.T) {
	info, err := deriveInfo(fullSpec())
	if err != nil {
		t.Fatalf("deriveInfo: %v", err)
	}
	if info.Name != "greeter" || info.Version != "0.3.0" || info.Description != "greets politely" || info.SourceUrl != "https://example.test/greeter" {
		t.Errorf("metadata mismatch: %+v", info)
	}
	if info.SdkProtocolVersion != wireProtocolVersion {
		t.Errorf("SdkProtocolVersion = %q; want %q", info.SdkProtocolVersion, wireProtocolVersion)
	}
	if len(info.Capabilities) != 1 || info.Capabilities[0] != CapabilityAdapterTools {
		t.Errorf("Capabilities = %v", info.Capabilities)
	}
	if len(info.Platforms) != 1 || info.Platforms[0] != "linux/amd64" {
		t.Errorf("Platforms = %v", info.Platforms)
	}
	if len(info.CompatibleEnvironments) != 1 || info.CompatibleEnvironments[0] != "docker" {
		t.Errorf("CompatibleEnvironments = %v", info.CompatibleEnvironments)
	}
	if info.ContainerImage != "ghcr.io/test/greeter:0.3.0" {
		t.Errorf("ContainerImage = %q", info.ContainerImage)
	}
	if info.MaxChunkBytes != 1024 {
		t.Errorf("MaxChunkBytes = %d; want 1024", info.MaxChunkBytes)
	}
	if len(info.Secrets) != 1 || info.Secrets["api_key"] != "upstream auth" {
		t.Errorf("Secrets = %v", info.Secrets)
	}
	if len(info.Permissions) != 1 || info.Permissions[0] != "tool.call.example" {
		t.Errorf("Permissions = %v", info.Permissions)
	}
	if len(info.Examples) != 1 || info.Examples[0].Name != "greeter" || info.Examples[0].Hcl != "workflow \"example\" { }" {
		t.Errorf("Examples = %+v", info.Examples)
	}

	// Schemas are the keyed proto form derived from the ordered plain list.
	if info.ConfigSchema == nil || len(info.ConfigSchema.Fields) != 2 {
		t.Fatalf("ConfigSchema = %+v", info.ConfigSchema)
	}
	mode, ok := info.ConfigSchema.Fields["mode"]
	if !ok || mode.Type != SchemaTypeString || !mode.Required || mode.Description != "greeter mode" || mode.DefaultStr != "" || mode.Sensitive {
		t.Errorf("ConfigSchema[major] = %+v", mode)
	}
	threshold := info.ConfigSchema.Fields["threshold"]
	if threshold == nil || threshold.Type != SchemaTypeNumber || threshold.Required || threshold.DefaultStr != "3" || !threshold.Sensitive {
		t.Errorf("ConfigSchema[threshold] = %+v", threshold)
	}
	if info.InputSchema == nil || info.InputSchema.Fields["name"] == nil {
		t.Errorf("InputSchema = %+v", info.InputSchema)
	}
	if info.OutputSchema == nil || info.OutputSchema.Fields["greeting"] == nil {
		t.Errorf("OutputSchema = %+v", info.OutputSchema)
	}

	// set-what-you-declare, canonical order.
	want := strings.Join([]string{FeaturePause, FeatureResume, FeatureSnapshot, FeatureRestore, FeatureInspect}, ",")
	if got := strings.Join(info.SupportedFeatures, ","); got != want {
		t.Errorf("SupportedFeatures = %v; want %v", info.SupportedFeatures, want)
	}
}

// TestDeriveInfoMinSpec checks the zero-value paths: unset optional lists go
// over the wire unset, chunking falls back to the protocol default, and a
// spec without any lifecycle verbs advertises none.
func TestDeriveInfoMinSpec(t *testing.T) {
	info, err := deriveInfo(baseSpec())
	if err != nil {
		t.Fatalf("deriveInfo: %v", err)
	}
	if info.MaxChunkBytes != 0 { // 0 = protocol default, applied by the host
		t.Errorf("MaxChunkBytes = %d; want 0", info.MaxChunkBytes)
	}
	if len(info.SupportedFeatures) != 0 {
		t.Errorf("SupportedFeatures = %v; want empty", info.SupportedFeatures)
	}
	if info.Secrets != nil || info.Examples != nil || info.ConfigSchema != nil || info.InputSchema != nil || info.OutputSchema != nil {
		t.Errorf("unset spec regions leaked to Info: %+v", info)
	}
}

// TestSupportedFeaturesOverride pins the override escape hatch: a non-nil
// SupportedFeatures wins verbatim — including an explicitly empty advertise
// list — while nil keeps the derived list.
func TestSupportedFeaturesOverride(t *testing.T) {
	spec := baseSpec()
	spec.Snapshot = func(context.Context, string) (SnapshotState, error) { return SnapshotState{}, nil }
	info, err := deriveInfo(spec)
	if err != nil {
		t.Fatalf("deriveInfo: %v", err)
	}
	if strings.Join(info.SupportedFeatures, ",") != FeatureSnapshot {
		t.Errorf("derived features = %v; want [snapshot]", info.SupportedFeatures)
	}

	spec.SupportedFeatures = []string{}
	info, err = deriveInfo(spec) // still valid: the verb gate stays per-handler
	if err != nil {
		t.Fatalf("deriveInfo with empty override: %v", err)
	}
	if len(info.SupportedFeatures) != 0 {
		t.Errorf("empty override = %v; want verbatim empty", info.SupportedFeatures)
	}

	spec.SupportedFeatures = []string{FeaturePause}
	info, err = deriveInfo(spec)
	if err != nil {
		t.Fatalf("deriveInfo with override: %v", err)
	}
	if strings.Join(info.SupportedFeatures, ",") != "pause" {
		t.Errorf("override = %v; want [pause]", info.SupportedFeatures)
	}
}

// TestSpecValidation pins the loud author errors.
func TestSpecValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Spec)
		want   string
	}{
		{"no name", func(s *Spec) { s.Name = "" }, "Name is required"},
		{"no version", func(s *Spec) { s.Version = "" }, "Version is required"},
		{"no execute", func(s *Spec) { s.Execute = nil }, "Execute handler is required"},
		{"duplicate config field", func(s *Spec) {
			s.ConfigSchema = Schema{Field("a", "string", false, ""), Field("a", "string", false, "")}
		}, `duplicate field "a"`},
		{"empty field type", func(s *Spec) {
			s.InputSchema = Schema{Field("x", "", false, "d")}
		}, `field "x": type is required`},
		{"duplicate secret", func(s *Spec) {
			s.Secrets = []Secret{{Name: "k"}, {Name: "k"}}
		}, `duplicate secret "k"`},
		{"example without name", func(s *Spec) {
			s.Examples = []Example{{HCL: "wf"}}
		}, "example 0: name is required"},
		{"example name only", func(s *Spec) {
			s.Examples = []Example{{Name: "a"}}
		}, `example "a": declares neither`},
		{"example hcl and file", func(s *Spec) {
			s.Examples = []Example{{Name: "a", HCL: "wf", File: "/tmp/a.hcl"}}
		}, "both inline HCL and File"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := baseSpec()
			tc.mutate(spec)
			if _, err := SpecService(spec); err == nil {
				t.Fatalf("expected validation error for %s", tc.name)
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q; want substring %q", err, tc.want)
			}
		})
	}
}

// TestExamplesResolvedFromSpec covers D5 inline delivery: file examples are
// read and inlined at Info time; missing files fail loudly with the path.
func TestExamplesResolvedFromSpec(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "greeter.hcl")
	if err := os.WriteFile(path, []byte("workflow \"example\" { }"), 0o600); err != nil {
		t.Fatal(err)
	}
	spec := baseSpec()
	spec.Examples = []Example{
		{Name: "inline", Description: "declared inline", HCL: "workflow \"i\" { }"},
		{Name: "fromfile", File: path},
	}
	info, err := deriveInfo(spec)
	if err != nil {
		t.Fatalf("deriveInfo: %v", err)
	}
	if len(info.Examples) != 2 {
		t.Fatalf("Examples = %+v", info.Examples)
	}
	if info.Examples[1].Hcl != "workflow \"example\" { }" {
		t.Errorf("file example not inlined: %q", info.Examples[1].Hcl)
	}

	spec.Examples = []Example{{Name: "gone", File: filepath.Join(dir, "missing.hcl")}}
	_, err = deriveInfo(spec)
	if err == nil || !strings.Contains(err.Error(), "missing.hcl") {
		t.Fatalf("missing file err = %v; want loud read failure mentioning the path", err)
	}
}

// TestInspectResponseFromInfo pins the plain-InspectInfo → wire mapping,
// including zero LastActivity not emitting a timestamp and unencodable
// inspect values failing loudly.
func TestInspectResponseFromInfo(t *testing.T) {
	resp, err := inspectResponseFromInfo(InspectInfo{
		CurrentStep:        "greet",
		PendingPermissions: 2,
		LastActivity:       time.Unix(1700000000, 0).UTC(),
		Fields:             []InspectField{{Key: "user", Label: "User", Value: "world"}},
		Extra:              map[string]any{"kind": "demo"},
	})
	if err != nil {
		t.Fatalf("inspectResponseFromInfo: %v", err)
	}
	if resp.CurrentStep != "greet" || resp.PendingPermissionCount != 2 || resp.LastActivityAt == nil || resp.Extra == nil || resp.Extra.Fields["kind"].GetStringValue() != "demo" {
		t.Errorf("InspectResponse = %+v", resp)
	}
	if len(resp.Fields) != 1 || resp.Fields[0].Key != "user" || resp.Fields[0].Value.GetStringValue() != "world" {
		t.Errorf("InspectFields = %+v", resp.Fields)
	}

	resp, err = inspectResponseFromInfo(InspectInfo{}) // zero info: no timestamps, no extras
	if err != nil || resp.CurrentStep != "" || resp.LastActivityAt != nil || resp.Fields != nil || resp.Extra != nil {
		t.Errorf("zero InspectInfo = (%+v, %v); want clean empty response", resp, err)
	}

	// A channel of unencodable Values is an adapter-side data bug: fail the
	// call rather than ship a field the host cannot parse.
	_, err = inspectResponseFromInfo(InspectInfo{Fields: []InspectField{{Key: "bad", Value: func() {}}}})
	if err == nil || !strings.Contains(err.Error(), `inspect field "bad"`) {
		t.Fatalf("unencodable err = %v; want loud per-field failure", err)
	}
}

// TestSpecServiceNil guards the converter entrypoint.
func TestSpecServiceNil(t *testing.T) {
	if _, err := SpecService(nil); err == nil {
		t.Fatal("SpecService(nil): expected error, got nil")
	}
}
