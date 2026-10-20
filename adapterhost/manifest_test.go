package adapterhost

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"reflect"
	"testing"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

func TestBuildManifestDoc_MapsInfoResponse(t *testing.T) {
	info := &v2.InfoResponse{
		Name:                   "demo",
		Version:                "1.2.3",
		Description:            "a demo adapter",
		SourceUrl:              "https://github.com/org/demo",
		Capabilities:           []string{"parallel_safe"},
		Platforms:              []string{"linux/amd64", "darwin/arm64", "weirdonly"},
		SdkProtocolVersion:     "2",
		Permissions:            []string{"shell"},
		CompatibleEnvironments: []string{"container"},
		Secrets:                map[string]string{"API_KEY": "the key"},
		ConfigSchema: &v2.AdapterSchemaProto{
			Fields: map[string]*v2.ConfigFieldProto{
				"model": {Type: "string", Required: true, Description: "model id"},
			},
		},
		ContainerImage: "ghcr.io/org/demo:1.2.3-image",
	}

	doc := buildManifestDoc(info)

	if doc.SchemaVersion != 1 {
		t.Errorf("schema_version = %d, want 1", doc.SchemaVersion)
	}
	if doc.Name != "demo" || doc.Version != "1.2.3" || doc.SourceURL != "https://github.com/org/demo" {
		t.Errorf("scalar fields wrong: %+v", doc)
	}
	if doc.SDKProtocolVersion != 2 {
		t.Errorf("sdk_protocol_version = %d, want 2", doc.SDKProtocolVersion)
	}
	// Platforms split on "/", os-only fallback for malformed entries.
	want := []platformDoc{{OS: "linux", Arch: "amd64"}, {OS: "darwin", Arch: "arm64"}, {OS: "weirdonly"}}
	if len(doc.Platforms) != len(want) {
		t.Fatalf("platforms = %+v, want %+v", doc.Platforms, want)
	}
	for i := range want {
		if doc.Platforms[i] != want[i] {
			t.Errorf("platform[%d] = %+v, want %+v", i, doc.Platforms[i], want[i])
		}
	}
	if f, ok := doc.ConfigSchema.Fields["model"]; !ok || f.Type != "string" || !f.Required {
		t.Errorf("config_schema.fields[model] wrong: %+v", doc.ConfigSchema.Fields)
	}
	if len(doc.Secrets) != 1 || doc.Secrets[0].Name != "API_KEY" || !doc.Secrets[0].Required {
		t.Errorf("secrets wrong: %+v", doc.Secrets)
	}
	if doc.ContainerImage == nil || doc.ContainerImage.Ref != "ghcr.io/org/demo:1.2.3-image" {
		t.Errorf("container_image wrong: %+v", doc.ContainerImage)
	}
}

func TestBuildManifestDoc_DefaultsAndEmpties(t *testing.T) {
	doc := buildManifestDoc(&v2.InfoResponse{Name: "x"})
	if doc.SDKProtocolVersion != 2 {
		t.Errorf("empty sdk version should default to 2, got %d", doc.SDKProtocolVersion)
	}
	// Slice fields must marshal as [] (not null) so the host parser sees empty sets.
	if doc.Capabilities == nil || doc.Permissions == nil || doc.CompatibleEnvironments == nil {
		t.Error("slice fields must be non-nil")
	}
	if doc.ContainerImage != nil {
		t.Error("container_image must be omitted when unset")
	}
}

func TestEmitManifest_FromDerivedSpecInfo_ProducesValidJSON(t *testing.T) {
	var buf bytes.Buffer
	info, err := deriveInfo(greeterEmitSpec())
	if err != nil {
		t.Fatalf("deriveInfo: %v", err)
	}
	if err := emitManifest(info, &buf); err != nil {
		t.Fatalf("emitManifest: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("emitted manifest is not valid JSON (so not valid YAML): %v", err)
	}
	if parsed["name"] != "greeter" || parsed["schema_version"].(float64) != 1 {
		t.Errorf("unexpected manifest: %v", parsed)
	}
}

// TestEmitManifest_MatchesServiceTierInfo pins the single-source rule
// (ADR-0011 D3): the emit path derives its Info with deriveInfo(spec) — the
// exact function behind specService.Info on the wire — so the manifest the
// pipeline extracts is the document the Info response maps to, byte for byte,
// with no second Info implementation involved.
func TestEmitManifest_MatchesServiceTierInfo(t *testing.T) {
	spec := greeterEmitSpec()
	derived, err := deriveInfo(spec)
	if err != nil {
		t.Fatalf("deriveInfo: %v", err)
	}
	svc, err := SpecService(spec)
	if err != nil {
		t.Fatalf("SpecService: %v", err)
	}
	info, err := svc.Info(context.Background(), &v2.InfoRequest{})
	if err != nil {
		t.Fatalf("SpecService.Info: %v", err)
	}
	if !reflect.DeepEqual(buildManifestDoc(derived), buildManifestDoc(info)) {
		t.Errorf("emit source ≠ Info source: deriveInfo(spec) %v vs SpecService.Info %v", buildManifestDoc(derived), buildManifestDoc(info))
	}
}

func TestEmitManifest_NilInfoIsLoud(t *testing.T) {
	if err := emitManifest(nil, io.Discard); err == nil {
		t.Error("emitManifest(nil) must fail loudly, not emit an empty manifest")
	}
}

func TestEmitManifestRequested(t *testing.T) {
	if !emitManifestRequested([]string{"--emit-manifest"}) {
		t.Error("expected true when flag present")
	}
	if emitManifestRequested([]string{"serve", "--other"}) {
		t.Error("expected false when flag absent")
	}
}

// greeterEmitSpec is the spec-object greeter the emit contract tests run
// against: a tier-1 declaration carrying the publish-critical metadata
// (source_url, platforms) the host manifest parser requires, shaped like the
// examples/greeter spec. There is deliberately no Info implementation here —
// the emit path must survive on deriveInfo(spec) alone.
func greeterEmitSpec() *Spec {
	return &Spec{
		Name:        "greeter",
		Version:     "0.1.0",
		Description: "greets politely",
		SourceURL:   "https://github.com/brokenbots/criteria-go-adapter-sdk",
		Platforms:   []string{"linux/amd64", "linux/arm64", "darwin/arm64"},
		InputSchema: Schema{Field("name", SchemaTypeString, false, "who to greet")},
		OutputSchema: Schema{
			Field("greeting", SchemaTypeString, true, "the greeting"),
		},
		Execute: func(context.Context, *ExecuteInput) (*ExecuteResult, error) {
			return &ExecuteResult{Outcome: "success"}, nil
		},
	}
}
