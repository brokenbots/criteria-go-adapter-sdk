package adapterhost

import (
	"bytes"
	"testing"

	"github.com/brokenbots/criteria-go-adapter-sdk/internal/strictmanifest"
)

// TestEmitManifest_SpecGreeterValidatesStrictHostParser is the KB-177b
// acceptance test: --emit-manifest JSON from a spec-object greeter (no Info
// implementation anywhere — deriveInfo(spec) is the source) parses cleanly
// under the strict host parser mirror and passes the host's validation rules.
func TestEmitManifest_SpecGreeterValidatesStrictHostParser(t *testing.T) {
	fixture := greeterEmitSpec()

	// Prove the emit source is deriveInfo(spec) with no second Info
	// implementation path involved.
	info, err := deriveInfo(fixture)
	if err != nil {
		t.Fatalf("deriveInfo: %v", err)
	}
	var buf bytes.Buffer
	if err := emitManifest(info, &buf); err != nil {
		t.Fatalf("emitManifest: %v", err)
	}

	m, err := strictmanifest.Parse(buf.Bytes())
	if err != nil {
		t.Fatalf("emitted manifest rejected by strict parser: %v\n%s", err, buf.String())
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("emitted manifest failed strict validation: %v\n%s", err, buf.String())
	}
}

// TestEmitManifest_FullSpecValidatesStrictHostParser runs the same check over
// the full fixture (capabilities, secrets, container image, compatible
// environments, permissions) so every emitted manifest key path is covered.
func TestEmitManifest_FullSpecValidatesStrictHostParser(t *testing.T) {
	info, err := deriveInfo(fullSpec())
	if err != nil {
		t.Fatalf("deriveInfo: %v", err)
	}
	var buf bytes.Buffer
	if err := emitManifest(info, &buf); err != nil {
		t.Fatalf("emitManifest: %v", err)
	}
	m, err := strictmanifest.Parse(buf.Bytes())
	if err != nil {
		t.Fatalf("emitted manifest rejected by strict parser: %v\n%s", err, buf.String())
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("emitted manifest failed strict validation: %v\n%s", err, buf.String())
	}
	if m.ConfigSchema == nil || m.Secrets == nil || m.ContainerImage == nil {
		t.Errorf("full-spec manifest missing extended sections: %+v", m)
	}
}
