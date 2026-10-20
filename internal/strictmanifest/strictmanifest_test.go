package strictmanifest

import (
	"strings"
	"testing"
)

func mustParse(t *testing.T, doc string) *Manifest {
	t.Helper()
	m, err := Parse([]byte(doc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	return m
}

const validDoc = `{
  "schema_version": 1,
  "name": "greeter",
  "version": "0.1.0",
  "description": "greets",
  "source_url": "https://example.com/greeter",
  "platforms": [{"os": "linux", "arch": "amd64"}],
  "sdk_protocol_version": 2,
  "input_schema": {"fields": {"name": {"type": "string", "required": false}}},
  "container_image": {"ref": "ghcr.io/x/greeter:1", "digest": "sha256:abc"}
}`

func TestParseValidDoc(t *testing.T) {
	m := mustParse(t, validDoc)
	if m.Name != "greeter" || m.SchemaVersion != 1 || len(m.Platforms) != 1 {
		t.Fatalf("unexpected doc: %+v", m)
	}
}

func TestParseRejectsUnknownKeys(t *testing.T) {
	// The host's strict parser (KB-180d) must reject drift like an added
	// top-level key the Manifest schema does not know.
	_, err := Parse([]byte(`{"name":"x","examples":[]}`))
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected unknown-field rejection, got %v", err)
	}
}

func TestValidateRejections(t *testing.T) {
	cases := map[string]func(*Manifest){
		"name not kebab-case":  func(m *Manifest) { m.Name = "Greeter" },
		"version not semver":   func(m *Manifest) { m.Version = "1.0" },
		"prerelease rejected":  func(m *Manifest) { m.Version = "1.0.0-rc.1" },
		"source_url required":  func(m *Manifest) { m.SourceURL = "" },
		"source_url no scheme": func(m *Manifest) { m.SourceURL = "github.com/x/y" },
		"schema_version 2":     func(m *Manifest) { m.SchemaVersion = 2 },
		"schema_version 0":     func(m *Manifest) { m.SchemaVersion = 0 },
		"no platforms":         func(m *Manifest) { m.Platforms = nil },
		"bad platform os":      func(m *Manifest) { m.Platforms[0].OS = "Linux" },
		"bad platform arch":    func(m *Manifest) { m.Platforms[0].Arch = "amd-64" },
		"platform arch empty":  func(m *Manifest) { m.Platforms[0].Arch = "" },
		"protocol version 3":   func(m *Manifest) { m.SDKProtocolVersion = 3 },
		"field type bool":      func(m *Manifest) { setFieldType(m, "bool") },
		"field type list_str":  func(m *Manifest) { setFieldType(m, "list_string") },
		"field type empty":     func(m *Manifest) { setFieldType(m, "") },
		"secret without name":  func(m *Manifest) { m.Secrets = []Secret{{}} },
		"bad environment":      func(m *Manifest) { m.CompatibleEnvironments = []string{"Docker"} },
		"container ref empty":  func(m *Manifest) { m.ContainerImage.Ref = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			m := mustParse(t, validDoc)
			mutate(m)
			if err := m.Validate(); err == nil {
				t.Errorf("Validate() = nil, want error for %q", name)
			} else {
				t.Logf("rejected as expected: %v", err)
			}
		})
	}
}

func TestValidateAcceptsStarEnvironment(t *testing.T) {
	m := mustParse(t, validDoc)
	m.CompatibleEnvironments = []string{"*", "sandbox"}
	if err := m.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil for '*' and lowercase envs", err)
	}
}

func setFieldType(m *Manifest, typ string) {
	f := m.InputSchema.Fields["name"]
	f.Type = typ
	m.InputSchema.Fields["name"] = f
}
