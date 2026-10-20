// Package strictmanifest is test-support for the SDK's manifest emit path: a
// strict mirror of the host's manifest parser (brokenbots/criteria
// internal/adapter/manifest, mirrored from main @ 2026-10-20 / proto pin
// 3a3aa0c1). The SDK cannot import the host's internal packages, so this
// package re-states the parser's contract with unknown-field rejection and
// the Validate rules, so emit tests can prove emitted manifests are
// publishable — including the strict-parser behaviour KB-180d adds (unknown
// fields rejected, per-key field validation). The KB-180d fixture set may
// replace or extend this package once it lands.
package strictmanifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
)

// Manifest mirrors the host Manifest yaml keys via json tags; emitted bytes
// are pure JSON (a YAML subset), so the same tags match both parsers.
type Manifest struct {
	SchemaVersion          int             `json:"schema_version"`
	Name                   string          `json:"name"`
	Version                string          `json:"version"`
	Description            string          `json:"description"`
	SourceURL              string          `json:"source_url"`
	Capabilities           []string        `json:"capabilities,omitempty"`
	Platforms              []Platform      `json:"platforms"`
	SDKProtocolVersion     int             `json:"sdk_protocol_version"`
	ConfigSchema           *Schema         `json:"config_schema,omitempty"`
	InputSchema            *Schema         `json:"input_schema,omitempty"`
	OutputSchema           *Schema         `json:"output_schema,omitempty"`
	Secrets                []Secret        `json:"secrets,omitempty"`
	Permissions            []string        `json:"permissions,omitempty"`
	CompatibleEnvironments []string        `json:"compatible_environments,omitempty"`
	ContainerImage         *ContainerImage `json:"container_image,omitempty"`
}

// Platform mirrors the host platform document.
type Platform struct {
	OS   string `json:"os"`
	Arch string `json:"arch,omitempty"`
}

// Schema mirrors the host schema document (fields only).
type Schema struct {
	Fields map[string]Field `json:"fields"`
}

// Field mirrors the host field document. Type must be one of the host's
// well-known types; the emitter carries spec types verbatim, so the strict
// parse doubles as the type-vocabulary cross-check.
type Field struct {
	Type        string `json:"type"`
	Required    bool   `json:"required,omitempty"`
	Description string `json:"description,omitempty"`
	Default     any    `json:"default,omitempty"`
	Sensitive   bool   `json:"sensitive,omitempty"`
}

// Secret mirrors the host secret document.
type Secret struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
}

// ContainerImage mirrors the host container image document.
type ContainerImage struct {
	Ref    string `json:"ref"`
	Digest string `json:"digest,omitempty"`
}

// Parse decodes a manifest strictly: unknown keys are rejected (the KB-180d
// strict behaviour) and the document structure is validated. The mirror is
// deliberately stricter than the host on semver (full MAJOR.MINOR.PATCH, no
// prerelease/build suffixes) and does not verify container digests (the SDK
// never emits one; the host checks them with go-digest).
func Parse(data []byte) (*Manifest, error) {
	var m Manifest
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	if dec.More() {
		return nil, fmt.Errorf("parse: unexpected trailing content after manifest")
	}
	return &m, nil
}

// Validate mirrors the host's parse-time validation rules.
func (m *Manifest) Validate() error {
	if m.SchemaVersion < 1 || m.SchemaVersion > manifestMaxSchemaVersion {
		return fmt.Errorf("manifest: schema_version %d outside supported range [1,%d]", m.SchemaVersion, manifestMaxSchemaVersion)
	}
	if !nameRe.MatchString(m.Name) {
		return fmt.Errorf("manifest: name %q does not match %s", m.Name, nameRe)
	}
	if !versionRe.MatchString(m.Version) {
		return fmt.Errorf("manifest: version %q is not a valid semver", m.Version)
	}
	if m.SourceURL == "" {
		return fmt.Errorf("manifest: source_url is required")
	}
	if !urlSchemeRe.MatchString(m.SourceURL) {
		return fmt.Errorf("manifest: source_url %q must include a scheme", m.SourceURL)
	}
	if m.SDKProtocolVersion != 2 {
		return fmt.Errorf("manifest: sdk_protocol_version %d outside supported range", m.SDKProtocolVersion)
	}
	if len(m.Platforms) == 0 {
		return fmt.Errorf("manifest: at least one platform is required")
	}
	for _, p := range m.Platforms {
		if !osRe.MatchString(p.OS) {
			return fmt.Errorf("manifest: platform os %q invalid", p.OS)
		}
		if p.Arch != "" && !archRe.MatchString(p.Arch) {
			return fmt.Errorf("manifest: platform arch %q invalid", p.Arch)
		}
	}
	if err := validateSchema("config_schema", m.ConfigSchema); err != nil {
		return err
	}
	if err := validateSchema("input_schema", m.InputSchema); err != nil {
		return err
	}
	if err := validateSchema("output_schema", m.OutputSchema); err != nil {
		return err
	}
	for i, s := range m.Secrets {
		if s.Name == "" {
			return fmt.Errorf("manifest: secrets[%d]: name is required", i)
		}
	}
	for _, env := range m.CompatibleEnvironments {
		if env != "*" && !envRe.MatchString(env) {
			return fmt.Errorf("manifest: compatible_environment %q invalid", env)
		}
	}
	if m.ContainerImage != nil && m.ContainerImage.Ref == "" {
		return fmt.Errorf("manifest: container_image.ref is required when set")
	}
	return nil
}

func validateSchema(key string, s *Schema) error {
	if s == nil {
		return nil
	}
	for name, f := range s.Fields {
		if !fieldTypeRe.MatchString(f.Type) {
			return fmt.Errorf("manifest: %s: field %q: type %q is not one of string, number, boolean, object, array", key, name, f.Type)
		}
	}
	return nil
}

const manifestMaxSchemaVersion = 1

var (
	nameRe      = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	osRe        = regexp.MustCompile(`^[a-z][a-z0-9]*$`)
	archRe      = regexp.MustCompile(`^[a-z0-9_]+$`)
	envRe       = regexp.MustCompile(`^[a-z][a-z_]*$`)
	urlSchemeRe = regexp.MustCompile(`^[a-z][a-z0-9+.-]+://`)
	// Full semver only: the mirror is deliberately stricter than the host's
	// golang.org/x/mod/semver check (dependency-free, catches sloppy values).
	versionRe   = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)
	fieldTypeRe = regexp.MustCompile(`^(string|number|boolean|object|array)$`)
)
