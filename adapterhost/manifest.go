package adapterhost

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// manifestSchemaVersion is the adapter.yaml schema version for protocol v2.
// Mirrors internal/adapter/manifest.ManifestMaxSchemaVersion (kept in sync
// across the module boundary — the sdk cannot import the host's internal pkg).
const manifestSchemaVersion = 1

// emitManifestRequested reports whether the process was invoked with the
// --emit-manifest flag. The build pipeline (and `criteria adapter publish`)
// run the adapter binary once with this flag to extract adapter.yaml.
func emitManifestRequested(args []string) bool {
	for _, a := range args {
		if a == "--emit-manifest" {
			return true
		}
	}
	return false
}

// emitManifest writes the adapter's static manifest (adapter.yaml) to w by
// calling the adapter's own Info RPC and mapping the response into the schema
// the host's manifest parser consumes. Output is JSON, which is valid YAML, so
// it parses identically — this avoids a YAML dependency in the sdk module.
func emitManifest(ctx context.Context, impl Service, w io.Writer) error {
	if impl == nil {
		return fmt.Errorf("adapterhost: nil adapter implementation")
	}
	info, err := impl.Info(ctx, &v2.InfoRequest{})
	if err != nil {
		return fmt.Errorf("adapterhost: Info for --emit-manifest: %w", err)
	}
	doc := buildManifestDoc(info)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(doc); err != nil {
		return fmt.Errorf("adapterhost: encode manifest: %w", err)
	}
	return nil
}

// manifestDoc is the adapter.yaml document. The json tags equal the host
// manifest schema's yaml keys so the emitted JSON parses as the host's
// manifest.Manifest.
type manifestDoc struct {
	SchemaVersion          int                `json:"schema_version"`
	Name                   string             `json:"name"`
	Version                string             `json:"version"`
	Description            string             `json:"description"`
	SourceURL              string             `json:"source_url"`
	Capabilities           []string           `json:"capabilities"`
	Platforms              []platformDoc      `json:"platforms"`
	SDKProtocolVersion     int                `json:"sdk_protocol_version"`
	ConfigSchema           schemaDoc          `json:"config_schema"`
	InputSchema            schemaDoc          `json:"input_schema"`
	OutputSchema           schemaDoc          `json:"output_schema"`
	Secrets                []secretDoc        `json:"secrets"`
	Permissions            []string           `json:"permissions"`
	CompatibleEnvironments []string           `json:"compatible_environments"`
	ContainerImage         *containerImageDoc `json:"container_image,omitempty"`
}

type platformDoc struct {
	OS   string `json:"os"`
	Arch string `json:"arch"`
}

type schemaDoc struct {
	Fields map[string]fieldDoc `json:"fields"`
}

type fieldDoc struct {
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Description string `json:"description"`
	Default     string `json:"default,omitempty"`
	Sensitive   bool   `json:"sensitive,omitempty"`
}

type secretDoc struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Required    bool   `json:"required"`
}

type containerImageDoc struct {
	Ref    string `json:"ref"`
	Digest string `json:"digest"`
}

func buildManifestDoc(info *v2.InfoResponse) manifestDoc {
	doc := manifestDoc{
		SchemaVersion:          manifestSchemaVersion,
		Name:                   info.GetName(),
		Version:                info.GetVersion(),
		Description:            info.GetDescription(),
		SourceURL:              info.GetSourceUrl(),
		Capabilities:           nonNil(info.GetCapabilities()),
		Platforms:              platformsFromStrings(info.GetPlatforms()),
		SDKProtocolVersion:     sdkProtocolVersion(info.GetSdkProtocolVersion()),
		ConfigSchema:           schemaFromProto(info.GetConfigSchema()),
		InputSchema:            schemaFromProto(info.GetInputSchema()),
		OutputSchema:           schemaFromProto(info.GetOutputSchema()),
		Secrets:                secretsFromMap(info.GetSecrets()),
		Permissions:            nonNil(info.GetPermissions()),
		CompatibleEnvironments: nonNil(info.GetCompatibleEnvironments()),
	}
	if ci := info.GetContainerImage(); ci != "" {
		doc.ContainerImage = &containerImageDoc{Ref: ci}
	}
	return doc
}

// platformsFromStrings converts "os/arch" platform strings into the host's
// {os, arch} structure. A string without a "/" is treated as os-only.
func platformsFromStrings(ps []string) []platformDoc {
	out := make([]platformDoc, 0, len(ps))
	for _, p := range ps {
		os, arch, found := strings.Cut(p, "/")
		if !found {
			out = append(out, platformDoc{OS: p})
			continue
		}
		out = append(out, platformDoc{OS: os, Arch: arch})
	}
	return out
}

// sdkProtocolVersion parses the proto's string SDK version into the manifest's
// integer field, defaulting to the v2 wire version when absent or malformed.
func sdkProtocolVersion(s string) int {
	if v, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
		return v
	}
	return 2
}

func schemaFromProto(s *v2.AdapterSchemaProto) schemaDoc {
	fields := map[string]fieldDoc{}
	for name, f := range s.GetFields() {
		fields[name] = fieldDoc{
			Type:        f.GetType(),
			Required:    f.GetRequired(),
			Description: f.GetDescription(),
			Default:     f.GetDefaultStr(),
			Sensitive:   f.GetSensitive(),
		}
	}
	return schemaDoc{Fields: fields}
}

// secretsFromMap converts the InfoResponse name→description secrets map into the
// manifest's secret declarations. The wire form does not carry per-secret
// required-ness; declared secrets are emitted as required (the common case and
// the safe default — the host verifies secrets by name only).
func secretsFromMap(m map[string]string) []secretDoc {
	out := make([]secretDoc, 0, len(m))
	for name, desc := range m {
		out = append(out, secretDoc{Name: name, Description: desc, Required: true})
	}
	return out
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// runEmitManifestIfRequested handles the --emit-manifest flag at process start.
// Returns true if the flag was handled (caller should exit).
func runEmitManifestIfRequested(impl Service) bool {
	if !emitManifestRequested(os.Args[1:]) {
		return false
	}
	if err := emitManifest(context.Background(), impl, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return true
}
