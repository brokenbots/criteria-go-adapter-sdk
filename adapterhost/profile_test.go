// The adapter.core conformance profile (KB-181, KB-177c): the executable
// restatement of what a Criteria host expects from an adapter built on this
// SDK. This SDK's own implementation is the reference the other two SDKs port;
// the profile is the in-repo gate that stays green on every PR.
//
// Cases (each one subtest of TestProfileAdapterCore):
//
//   - handshake_and_info_round_trip_vs_spec: a real go-plugin handshake against
//     this repo's test binary running ServeSpec in a helper subprocess
//     (-test.run TestProfileHelperServeSpec), then a field-by-field Info
//     round-trip vs the spec object.
//   - manifest_emit_parses_under_pinned_host_parser: the --emit-manifest bytes
//     parsed + validated by internal/strictmanifest, the pinned restatement of
//     the host's manifest parser (host main tip 70959d34).
//   - execute_chunks_reassemble_in_arrival_order: oversized results delivered
//     as ordered chunks and joined the way the host's resultChunkNextSeq does.
//   - log_heartbeat_liveness_and_cadence: the SDK-owned log stream heartbeats
//     for the full stream lifetime at the declared cadence. This is the case
//     the suite's documented mutation check kills (see
//     TestProfileMutationChecks and docs/conformance-adapter-core.md).
//   - concurrent_execute_spec_tier / concurrent_execute_tier2_reference:
//     RunConcurrentExecuteConformance against the spec tier (SpecService) and
//     a tier-2 reference Service — per-call result correctness over the real
//     gRPC bridge, chunked and unchunked.
//   - secrets_resolve_once: secrets arrive as wire inputs, overlay per step
//     without mutating the session view, and never fall back to the process
//     environment (D69).
//   - permissions_per_wire: the permissionless default (UnimplementedPermissions
//     → allow with a matching request id) vs an implemented deny, both answered
//     on the bidi wire stream.
//
// Run the full profile with `make conformance` or
// `go test ./adapterhost -run TestProfileAdapterCore`. The suite is sequential
// by design: the heartbeat cases swap the logHeartbeatRunner test seam, which
// is package state.
package adapterhost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	hplugin "github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"

	"github.com/brokenbots/criteria-go-adapter-sdk/internal/strictmanifest"
)

// profileChunkBytes is the spec MaxChunkBytes the profile fixtures use: small
// enough that ordinary outputs chunk into multiple fragments, so the chunk
// cases exercise the framer rather than trusting output size.
const profileChunkBytes = 64

// profileHelperEnv gates the helper subprocess. This test binary re-runs
// itself under -test.run=TestProfileHelperServeSpec with the env set; without
// the gate the helper test returns immediately in the parent run.
const profileHelperEnv = "CRITERIA_PROFILE_HELPER_SERVE"

// profileHeartbeatSessionID is the session id the heartbeat case attaches to
// (an idle spec-emitted Log stream carries heartbeats regardless of session).
const profileHeartbeatSessionID = "s-heartbeat"

// profileReferenceSpec is the full-surface spec object the profile pins. It
// declares every Info/manifest surface (metadata, capabilities, platforms,
// environments, all three schemas, secrets, permissions, examples) and every
// lifecycle verb, so Info round-trip and manifest emit are pinned against the
// whole spec shape. Schemas deliberately use manifest-publishable types
// (string|number|boolean|object|array — the host's emit vocabulary) plus the
// full set of lifecycle verbs, so set-what-you-declare is round-tripped too.
func profileReferenceSpec() *Spec {
	return &Spec{
		Name:                   "profile-ref",
		Version:                "1.2.3",
		Description:            "adapter.core profile reference adapter",
		SourceURL:              "https://github.com/brokenbots/criteria-go-adapter-sdk",
		Capabilities:           []string{CapabilityAdapterTools},
		Platforms:              []string{"linux/amd64", "linux/arm64"},
		CompatibleEnvironments: []string{"docker"},
		MaxChunkBytes:          profileChunkBytes,
		ConfigSchema: Schema{
			Field("mode", SchemaTypeString, false, "greeting mode"),
			Field("retries", SchemaTypeNumber, false, "retry count"),
			Field("strict", "boolean", false, "fail loudly on contract drift"),
		},
		InputSchema: Schema{
			Field("name", SchemaTypeString, true, "who to greet"),
		},
		OutputSchema: Schema{
			Field("greeting", SchemaTypeString, true, "the greeting"),
			Field("call_id", SchemaTypeString, false, "per-call correlation id"),
			Field("blob", "array", false, "chunked-payload carrier"),
		},
		Secrets:     []Secret{{Name: "api_key", Description: "greeter API key"}},
		Permissions: []string{"adapter_tools.invoke"},
		Examples: []Example{{
			Name:        "greet",
			Description: "greet one target",
			HCL:         "# profile example workflow",
		}},
		Execute:      profileEchoExecute,
		CloseSession: func(context.Context, string) error { return nil },
		Pause:        func(context.Context, string) error { return nil },
		Resume:       func(context.Context, string) error { return nil },
		Snapshot: func(context.Context, string) (SnapshotState, error) {
			return SnapshotState{State: []byte("ref"), SchemaVersion: 1}, nil
		},
		Restore: func(context.Context, *RestoreInput) error { return nil },
		Inspect: func(context.Context, string) (InspectInfo, error) {
			return InspectInfo{CurrentStep: "idle"}, nil
		},
		// Log stays nil: an idle adapter session emits no lines; liveness is
		// the SDK's job (the heartbeat case asserts it).
	}
}

// profileEchoExecute is the profile's reference Execute handler: it echoes the
// correlation id back as an output, optionally carries a sized blob (which
// forces chunking under a small MaxChunkBytes), and always reports
// "succeeded". Deterministic and stateless, so every profile case can compute
// the exact result it expects.
func profileEchoExecute(_ context.Context, in *ExecuteInput) (*ExecuteResult, error) {
	outputs := map[string]any{
		"greeting": "hello, " + in.Input["name"],
		"call_id":  in.Input["call_id"],
	}
	if n, err := strconv.Atoi(in.Input["blob_bytes"]); err == nil && n > 0 {
		outputs["blob"] = strings.Repeat("x", n)
	}
	return &ExecuteResult{Outcome: "succeeded", Comment: "profile", Outputs: outputs}, nil
}

// profileEchoWant builds the logical ExecuteResult profileEchoExecute must
// produce for input map in. Expected outputs are marshalled with the same
// helper the wire uses, so the comparison is byte-exact and key-order-stable.
func profileEchoWant(in map[string]string) (*v2.ExecuteResult, error) {
	outputs := map[string]any{
		"greeting": "hello, " + in["name"],
		"call_id":  in["call_id"],
	}
	if n, err := strconv.Atoi(in["blob_bytes"]); err == nil && n > 0 {
		outputs["blob"] = strings.Repeat("x", n)
	}
	out, err := v2.MarshalOutputs(outputs)
	if err != nil {
		return nil, err
	}
	return &v2.ExecuteResult{Outcome: "succeeded", OutputsJson: out}, nil
}

// profileClientPlugin is the host-side go-plugin plugin used by the handshake
// case: go-plugin hands the dispensed connection back and the profile speaks
// the generated v2.AdapterServiceClient against it.
type profileClientPlugin struct {
	hplugin.NetRPCUnsupportedPlugin
}

func (profileClientPlugin) GRPCServer(_ *hplugin.GRPCBroker, _ *grpc.Server) error {
	return errors.New("profileClientPlugin serves nothing")
}

func (profileClientPlugin) GRPCClient(_ context.Context, _ *hplugin.GRPCBroker, conn *grpc.ClientConn) (interface{}, error) {
	return v2.NewAdapterServiceClient(conn), nil
}

// profileServeSubprocess spawns the profile's helper subprocess (this test
// binary serving profileReferenceSpec through the real ServeSpec handshake)
// and returns the v2 client connected over it.
func profileServeSubprocess(t *testing.T) v2.AdapterServiceClient {
	t.Helper()
	// Fail fast on spec bugs before the subprocess obfuscates the reason.
	if err := profileReferenceSpec().validate(); err != nil {
		t.Fatalf("profileReferenceSpec invalid: %v", err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestProfileHelperServeSpec$")
	cmd.Env = append(os.Environ(), profileHelperEnv+"=1")
	c := hplugin.NewClient(&hplugin.ClientConfig{
		HandshakeConfig:  HandshakeConfig,
		Plugins:          map[string]hplugin.Plugin{AdapterName: profileClientPlugin{}},
		Cmd:              cmd,
		AllowedProtocols: []hplugin.Protocol{hplugin.ProtocolGRPC},
		StartTimeout:     30 * time.Second,
		Logger:           hclog.New(&hclog.LoggerOptions{Level: hclog.Off, Output: io.Discard}),
	})
	t.Cleanup(func() { c.Kill() })

	if _, err := c.Start(); err != nil {
		t.Fatalf("go-plugin handshake: %v", err)
	}
	if got, want := c.NegotiatedVersion(), int(HandshakeConfig.ProtocolVersion); got != want {
		t.Fatalf("negotiated wire protocol version = %d; want %d", got, want)
	}
	proto, err := c.Client()
	if err != nil {
		t.Fatalf("go-plugin protocol client: %v", err)
	}
	raw, err := proto.Dispense(AdapterName)
	if err != nil {
		t.Fatalf("dispense %q: %v", AdapterName, err)
	}
	client, ok := raw.(v2.AdapterServiceClient)
	if !ok {
		t.Fatalf("dispensed %T; want v2.AdapterServiceClient", raw)
	}
	return client
}

// TestProfileHelperServeSpec is this binary's helper subprocess entrypoint:
// serve the reference spec and block (a no-op in the parent test run, where
// the gate env is unset). The go-plugin server exits when the host hangs up.
func TestProfileHelperServeSpec(t *testing.T) {
	if os.Getenv(profileHelperEnv) != "1" {
		return
	}
	ServeSpec(profileReferenceSpec())
}

// TestProfileAdapterCore runs the full adapter.core profile. Subtests are
// sequential: the heartbeat cases swap the logHeartbeatRunner seam, which is
// package state shared across the test binary.
func TestProfileAdapterCore(t *testing.T) {
	// ── Case: handshake + Info round-trip vs the spec object ──
	t.Run("handshake_and_info_round_trip_vs_spec", func(t *testing.T) {
		client := profileServeSubprocess(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		info, err := client.Info(ctx, &v2.InfoRequest{})
		if err != nil {
			t.Fatalf("Info over go-plugin handshake: %v", err)
		}
		assertProfileInfoMatchesSpec(t, info)
	})

	// ── Case: --emit-manifest bytes parse under the pinned host parser ──
	t.Run("manifest_emit_parses_under_pinned_host_parser", func(t *testing.T) {
		spec := profileReferenceSpec()
		b, err := emitManifestFromSpec(spec)
		if err != nil {
			t.Fatalf("emitManifestFromSpec: %v", err)
		}
		doc, err := strictmanifest.Parse(b)
		if err != nil {
			t.Fatalf("strictmanifest.Parse on emitted manifest: %v\nemitted:\n%s", err, b)
		}
		if err := doc.Validate(); err != nil {
			t.Fatalf("strictmanifest.Validate on emitted manifest: %v\nemitted:\n%s", err, b)
		}

		if doc.SchemaVersion != manifestSchemaVersion {
			t.Errorf("schema_version = %d; want %d", doc.SchemaVersion, manifestSchemaVersion)
		}
		if doc.Name != spec.Name || doc.Version != spec.Version || doc.Description != spec.Description {
			t.Errorf("metadata = %q %q %q; want %q %q %q", doc.Name, doc.Version, doc.Description, spec.Name, spec.Version, spec.Description)
		}
		if doc.SourceURL != spec.SourceURL {
			t.Errorf("source_url = %q; want %q", doc.SourceURL, spec.SourceURL)
		}
		// Publish-critical platform mapping: spec "os/arch" strings become
		// the host's {os, arch} documents.
		if len(doc.Platforms) != len(spec.Platforms) {
			t.Fatalf("platforms = %+v; want one doc per spec string %v", doc.Platforms, spec.Platforms)
		}
		for i, p := range doc.Platforms {
			wantOS, wantArch, _ := strings.Cut(spec.Platforms[i], "/")
			if p.OS != wantOS || p.Arch != wantArch {
				t.Errorf("platforms[%d] = {%s %s}; want {%s %s}", i, p.OS, p.Arch, wantOS, wantArch)
			}
		}
		if doc.SDKProtocolVersion != 2 {
			t.Errorf("sdk_protocol_version = %d; want 2", doc.SDKProtocolVersion)
		}
		if got, want := strings.Join(doc.Capabilities, ","), strings.Join(spec.Capabilities, ","); got != want {
			t.Errorf("capabilities = %q; want %q", got, want)
		}
		if got, want := strings.Join(doc.CompatibleEnvironments, ","), strings.Join(spec.CompatibleEnvironments, ","); got != want {
			t.Errorf("compatible_environments = %q; want %q", got, want)
		}
		if doc.ContainerImage != nil {
			t.Errorf("container_image = %+v; want unset for a spec with no container image", doc.ContainerImage)
		}
		assertManifestSchemasMatchSpec(t, doc, spec)
		if len(doc.Secrets) != len(spec.Secrets) {
			t.Fatalf("secrets = %+v; want %d", doc.Secrets, len(spec.Secrets))
		}
		for i, s := range doc.Secrets {
			want := spec.Secrets[i]
			if s.Name != want.Name || s.Description != want.Description || !s.Required {
				t.Errorf("secrets[%d] = %#v; want name %q desc %q required", i, s, want.Name, want.Description)
			}
		}
		if got, want := strings.Join(doc.Permissions, ","), strings.Join(spec.Permissions, ","); got != want {
			t.Errorf("permissions = %q; want %q", got, want)
		}
	})

	// ── Case: oversized Execute results chunk and reassemble in arrival order ──
	t.Run("execute_chunks_reassemble_in_arrival_order", func(t *testing.T) {
		spec := profileReferenceSpec()
		client := profileLoopbackSpec(t, spec)
		in := map[string]string{"name": "chunk target", "call_id": "call-chunk", "blob_bytes": "512"}
		req := &v2.ExecuteRequest{SessionId: "s1", StepName: "chunk", Input: in}
		frags, want, err := profileDriveExecute(t, client, req)
		if err != nil {
			t.Fatalf("execute: %v", err)
		}
		exp, err := profileEchoWant(in)
		if err != nil {
			t.Fatalf("expected result: %v", err)
		}
		if len(frags) <= 1 {
			t.Fatalf("a %d-byte result under MaxChunkBytes=%d must arrive as multiple fragments; got %d messages", len(want.GetOutputsJson()), spec.MaxChunkBytes, len(frags))
		}
		assertChunkFraming(t, frags)
		if !sameExecuteResult(want, exp) {
			t.Errorf("reassembled result = outcome %q outputs %s; want %q %s",
				want.GetOutcome(), string(want.GetOutputsJson()), exp.GetOutcome(), string(exp.GetOutputsJson()))
		}
	})

	// ── Case: the SDK-owned log stream heartbeats at the declared cadence,
	// for the full stream lifetime, even with an idle adapter (no Log handler).
	t.Run("log_heartbeat_liveness_and_cadence", func(t *testing.T) {
		const interval = 20 * time.Millisecond
		withFastLogHeartbeat(t, interval)
		client := profileLoopbackSpec(t, profileReferenceSpec())
		if err := driveProfileHeartbeatWindow(client, interval, 40*interval); err != nil {
			t.Fatalf("heartbeat case failed for the reference implementation: %v", err)
		}
	})

	// ── Case: concurrent Execute, spec tier — one session, N calls, per-call
	// result correctness, unchunked and chunked.
	t.Run("concurrent_execute_spec_tier", func(t *testing.T) {
		t.Run("unchunked", func(t *testing.T) {
			svc, err := SpecService(profileReferenceSpec())
			if err != nil {
				t.Fatalf("SpecService: %v", err)
			}
			if err := RunConcurrentExecuteConformance(svc, profileEchoScript(false), WithConcurrentExecuteCalls(5)); err != nil {
				t.Fatalf("spec tier failed the concurrent-execute case: %v", err)
			}
		})
		t.Run("chunked", func(t *testing.T) {
			spec := profileReferenceSpec()
			spec.MaxChunkBytes = profileChunkBytes
			svc, err := SpecService(spec)
			if err != nil {
				t.Fatalf("SpecService: %v", err)
			}
			if err := RunConcurrentExecuteConformance(svc, profileEchoScript(true), WithConcurrentExecuteCalls(5)); err != nil {
				t.Fatalf("chunking spec tier failed the concurrent-execute case: %v", err)
			}
		})
	})

	// The tier-2 reference Service (the implementation the other two SDKs
	// port) satisfies the same case with delayed, out-of-order completions.
	t.Run("concurrent_execute_tier2_reference", func(t *testing.T) {
		delays := []time.Duration{40 * time.Millisecond, 30 * time.Millisecond, 20 * time.Millisecond, 10 * time.Millisecond, 0}
		err := RunConcurrentExecuteConformance(&conformanceReferenceService{}, conformanceScript(delays), WithConcurrentExecuteCalls(len(delays)))
		if err != nil {
			t.Fatalf("tier-2 reference failed the concurrent-execute case: %v", err)
		}
	})

	// ── Case: secrets resolve once — session store + per-step overlay over the
	// wire, no session-view mutation, no process-env fallback.
	t.Run("secrets_resolve_once_over_wire", func(t *testing.T) {
		var (
			capturedExecutes []*ExecuteInput
			capturedSession  *SessionStart
			capturedClosedID string
		)
		spec := profileReferenceSpec()
		spec.OpenSession = func(_ context.Context, start *SessionStart) error {
			capturedSession = start
			return nil
		}
		spec.Execute = func(_ context.Context, in *ExecuteInput) (*ExecuteResult, error) {
			capturedExecutes = append(capturedExecutes, in)
			return profileEchoExecute(context.Background(), in)
		}
		spec.CloseSession = func(_ context.Context, id string) error {
			capturedClosedID = id
			return nil
		}
		client := profileLoopbackSpec(t, spec)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// The env fallback exists but must never reach an adapter handler:
		// set it BEFORE the calls so any env-based resolution would show up
		// in the captured secrets and fail the assertions below (D69).
		t.Setenv("api_key", "ENVIRONMENT-FALLBACK")

		session := map[string]string{"api_key": "sk-session", "db_url": "postgres://session"}
		step := map[string]string{"api_key": "sk-step-0"}
		if _, err := client.OpenSession(ctx, &v2.OpenSessionRequest{SessionId: "s-sec", Secrets: session}); err != nil {
			t.Fatalf("OpenSession: %v", err)
		}
		if _, _, err := profileDriveExecute(t, client, &v2.ExecuteRequest{
			SessionId:    "s-sec",
			StepName:     "step-a",
			Input:        map[string]string{"name": "sec", "call_id": "call-sec-0"},
			SecretInputs: step,
		}); err != nil {
			t.Fatalf("Execute: %v", err)
		}

		// Session secrets travel the wire once, at OpenSession.
		if capturedSession == nil || capturedSession.Secrets["api_key"] != "sk-session" || capturedSession.Secrets["db_url"] != "postgres://session" {
			t.Fatalf("session secrets = %+v; want the resolved session values delivered once at OpenSession", capturedSession)
		}
		// Per call, the spec tier delivers the wire SecretInputs verbatim to
		// the handler (resolved at the host; resolve-once — never re-resolved,
		// never merged, never taken from the process env).
		if len(capturedExecutes) != 1 || capturedExecutes[0].Secrets["api_key"] != "sk-step-0" || len(capturedExecutes[0].Secrets) != 1 {
			t.Fatalf("Execute handler secret inputs = %v; want exactly the step overlay delivered per call", secretsOf(capturedExecutes))
		}

		// Second call on the same session must resolve to its OWN step secrets
		// (resolve once per call, no cross-step leakage of the prior overlay).
		stepB := map[string]string{"api_key": "sk-step-1"}
		if _, _, err := profileDriveExecute(t, client, &v2.ExecuteRequest{
			SessionId:    "s-sec",
			StepName:     "step-b",
			SecretInputs: stepB,
		}); err != nil {
			t.Fatalf("Execute step-b: %v", err)
		}
		if len(capturedExecutes) != 2 || capturedExecutes[1].Secrets["api_key"] != "sk-step-1" {
			t.Fatalf("second call secrets = %v; want only step-b's own overlay", secretsOf(capturedExecutes))
		}

		// Close the session over the wire: the SDK must deliver the id to the
		// adapter's CloseSession handler.
		if _, err := client.CloseSession(ctx, &v2.CloseSessionRequest{SessionId: "s-sec"}); err != nil {
			t.Fatalf("CloseSession: %v", err)
		}
		if capturedClosedID != "s-sec" {
			t.Fatalf("CloseSession handler id = %q; want \"s-sec\"", capturedClosedID)
		}

		// The SDK's resolution helper contract the spec tier builds on
		// (adapter-side view of the wire channel):
		declared, err := deriveInfo(profileReferenceSpec())
		if err != nil {
			t.Fatalf("deriveInfo: %v", err)
		}
		sv := NewSecrets(declared.GetSecrets(), session)
		if got, ok := sv.Get("api_key"); !ok || got != "sk-session" {
			t.Errorf("session Get(api_key) = %q %v; want the wire-resolved value (never the process env)", got, ok)
		}
		if _, ok := sv.Get("undeclared_secret"); ok {
			t.Error("Get returned a value for an undeclared secret name")
		}
		svStep := sv.WithStepSecrets(step)
		if got, want := mustStepSecret(t, svStep, "api_key"), "sk-step-0"; got != want {
			t.Errorf("step view Get(api_key) = %q; want %q (step overlay wins)", got, want)
		}
		if got, want := mustStepSecret(t, sv, "api_key"), "sk-session"; got != want {
			t.Errorf("session view mutated by the step overlay: Get(api_key) = %q; want %q (resolve-once, session view unmodified)", got, want)
		}
		env, err := sv.SpawnEnv("api_key")
		if err != nil || len(env) != 1 || env[0] != "api_key=sk-session" {
			t.Errorf("SpawnEnv = %q, err %v; want only the declared, delivered secret", env, err)
		}
		if _, err := sv.SpawnEnv("not_in_info_secrets"); err == nil {
			t.Error("SpawnEnv accepted an undeclared secret name")
		}
	})

	// ── Case: permissions answered on the bidi wire — the unimplemented
	// default (allow + request-id echo) vs an implemented deny.
	t.Run("permissions_per_wire", func(t *testing.T) {
		t.Run("unimplemented_default_allows", func(t *testing.T) {
			// Tier-2 shape: an adapter embedding UnimplementedPermissions.
			svc := &conformanceBaseService{}
			client, stop, err := serveAdapterOnLoopback(svc)
			if err != nil {
				t.Fatalf("serve: %v", err)
			}
			defer stop()
			stream := profilePermissionsStream(t, client)
			for _, id := range []string{"perm-1", "perm-2"} {
				wantDecision(t, stream, id, "allow")
			}
		})
		t.Run("implemented_denies", func(t *testing.T) {
			spec := profileReferenceSpec()
			spec.PermissionsFunc = func(ctx context.Context, stream PermissionsStream) error {
				return profileDenyAll(ctx, stream)
			}
			svc, err := SpecService(spec)
			if err != nil {
				t.Fatalf("SpecService: %v", err)
			}
			client, stop, err := serveAdapterOnLoopback(svc)
			if err != nil {
				t.Fatalf("serve: %v", err)
			}
			defer stop()
			stream := profilePermissionsStream(t, client)
			for _, id := range []string{"perm-d1", "perm-d2"} {
				wantDecision(t, stream, id, "deny")
			}
		})
	})
}

// TestProfileMutationChecks is the documented suite mutation check: break a
// host-facing behavior the profile guards and prove the suite fails. There is
// one case mutated two ways, documented once, in
// docs/conformance-adapter-core.md — the log-stream heartbeat cadence. If the
// SDK's heartbeat runner goes silent (cadence broken → the host's 90s stall
// detector unfed; idle sessions falsely declared crashed),
// driveProfileHeartbeatWindow must fail the profile — whether it never beats
// (log_heartbeat_cadence_stopped_fails_suite) or bursts the minimum count and
// then stops halfway through the window
// (log_heartbeat_cadence_partial_then_silent_fails_suite).
func TestProfileMutationChecks(t *testing.T) {
	t.Run("log_heartbeat_cadence_stopped_fails_suite", func(t *testing.T) {
		// The mutation: the heartbeat runner never sends (cadence broken).
		withSilentLogHeartbeat(t)
		client := profileLoopbackSpec(t, profileReferenceSpec())
		err := driveProfileHeartbeatWindow(client, 20*time.Millisecond, 10*20*time.Millisecond)
		if err == nil {
			t.Fatal("mutation not detected: a silent heartbeat runner passed the heartbeat case; the profile fails to catch a dead-heartbeat regression")
		}
		if !strings.Contains(err.Error(), "no log heartbeats") {
			t.Fatalf("mutation failure = %v; want the no-heartbeats reason", err)
		}
	})
	t.Run("log_heartbeat_cadence_partial_then_silent_fails_suite", func(t *testing.T) {
		// The second mutation form: the runner bursts the minimum cadence
		// count promptly and then stops emitting for the rest of the window.
		// Prompt, gap, and count budgets all still pass against such a run,
		// so only the end-of-window span assertion detects an adapter that
		// dies mid-window — the form a half-torn-down adapter process takes.
		withPartialThenSilentLogHeartbeat(t, 20*time.Millisecond, 10*20*time.Millisecond)
		client := profileLoopbackSpec(t, profileReferenceSpec())
		err := driveProfileHeartbeatWindow(client, 20*time.Millisecond, 10*20*time.Millisecond)
		if err == nil {
			t.Fatal("mutation not detected: a heartbeat runner that burst its beats and went silent mid-window passed the heartbeat case; the profile fails to catch a dead-heartbeat regression")
		}
		if !strings.Contains(err.Error(), "full lifetime") {
			t.Fatalf("mutation failure = %v; want the span/full-lifetime reason", err)
		}
	})
}

// driveProfileHeartbeatWindow opens a Log stream on the serving adapter and
// verifies the SDK's heartbeat contract over a timed window at the given
// interval: heartbeats flow for the full stream lifetime (even with an idle
// adapter), start promptly after the stream opens, and keep a cadence whose
// budgets tolerate a few missed ticks on a loaded CI runner. The stream is
// driven entirely on this goroutine: the window ends by cancelling the
// context, the next Recv returns the cancelled stream (context.Canceled or a
// grpc codes.Canceled status), and only then are the collected beats
// inspected — no cross-goroutine access, no torn reads, and a real stream
// error always surfaces instead of being dropped. The mutation check relies
// on this function failing with "no log heartbeats" when the runner is silent.
func driveProfileHeartbeatWindow(client v2.AdapterServiceClient, interval, window time.Duration) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.Log(ctx, &v2.LogRequest{SessionId: profileHeartbeatSessionID, StepName: "heartbeat"})
	if err != nil {
		return fmt.Errorf("open log stream: %w", err)
	}
	streamStart := time.Now()

	// The window ends by cancelling the context so Recv unblocks here; the
	// timer starts after the stream is open, and the prompt budget below
	// counts from streamStart — gRPC setup time is excluded either way.
	timer := time.AfterFunc(window, cancel)
	defer timer.Stop()

	var beats []time.Time
	for {
		ev, err := stream.Recv()
		if err != nil {
			// Our own window-cancel ends the stream. grpc surfaces the
			// cancelled stream context as a codes.Canceled status error,
			// which does not necessarily match errors.Is(context.Canceled),
			// so check both; only real errors surface.
			if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
				break
			}
			return fmt.Errorf("log stream failed during the heartbeat window: %w", err)
		}
		if ev.GetHeartbeat() != nil {
			beats = append(beats, time.Now())
		}
	}

	for i, beat := range beats {
		switch i {
		case 0:
			// First beat must be prompt: production cadence is 30s; a runner
			// that only beats once and then stalls must fail.
			if d := beat.Sub(streamStart); d > 8*interval {
				return fmt.Errorf("first log heartbeat took %v from stream start; expected within %v", d, 8*interval)
			}
		default:
			if d := beat.Sub(beats[i-1]); d > 6*interval {
				return fmt.Errorf("log heartbeat gap %v exceeds the cadence budget %v: the stream went silent", d, 6*interval)
			}
		}
	}
	if len(beats) == 0 {
		return fmt.Errorf("no log heartbeats within %s (interval %s): the SDK-owned log stream stopped heartbeat and the host's stall detector would starve", window, interval)
	}
	if n := window / (6 * interval); len(beats) < int(n) {
		return fmt.Errorf("%d log heartbeats in %s at interval %s; want at least %d — the stream must stay alive for its full lifetime", len(beats), window, interval, n)
	}
	// The beats must span the window: a runner that bursts the minimum count
	// promptly and then dies mid-window passes every budget above, but its
	// session goes silent from there on and the host's stall detector takes
	// over. Require the last beat to sit within one gap budget of the window
	// end (at window end the next tick is still allowed to be pending).
	if span := beats[len(beats)-1].Sub(beats[0]); span < window-6*interval {
		return fmt.Errorf("final log heartbeat spanned only %v of the %s window; want at least %v (last beat %v before stream end) — the stream must stay alive for its full lifetime", span, window, window-6*interval, time.Since(beats[len(beats)-1]))
	}
	return nil
}

// withSilentLogHeartbeat swaps the heartbeat runner for one that never sends —
// the documented mutation (wrong cadence → no heartbeats). Restored on cleanup.
func withSilentLogHeartbeat(t *testing.T) {
	t.Helper()
	orig := setLogHeartbeatRunner(func(context.Context, LogEventSender) {})
	t.Cleanup(func() { setLogHeartbeatRunner(orig) })
}

// withPartialThenSilentLogHeartbeat swaps the heartbeat runner for one that
// emits exactly the minimum-count floor the heartbeat case asserts
// (window/(6*interval) beats at the declared interval, promptly after the
// stream opens) and then stays silent for the rest of the window — a run that
// bursts its beats and dies early, which every prompt/gap/count budget alone
// accepts. Restored on cleanup.
func withPartialThenSilentLogHeartbeat(t *testing.T, interval, window time.Duration) {
	t.Helper()
	minCount := max(1, int(window/(6*interval)))
	orig := setLogHeartbeatRunner(func(ctx context.Context, sender LogEventSender) {
		go func() {
			tick := time.NewTicker(interval)
			defer tick.Stop()
			for i := 0; i < minCount; i++ {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					_ = sender.Send(&v2.LogEvent{Heartbeat: &v2.Heartbeat{}})
				}
			}
		}()
	})
	t.Cleanup(func() { setLogHeartbeatRunner(orig) })
}

// profileEchoScript builds a concurrent-execute script against
// profileEchoExecute's handler contract: call i echoes call_id and greets
// peer-i; chunked runs additionally carry a sized blob so fragments are joined
// per call under concurrency.
func profileEchoScript(chunked bool) ConcurrentExecuteScript {
	return func(i int) (*v2.ExecuteRequest, *v2.ExecuteResult) {
		in := map[string]string{"call_id": fmt.Sprintf("call-%d", i), "name": "peer-" + strconv.Itoa(i)}
		if chunked {
			in["blob_bytes"] = "512"
		}
		req := &v2.ExecuteRequest{StepName: fmt.Sprintf("step-%d", i), Input: in}
		want, err := profileEchoWant(in)
		if err != nil {
			panic(fmt.Sprintf("profileEchoScript: %v", err)) // cannot happen with string outputs
		}
		return req, want
	}
}

// profileLoopbackSpec serves spec on the loopback gRPC harness (the same
// serving shape a host uses) and returns the connected client.
func profileLoopbackSpec(t *testing.T, spec *Spec) v2.AdapterServiceClient {
	t.Helper()
	return specServerForTest(t, spec)
}

// profileDriveExecute is driveSpecExecute with the timeout the profile uses.
func profileDriveExecute(t *testing.T, client v2.AdapterServiceClient, req *v2.ExecuteRequest) ([]*v2.ExecuteResult, *v2.ExecuteResult, error) {
	t.Helper()
	frags, res, err := driveSpecExecute(context.Background(), client, req, 5*time.Second)
	return frags, res, err
}

// assertChunkFraming pins the chunk-framing wire rules the host enforces when
// reassembling: strict seq == arrival index, totals agree, final only on the
// last fragment. The joined logical result is checked by the caller.
func assertChunkFraming(t *testing.T, frags []*v2.ExecuteResult) {
	t.Helper()
	var total uint32
	for i, frag := range frags {
		if frag.GetChunk() == nil {
			t.Fatalf("fragment %d is unchunked; a multi-message delivery must be one chunked logical result", i)
		}
		if i == 0 {
			total = frag.GetChunk().GetTotal()
			if total == 0 {
				t.Fatal("fragment 0 declares total 0")
			}
			if got, want := int(total), len(frags); got != want {
				t.Fatalf("declared total %d chunks but %d received", got, want)
			}
		}
		if frag.GetChunk().GetSeq() != uint32(i) {
			t.Fatalf("fragment %d arrived with Chunk.Seq %d (non-zero seq value 0 is invalid, must equal arrival index)", i, frag.GetChunk().GetSeq())
		}
		if i < int(total)-1 && frag.GetChunk().GetFinal() {
			t.Fatalf("fragment %d sets the final flag early", i)
		}
	}
	if !frags[total-1].GetChunk().GetFinal() {
		t.Fatal("last fragment does not carry the final flag")
	}
	if frags[0].GetOutcome() == "" {
		t.Fatal("fragment 0 carries no outcome; the host records the outcome from seq 0 only")
	}
}

// assertProfileInfoMatchesSpec asserts the Info round-trip against the spec
// object field by field — the Info contract this SDK derives from the spec
// (D3): every declared surface must survive the wire unchanged.
func assertProfileInfoMatchesSpec(t *testing.T, info *v2.InfoResponse) {
	t.Helper()
	spec := profileReferenceSpec()

	if info.GetName() != spec.Name {
		t.Errorf("Info.Name = %q; want %q", info.GetName(), spec.Name)
	}
	if info.GetVersion() != spec.Version {
		t.Errorf("Info.Version = %q; want %q", info.GetVersion(), spec.Version)
	}
	if info.GetDescription() != spec.Description {
		t.Errorf("Info.Description = %q; want %q", info.GetDescription(), spec.Description)
	}
	if info.GetSourceUrl() != spec.SourceURL {
		t.Errorf("Info.SourceUrl = %q; want %q", info.GetSourceUrl(), spec.SourceURL)
	}
	if info.GetSdkProtocolVersion() != wireProtocolVersion {
		t.Errorf("Info.SdkProtocolVersion = %q; want %q", info.GetSdkProtocolVersion(), wireProtocolVersion)
	}
	if got, want := strings.Join(info.GetCapabilities(), ","), strings.Join(spec.Capabilities, ","); got != want {
		t.Errorf("Info.Capabilities = %q; want %q", got, want)
	}
	if got, want := strings.Join(info.GetPlatforms(), ","), strings.Join(spec.Platforms, ","); got != want {
		t.Errorf("Info.Platforms = %q; want %q", got, want)
	}
	if got, want := strings.Join(info.GetCompatibleEnvironments(), ","), strings.Join(spec.CompatibleEnvironments, ","); got != want {
		t.Errorf("Info.CompatibleEnvironments = %q; want %q", got, want)
	}
	if info.GetContainerImage() != "" {
		t.Errorf("Info.ContainerImage = %q; want unset for a spec with no container image", info.GetContainerImage())
	}
	if got, want := info.GetMaxChunkBytes(), spec.MaxChunkBytes; got != want {
		t.Errorf("Info.MaxChunkBytes = %d; want %d", got, want)
	}
	if got, want := strings.Join(info.GetPermissions(), ","), strings.Join(spec.Permissions, ","); got != want {
		t.Errorf("Info.Permissions = %q; want %q", got, want)
	}
	if got, want := strings.Join(info.GetSupportedFeatures(), ","), strings.Join(supportedFeaturesFromSpec(spec), ","); got != want {
		t.Errorf("Info.SupportedFeatures = %q; want the derived set %q", got, want)
	}
	if len(info.GetExamples()) != len(spec.Examples) {
		t.Errorf("Info has %d examples; want %d", len(info.GetExamples()), len(spec.Examples))
	}
	for i, ex := range info.GetExamples() {
		if i < len(spec.Examples) {
			want := spec.Examples[i]
			if ex.GetName() != want.Name || ex.GetDescription() != want.Description || ex.GetHcl() != want.HCL {
				t.Errorf("Info.Examples[%d] = %q %q %q; want %q %q %q (inline HCL round-trip)", i, ex.GetName(), ex.GetDescription(), ex.GetHcl(), want.Name, want.Description, want.HCL)
			}
		}
	}

	// Secrets round-trip as a name → description map.
	if len(info.GetSecrets()) != len(spec.Secrets) {
		t.Fatalf("Info.Secrets has %d entries; want %d", len(info.GetSecrets()), len(spec.Secrets))
	}
	for _, s := range spec.Secrets {
		if desc := info.GetSecrets()[s.Name]; desc != s.Description {
			t.Errorf("Info.Secrets[%q] = %q; want %q", s.Name, desc, s.Description)
		}
	}

	assertInfoSchemasMatchSpec(t, info, spec)
}

// assertInfoSchemasMatchSpec compares all three wire schemas against the spec
// object: each field must carry type, required, description, default and
// sensitive exactly as declared.
func assertInfoSchemasMatchSpec(t *testing.T, info *v2.InfoResponse, spec *Spec) {
	t.Helper()
	for _, tc := range []struct {
		name   string
		fields Schema
		got    *v2.AdapterSchemaProto
	}{
		{"config_schema", spec.ConfigSchema, info.GetConfigSchema()},
		{"input_schema", spec.InputSchema, info.GetInputSchema()},
		{"output_schema", spec.OutputSchema, info.GetOutputSchema()},
	} {
		if len(tc.fields) == 0 {
			if tc.got != nil {
				t.Errorf("%s delivered an empty schema; want unset", tc.name)
			}
			continue
		}
		if tc.got == nil {
			t.Fatalf("%s is unset; want %d declared fields", tc.name, len(tc.fields))
		}
		if got, want := len(tc.got.GetFields()), len(tc.fields); got != want {
			t.Fatalf("%s has %d fields; want %d", tc.name, got, want)
		}
		for _, f := range tc.fields {
			got := tc.got.GetFields()[f.Name]
			if got == nil {
				t.Fatalf("%s: field %q missing", tc.name, f.Name)
			}
			if got.GetType() != f.Type || got.GetRequired() != f.Required ||
				got.GetDescription() != f.Description || got.GetDefaultStr() != f.Default ||
				got.GetSensitive() != f.Sensitive {
				t.Errorf("%s: field %q = {type %q required %v desc %q default %q sensitive %v}; want {type %q required %v desc %q default %q sensitive %v}",
					tc.name, f.Name,
					got.GetType(), got.GetRequired(), got.GetDescription(), got.GetDefaultStr(), got.GetSensitive(),
					f.Type, f.Required, f.Description, f.Default, f.Sensitive)
			}
		}
	}
}

// assertManifestSchemasMatchSpec compares the emitted manifest's schemas
// against the spec object (the host's parse of the emit path).
func assertManifestSchemasMatchSpec(t *testing.T, doc *strictmanifest.Manifest, spec *Spec) {
	t.Helper()
	for _, tc := range []struct {
		name   string
		fields Schema
		got    *strictmanifest.Schema
	}{
		{"config_schema", spec.ConfigSchema, doc.ConfigSchema},
		{"input_schema", spec.InputSchema, doc.InputSchema},
		{"output_schema", spec.OutputSchema, doc.OutputSchema},
	} {
		if len(tc.fields) == 0 {
			continue
		}
		if tc.got == nil {
			t.Fatalf("%s missing from emitted manifest", tc.name)
		}
		if got, want := len(tc.got.Fields), len(tc.fields); got != want {
			t.Fatalf("%s has %d fields; want %d", tc.name, got, want)
		}
		for _, f := range tc.fields {
			got := tc.got.Fields[f.Name]
			def, _ := got.Default.(string)
			if got.Type != f.Type || got.Required != f.Required || got.Description != f.Description ||
				def != f.Default || got.Sensitive != f.Sensitive {
				t.Errorf("%s: field %q = {type %q required %v desc %q default %v sensitive %v}; want {type %q required %v desc %q default %v sensitive %v}",
					tc.name, f.Name,
					got.Type, got.Required, got.Description, got.Default, got.Sensitive,
					f.Type, f.Required, f.Description, f.Default, f.Sensitive)
			}
		}
	}
}

// profileDrivePermissionsStream opens a bidi Permissions stream in a windowed
// context for the caller's assertions.
func profilePermissionsStream(t *testing.T, client v2.AdapterServiceClient) v2.AdapterService_PermissionsClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	stream, err := client.Permissions(ctx)
	if err != nil {
		t.Fatalf("Permissions: %v", err)
	}
	return stream
}

// wantDecision sends one PermissionRequest and asserts the decision the
// adapter sends back: request id echoed, expected decision.
func wantDecision(t *testing.T, stream v2.AdapterService_PermissionsClient, requestID, decision string) {
	t.Helper()
	if err := stream.Send(&v2.PermissionEvent{Event: &v2.PermissionEvent_Request{
		Request: &v2.PermissionRequest{RequestId: requestID},
	}}); err != nil {
		t.Fatalf("send permission request %q: %v", requestID, err)
	}
	dec, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv decision for %q: %v", requestID, err)
	}
	if got := dec.GetRequestId(); got != requestID {
		t.Fatalf("decision request id = %q; want %q", got, requestID)
	}
	if got := dec.GetDecision(); got != decision {
		t.Fatalf("decision for %q = %q; want %q", requestID, got, decision)
	}
}

// profileDenyAll is the implemented permissions shape: answer every request
// with a matching deny decision (fail closed; post-hoc enforcement is the
// host's job for permissionless adapters only).
func profileDenyAll(_ context.Context, stream PermissionsStream) error {
	for {
		ev, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		req := ev.GetRequest()
		if req == nil {
			continue
		}
		if err := stream.Send(&v2.PermissionDecision{RequestId: req.GetRequestId(), Decision: "deny"}); err != nil {
			return err
		}
	}
}

// mustStepSecret reads one step secret from a Secrets view and fails t when
// the name was not resolved.
func mustStepSecret(t *testing.T, s *Secrets, name string) string {
	t.Helper()
	got, ok := s.Get(name)
	if !ok {
		t.Fatalf("secret %q was not resolved in the step view", name)
	}
	return got
}

// secretsOf flattens captured Execute inputs for failure messages.
func secretsOf(captured []*ExecuteInput) []map[string]string {
	out := make([]map[string]string, 0, len(captured))
	for _, in := range captured {
		out = append(out, in.Secrets)
	}
	return out
}
