// Package adapterhost provides the public contract for Criteria adapter
// authors. An out-of-process adapter binary that calls [ServeSpec] (tier 1,
// declarative) or implements [Service] and calls [Serve] (tier 2, escape
// hatch) interoperates with any Criteria host without reaching through the
// internal/ package tree.
//
// # Tier 1: the declarative Spec (recommended)
//
// A [Spec] describes the adapter completely: metadata, capabilities,
// platforms, plain-object schemas ([Schema] assembled from [Field]), secrets,
// permissions, compatible environments, container image, examples, and handler
// funcs for the wire verbs. [ServeSpec] runs it as a spawn-served adapter;
// [ServeSpecRemote] phones home with it. Everything on the wire — including
// the Info response with its embedded schemas and examples — is derived from
// the spec: author code never constructs or touches wire schema types, the
// spec surface is a plain-object vocabulary (ADR-0011 D2/D3).
//
//	package main
//
//	import (
//		"context"
//		adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
//	)
//
//	func main() {
//		adapterhost.ServeSpec(&adapterhost.Spec{
//			Name:    "greeter",
//			Version: "0.1.0",
//			InputSchema: adapterhost.Schema{
//				adapterhost.Field("name", adapterhost.SchemaTypeString, true, "who to greet"),
//			},
//			Execute: func(_ context.Context, in *adapterhost.ExecuteInput) (*adapterhost.ExecuteResult, error) {
//				return &adapterhost.ExecuteResult{
//					Outcome: "success",
//					Outputs: map[string]any{"greeting": "hello, " + in.Input["name"]},
//				}, nil
//			},
//		})
//	}
//
// Lifecycle verbs are optional: set-what-you-declare. A spec that sets
// Pause/Resume/Snapshot/Restore/Inspect handlers advertises those features
// (see [Spec.SupportedFeatures] for the verbatim override); unset verbs stay
// Unimplemented on the wire with the same deterministic error the wire
// default produces, so a host sees exactly a not-declared verb.
//
// An invalid spec is a loud author error: [ServeSpec] fails the process with
// the reason on stderr (exit 1) before any wire work, and --emit-manifest
// inherits it through the same Info path.
//
// # Tier 2: the Service escape hatch
//
// Author code that needs control the declarative spec does not express —
// custom tool-call orchestration, per-request wire shaping, streaming
// behaviors — keeps the [Service] interface:
//
//	type myAdapter struct{ adapterhost.UnimplementedPermissions }
//
//	func (a *myAdapter) Info(ctx context.Context, req *v2.InfoRequest) (*v2.InfoResponse, error) { ... }
//	// ... implement remaining Service methods ...
//
//	func main() { adapterhost.Serve(&myAdapter{}) }
//
// A tier-2 implementation can opt into the lifecycle verbs by also
// implementing [OptionalVerbs]; without it the verbs stay on the generated
// Unimplemented stubs. The adapter serving shape underneath [ServeSpec] is
// the same bridge as [Serve] (see [SpecService]) — one code path, two tiers
// (D1).
//
// # Remote entrypoint
//
// Adapters that phone home to a criteria host (instead of being spawned by
// it) call [ServeRemote] (tier 2) or [ServeSpecRemote] (tier 1) with the
// host address and identity credentials:
//
//	func main() {
//		adapterhost.ServeSpecRemote(spec, &adapterhost.ServeRemoteOptions{
//			Host: "criteria.example.com:7778",
//			Identity: adapterhost.RemoteIdentity{
//				Name:    "greeter",       // defaults from the spec
//				Version: "0.1.0",
//				Digest:  "sha256:...",
//			},
//		})
//	}
//
// # v1 → v2 protocol break (WS03)
//
// WS03 migrated the host wire layer to v2 proto types and deleted the v1
// adapter-plugin protocol (proto/criteria/v1/adapter_plugin.proto) and its
// generated bindings. All bundled adapter binaries (copilot, mcp, noop,
// shell-builtin) and the greeter example have been updated to the v2 wire
// contract. Adapter binaries compiled against the v1 SDK will fail the
// go-plugin handshake with a protocol version mismatch.
//
// WS03 also shipped blocking permission enforcement: adapters that implement
// Permissions themselves (e.g. copilot, mcp) block tool execution until the
// host grants or denies the request. Adapters that embed UnimplementedPermissions
// receive post-hoc enforcement only (outcome override to needs_review on denial).
//
// # Package stability
//
// This package is v0. The [Spec] declarative surface, the [Service]
// interface, and the v2 wire protocol are the stable surface for adapter
// authors; breaking changes follow the SDK bump policy in CONTRIBUTING.md.
//
// # CHANGELOG forward-pointer
//
// WS01 renamed this package from sdk/pluginhost to sdk/adapterhost. The
// CHANGELOG entry is deferred to the WS39 cleanup gate.
package adapterhost
