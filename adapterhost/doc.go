// Package adapterhost provides the public contract for Criteria adapter authors.
// An out-of-process adapter binary that implements [Service] and calls [Serve]
// will interoperate with any Criteria host without reaching through the
// internal/ package tree.
//
// # Minimum entrypoint
//
//	package main
//
//	import (
//		"context"
//		adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
//		v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
//	)
//
//	type myAdapter struct{ adapterhost.UnimplementedPermissions }
//
//	func (a *myAdapter) Info(ctx context.Context, req *v2.InfoRequest) (*v2.InfoResponse, error) { ... }
//	// ... implement remaining Service methods ...
//
//	func main() { adapterhost.Serve(&myAdapter{}) }
//
// # Remote entrypoint
//
// Adapters that phone home to a criteria host (instead of being spawned by
// it) call [ServeRemote] with the host address and identity credentials:
//
//	func main() {
//		adapterhost.ServeRemote(&myAdapter{}, &adapterhost.ServeRemoteOptions{
//			Host: "criteria.example.com:7778",
//			Identity: adapterhost.RemoteIdentity{
//				Name:    "my-adapter",
//				Version: "1.0.0",
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
// This package is v0. The [Service] interface and v2 wire protocol are the
// stable surface for adapter authors; breaking changes follow the SDK bump
// policy in CONTRIBUTING.md.
//
// # CHANGELOG forward-pointer
//
// WS01 renamed this package from sdk/pluginhost to sdk/adapterhost. The
// CHANGELOG entry is deferred to the WS39 cleanup gate.
package adapterhost
