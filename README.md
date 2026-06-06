# criteria-go-adapter-sdk

Go SDK for building [Criteria](https://github.com/brokenbots/criteria) adapters
on protocol **v2**. Implement the `Service` interface and call `adapterhost.Serve`.

```go
import "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"

func main() { adapterhost.Serve(&myAdapter{}) }
```

`serve()` handles the go-plugin handshake, the v2 RPCs, and `--emit-manifest`
(emits `adapter.yaml` from the adapter's `Info`). Wire types come from the
versioned [`criteria-adapter-proto`](https://github.com/brokenbots/criteria-adapter-proto)
module.

## Running as a remote adapter

By default an adapter is launched locally by the Criteria host. An adapter can
instead run anywhere (Kubernetes, ECS, a VM, a systemd unit) and *dial out* to
the host's `remote` environment shim. Call `adapterhost.ServeRemote` instead of
`adapterhost.Serve`; the `Service` implementation is identical.

```go
package main

import (
	"os"

	"github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

func main() {
	tlsConf, err := adapterhost.LoadClientTLS(
		os.Getenv("CRITERIA_REMOTE_TLS_CERT"),
		os.Getenv("CRITERIA_REMOTE_TLS_KEY"),
		os.Getenv("CRITERIA_REMOTE_CA"),
	)
	if err != nil {
		panic(err)
	}
	if err := adapterhost.ServeRemote(&myAdapter{}, &adapterhost.ServeRemoteOptions{
		// host:port of the workflow's `remote` environment listen_address.
		Host:        os.Getenv("CRITERIA_REMOTE_HOST"),
		TLSConfig:   tlsConf,
		AcceptToken: os.Getenv("CRITERIA_REMOTE_TOKEN"),
		Identity: adapterhost.RemoteIdentity{
			Name:    "my-adapter",
			Version: "1.0.0",
			// The host verifies this digest against its lockfile entry.
			Digest: os.Getenv("CRITERIA_REMOTE_DIGEST"),
		},
		// Redial with backoff when the host connection drops.
		Reconnect: true,
	}); err != nil {
		panic(err)
	}
}
```

`ServeRemote` opens a (m)TLS connection to the host, sends a single
newline-terminated identity frame (`{name, version, digest, token}`), then
serves the gRPC `AdapterService` over the held connection. The host shim bridges
that connection to a local socket and consumes it as if the adapter were local.
With `Reconnect: false` (the default) it serves a single connection and returns;
with `Reconnect: true` it redials with exponential backoff.

**Deployment examples** — copy-pasteable Kubernetes `Deployment`,
`docker-compose`, and `systemd` manifests live under
[`examples/remote/`](examples/remote/). Adapter launch and network reachability
(VPN, Tailscale, ngrok, a public address) are the operator's responsibility;
Criteria does not start or tunnel to remote adapters.

## Status

Extracted from the in-tree `sdk/adapterhost`. Builds and tests standalone
against `criteria-adapter-proto`. The criteria **host** still uses its in-tree
copy; the switchover (host depends on this module + the proto module, in-tree
`sdk/` deleted) is a tracked follow-up — see RECONCILE.md. Versioned to track the
criteria release line.
