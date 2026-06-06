# Remote adapter deployment examples

Copy-pasteable starting points for running a Go adapter in *remote* (phone-home)
mode, where the adapter dials out to a Criteria host's `remote` environment shim
instead of being launched locally. See the
["Running as a remote adapter"](../../README.md#running-as-a-remote-adapter)
section of the SDK README for the `adapterhost.ServeRemote` API.

| File | Use |
|------|-----|
| [`deployment.yaml`](deployment.yaml) | Kubernetes `Deployment` (+ ConfigMap/Secret) |
| [`docker-compose.yml`](docker-compose.yml) | Local / single-host Docker Compose |
| [`criteria-adapter.service`](criteria-adapter.service) | systemd unit for a VM/bare-metal host |

All three configure the adapter through the same environment variables:

| Variable | Meaning |
|----------|---------|
| `CRITERIA_REMOTE_HOST` | `host:port` of the workflow's `remote` environment |
| `CRITERIA_REMOTE_TOKEN` | bearer token the host requires on connect (optional) |
| `CRITERIA_REMOTE_TLS_CERT` | path to the adapter's mTLS client certificate (PEM) |
| `CRITERIA_REMOTE_TLS_KEY` | path to the adapter's mTLS client key (PEM) |
| `CRITERIA_REMOTE_CA` | path to the CA bundle that signs the host's server cert (PEM) |
| `CRITERIA_REMOTE_DIGEST` | the adapter artifact digest the host pins in its lockfile |

`adapterhost.LoadClientTLS(cert, key, ca)` builds the `*tls.Config` from these
paths.

> **Note:** Adapter launch and network reachability are the operator's
> responsibility. Criteria does not start remote adapters or tunnel to them.
