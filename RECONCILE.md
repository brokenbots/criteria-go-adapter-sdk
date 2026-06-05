# Switchover notes (Go SDK extraction)

This repo contains the **adapter** SDK only (`adapterhost`), extracted cleanly:
production code depends solely on `criteria-adapter-proto` + go-plugin + grpc.

Deferred / follow-up for the host switchover:
- `serve_remote_test.go` was **dropped** — it imported the host's
  `internal/adapter/environment/remote` (serveRemote, a deferred feature). Re-add
  a host-independent test, or restore at switchover.
- The in-tree `criteria/sdk/` module ALSO contains an unrelated **events / v1
  server-API client** (root package + `pb/criteria/v1` + connectrpc, importing
  host `internal/`); that is NOT part of this adapter SDK and stays with the host
  (or becomes its own client package).
- Switchover: repoint host adapters (`cmd/criteria-adapter-*`, `adapters/shell`)
  to `github.com/brokenbots/criteria-go-adapter-sdk/adapterhost` and the proto
  module, then delete the in-tree `sdk/adapterhost` + `sdk/pb`.
