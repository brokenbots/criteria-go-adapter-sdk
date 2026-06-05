# Deferred: serve_remote_test.go

`adapterhost/serve_remote_test.go` exercises `ServeRemote` end-to-end against the
Criteria host's `internal/adapter/environment/remote` harness. That harness is
internal to the criteria monorepo and is **not** importable from this standalone
SDK module, so the test cannot live on `main`.

`serve_remote.go` (the implementation) ships on `main`; only this test is deferred.
When the remote-serve path is picked up, port the test to a self-contained harness
(or a host-side integration test) and move it back to `main`.

Provenance: monorepo `sdk/adapterhost/serve_remote_test.go`, preserved 2026-06-05
during the Go-SDK switchover.
