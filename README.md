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

## Status

Extracted from the in-tree `sdk/adapterhost`. Builds and tests standalone
against `criteria-adapter-proto`. The criteria **host** still uses its in-tree
copy; the switchover (host depends on this module + the proto module, in-tree
`sdk/` deleted) is a tracked follow-up — see RECONCILE.md. Versioned to track the
criteria release line.
