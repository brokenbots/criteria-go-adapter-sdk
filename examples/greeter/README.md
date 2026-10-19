# criteria-adapter-greeter

A minimal example of a third-party [Criteria](https://github.com/brokenbots/criteria) adapter, written on the [declarative `adapterhost.Spec` surface](../../adapterhost) (tier 1). It demonstrates the full path from writing an adapter to running it with `criteria apply`.

The greeter accepts one input key (`name`) and returns:

- **outcome** `success`
- **output** `greeting = "hello, <name>"`

## Prerequisites

- Go 1.26+ (matches the `go` directive in `go.mod`)
- The `criteria` binary (built with `make build` from the criteria repo root)

## Build and install

```bash
# From this directory:
go build -o bin/criteria-adapter-greeter .

# Install into your personal adapter directory:
mkdir -p ~/.local/criteria/adapters
cp bin/criteria-adapter-greeter ~/.local/criteria/adapters/
chmod +x ~/.local/criteria/adapters/criteria-adapter-greeter
```

Or use a temporary directory to avoid touching your home directory:

```bash
tmpdir=$(mktemp -d)
go build -o "$tmpdir/criteria-adapter-greeter" .
chmod +x "$tmpdir/criteria-adapter-greeter"
CRITERIA_ADAPTERS="$tmpdir" criteria apply example.hcl
```

## Run the example workflow

```bash
criteria apply example.hcl
```

Expected output — the greet step succeeds with `hello, world`, and the run completes in state `done`:

```
step greet outcome=success outputs.greeting="hello, world" → done
```

The `greeting` output is available to any downstream step as `steps.greet.greeting`. Run `criteria adapter info greeter` to see the contract the host derives from the spec.

## How it works

`main.go` builds one `adapterhost.Spec` — metadata, input/output schemas, and the `Execute` handler — and calls `adapterhost.ServeSpec`. That is the entire adapter contract on tier 1:

```go
func main() {
	adapterhost.ServeSpec(greeterSpec())
}
```

The host learns everything else from the spec alone:

- `Name`/`Version`/`Description` become the adapter identity.
- `InputSchema`/`OutputSchema` are built with `adapterhost.Field(...)` plain-object declarations; the SDK maps them to the wire form at the Info boundary, so no proto schema type appears anywhere in this package.
- `Execute` receives plain values (`ExecuteInput.Input`) and returns native typed outputs (`ExecuteResult.Outputs`).
- `--emit-manifest` (used by `criteria adapter publish`) prints the derived adapter.yaml — try: `go run . --emit-manifest`.

Lifecycle handlers (`Pause`/`Resume`/`Snapshot`/`Restore`/`Inspect`) are optional: set what you declare, and `Spec.SupportedFeatures` is derived from what is set. The greeter sets none.

Implementing `adapterhost.Service` (tier 2) remains the escape hatch for needs beyond the spec — see the package docs.

## Tests

```bash
go test .
```

The tests cover the spec→Info mapping, the Execute handler (including the default name), the `--emit-manifest` output of the real binary, and — when the `criteria` binary is available — a full engine session applying `example.hcl`.