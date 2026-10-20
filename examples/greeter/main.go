// Package main is the greeter adapter — a minimal example of a third-party
// Criteria adapter, rewritten on the declarative [adapterhost.Spec] surface
// (tier 1). The adapter accepts one input key, "name", and returns:
//   - outcome:           "success"
//   - output "greeting": "hello, <name>"
//
// Everything the host learns about the adapter — metadata, schema, secrets,
// examples — lives in the Spec; there is no Info method to write and no wire
// (proto) schema type for the author to construct. See example.hcl for a
// workflow that exercises this adapter with `criteria apply`.
package main

import (
	"context"
	"fmt"

	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

// greeterSpec is the adapter's whole declaration: tier 1 is one object.
// SourceURL and Platforms are the publish-critical metadata the host's
// manifest parser requires (same contract the tier-2 noop example declares).
func greeterSpec() *adapterhost.Spec {
	return &adapterhost.Spec{
		Name:        "greeter",
		Version:     "0.1.0",
		Description: "greets politely",
		SourceURL:   "https://github.com/brokenbots/criteria-go-adapter-sdk",
		Platforms:   []string{"linux/amd64", "linux/arm64", "darwin/arm64"},
		InputSchema: adapterhost.Schema{
			adapterhost.Field("name", adapterhost.SchemaTypeString, false, "who to greet"),
		},
		OutputSchema: adapterhost.Schema{
			adapterhost.Field("greeting", adapterhost.SchemaTypeString, true, "the greeting"),
		},
		Execute: executeGreet,
	}
}

// greeterGreeting is the adapter's business logic, kept separate from the
// spec so tests can call it directly.
func greeterGreeting(name string) string {
	if name == "" {
		name = "world"
	}
	return fmt.Sprintf("hello, %s", name)
}

func executeGreet(_ context.Context, in *adapterhost.ExecuteInput) (*adapterhost.ExecuteResult, error) {
	// Return the greeting as a named output so downstream steps can reference
	// it via steps.<step_name>.greeting. Outputs keep their native JSON types
	// on the typed outputs channel; no wire types in sight.
	return &adapterhost.ExecuteResult{
		Outcome: "success",
		Outputs: map[string]any{"greeting": greeterGreeting(in.Input["name"])},
	}, nil
}

func main() {
	adapterhost.ServeSpec(greeterSpec())
}
