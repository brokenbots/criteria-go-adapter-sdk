module example.com/criteria-adapter-greeter

go 1.26.9

// This in-repo example builds against the LOCAL SDK tree (replace below) so
// `go test ./...` from the repository root exercises the same code an external
// author ships. External adapter authors instead require the published SDK
// version and drop the replace.
require (
	github.com/brokenbots/criteria-adapter-proto v0.7.1-0.20261019051615-3a3aa0c1cb85 // indirect
	github.com/brokenbots/criteria-go-adapter-sdk v0.5.1
)

require (
	github.com/fatih/color v1.13.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/hashicorp/go-hclog v1.6.3 // indirect
	github.com/hashicorp/go-plugin v1.8.0 // indirect
	github.com/hashicorp/yamux v0.1.2 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	github.com/oklog/run v1.1.0 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/grpc v1.83.2 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/brokenbots/criteria-go-adapter-sdk => ../../
