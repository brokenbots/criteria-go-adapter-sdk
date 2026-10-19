package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

// greeterBinOnce builds the greeter binary once (the engine discovers plugins
// by exact filename) for the spawn-based tests.
var (
	greeterBinOnce sync.Once
	greeterBinPath string
	greeterBinErr  error
)

func greeterBin(t *testing.T) string {
	t.Helper()
	greeterBinOnce.Do(func() {
		dir, err := os.MkdirTemp("", "greeter-bin-*")
		if err != nil {
			greeterBinErr = err
			return
		}
		greeterBinPath = filepath.Join(dir, "criteria-adapter-greeter")
		out, err := exec.Command("go", "build", "-o", greeterBinPath, ".").CombinedOutput()
		if err != nil {
			greeterBinErr = err
			t.Logf("build greeter: %v\n%s", err, out)
		}
	})
	if greeterBinErr != nil {
		if os.IsNotExist(greeterBinErr) && greeterBinPath == "" {
			t.Skipf("go toolchain unavailable: %v", greeterBinErr)
		}
		t.Fatalf("build greeter: %v", greeterBinErr)
	}
	return greeterBinPath
}

// TestGreeterInfoDerivedFromSpec proves deriveInfo: the greeter declares a
// spec, never touches proto, and still yields a correct InfoResponse.
func TestGreeterInfoDerivedFromSpec(t *testing.T) {
	svc, err := adapterhost.SpecService(greeterSpec())
	if err != nil {
		t.Fatalf("SpecService: %v", err)
	}
	info, err := svc.Info(context.Background(), &v2.InfoRequest{})
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if got := info.GetName(); got != "greeter" {
		t.Errorf("Info.Name = %q, want greeter", got)
	}
	if got := info.GetVersion(); got != "0.1.0" {
		t.Errorf("Info.Version = %q, want 0.1.0", got)
	}
	if got := info.GetDescription(); got != "greets politely" {
		t.Errorf("Info.Description = %q, want greets politely", got)
	}
	if f := info.GetInputSchema().GetFields()["name"]; f.GetType() != "string" {
		t.Errorf("Info.InputSchema[name].Type = %q, want string", f.GetType())
	}
	if f := info.GetOutputSchema().GetFields()["greeting"]; !f.GetRequired() {
		t.Error("Info.OutputSchema[greeting].Required = false, want true")
	}
}

// executeCollector satisfies adapterhost.ExecuteEventSender for tests.
type executeCollector struct {
	events []*v2.ExecuteEvent
}

func (c *executeCollector) Send(ev *v2.ExecuteEvent) error {
	c.events = append(c.events, ev)
	return nil
}

// TestGreeterExecute exercises the greeter's handler through the same Service
// surface ServeSpec exposes, including the default name.
func TestGreeterExecute(t *testing.T) {
	svc, err := adapterhost.SpecService(greeterSpec())
	if err != nil {
		t.Fatalf("SpecService: %v", err)
	}

	tests := []struct {
		name string
		in   map[string]string
		want string
	}{
		{name: "explicit", in: map[string]string{"name": "world"}, want: "hello, world"},
		{name: "other", in: map[string]string{"name": "criteria"}, want: "hello, criteria"},
		{name: "default", in: map[string]string{}, want: "hello, world"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coll := &executeCollector{}
			err := svc.Execute(context.Background(), &v2.ExecuteRequest{Input: tt.in}, coll)
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if len(coll.events) != 1 {
				t.Fatalf("sent %d events, want 1 (small result, unchunked)", len(coll.events))
			}
			res := coll.events[0].GetResult()
			if res.GetOutcome() != "success" {
				t.Errorf("outcome = %q, want success", res.GetOutcome())
			}
			var outs map[string]string
			if err := json.Unmarshal(res.GetOutputsJson(), &outs); err != nil {
				t.Fatalf("outputs JSON: %v\n%s", err, res.GetOutputsJson())
			}
			if outs["greeting"] != tt.want {
				t.Errorf("greeting = %q, want %q", outs["greeting"], tt.want)
			}
		})
	}
}

// TestGreeterExecuteInvalidOutputs pins the loud-failure path: outputs that
// cannot be marshalled fail the call instead of dropping silently.
func TestGreeterExecuteInvalidOutputs(t *testing.T) {
	svc, err := adapterhost.SpecService(&adapterhost.Spec{
		Name:    "bad-outputs",
		Version: "0.1.0",
		Execute: func(context.Context, *adapterhost.ExecuteInput) (*adapterhost.ExecuteResult, error) {
			return &adapterhost.ExecuteResult{
				Outcome: "success",
				Outputs: map[string]any{"bad": make(chan int)},
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("SpecService: %v", err)
	}
	err = svc.Execute(context.Background(), &v2.ExecuteRequest{}, &executeCollector{})
	if status.Code(err) != codes.Unknown {
		t.Fatalf("Execute with unencodable output: err = %v, want Unknown", err)
	}
}

// TestGreeterEmitManifest runs the built binary with --emit-manifest, the
// build-pipeline entry, and validates the derived adapter.yaml document.
func TestGreeterEmitManifest(t *testing.T) {
	bin := greeterBin(t)
	out := &bytes.Buffer{}
	cmd := exec.Command(bin, "--emit-manifest")
	cmd.Stdout = out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("run %s --emit-manifest: %v", bin, err)
	}
	var doc struct {
		SchemaVersion int    `json:"schema_version"`
		Name          string `json:"name"`
		Version       string `json:"version"`
		InputSchema   struct {
			Fields map[string]struct {
				Type string `json:"type"`
			} `json:"fields"`
		} `json:"input_schema"`
	}
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
		t.Fatalf("manifest not JSON: %v\n%s", err, out)
	}
	if doc.SchemaVersion != 1 || doc.Name != "greeter" || doc.Version != "0.1.0" {
		t.Errorf("manifest header = %+v, want version 1 / greeter / 0.1.0", doc)
	}
	if doc.InputSchema.Fields["name"].Type != "string" {
		t.Errorf("manifest input_schema[name].Type = %q, want string", doc.InputSchema.Fields["name"].Type)
	}
}

// TestGreeterEngineSession is the acceptance e2e: a real session against the
// engine host, greeter discovered from a CRITERIA_ADAPTERS directory.
// Skipped when the criteria binary is unavailable.
func TestGreeterEngineSession(t *testing.T) {
	criteria, err := exec.LookPath("criteria")
	if err != nil {
		t.Skip("criteria binary not on PATH")
	}
	out, err := runGreeterEngine(t, criteria, "apply", exampleHCLPath(t))
	if err != nil {
		t.Fatalf("criteria apply: %v\noutput:\n%s", err, out)
	}
	for _, want := range []string{"hello, world", "success"} {
		if !strings.Contains(out, want) {
			t.Errorf("criteria apply output missing %q:\n%s", want, out)
		}
	}
}

// runGreeterEngine points CRITERIA_ADAPTERS at a fresh dir holding the built
// greeter binary and runs `criteria <args...>` from an isolated working +
// HOME directory. Returns the accumulated output.
func runGreeterEngine(t *testing.T, criteria string, args ...string) (string, error) {
	t.Helper()
	binDir := filepath.Dir(greeterBin(t))
	cwd, err := os.MkdirTemp("", "greeter-e2e-*")
	if err != nil {
		return "", err
	}
	t.Cleanup(func() { os.RemoveAll(cwd) })

	cmd := exec.Command(criteria, args...)
	cmd.Dir = cwd
	cmd.Env = append(os.Environ(), "CRITERIA_ADAPTERS="+binDir, "HOME="+cwd)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err = cmd.Run()
	return buf.String(), err
}

// exampleHCLPath returns the example workflow shipped next to this test.
func exampleHCLPath(t *testing.T) string {
	t.Helper()
	p, err := filepath.Abs("example.hcl")
	if err != nil {
		t.Fatalf("resolve example.hcl: %v", err)
	}
	return p
}
