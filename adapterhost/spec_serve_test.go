package adapterhost

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// specServerForTest serves spec on the conformance loopback harness.
func specServerForTest(t *testing.T, spec *Spec) v2.AdapterServiceClient {
	t.Helper()
	svc, err := SpecService(spec)
	if err != nil {
		t.Fatalf("SpecService: %v", err)
	}
	client, stop, err := serveAdapterOnLoopback(svc)
	if err != nil {
		t.Fatalf("serveAdapterOnLoopback: %v", err)
	}
	t.Cleanup(stop)
	return client
}

// echoSpec is the greeter-shaped fixture most tier-1 wire tests run against.
func echoSpec(mods ...func(*Spec)) *Spec {
	spec := &Spec{
		Name:        "echo",
		Version:     "1.0.0",
		Description: "test echo adapter",
		InputSchema: Schema{Field("name", SchemaTypeString, true, "who to greet")},
		Execute: func(_ context.Context, in *ExecuteInput) (*ExecuteResult, error) {
			return &ExecuteResult{
				Outcome: "success",
				Comment: "echoed",
				Outputs: map[string]any{"greeting": "hello, " + in.Input["name"]},
			}, nil
		},
	}
	for _, m := range mods {
		m(spec)
	}
	return spec
}

// driveSpecExecute runs one Execute against client and returns the raw
// ExecuteResult messages in arrival order (no joining) plus the logical
// result the host would produce from them.
func driveSpecExecute(ctx context.Context, client v2.AdapterServiceClient, req *v2.ExecuteRequest, timeout time.Duration) ([]*v2.ExecuteResult, *v2.ExecuteResult, error) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stream, err := client.Execute(callCtx, req)
	if err != nil {
		return nil, nil, err
	}
	var frags []*v2.ExecuteResult
	for {
		ev, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, nil, err
		}
		if res := ev.GetResult(); res != nil {
			frags = append(frags, res)
		}
	}
	res, err := joinExecuteResultFragments(frags)
	return frags, res, err
}

// TestSpecTierInfoOverWire asserts the Info contract survives the bridge.
func TestSpecTierInfoOverWire(t *testing.T) {
	client := specServerForTest(t, fullSpec())
	info, err := client.Info(context.Background(), &v2.InfoRequest{})
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Name != "greeter" || info.Version != "0.3.0" || info.SdkProtocolVersion != "2" {
		t.Errorf("Info = %+v", info)
	}
	if info.InputSchema.Fields["name"] == nil {
		t.Errorf("input schema not delivered: %+v", info.InputSchema)
	}
	if strings.Join(info.SupportedFeatures, ",") != FeaturePause+","+FeatureResume+","+FeatureSnapshot+","+FeatureRestore+","+FeatureInspect {
		t.Errorf("SupportedFeatures = %v", info.SupportedFeatures)
	}
}

// TestSpecTierOpenSessionPassthrough keeps the session-start contract: the
// host's config, secrets, and outcomes reach the handler untouched.
func TestSpecTierOpenSessionPassthrough(t *testing.T) {
	var got *SessionStart
	spec := echoSpec(func(s *Spec) {
		s.OpenSession = func(_ context.Context, start *SessionStart) error {
			got = start
			return nil
		}
	})
	client := specServerForTest(t, spec)
	resp, err := client.OpenSession(context.Background(), &v2.OpenSessionRequest{
		SessionId:       "sess-1",
		Config:          map[string]string{"mode": "polite"},
		Secrets:         map[string]string{"api_key": "hunter2"},
		AllowedOutcomes: []string{"success"},
	})
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	if resp == nil {
		t.Fatal("OpenSession returned nil response")
	}
	if got == nil {
		t.Fatal("OpenSession handler never called")
	}
	if got.SessionID != "sess-1" || got.Config["mode"] != "polite" || got.Secrets["api_key"] != "hunter2" || strings.Join(got.AllowedOutcomes, ",") != "success" {
		t.Errorf("SessionStart = %+v", got)
	}

	// Handler errors propagate with their message intact.
	spec2 := echoSpec(func(s *Spec) {
		s.OpenSession = func(context.Context, *SessionStart) error { return errors.New("no capacity") }
	})
	client2 := specServerForTest(t, spec2)
	_, err = client2.OpenSession(context.Background(), &v2.OpenSessionRequest{})
	if status.Code(err) != codes.Unknown || !strings.Contains(err.Error(), "no capacity") {
		t.Errorf("OpenSession handler error = %v; want handler message", err)
	}
}

// TestSpecTierExecuteUnchunked pins the small-result path: exactly one
// ExecuteResult with outcome, comment, and native outputs preserved (no
// chunk framing for small payloads).
func TestSpecTierExecuteUnchunked(t *testing.T) {
	client := specServerForTest(t, echoSpec())
	frags, res, err := driveSpecExecute(context.Background(), client, &v2.ExecuteRequest{
		SessionId: "s", StepName: "greet", Input: map[string]string{"name": "world"},
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(frags) != 1 || frags[0].GetChunk() != nil {
		t.Fatalf("small result must be one unchunked message; got %d: %+v", len(frags), frags)
	}
	if frags[0].Outcome != "success" || frags[0].Comment != "echoed" { // the joiner's logical view carries no comment; read the fragment
		t.Errorf("result = %+v", frags[0])
	}
	var out map[string]any
	if err := json.Unmarshal(res.OutputsJson, &out); err != nil {
		t.Fatalf("outputs_json: %v", err)
	}
	if out["greeting"] != "hello, world" {
		t.Errorf("outputs = %v", out)
	}
}

// TestSpecTierExecuteEmptyOutputs pins the empty-outputs path: no chunking,
// nothing forced onto outputs_json.
func TestSpecTierExecuteEmptyOutputs(t *testing.T) {
	spec := echoSpec(func(s *Spec) {
		s.Execute = func(context.Context, *ExecuteInput) (*ExecuteResult, error) {
			return &ExecuteResult{Outcome: "success"}, nil
		}
	})
	client := specServerForTest(t, spec)
	frags, res, err := driveSpecExecute(context.Background(), client, &v2.ExecuteRequest{SessionId: "s", StepName: "greet"}, 2*time.Second)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(frags) != 1 || res.Outcome != "success" || len(res.OutputsJson) != 0 {
		t.Errorf("empty outputs delivered = %d msgs: %+v", len(frags), res)
	}
}

// TestSpecTierExecuteChunked pins the big-result path: MaxChunkBytes is
// honored on the wire (multiple fragments in order) and the host's joiner
// reassembles the logical result identically.
func TestSpecTierExecuteChunked(t *testing.T) {
	spec := echoSpec(func(s *Spec) {
		s.MaxChunkBytes = 128
		s.Execute = func(_ context.Context, in *ExecuteInput) (*ExecuteResult, error) {
			return &ExecuteResult{
				Outcome: "success",
				Outputs: map[string]any{"blob": strings.Repeat("payload!", 256)},
			}, nil
		}
	})
	client := specServerForTest(t, spec)
	frags, res, err := driveSpecExecute(context.Background(), client, &v2.ExecuteRequest{
		SessionId: "s", StepName: "greet", Input: map[string]string{"name": "chunk"},
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("chunked execute: %v", err)
	}
	if len(frags) <= 1 {
		t.Fatalf("expected chunked delivery; got %d fragments", len(frags))
	}
	var out map[string]any
	if err := json.Unmarshal(res.OutputsJson, &out); err != nil {
		t.Fatalf("outputs_json: %v", err)
	}
	if len(out["blob"].(string)) != 256*len("payload!") {
		t.Errorf("reassembled blob len = %d", len(out["blob"].(string)))
	}
}

// TestSpecTierExecuteNilResultLoud keeps the handler contract strict: a nil
// result with a nil error is a loud handler bug, never a silent no-result.
func TestSpecTierExecuteNilResultLoud(t *testing.T) {
	spec := echoSpec(func(s *Spec) {
		s.Execute = func(context.Context, *ExecuteInput) (*ExecuteResult, error) { return nil, nil }
	})
	client := specServerForTest(t, spec)
	_, _, err := driveSpecExecute(context.Background(), client, &v2.ExecuteRequest{StepName: "greet"}, 2*time.Second)
	if err == nil || !strings.Contains(err.Error(), "neither a result nor an error") {
		t.Fatalf("nil result err = %v; want loud handler bug", err)
	}
}

// TestSpecTierExecuteMarshalError keeps output marshalling loud: an
// unencodable output fails the step instead of shipping corrupted JSON.
func TestSpecTierExecuteMarshalError(t *testing.T) {
	spec := echoSpec(func(s *Spec) {
		s.Execute = func(context.Context, *ExecuteInput) (*ExecuteResult, error) {
			return &ExecuteResult{Outcome: "success", Outputs: map[string]any{"bad": make(chan int)}}, nil
		}
	})
	client := specServerForTest(t, spec)
	_, _, err := driveSpecExecute(context.Background(), client, &v2.ExecuteRequest{StepName: "greet"}, 2*time.Second)
	if err == nil || !strings.Contains(err.Error(), "outputs") {
		t.Fatalf("marshal err = %v; want loud failure", err)
	}
}

// TestSpecTierTimeoutInput keeps the wire's timeout_ms reachable by the
// handler, in time units the handler does not have to convert.
func TestSpecTierTimeoutInput(t *testing.T) {
	spec := echoSpec(func(s *Spec) {
		s.Execute = func(_ context.Context, in *ExecuteInput) (*ExecuteResult, error) {
			return &ExecuteResult{Outcome: "success", Comment: in.Timeout.String()}, nil
		}
	})
	client := specServerForTest(t, spec)
	frags, _, err := driveSpecExecute(context.Background(), client, &v2.ExecuteRequest{StepName: "greet", TimeoutMs: 1500}, 2*time.Second)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(frags) != 1 || frags[0].Comment != "1.5s" {
		t.Errorf("comment = %q; want 1.5s", frags[0].Comment)
	}
}

// TestSpecTierLogWriter keeps log framing: the writer stamps session and
// step, validates the stream name, and an invalid name fails the stream
// loudly instead of shipping a malformed event.
func TestSpecTierLogWriter(t *testing.T) {
	spec := echoSpec(func(s *Spec) {
		s.Log = func(_ context.Context, in *LogInput, w LogWriter) error {
			if in.SessionID == "unstyled" {
				return w.Line("INVALID NAME", "should not go out")
			}
			if err := w.Line("stdout", "hello from greet"); err != nil {
				return err
			}
			return w.Line("stderr", "bye from greet")
		}
	})
	client := specServerForTest(t, spec)

	for _, tc := range []struct {
		name      string
		session   string
		wantLines int
		wantErr   bool
	}{
		{"stamps", "sess", 2, false},
		{"bad stream name", "unstyled", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The serve-side Log wrapper keeps the stream open until the
			// host's context ends: cancel after collecting what the case
			// needs instead of waiting for an EOF.
			callCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := client.Log(callCtx, &v2.LogRequest{SessionId: tc.session, StepName: "greet"})
			if err != nil {
				t.Fatalf("Log: %v", err)
			}
			var lines []string
			var gotErr error
			for {
				ev, err := stream.Recv()
				if err != nil {
					// Our own cancel ends the serve-side wrapper; treat
					// either flavor of Canceled as the expected stream end.
					if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
						break
					}
					gotErr = err
					break
				}
				lines = append(lines, ev.StreamName+"|"+ev.SessionId+"|"+ev.StepName+"|"+string(ev.Line))
				if len(lines) >= tc.wantLines {
					cancel() // release the serve-side wrapper
				}
			}
			if tc.wantErr {
				if gotErr == nil || !strings.Contains(gotErr.Error(), "stream name") {
					t.Fatalf("error = %v; want stream name validation", gotErr)
				}
				return
			}
			if gotErr != nil {
				t.Fatalf("unexpected stream error: %v", gotErr)
			}
			if len(lines) != tc.wantLines {
				t.Fatalf("lines = %v; want %d", lines, tc.wantLines)
			}
			if lines[0] != "stdout|sess|greet|hello from greet" || lines[1] != "stderr|sess|greet|bye from greet" {
				t.Errorf("lines = %v", lines)
			}
		})
	}
}

// TestSpecTierLogNilHandler keeps the stream alive with no handler: the
// serve liveness contract depends on the stream staying open (and its
// heartbeats running) even when the spec declares nothing to log.
func TestSpecTierLogNilHandler(t *testing.T) {
	withFastLogHeartbeat(t, 10*time.Millisecond)
	spec := echoSpec()
	client := specServerForTest(t, spec)
	stream, err := client.Log(context.Background(), &v2.LogRequest{SessionId: "sess", StepName: "greet"})
	if err != nil {
		t.Fatalf("Log: %v", err)
	}
	// A nil handler must not end the stream: the host expects heartbeats so
	// its stall detector stays fed. The first event must therefore arrive
	// quickly (heartbeat cadence), not at any arbitrary later time.
	done := make(chan error, 1)
	go func() {
		started := time.Now()
		for {
			_, err := stream.Recv()
			if err == nil {
				if d := time.Since(started); d > 4*time.Second {
					done <- errors.New("first Log event took " + d.String() + "; heartbeats presumably not running")
				} else {
					close(done)
				}
				return
			}
			if time.Since(started) > 5*time.Second {
				done <- errors.New("no Log event within 5s: " + err.Error())
				return
			}
		}
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("Log stream with nil handler never sent an event")
	}
	_ = stream.CloseSend()
}

// TestSpecTierCloseSession keeps the session-end id passthrough.
func TestSpecTierCloseSession(t *testing.T) {
	var closed []string
	spec := echoSpec(func(s *Spec) {
		s.CloseSession = func(_ context.Context, sessionID string) error {
			closed = append(closed, sessionID)
			return nil
		}
	})
	client := specServerForTest(t, spec)
	if _, err := client.CloseSession(context.Background(), &v2.CloseSessionRequest{SessionId: "sess-9"}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	if len(closed) != 1 || closed[0] != "sess-9" {
		t.Errorf("closed = %v", closed)
	}
}

// TestSpecTierPermissionsRouting covers both branches of the permission
// bridge: the spec's func wins when set; without one the same fail-closed
// allow-echo loop tier-2 embeds runs.
func TestSpecTierPermissionsRouting(t *testing.T) {
	// Default. The echo loop answers "allow"; end the call to release it.
	spec := echoSpec()
	client := specServerForTest(t, spec)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	stream, err := client.Permissions(ctx)
	if err != nil {
		t.Fatalf("Permissions: %v", err)
	}
	if err := stream.Send(&v2.PermissionEvent{Event: &v2.PermissionEvent_Request{Request: &v2.PermissionRequest{RequestId: "r1"}}}); err != nil {
		t.Fatalf("send request: %v", err)
	}
	dec, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv decision: %v", err)
	}
	if dec.GetRequestId() != "r1" || dec.GetDecision() != "allow" {
		t.Errorf("default decision = %+v", dec)
	}
	cancel()

	// Custom PermissionsFunc: the request reaches the handler, its decision
	// reaches the host.
	spec2 := echoSpec(func(s *Spec) {
		s.PermissionsFunc = func(_ context.Context, stream PermissionsStream) error {
			ev, err := stream.Recv()
			if err != nil {
				return err
			}
			return stream.Send(&v2.PermissionDecision{
				RequestId: ev.GetRequest().GetRequestId(),
				Decision:  "deny",
			})
		}
	})
	client2 := specServerForTest(t, spec2)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	stream2, err := client2.Permissions(ctx2)
	if err != nil {
		t.Fatalf("Permissions: %v", err)
	}
	if err := stream2.Send(&v2.PermissionEvent{Event: &v2.PermissionEvent_Request{Request: &v2.PermissionRequest{RequestId: "r2"}}}); err != nil {
		t.Fatalf("send request: %v", err)
	}
	dec2, err := stream2.Recv()
	if err != nil {
		t.Fatalf("recv decision: %v", err)
	}
	if dec2.GetRequestId() != "r2" || dec2.GetDecision() != "deny" {
		t.Errorf("custom decision = %+v", dec2)
	}
}

// TestSpecTierOptionalVerbs pins the optional-verb wire contract on the full
// set: each handler gets only the session id and returns its response shape.
func TestSpecTierOptionalVerbs(t *testing.T) {
	spec := echoSpec(func(s *Spec) {
		s.Pause = func(_ context.Context, id string) error {
			if id != "s-verb" {
				t.Errorf("Pause session id = %q", id)
			}
			return nil
		}
		s.Resume = func(context.Context, string) error { return nil }
		s.Snapshot = func(context.Context, string) (SnapshotState, error) {
			return SnapshotState{State: []byte("snap"), SchemaVersion: 3}, nil
		}
		s.Restore = func(_ context.Context, in *RestoreInput) error {
			if in.SessionID != "s-verb" || string(in.State) != "snap" || in.SchemaVersion != 3 {
				t.Errorf("Restore input = %+v", in)
			}
			return nil
		}
		s.Inspect = func(context.Context, string) (InspectInfo, error) {
			return InspectInfo{CurrentStep: "greet", PendingPermissions: 1}, nil
		}
	})
	client := specServerForTest(t, spec)
	info, err := client.Info(context.Background(), &v2.InfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(info.SupportedFeatures, ","), FeatureSnapshot) {
		t.Errorf("supported features = %v", info.SupportedFeatures)
	}

	ctx := context.Background()
	if _, err = client.Pause(ctx, &v2.PauseRequest{SessionId: "s-verb"}); err != nil {
		t.Errorf("Pause: %v", err)
	}
	if _, err = client.Resume(ctx, &v2.ResumeRequest{SessionId: "s-verb"}); err != nil {
		t.Errorf("Resume: %v", err)
	}
	snap, err := client.Snapshot(ctx, &v2.SnapshotRequest{SessionId: "s-verb"})
	if err != nil || string(snap.State) != "snap" || snap.SchemaVersion != 3 {
		t.Errorf("Snapshot = %+v, err %v", snap, err)
	}
	if _, err = client.Restore(ctx, &v2.RestoreRequest{SessionId: "s-verb", State: []byte("snap"), SchemaVersion: 3}); err != nil {
		t.Errorf("Restore: %v", err)
	}
	insp, err := client.Inspect(ctx, &v2.InspectRequest{SessionId: "s-verb"})
	if err != nil || insp.CurrentStep != "greet" || insp.PendingPermissionCount != 1 {
		t.Errorf("Inspect = %+v, err %v", insp, err)
	}
}

// TestSpecTierUnsetVerbsUnimplemented pins the unset-verb contract: each
// unset verb stays at the generated default's deterministic Unimplemented
// error (same code, same message), so hosts cannot distinguish a spec-
// dropped verb from one the SDK never knew about.
func TestSpecTierUnsetVerbsUnimplemented(t *testing.T) {
	client := specServerForTest(t, echoSpec())
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"Pause", func() error { _, err := client.Pause(ctx, &v2.PauseRequest{}); return err }},
		{"Resume", func() error { _, err := client.Resume(ctx, &v2.ResumeRequest{}); return err }},
		{"Snapshot", func() error { _, err := client.Snapshot(ctx, &v2.SnapshotRequest{}); return err }},
		{"Restore", func() error { _, err := client.Restore(ctx, &v2.RestoreRequest{}); return err }},
		{"Inspect", func() error { _, err := client.Inspect(ctx, &v2.InspectRequest{}); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil {
				t.Fatalf("%s: expected error", tc.name)
			}
			if status.Code(err) != codes.Unimplemented {
				t.Errorf("%s: code = %s", tc.name, status.Code(err))
			}
			want := "method " + tc.name + " not implemented"
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: message %q; want %q", tc.name, err.Error(), want)
			}
		})
	}
}

// TestTier2ServiceWithoutVerbs is the tier-2 parity regression guard: a
// Service that does not implement OptionalVerbs keeps the exact Unimplemented
// behavior the bridge had before the spec tier landed.
func TestTier2ServiceWithoutVerbs(t *testing.T) {
	client, stop, err := serveAdapterOnLoopback(&logFixtureService{})
	if err != nil {
		t.Fatalf("serveAdapterOnLoopback: %v", err)
	}
	defer stop()
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"Pause", func() error { _, err := client.Pause(ctx, &v2.PauseRequest{}); return err }},
		{"Resume", func() error { _, err := client.Resume(ctx, &v2.ResumeRequest{}); return err }},
		{"Snapshot", func() error { _, err := client.Snapshot(ctx, &v2.SnapshotRequest{}); return err }},
		{"Restore", func() error { _, err := client.Restore(ctx, &v2.RestoreRequest{}); return err }},
		{"Inspect", func() error { _, err := client.Inspect(ctx, &v2.InspectRequest{}); return err }},
	} {
		if err := tc.call(); status.Code(err) != codes.Unimplemented {
			t.Errorf("%s: code = %s; want Unimplemented on tier-2-only service", tc.name, status.Code(err))
		}
	}
}
