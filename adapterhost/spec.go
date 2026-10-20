package adapterhost

import (
	"context"
	"fmt"
	"maps"
	"os"
	"regexp"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// Well-known schema field types. The wire accepts these spellings verbatim;
// "boolean" is accepted by the host as an alias for SchemaTypeBool, and the
// vocabulary is deliberately open: hosts reject types they do not understand,
// loudly, so declarations never degrade silently.
//
// Do not validate fields against this list in adapter code — it is a
// vocabulary snapshot, not a closed set.
const (
	SchemaTypeString     = "string"
	SchemaTypeNumber     = "number"
	SchemaTypeBool       = "bool"
	SchemaTypeListString = "list_string"
	SchemaTypeObject     = "object"
)

// Well-known values for [Spec.SupportedFeatures]. The set-what-you-declare
// rule derives these from the lifecycle handlers set on the [Spec]; the
// constants exist for the explicit-override form.
const (
	FeaturePause    = "pause"
	FeatureResume   = "resume"
	FeatureSnapshot = "snapshot"
	FeatureRestore  = "restore"
	FeatureInspect  = "inspect"
)

// schemaTypePattern is the wire rule for LogWriter.Line stream names
// (^stream_name must match ^[a-z][a-z0-9_-]{0,31}$). Well-known values are
// "stdout", "stderr", and "agent"; extensions like "tool", "trace", and
// "metric" are accepted without a proto bump.
var logStreamNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

// SchemaField is one entry of a [Spec] schema: a contract item in the
// adapter's config, input, or output set. Build fields with [Field] and
// assemble a [Schema] from the results. [Field] fills the name in from its
// first argument, because the schema is delivered as an ordered list at the
// spec and mapped to a keyed map only at the Info boundary (D2).
type SchemaField struct {
	// Name is the key the host addresses this field by. Required, and unique
	// within its schema: a duplicate name is a loud spec error, never a
	// silent overwrite.
	Name string
	// Type is the field's value vocabulary, e.g. [SchemaTypeString]. The
	// vocabulary is open — unknown types are rejected loudly by the host.
	Type string
	// Required marks a field the host must provide.
	Required bool
	// Description is free-form author text, surfaced by hosts and generated
	// catalogs.
	Description string
	// Default is the wire's default_str: the host's value when the field is
	// omitted. Empty string means no declared default.
	Default string
	// Sensitive marks the field as a taint-source for downstream tools.
	Sensitive bool
}

// Field is the schema builder: Field("name", "string", true, "who to greet").
// Positional arguments cover the core contract; [SchemaField.Default] and
// [SchemaField.Sensitive] are set on the returned value (or via a struct
// literal when several non-core fields are needed at once).
func Field(name, typ string, required bool, description string) SchemaField {
	return SchemaField{Name: name, Type: typ, Required: required, Description: description}
}

// Schema is the plain-object form of one of the adapter's three schemas
// (config, input, output) — an ordered list of [SchemaField] entries. The
// ordered form keeps author declarations stable and lets duplicate names
// fail loudly instead of being silently dropped by map semantics (D2
// declares the keyed proto form an Info-boundary artifact only).
type Schema []SchemaField

// Secret declares one named secret an adapter needs at session time.
type Secret struct {
	Name        string
	Description string
}

// Example is one showcase workflow shipped with the adapter. The payload
// always travels the wire inline as HCL (D5): a spec declaring [Example.File]
// has the file read at Info time, and a file that cannot be read fails the
// Info call loudly rather than dropping the example.
type Example struct {
	// Name is a short human-readable title for the showcase workflow.
	Name string
	// Description is a one-line summary of when to reach for this workflow.
	Description string
	// HCL is the workflow body when the example is declared inline.
	HCL string
	// File is the path to a workflow file, read and inlined at Info time.
	// Relative paths resolve against the adapter process's working directory
	// (the build pipeline's --emit-manifest run); absolute paths are
	// recommended for adapters serving from anywhere.
	File string
}

// SessionStart carries the open-session contract a [Spec.OpenSession]
// handler receives. SessionID is assigned by the host.
type SessionStart struct {
	SessionID       string
	Config          map[string]string
	Secrets         map[string]string // never log these values
	AllowedOutcomes []string
}

// ExecuteInput is the step contract a [Spec.Execute] handler receives.
type ExecuteInput struct {
	SessionID string
	StepName  string
	// Input holds the step's declared inputs; values are plain strings on
	// the wire.
	Input map[string]string
	// Secrets holds per-step secret inputs; never log these values.
	Secrets map[string]string
	// AllowedOutcomes lists the outcome names the step declared; a result
	// outside them fails the step on the host.
	AllowedOutcomes []string
	// Timeout is the workflow's declared step timeout, forwarded so the
	// adapter can enforce the host's deadline on its own execution.
	// Zero means the host declared none; enforcement stays the adapter's
	// policy.
	Timeout time.Duration
}

// ExecuteResult is the step finalization a [Spec.Execute] handler returns.
// Exactly one result must be produced per Execute call (a nil result with a
// nil error is a loud handler error); returning a non-nil error instead
// fails the step with that error.
type ExecuteResult struct {
	// Outcome names the step's result; it should be one of the step's
	// AllowedOutcomes (the host validates).
	Outcome string
	// Comment is the finalize comment for operators; hosts surface it
	// without scraping events, and contract mode may require it.
	Comment string
	// Outputs carries the step's outputs as natively-typed JSON values
	// (string/number/bool/object/array); the SDK serializes them onto the
	// typed outputs_json channel, chunking when they exceed the spec's
	// [Spec.MaxChunkBytes].
	Outputs map[string]any
}

// LogInput identifies the step whose log stream a [Spec.Log] handler serves.
type LogInput struct {
	SessionID string
	StepName  string
}

// LogWriter writes one log line at a time on the session's log stream,
// stamping the session, step, and timestamp per line. Write on one of the
// well-known streams ("stdout", "stderr", "agent") or a custom lowercase
// extension ("tool", "trace", "metric"). The SDK owns the stream's
// heartbeats for the full session lifetime regardless of what (or whether)
// the handler writes.
type LogWriter interface {
	// Line writes one line on the named stream. Stream names must match
	// ^[a-z][a-z0-9_-]{0,31}$; other names fail loudly.
	Line(streamName, text string) error
}

// SnapshotState is the checkpoint a [Spec.Snapshot] handler returns: opaque
// state bytes plus the schema tag the adapter will expect on restore.
// The state bytes are marked sensitive on the wire.
type SnapshotState struct {
	State         []byte
	SchemaVersion uint32
}

// RestoreInput carries a previously snapshotted state back to the
// [Spec.Restore] handler. A handler that cannot read a state shape fails
// loudly (its error becomes the restore error); SnapshotVersionMismatch-style
// details are the handler's own error vocabulary.
type RestoreInput struct {
	SessionID     string
	State         []byte
	SchemaVersion uint32
}

// InspectField is one adapter-defined structured field for operator UIs.
// Value is any structpb-encodable value (string, number, bool, list, nested
// object); an unencodable value fails the Inspect call loudly.
type InspectField struct {
	Key   string
	Label string
	Value any
}

// InspectInfo is the operator-facing health summary a [Spec.Inspect] handler
// returns. Every field is optional.
type InspectInfo struct {
	// CurrentStep is the step the session is presently working on.
	CurrentStep string
	// PendingPermissions counts tool calls awaiting host decisions.
	PendingPermissions uint32
	// LastActivity is when the adapter last made observable progress.
	LastActivity time.Time
	// Fields are the adapter-defined structured fields for operator UIs.
	Fields []InspectField
	// Extra is the freeform escape hatch for adapter-specific health data.
	Extra map[string]any
}

// OptionalVerbs carries the optional lifecycle verbs (Pause, Resume,
// Snapshot, Restore, Inspect) through the gRPC bridge. The spec tier wires
// them automatically from the handler funcs set on the [Spec]; a tier-2
// [Service] implementation may opt the same verbs in by also implementing
// this interface. Implementations that opt out keep the deterministic
// Unimplemented behavior today's Service-only serving has.
type OptionalVerbs interface {
	Pause(context.Context, *v2.PauseRequest) (*v2.PauseResponse, error)
	Resume(context.Context, *v2.ResumeRequest) (*v2.ResumeResponse, error)
	Snapshot(context.Context, *v2.SnapshotRequest) (*v2.SnapshotResponse, error)
	Restore(context.Context, *v2.RestoreRequest) (*v2.RestoreResponse, error)
	Inspect(context.Context, *v2.InspectRequest) (*v2.InspectResponse, error)
}

// Spec declares an adapter as plain Go data plus handler funcs — the tier-1
// author surface. Everything the host learns about the adapter during the
// Info handshake derives from the spec (ADR-0011 D3): an adapter built on
// Spec references no proto types at all, not even for schemas (the SDK maps
// the plain-object schemas onto the wire form at the Info boundary, D2).
//
// Handlers: [Spec.Execute] is required — the host executes steps through it.
// The rest may be nil: [Spec.OpenSession] and [Spec.CloseSession] no-op,
// [Spec.Log] keeps the SDK-owned liveness stream running with no lines,
// [Spec.PermissionsFunc] falls back to the permissionless default (post-hoc
// enforcement), and each unset lifecycle verb keeps its deterministic
// Unimplemented behavior. Set what you declare: [Spec.SupportedFeatures] is
// derived from which lifecycle handlers are set, and setting the field
// explicitly is the override escape hatch.
//
// Tool-invoking adapters that must stream semantic events from inside
// Execute (adapter tool calls, etc.) use the [Service] escape hatch instead;
// the spec tier's Execute is return-based by design.
type Spec struct {
	// ── Metadata ──
	// Name and Version are required.
	Name, Version, Description, SourceURL string

	// ── Declaration ──
	// Capabilities ([CapabilityAdapterTools] and friends), Platforms
	// ("os/arch" strings), CompatibleEnvironments, ContainerImage, and
	// MaxChunkBytes (0 = the 4 MiB protocol default) map to their
	// InfoResponse fields.
	Capabilities           []string
	Platforms              []string
	CompatibleEnvironments []string
	ContainerImage         string
	MaxChunkBytes          uint32
	// SupportedFeatures advertises the lifecycle verbs. When nil, the SDK
	// derives it from the handlers set on the spec (pause, resume, snapshot,
	// restore, inspect — in that canonical order). When set — even to an
	// empty slice — it is used verbatim: the override escape hatch.
	SupportedFeatures []string

	// ── Schemas ──
	// ConfigSchema / InputSchema / OutputSchema are the plain-object
	// contracts, assembled from [Field] builders. They are mapped onto the
	// wire form at the Info boundary only; author code never touches the
	// wire's schema types.
	ConfigSchema Schema
	InputSchema  Schema
	OutputSchema Schema
	Secrets      []Secret
	Permissions  []string
	Examples     []Example

	// ── Handlers ──
	// Execute is required; every other handler may be nil (see the type
	// comment). PermissionsFunc takes its name from the Permissions
	// declaration above — the same name would otherwise be a field/func
	// collision.
	OpenSession     func(context.Context, *SessionStart) error
	Execute         func(context.Context, *ExecuteInput) (*ExecuteResult, error)
	Log             func(context.Context, *LogInput, LogWriter) error
	CloseSession    func(context.Context, string) error
	PermissionsFunc func(context.Context, PermissionsStream) error

	// ── Optional lifecycle verbs ──
	// Each handler receives the host-assigned session id. Unset handlers are
	// simply not advertised (and stay Unimplemented on the wire), except
	// where [Spec.SupportedFeatures] overrides.
	Pause    func(context.Context, string) error
	Resume   func(context.Context, string) error
	Snapshot func(context.Context, string) (SnapshotState, error)
	Restore  func(context.Context, *RestoreInput) error
	Inspect  func(context.Context, string) (InspectInfo, error)
}

// validate enforces the spec contract: required metadata, the required
// execute handler, unambiguous schemas, and well-formed examples. All
// failures are loud author errors.
func (spec *Spec) validate() error {
	if spec.Name == "" {
		return fmt.Errorf("adapterhost: spec.Name is required")
	}
	if spec.Version == "" {
		return fmt.Errorf("adapterhost: spec.Version is required")
	}
	if spec.Execute == nil {
		return fmt.Errorf("adapterhost: spec %q: Execute handler is required; the host executes steps through it", spec.Name)
	}
	for _, schema := range []struct {
		name   string
		fields Schema
	}{
		{"ConfigSchema", spec.ConfigSchema},
		{"InputSchema", spec.InputSchema},
		{"OutputSchema", spec.OutputSchema},
	} {
		seen := map[string]bool{}
		for _, f := range schema.fields {
			if seen[f.Name] {
				return fmt.Errorf("adapterhost: spec %q: %s: duplicate field %q", spec.Name, schema.name, f.Name)
			}
			seen[f.Name] = true
			if f.Type == "" {
				return fmt.Errorf("adapterhost: spec %q: %s: field %q: type is required", spec.Name, schema.name, f.Name)
			}
		}
	}
	secretNames := map[string]bool{}
	for _, s := range spec.Secrets {
		if secretNames[s.Name] {
			return fmt.Errorf("adapterhost: spec %q: duplicate secret %q", spec.Name, s.Name)
		}
		secretNames[s.Name] = true
	}
	for i, ex := range spec.Examples {
		if ex.Name == "" {
			return fmt.Errorf("adapterhost: spec %q: example %d: name is required", spec.Name, i)
		}
		switch {
		case ex.HCL != "" && ex.File != "":
			return fmt.Errorf("adapterhost: spec %q: example %q: both inline HCL and File are set; declare exactly one", spec.Name, ex.Name)
		case ex.HCL == "" && ex.File == "":
			return fmt.Errorf("adapterhost: spec %q: example %q: declares neither inline HCL nor a File", spec.Name, ex.Name)
		}
	}
	return nil
}

// supportedFeaturesFromSpec implements set-what-you-declare: the spec's
// SupportedFeatures field, when set, wins verbatim; otherwise the feature
// list is derived from the lifecycle handlers that are set.
func supportedFeaturesFromSpec(spec *Spec) []string {
	if spec.SupportedFeatures != nil {
		return spec.SupportedFeatures
	}
	var out []string
	for _, f := range []struct {
		name string
		set  bool
	}{
		{FeaturePause, spec.Pause != nil},
		{FeatureResume, spec.Resume != nil},
		{FeatureSnapshot, spec.Snapshot != nil},
		{FeatureRestore, spec.Restore != nil},
		{FeatureInspect, spec.Inspect != nil},
	} {
		if f.set {
			out = append(out, f.name)
		}
	}
	return out
}

// SpecService adapts a [Spec] onto the [Service] contract. Serve-side
// plumbing is wired by [ServeSpec] and [ServeSpecRemote]; this conversion is
// exported for the spec tier's own tests and for authors migrating a spec
// onto the Service escape hatch.
func SpecService(spec *Spec) (Service, error) {
	if spec == nil {
		return nil, fmt.Errorf("adapterhost: SpecService: spec is nil")
	}
	if err := spec.validate(); err != nil {
		return nil, err
	}
	return &specService{spec: spec}, nil
}

// specService serves a Spec behind the Service contract. It never embeds a
// proto schema type in author-reachable form; the only proto schema
// construction is inside deriveInfo, at the Info boundary (D2).
type specService struct {
	spec *Spec
}

// deriveInfo sources the InfoResponse entirely from the spec (ADR-0011 D3).
// Validation and example resolution are loud: a spec problem fails the Info
// call (and, at --emit-manifest time, the process) with the reason visible.
func deriveInfo(spec *Spec) (*v2.InfoResponse, error) {
	if err := spec.validate(); err != nil {
		return nil, err
	}
	examples, err := resolveExamples(spec)
	if err != nil {
		return nil, err
	}
	features := supportedFeaturesFromSpec(spec)

	info := &v2.InfoResponse{
		Name:                   spec.Name,
		Version:                spec.Version,
		Description:            spec.Description,
		Capabilities:           spec.Capabilities,
		Platforms:              spec.Platforms,
		SdkProtocolVersion:     wireProtocolVersion,
		SourceUrl:              spec.SourceURL,
		ConfigSchema:           adapterSchemaProto(spec.ConfigSchema),
		InputSchema:            adapterSchemaProto(spec.InputSchema),
		OutputSchema:           adapterSchemaProto(spec.OutputSchema),
		Secrets:                secretMap(spec.Secrets),
		Permissions:            spec.Permissions,
		CompatibleEnvironments: spec.CompatibleEnvironments,
		ContainerImage:         spec.ContainerImage,
		SupportedFeatures:      features,
		MaxChunkBytes:          spec.MaxChunkBytes,
		Examples:               examples,
	}
	return info, nil
}

// wireProtocolVersion is the v2 protocol version constant declared in the
// Info handshake.
const wireProtocolVersion = "2"

// adapterSchemaProto maps a plain-object [Schema] onto the wire's keyed
// schema form. This is the only place the SDK builds a proto schema object
// from a [Spec] — at the Info boundary, per D2. Duplicate names cannot occur
// here ([Spec.validate] rejects them loudly). An empty schema leaves the
// Info field unset, matching a tier-2 author who simply omits it.
func adapterSchemaProto(schema Schema) *v2.AdapterSchemaProto {
	if len(schema) == 0 {
		return nil
	}
	fields := make(map[string]*v2.ConfigFieldProto, len(schema))
	for _, f := range schema {
		fields[f.Name] = &v2.ConfigFieldProto{
			Type:        f.Type,
			Required:    f.Required,
			Description: f.Description,
			DefaultStr:  f.Default,
			Sensitive:   f.Sensitive,
		}
	}
	return &v2.AdapterSchemaProto{Fields: fields}
}

// secretMap keys the declared secrets by name; the wire carries the
// description as the value.
func secretMap(secrets []Secret) map[string]string {
	if len(secrets) == 0 {
		return nil
	}
	out := make(map[string]string, len(secrets))
	for _, s := range secrets {
		out[s.Name] = s.Description
	}
	return out
}

// resolveExamples turns the spec's examples into wire form. File-declared
// examples are read and inlined here (D5): the payload always travels the
// wire inline, and a file that cannot be read fails loudly — never a silent
// example loss.
func resolveExamples(spec *Spec) ([]*v2.Example, error) {
	if len(spec.Examples) == 0 {
		return nil, nil
	}
	out := make([]*v2.Example, 0, len(spec.Examples))
	for _, ex := range spec.Examples {
		hcl := ex.HCL
		if ex.File != "" {
			data, err := os.ReadFile(ex.File)
			if err != nil {
				return nil, fmt.Errorf("adapterhost: example %q: read %q: %w (example File paths are read and inlined at Info time)", ex.Name, ex.File, err)
			}
			hcl = string(data)
		}
		out = append(out, &v2.Example{Name: ex.Name, Description: ex.Description, Hcl: hcl})
	}
	return out, nil
}

func (s *specService) Info(ctx context.Context, req *v2.InfoRequest) (*v2.InfoResponse, error) {
	return deriveInfo(s.spec)
}

func (s *specService) OpenSession(ctx context.Context, req *v2.OpenSessionRequest) (*v2.OpenSessionResponse, error) {
	if s.spec.OpenSession != nil {
		start := &SessionStart{
			SessionID:       req.GetSessionId(),
			Config:          maps.Clone(req.GetConfig()),
			Secrets:         maps.Clone(req.GetSecrets()),
			AllowedOutcomes: req.GetAllowedOutcomes(),
		}
		if err := s.spec.OpenSession(ctx, start); err != nil {
			return nil, err
		}
	}
	return &v2.OpenSessionResponse{}, nil
}

// Execute drives the spec's Execute handler and delivers its one result.
// Results exceeding the chunk limit are split with the SDK's chunk framer so
// the host reassembles them in arrival order.
func (s *specService) Execute(ctx context.Context, req *v2.ExecuteRequest, sender ExecuteEventSender) error {
	in := &ExecuteInput{
		SessionID:       req.GetSessionId(),
		StepName:        req.GetStepName(),
		Input:           req.GetInput(),
		Secrets:         req.GetSecretInputs(),
		AllowedOutcomes: req.GetAllowedOutcomes(),
		Timeout:         time.Duration(req.GetTimeoutMs()) * time.Millisecond,
	}
	res, err := s.spec.Execute(ctx, in)
	if err != nil {
		return err
	}
	if res == nil {
		return fmt.Errorf("adapterhost: Execute returned neither a result nor an error for step %q; exactly one result is required", in.StepName)
	}
	chunkSize := s.spec.MaxChunkBytes
	if chunkSize == 0 {
		chunkSize = v2.DefaultMaxChunkBytes
	}
	outputsJSON, err := v2.MarshalOutputs(res.Outputs)
	if err != nil {
		return fmt.Errorf("adapterhost: step %q outputs: %w", in.StepName, err)
	}
	base := &v2.ExecuteResult{Outcome: res.Outcome, Comment: res.Comment}
	if !v2.NeedsChunking(outputsJSON, chunkSize) {
		// Small result: one unchunked message (no framing), like a tier-2
		// author would emit.
		base.OutputsJson = outputsJSON
		if err := sender.Send(&v2.ExecuteEvent{Event: &v2.ExecuteEvent_Result{Result: base}}); err != nil {
			return fmt.Errorf("adapterhost: step %q: send result: %w", in.StepName, err)
		}
		return nil
	}
	for _, frag := range v2.ChunkExecuteResultOutputs(base, outputsJSON, chunkSize) {
		if err := sender.Send(&v2.ExecuteEvent{Event: &v2.ExecuteEvent_Result{Result: frag}}); err != nil {
			return fmt.Errorf("adapterhost: step %q: send result: %w", in.StepName, err)
		}
	}
	return nil
}

func (s *specService) Log(ctx context.Context, req *v2.LogRequest, sender LogEventSender) error {
	if s.spec.Log == nil {
		return nil // nothing to log; serve keeps the stream and its heartbeats alive
	}
	in := &LogInput{SessionID: req.GetSessionId(), StepName: req.GetStepName()}
	return s.spec.Log(ctx, in, &specLogWriter{sender: sender, input: in})
}

func (s *specService) CloseSession(ctx context.Context, req *v2.CloseSessionRequest) (*v2.CloseSessionResponse, error) {
	if s.spec.CloseSession != nil {
		if err := s.spec.CloseSession(ctx, req.GetSessionId()); err != nil {
			return nil, err
		}
	}
	return &v2.CloseSessionResponse{}, nil
}

// Permissions routes the bidi permission stream to the spec's handler,
// falling back to the same fail-closed default tier-2 embeds.
func (s *specService) Permissions(ctx context.Context, stream PermissionsStream) error {
	if s.spec.PermissionsFunc == nil {
		return runUnimplementedPermissions(ctx, stream)
	}
	return s.spec.PermissionsFunc(ctx, stream)
}

// specLogWriter stamps every line with the session, step, and timestamp
// before it hits the (thread-safe) log stream sender.
type specLogWriter struct {
	sender LogEventSender
	input  *LogInput
}

func (w *specLogWriter) Line(streamName, text string) error {
	if !logStreamNamePattern.MatchString(streamName) {
		return fmt.Errorf("adapterhost: log stream name %q must match ^[a-z][a-z0-9_-]{0,31}$", streamName)
	}
	return w.sender.Send(&v2.LogEvent{
		SessionId:  w.input.SessionID,
		StepName:   w.input.StepName,
		StreamName: streamName,
		Line:       []byte(text),
		Timestamp:  timestamppb.Now(),
	})
}

// ─── Optional lifecycle verbs: set-what-you-declare over the wire ───
//
// An unset handler is Unimplemented on the wire, with the message byte-
// identical to the generated UnimplementedAdapterServiceServer default so a
// host cannot distinguish a spec-dropped verb from a tier-2 verb it has
// never heard of. Derived supported_features never advertise an unset verb.
// The session id travels the wire on every verb request; each handler
// receives it alone.

func (s *specService) Pause(ctx context.Context, req *v2.PauseRequest) (*v2.PauseResponse, error) {
	if s.spec.Pause == nil {
		return nil, unimplementedVerb("Pause")
	}
	if err := s.spec.Pause(ctx, req.GetSessionId()); err != nil {
		return nil, err
	}
	return &v2.PauseResponse{}, nil
}

func (s *specService) Resume(ctx context.Context, req *v2.ResumeRequest) (*v2.ResumeResponse, error) {
	if s.spec.Resume == nil {
		return nil, unimplementedVerb("Resume")
	}
	if err := s.spec.Resume(ctx, req.GetSessionId()); err != nil {
		return nil, err
	}
	return &v2.ResumeResponse{}, nil
}

func (s *specService) Snapshot(ctx context.Context, req *v2.SnapshotRequest) (*v2.SnapshotResponse, error) {
	if s.spec.Snapshot == nil {
		return nil, unimplementedVerb("Snapshot")
	}
	state, err := s.spec.Snapshot(ctx, req.GetSessionId())
	if err != nil {
		return nil, err
	}
	return &v2.SnapshotResponse{State: state.State, SchemaVersion: state.SchemaVersion}, nil
}

func (s *specService) Restore(ctx context.Context, req *v2.RestoreRequest) (*v2.RestoreResponse, error) {
	if s.spec.Restore == nil {
		return nil, unimplementedVerb("Restore")
	}
	if err := s.spec.Restore(ctx, &RestoreInput{
		SessionID:     req.GetSessionId(),
		State:         req.GetState(),
		SchemaVersion: req.GetSchemaVersion(),
	}); err != nil {
		return nil, err
	}
	return &v2.RestoreResponse{}, nil
}

func (s *specService) Inspect(ctx context.Context, req *v2.InspectRequest) (*v2.InspectResponse, error) {
	if s.spec.Inspect == nil {
		return nil, unimplementedVerb("Inspect")
	}
	info, err := s.spec.Inspect(ctx, req.GetSessionId())
	if err != nil {
		return nil, err
	}
	return inspectResponseFromInfo(info)
}

// inspectResponseFromInfo maps the plain InspectInfo onto the wire: fields
// that cannot be encoded fail the call loudly rather than being dropped.
func inspectResponseFromInfo(info InspectInfo) (*v2.InspectResponse, error) {
	resp := &v2.InspectResponse{
		CurrentStep:            info.CurrentStep,
		PendingPermissionCount: info.PendingPermissions,
	}
	if !info.LastActivity.IsZero() {
		resp.LastActivityAt = timestamppb.New(info.LastActivity)
	}
	for _, f := range info.Fields {
		value, err := structpb.NewValue(f.Value)
		if err != nil {
			return nil, fmt.Errorf("adapterhost: inspect field %q: %w", f.Key, err)
		}
		resp.Fields = append(resp.Fields, &v2.InspectField{Key: f.Key, Label: f.Label, Value: value})
	}
	if len(info.Extra) > 0 {
		extra, err := structpb.NewStruct(info.Extra)
		if err != nil {
			return nil, fmt.Errorf("adapterhost: inspect extra: %w", err)
		}
		resp.Extra = extra
	}
	return resp, nil
}

// unimplementedVerb is the deterministic error for a verb a spec (or
// tier-2 service) did not set: identical in code and message to the generated
// UnimplementedAdapterServiceServer default.
func unimplementedVerb(method string) error {
	return status.Error(codes.Unimplemented, fmt.Sprintf("method %s not implemented", method))
}

// ─── Process entrypoints ───

// ServeSpec starts the adapter process from a [Spec] — the tier-1 entrypoint
// and counterpart to [Serve]. It validates the spec first: a broken spec is
// a loud author error that fails the process with the reason on stderr (a
// host that spawned the binary already fails the step; a one-shot build
// pipeline sees the exit code).
//
// When the process is invoked with --emit-manifest, the manifest is fed from
// deriveInfo(spec) — the exact function behind the Info RPC (ADR-0011 D3) —
// and the process exits, just as [Serve] does for the Service tier.
func ServeSpec(spec *Spec) {
	svc, err := SpecService(spec)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// D3: the emitted manifest is fed from deriveInfo(spec) — the same
	// single source the Info RPC serves — so spec-object authors never
	// implement Info to be publishable.
	if runEmitManifestIfRequested(func(context.Context) ([]byte, error) {
		return emitManifestFromSpec(spec)
	}) {
		return
	}
	Serve(svc)
}

// emitManifestFromSpec renders the --emit-manifest bytes from deriveInfo(spec)
// (ADR-0011 D3): no Info implementation is involved, and validation failure is
// loud — a spec problem exits the process with the reason on stderr, exactly
// like [ServeSpec]'s pre-emit validation.
func emitManifestFromSpec(spec *Spec) ([]byte, error) {
	info, err := deriveInfo(spec)
	if err != nil {
		return nil, err
	}
	return renderManifest(info)
}

// ServeSpecRemote dials the criteria host and serves a [Spec] over the
// phone-home connection — the tier-1 counterpart to [ServeRemote]. The spec
// is validated first and the error returned; when opts.Identity is unset,
// name and version derive from the spec so author main() does not repeat
// them.
func ServeSpecRemote(spec *Spec, opts *ServeRemoteOptions) error {
	if opts == nil {
		opts = &ServeRemoteOptions{}
	}
	svc, err := SpecService(spec)
	if err != nil {
		return err
	}
	if opts.Identity.Name == "" {
		opts.Identity.Name = spec.Name
	}
	if opts.Identity.Version == "" {
		opts.Identity.Version = spec.Version
	}
	return ServeRemote(svc, opts)
}

// optionalVerbsOf extracts the optional lifecycle verbs an implementation
// carries, if any. The spec tier always provides them (gated per verb);
// a tier-2 [Service] opts in by also implementing [OptionalVerbs].
func optionalVerbsOf(impl Service) OptionalVerbs {
	if ov, ok := impl.(OptionalVerbs); ok {
		return ov
	}
	return nil
}
