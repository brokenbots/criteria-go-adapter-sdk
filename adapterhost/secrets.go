package adapterhost

import (
	"fmt"
	"sort"
)

// Secrets is a read-only, redaction-aware view of the resolved secrets the
// Criteria host delivers to an adapter over the v2 secret channel. Construct it
// from the request structs the [Service] already receives: the names the
// adapter declared in its [InfoResponse].Secrets and the values the host
// resolved into [OpenSessionRequest].Secrets (optionally overlaid with the
// per-step [ExecuteRequest].SecretInputs via [Secrets.WithStepSecrets]).
//
// It deliberately offers no process-environment fallback: secrets reach the
// adapter only through this channel, so a sandbox that scrubs the adapter's
// environment (D29/D32) cannot cause a silent, unauthenticated fallback (D69).
//
// Redaction: the host owns the authoritative redaction registry (WS13) and
// masks these same values in host-emitted logs. This view does not re-mask the
// adapter's own stdout/stderr; an adapter that prints a secret is responsible
// for that itself.
type Secrets struct {
	declared map[string]struct{}
	values   map[string]string
}

// NewSecrets builds a [Secrets] view from the adapter's declared secret names
// (declared: name → description, as returned in [InfoResponse].Secrets) and the
// host-resolved values for the session (resolved: name → value, as carried in
// [OpenSessionRequest].Secrets). Both maps may be nil.
func NewSecrets(declared, resolved map[string]string) *Secrets {
	s := &Secrets{
		declared: make(map[string]struct{}, len(declared)),
		values:   make(map[string]string, len(resolved)),
	}
	for name := range declared {
		s.declared[name] = struct{}{}
	}
	for name, val := range resolved {
		s.values[name] = val
	}
	return s
}

// WithStepSecrets returns a new [Secrets] view that overlays per-step secret
// inputs ([ExecuteRequest].SecretInputs) on top of the session secrets; step
// values take precedence over session values for the same name. The declared
// set is unchanged and the receiver is not modified, so the session view stays
// valid for the next step.
func (s *Secrets) WithStepSecrets(step map[string]string) *Secrets {
	if s == nil {
		return NewSecrets(nil, step)
	}
	out := &Secrets{
		declared: s.declared, // immutable; safe to share
		values:   make(map[string]string, len(s.values)+len(step)),
	}
	for k, v := range s.values {
		out.values[k] = v
	}
	for k, v := range step {
		out.values[k] = v
	}
	return out
}

// Get returns the resolved value for name. ok is false if the host did not
// deliver that secret; Get never reads the process environment (D69).
func (s *Secrets) Get(name string) (value string, ok bool) {
	if s == nil {
		return "", false
	}
	v, ok := s.values[name]
	return v, ok
}

// SpawnEnv returns an [os/exec.Cmd]-ready environment slice ("NAME=value") for
// the named secrets, for adapters that forward credentials into a child process
// (D75). Every requested name must have been declared in the adapter's
// [InfoResponse].Secrets; SpawnEnv returns an error listing any that were not,
// so a typo cannot silently drop a credential. Declared-but-undelivered names
// are simply omitted from the slice (the host chose not to supply them).
func (s *Secrets) SpawnEnv(names ...string) ([]string, error) {
	if s == nil {
		s = &Secrets{}
	}
	var undeclared []string
	env := make([]string, 0, len(names))
	for _, name := range names {
		if _, ok := s.declared[name]; !ok {
			undeclared = append(undeclared, name)
			continue
		}
		if v, ok := s.values[name]; ok {
			env = append(env, name+"="+v)
		}
	}
	if len(undeclared) > 0 {
		sort.Strings(undeclared)
		return nil, fmt.Errorf("adapterhost: SpawnEnv: secrets not declared in InfoResponse.Secrets: %v", undeclared)
	}
	return env, nil
}
