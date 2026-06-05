package adapterhost

import (
	"slices"
	"testing"
)

func TestSecrets_Get_DeclaredAndUndelivered(t *testing.T) {
	declared := map[string]string{"GITHUB_TOKEN": "GitHub auth", "API_KEY": "provider key"}
	resolved := map[string]string{"GITHUB_TOKEN": "ghp_abc"}
	s := NewSecrets(declared, resolved)

	if v, ok := s.Get("GITHUB_TOKEN"); !ok || v != "ghp_abc" {
		t.Fatalf("Get(GITHUB_TOKEN) = %q, %v; want \"ghp_abc\", true", v, ok)
	}
	// Declared but not delivered by the host.
	if v, ok := s.Get("API_KEY"); ok || v != "" {
		t.Fatalf("Get(API_KEY) = %q, %v; want \"\", false", v, ok)
	}
	// Never delivered, never declared.
	if v, ok := s.Get("MISSING"); ok || v != "" {
		t.Fatalf("Get(MISSING) = %q, %v; want \"\", false", v, ok)
	}
}

func TestSecrets_Get_NoEnvFallback(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "from-env")
	s := NewSecrets(map[string]string{"GITHUB_TOKEN": ""}, nil)
	if v, ok := s.Get("GITHUB_TOKEN"); ok || v != "" {
		t.Fatalf("Get must not fall back to the process env; got %q, %v", v, ok)
	}
}

func TestSecrets_SpawnEnv_RefusesUndeclared(t *testing.T) {
	declared := map[string]string{"GITHUB_TOKEN": "", "API_KEY": ""}
	resolved := map[string]string{"GITHUB_TOKEN": "ghp_abc", "API_KEY": "k"}
	s := NewSecrets(declared, resolved)

	env, err := s.SpawnEnv("GITHUB_TOKEN", "API_KEY")
	if err != nil {
		t.Fatalf("SpawnEnv(declared) error = %v", err)
	}
	slices.Sort(env)
	want := []string{"API_KEY=k", "GITHUB_TOKEN=ghp_abc"}
	if !slices.Equal(env, want) {
		t.Fatalf("SpawnEnv = %v; want %v", env, want)
	}

	if _, err := s.SpawnEnv("GITHUB_TOKEN", "NOPE"); err == nil {
		t.Fatal("SpawnEnv(undeclared) must error")
	}
}

func TestSecrets_SpawnEnv_OmitsUndeliveredDeclared(t *testing.T) {
	// API_KEY is declared but the host didn't deliver it: omit, don't error.
	s := NewSecrets(map[string]string{"GITHUB_TOKEN": "", "API_KEY": ""}, map[string]string{"GITHUB_TOKEN": "ghp_abc"})
	env, err := s.SpawnEnv("GITHUB_TOKEN", "API_KEY")
	if err != nil {
		t.Fatalf("SpawnEnv error = %v", err)
	}
	if !slices.Equal(env, []string{"GITHUB_TOKEN=ghp_abc"}) {
		t.Fatalf("SpawnEnv = %v; want [GITHUB_TOKEN=ghp_abc]", env)
	}
}

func TestSecrets_WithStepSecrets_StepOverridesSession(t *testing.T) {
	session := NewSecrets(
		map[string]string{"GITHUB_TOKEN": "", "API_KEY": ""},
		map[string]string{"GITHUB_TOKEN": "session-tok", "API_KEY": "session-key"},
	)
	step := session.WithStepSecrets(map[string]string{"GITHUB_TOKEN": "step-tok"})

	if v, _ := step.Get("GITHUB_TOKEN"); v != "step-tok" {
		t.Fatalf("step Get(GITHUB_TOKEN) = %q; want step-tok", v)
	}
	if v, _ := step.Get("API_KEY"); v != "session-key" {
		t.Fatalf("step Get(API_KEY) = %q; want session-key (inherited)", v)
	}
	// Receiver unchanged.
	if v, _ := session.Get("GITHUB_TOKEN"); v != "session-tok" {
		t.Fatalf("session view mutated: Get(GITHUB_TOKEN) = %q; want session-tok", v)
	}
	// Declared set is inherited by the step view (SpawnEnv still enforced).
	if _, err := step.WithStepSecrets(nil).SpawnEnv("API_KEY"); err != nil {
		t.Fatalf("declared set should carry through WithStepSecrets: %v", err)
	}
}

func TestSecrets_NilSafe(t *testing.T) {
	var s *Secrets
	if v, ok := s.Get("X"); ok || v != "" {
		t.Fatalf("nil Get = %q, %v; want \"\", false", v, ok)
	}
	if _, err := s.SpawnEnv("X"); err == nil {
		t.Fatal("nil SpawnEnv(undeclared) must error")
	}
}
