# Security

## Reporting a vulnerability

Please report security issues privately via GitHub's **"Report a vulnerability"**
flow (Security → Advisories) on this repository, or email security@brokenbots.net.
Do not open a public issue for an undisclosed vulnerability.

## Supply-chain controls

This is a Go library (consumed as a module by Criteria adapters), so it ships no
binary. Dependency hygiene is enforced in CI and documented in
[docs/dependency-policy.md](docs/dependency-policy.md):

- **`osv-scan`** — osv-scanner runs on every PR/push as a **blocking** gate; no
  shipping known vulnerabilities. Exceptions are documented + dated in
  [`osv-scanner.toml`](osv-scanner.toml).
- **`deps-report`** — non-blocking freshness report (latest major.minor target).
- **Dependabot** — routine minor/patch updates with a 7-day supply-chain cooldown
  (security fixes exempt).

Reproduce the CI security checks locally with `make vuln-scan` and
`make deps-outdated`.
