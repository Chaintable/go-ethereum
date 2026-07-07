# Security Policy

This repository is a **fork**: upstream [ethereum/go-ethereum](https://github.com/ethereum/go-ethereum)
plus the [Chaintable pipeline](https://github.com/Chaintable/pipeline) tracer,
which exports block data (headers, transactions, call traces, receipts, events,
state diffs) to the Chaintable data pipeline.

**First, determine where the issue lives.** The key question: does it reproduce
on an unmodified upstream build?

- **Upstream issue** — reproduces on vanilla upstream (typically consensus, p2p
  networking, EVM execution, transaction pool, standard RPC, storage). It affects
  every user of the upstream client, not just this fork. **Follow the upstream
  security process, not this document:**
  - Current: https://github.com/ethereum/go-ethereum/security/policy
  - As of this fork's base (v1.17.4):
    https://github.com/ethereum/go-ethereum/blob/v1.17.4/SECURITY.md

  We pick up upstream security fixes through periodic upstream merges; please do
  not disclose upstream vulnerabilities here.

- **This fork's issue** — only reproduces with this fork's binaries or published
  images (`public.ecr.aws/b2h7a5c4/chaintable/go-ethereum` and the per-chain
  `<chain>-writer` aliases), or involves the Chaintable pipeline layer: the
  block-data tracer hooks, the `trace_debank*` RPC namespace (e.g.
  `trace_debankBlock`), pipeline data output, the Dockerfile / image build, or
  the CI workflows. **Follow our process below.**

- **Not sure, or cannot test against vanilla upstream?** Report it to us privately
  (see below). We will triage it, and if it turns out to be an upstream issue we
  will help you report it upstream so you retain credit and any bounty
  eligibility — or, with your consent, pass it on with attribution.

---

## Our Process (issues in the Chaintable pipeline layer)

### Supported Versions

We provide security updates for the latest `main` branch and recent releases.

| Version | Supported |
|---------|----------|
| main    | ✅       |
| latest `-debank-N` release | ✅ |
| older versions   | ❌ |

### Reporting a Vulnerability

If you discover a security issue in the Chaintable pipeline layer, **do not open
a public issue**.

Please report it privately:

- GitHub Security Advisory on this repository (preferred)
- Email: bugbounty@debank.com

Include:

- Description of the issue
- Impact / severity assessment
- Steps to reproduce
- Proof of concept (if available)

### Response Process

We aim to:

- Acknowledge within **72 hours**
- Provide initial assessment within **3–5 days**
- Fix and release as soon as possible depending on severity

### Disclosure Policy

- We follow **responsible disclosure**
- Fixes may be developed privately before public release
- Credit will be given unless you request anonymity

### Scope

Typical security-relevant areas of the Chaintable pipeline layer include:

- Integrity of the emitted block data (ordering, duplication, corruption)
- The `trace_debank*` RPC namespace
- Resource exhaustion introduced by the tracer hooks (memory / goroutine leaks)
- The published Docker images and the build / CI pipeline

### Notes

This fork is a data producer for the
[Chaintable pipeline](https://github.com/Chaintable/pipeline): its output feeds
downstream indexing and query systems. Security issues here may propagate
downstream — please report anything suspicious.
