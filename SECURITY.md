# Security Policy

This repository is a Chaintable **write-node fork** of [ethereum/go-ethereum](https://github.com/ethereum/go-ethereum).
It runs the upstream chain client plus a small layer of Chaintable additions that
export block data to the [leafage-evm](https://github.com/Chaintable/leafage-evm)
pipeline.

Security issues therefore fall into two categories, each with its own process:
**upstream issues follow the upstream security policy** (preserved unmodified in
[UPSTREAM_SECURITY.md](./UPSTREAM_SECURITY.md)); **issues in the Chaintable
additions follow ours** (this document). The key question: does the issue
reproduce on an unmodified upstream build?

## Reproducible on vanilla upstream → report upstream

If the issue reproduces on an unmodified ethereum/go-ethereum build or release —
typically issues in consensus, p2p networking, EVM execution, transaction pool,
standard RPC, or storage — it affects every user of the upstream client, not just
this fork.

Report it to the upstream project following **their** security policy — for
upstream issues the upstream process applies, not this document:

- [UPSTREAM_SECURITY.md](./UPSTREAM_SECURITY.md) — the upstream policy, preserved
  in this fork
- Canonical, current version:
  https://github.com/ethereum/go-ethereum/security/policy

We pick up upstream security fixes through periodic upstream merges; please do not
disclose upstream vulnerabilities here.

## Only in this fork's additions → report to us

If the issue only reproduces with this fork's binaries or published images
(`public.ecr.aws/b2h7a5c4/chaintable/go-ethereum` and the per-chain
`<chain>-writer` aliases), or involves our additions — the block-data tracer
hooks, the `trace_debank*` RPC namespace (e.g. `trace_debankBlock`), pipeline
data output, the Dockerfile / image build, or the CI workflows — **do not open
a public issue**.

Report it privately:

- GitHub Security Advisory on this repository (preferred)
- Email: bugbounty@debank.com

Include: description, impact / severity assessment, steps to reproduce, proof of
concept if available.

Our additions are the commits on top of the upstream base — see the fork notice
in the [README](./README.md).

## Not sure, or cannot test against vanilla upstream?

Report it to us privately (see above). We will triage it. If it turns out to be an
upstream issue, we will help you report it upstream so you retain credit and any
bounty eligibility — or, with your consent, pass it on with attribution.

## Supported Versions

Only the tip of `main` and the latest release / published image are supported.
Older versions are not.

## Response Process

We aim to acknowledge within **72 hours**, provide an initial assessment within
**3–5 days**, and fix as soon as possible depending on severity.

## Disclosure Policy

We follow responsible disclosure. We will not disclose upstream-inherited issues
ahead of the upstream project's own advisory. Credit will be given unless you
request anonymity.
