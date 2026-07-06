# Contributing

This repository is a Chaintable **write-node fork** of [ethereum/go-ethereum](https://github.com/ethereum/go-ethereum).
Its purpose is to run node(s) that produce block data for the
[leafage-evm](https://github.com/Chaintable/leafage-evm) pipeline, for the chain(s)
listed in this repository's CI configuration and README. It is not a
general-purpose fork of go-ethereum.

## What belongs here vs upstream (read this first)

| Change | Where it goes |
|---|---|
| Chain client features / bug fixes (consensus, p2p, EVM, RPC, txpool) | Upstream: https://github.com/ethereum/go-ethereum/issues |
| Block-data tracer hooks / pipeline output | Here |
| Dockerfile, published images, CI workflows | Here |
| Docs about running this write node | Here |

We cannot accept chain-core changes in this fork: they would diverge from upstream
and be lost or cause conflicts at the next upstream merge. If an upstream fix
matters to this fork, open an issue here linking the upstream PR/commit and we
will pull it in with the next sync.

## Reporting issues

- Reproducible on a vanilla upstream build → upstream issue tracker:
  https://github.com/ethereum/go-ethereum/issues
- Only reproducible with our image / our data output (tracer output, pipeline
  payloads, image build) → open an issue **here**, including the image tag, chain,
  and block height
- Security issues → see [SECURITY.md](./SECURITY.md); never open a public issue

## Development workflow

1. Fork the repository and create a branch from `main`
2. Keep changes focused on the Chaintable layer (see table above)
3. Build and toolchain follow upstream (Go); see the upstream
   [developer docs](https://geth.ethereum.org/docs) for environment setup
4. Open a PR. CI builds the Docker images for this repository. Note: the image
   publishing steps need repository credentials, which GitHub does not provide to
   pull requests from forks — those steps failing on a fork PR is expected, and a
   maintainer will build and verify your change on an internal branch.

Keep PRs small and focused.

## Upstream syncs & releases

- Upstream merges are performed by maintainers
- Release tags follow `<upstream-version>-debank-N` (e.g. `v1.17.4-debank-1`); a
  GitHub Release publishes the versioned images to
  `public.ecr.aws/b2h7a5c4/chaintable/go-ethereum` and the per-chain
  `<chain>-writer` aliases

## Pull requests

Before submitting: behavior changes explained, with a summary, motivation, and
testing details.

## License

By contributing, you agree that your contributions are licensed under the same
terms as this repository — see [COPYING](./COPYING) (GPL-3.0) and
[COPYING.LESSER](./COPYING.LESSER) (LGPL-3.0), inherited from upstream.
