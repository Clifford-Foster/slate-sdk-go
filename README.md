# slate-sdk-go

The Go SDK for the [Blackboard Platform](https://github.com/Clifford-Foster/slate) — the client
library a Go component uses to talk to its sidecar, plus the blackboard coordination library the
platform itself is built on.

## Install

    go get github.com/Clifford-Foster/slate-sdk-go

## Packages

| Import | What it is |
| --- | --- |
| `github.com/Clifford-Foster/slate-sdk-go/bbsdk` | The component SDK: activations, events, RPC, the loopback data plane, outbound invoke, watches. |
| `github.com/Clifford-Foster/slate-sdk-go/bbsdk/harness` | The in-process test harness — run a component against an in-memory board, no sidecar and no fleet. |
| `github.com/Clifford-Foster/slate-sdk-go/bbsdk/components` | The component runtime: bind a chain document, run a chain, write results back. |
| `github.com/Clifford-Foster/slate-sdk-go/bbsdk/schema` | The code-first shape surface: derive a schema from a struct type, decode a board value into one, generate the committed artifact. |
| `github.com/Clifford-Foster/slate-sdk-go/blackboard` | The coordination library: the JetStream KV blackboard, the precondition DSL, agent activation, the capability registry. |
| `github.com/Clifford-Foster/slate-sdk-go/manifest` | The component manifest parser and validator. |

Requires Go 1.26 or later.

## Source of truth

**This repository is a generated, read-only mirror.** Every one of these packages is developed in
[Clifford-Foster/slate](https://github.com/Clifford-Foster/slate) and mirrored here by that repo's
`scripts/publish_sdk`. Please do not open pull requests against this repository — they cannot be
merged, because the next mirror run would overwrite them. File issues and changes upstream.

The test suites are upstream too, deliberately: each package's contract compliance is judged by a
black-box suite in the platform repo's `tests/`, which is the acceptance gate for any change here.

## Contracts

Behavior is specified by contracts, not by this implementation — where the two disagree, the contract
is right and the implementation is a bug. They live upstream in `contracts/`. This snapshot
implements:

| Contract | Version | Covers |
| --- | --- | --- |
| `contracts/bb_sdk_go.md` | 0.12.1 | `bbsdk`, `bbsdk/harness`, `bbsdk/schema` |
| `contracts/components_runtime.md` | 0.24.0 | `bbsdk/components` |
| `contracts/sidecar.md` | 0.28.0 | the component ↔ sidecar boundary `bbsdk` speaks |
| `contracts/blackboard_platform.md` | 0.34.4 | `blackboard` |
| `contracts/manifest.md` | 0.17.0 | `manifest` |

The `// Contract:` annotations throughout the source resolve against that upstream `contracts/`
directory.
