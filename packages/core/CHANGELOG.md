# Changelog

All notable changes to `@fasttable/core` will be documented in this file.

The format follows Keep a Changelog and semantic versioning.

## [0.5.10] - 2026-05-01

### Changed

- Removed obsolete legacy sort-orchestrator fallback branch from runtime operations snapshot execution.
- Removed legacy sort-benchmark snapshot compatibility paths (`legacy-row-array-v1` and `snapshotPayload.rows`) and enforced object snapshot payload contract.
- Simplified filter runtime bridge controller-index synchronization to a single always-sync path (removed dead conditional branch).
- Deduplicated CLI data-loading/precompute plumbing by introducing shared `cli-common.mjs` used by both:
  - `benchmark-cli.mjs`
  - `runtime-cli.mjs`

### Refactor

- Reduced compatibility layering in hot orchestration paths to tighten contracts and lower maintenance surface.

### Tests

- Verified node suites remain green after cleanup:
  - parity
  - io roundtrip
  - benchmark smoke
  - orchestration smoke
  - hardcut refactor
  - worker adapter smoke (browser/node)

## [0.5.9] - 2026-04-03

### Changed

- Browser benchmark UI now defaults to macro tick policy (`setTimeout(0)`), so telemetry lines render progressively instead of appearing only at the end.
- Sorting benchmark precomputed state is now reset per benchmark invocation:
  - reset before timed runs start
  - reset again in final restore (`finally`)
  This prevents cross-run carryover where repeated button clicks could show near-zero warm-state timings.
- Runtime/engine/benchmark adapter contract extended with `resetPrecomputedSortState(...)` so benchmark orchestration can request explicit precomputed-state resets.

### Tests

- Updated hardcut regression expectation for benchmark tick defaults:
  - browser wrapper default: `macro`
  - CLI default: `micro`

## [0.5.8] - 2026-04-02

### Changed

- Browser static import-map contract simplified to one root entry:
  - `@fasttable/core` -> `packages/core/dist/index.js`
- Browser/root wrappers now use root-only imports (`@fasttable/core`) instead of many subpath imports.
- Root package entrypoint now re-exports browser-safe adapters used by the browser app:
  - browser I/O helpers from `io-browser`
  - browser worker helpers from `generation-workers-browser`
- Browser and benchmark wrapper file naming is now canonicalized around `*-browser.js`.

### Docs

- Updated `README.md`, `docs/API.md`, and `packages/core/README.md` to reflect:
  - root-only browser import usage
  - explicit note that Node-only adapters still live on subpaths.
- Added root `AGENTS.md` with architecture, naming, import, versioning, and verification conventions.

## [0.1.0] - 2026-04-01

### Added

- ESM modular core package structure with explicit `exports`
- Headless runtime API (`createFastTableRuntime`)
- Benchmark engine module for filtering/sorting
- Binary I/O codec and browser/node adapters
- Generation worker protocol module
- Browser generation/sort-precompute worker adapter (`generation-workers-browser`)
- Node `worker_threads` generation/sort-precompute adapter (`generation-workers-node`)
- Dist build script (`node build-core.mjs`)
- API reference docs and portability test suite
