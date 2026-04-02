# Changelog

All notable changes to `@fasttable/core` will be documented in this file.

The format follows Keep a Changelog and semantic versioning.

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
