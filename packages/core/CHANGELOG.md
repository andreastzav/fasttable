# Changelog

All notable changes to `@fasttable/core` will be documented in this file.

The format follows Keep a Changelog and semantic versioning.

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
