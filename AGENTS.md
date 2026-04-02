# FastTable Agent Notes

This file captures the repo-level conventions for future edits.

## Architecture Contract

- Core execution is engine-first:
  - `createFastTableEngine(...)` is the canonical caller-facing core API.
  - runtime is the backend owned by engine.
- Browser and CLI wrappers should stay thin:
  - wrappers do UI/IO/logging only.
  - filtering/sorting/snapshot execution should go through engine/runtime core modules.

## Naming Conventions

- Root browser wrappers use `*-browser.js` names.
  - examples: `io-browser.js`, `generation-workers-browser.js`,
    `filtering-benchmark-browser.js`, `sorting-benchmark-browser.js`.
- Root CLI entry scripts use `*-cli.mjs` names.
  - examples: `benchmark-cli.mjs`, `runtime-cli.mjs`.
- Core package keeps Node runtime adapter names as `*-node.js`
  (for package exports clarity), while root app scripts use `cli` naming.

## Import Contract

- Browser static app import map should prefer one root entry:
  - `@fasttable/core` -> `./packages/core/dist/index.js`
- Browser/root scripts should import from `@fasttable/core` (root-only imports).
- Node-specific functionality may still use explicit Node subpaths
  (for example `@fasttable/core/io-node` or `@fasttable/core/generation-workers-node`).

## Build + Versioning

- Rebuild core dist after `packages/core/src/*` changes:
  - `node build-core.mjs`
- Bump app/core version together:
  - `node scripts/bump-version.mjs`
- Keep these in sync:
  - `version.json`
  - `packages/core/package.json`
  - `packages/core/CHANGELOG.md`

## Verification (quick)

- `node tests/node/parity.test.mjs`
- `node tests/node/io-roundtrip.test.mjs`
- `node tests/node/benchmark-smoke.test.mjs`
- `node tests/node/orchestration-smoke.test.mjs`
- `node tests/node/hardcut-refactor.test.mjs`
