# Tests

## Test file map (quick)

- `tests/node/parity.test.mjs`: parity checks between `packages/core/src` and `packages/core/dist` for generation/filtering/sorting.
- `tests/node/io-roundtrip.test.mjs`: in-memory + file roundtrip checks for binary I/O.
- `tests/node/benchmark-smoke.test.mjs`: benchmark engine smoke tests against runtime APIs.
- `tests/node/orchestration-smoke.test.mjs`: orchestration-module export + fallback smoke (`src` and `dist`) including filter runtime bridge, filtering runtime orchestrator, sort runtime bridge, shared precomputed sorting runtime, sort benchmark runtime bridge, and benchmark runtime adapter.
- `tests/node/generation-workers-browser-export-smoke.test.mjs`: browser worker adapter export/attach smoke.
- `tests/node/generation-workers-node-smoke.test.mjs`: Node `worker_threads` generation + sort-precompute smoke.
- `tests/node/helpers.mjs`: shared test helpers.
- `tests/browser/smoke.html`, `tests/browser/smoke.js`: browser smoke harness page and checks.
- `tests/browser/run-smoke.mjs`: Playwright-based browser smoke runner used in CI.

## Node portability tests

Standard run:

```bash
node --test tests/node/*.test.mjs
```

Windows/sandbox fallback (no child-process spawn):

```bash
node tests/node/parity.test.mjs
node tests/node/io-roundtrip.test.mjs
node tests/node/benchmark-smoke.test.mjs
node tests/node/orchestration-smoke.test.mjs
node tests/node/generation-workers-browser-export-smoke.test.mjs
node tests/node/generation-workers-node-smoke.test.mjs
```

Use the fallback when `node --test ...` fails with `spawn EPERM` in restricted environments.

The Node suite covers:

- generation/filtering/sorting parity (`src` vs `dist`)
- io roundtrip (in-memory and file-based)
- benchmark engine smoke on runtime APIs
- orchestration module smoke on `src` and `dist`
- browser worker adapter export smoke
- node `worker_threads` generation + sort precompute smoke

## Browser smoke

Open the browser smoke page through a local/static server:

- `tests/browser/smoke.html`

It runs runtime + benchmark smoke checks in browser and prints `PASS`/`FAIL`.

Automated browser smoke runner (used by CI):

```bash
node tests/browser/run-smoke.mjs
```
