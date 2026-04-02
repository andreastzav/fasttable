# Fast Table

Inspired by [gabrielpetersson/fast-grid](https://github.com/gabrielpetersson/fast-grid), this project is my attempt to push web table performance as far as possible through **single-threaded filtering and sorting** improvements.

## Live demo

Live demo: [andreastzav.github.io/fasttable/](https://andreastzav.github.io/fasttable/)
- Load a table preset, e.g. the one with 1 million rows, and play with filtering and sorting. Alternatively you can generate your own table.

## Project direction

The work here intentionally focuses on algorithmic and data-structure improvements on the main thread:

- Faster filtering
- Faster sorting
- Better planning and indexing for both

I did not prioritize building a worker-based filtering/sorting pipeline that splits work into batches and returns partial early results for progressive UI updates. That is a valid product direction, but **it is not** the target of this project.

## Performance results

On a 1,000,000 row table, this project reaches (vs native browser implementations):

- Filtering: about **12 ms** total on average, roughly a **77x** improvement
- Single-column sorting: about **1 ms**, roughly a **1500x** improvement
- Two-column sorting: about **20 ms** on average, roughly a **340x** improvement

With this benchmark profile, I think this may now be the fastest web table implementation.

## Why it is fast

Key ideas used in this codebase include:

- Columnar and binary-columnar filtering paths
- Dictionary key search and dictionary intersection
- Smarter filter planning
- Precomputed sort indices and rank arrays
- Typed comparators and index-based sorting

## Recommended settings

The defaults already selected in the UI are the best-performing choices for this project.

### Filtering defaults

- Use columnar data: on
- Use binary columnar: on
- Use numeric rows: on
- Use normalized strings: on
- Search dict keys: on
- Intersect dicts: on
- Smarter planner: on
- Use smart filtering: off (**on**: reuse the previous filtered subset for stricter follow-up input)
- Use filter cache: off (**on**: store previous filter results in cache for repeated searches)

### Sorting defaults

- Sort mode: use precomp indices

## Notes

Table generation uses web workers by default, but filtering and sorting run on the main thread (single thread).

## Headless Runtime (Browser + Node)

The core package now includes a headless runtime facade:

- `createFastTableRuntime` from `@fasttable/core/runtime`

This runtime owns table dataset state and filter/sort orchestration without any DOM dependency, so the same logic can be used from browser adapters and from Node/CLI.

Worker adapters are also available in core:

- Browser: `@fasttable/core/generation-workers-browser`
- Node: `@fasttable/core/generation-workers-node`
- Conditional alias: `@fasttable/core/generation-workers`

I/O adapters are available as:

- Browser: `@fasttable/core/io-browser`
- Node: `@fasttable/core/io-node`
- Conditional alias: `@fasttable/core/io-adapter`

`@fasttable/core` root export is runtime-neutral; environment-specific adapters are consumed via the explicit/conditional subpaths above.

### Build core dist (no npm needed)

Core is consumed from `packages/core/dist` (not directly from `src`).

Run:

```bash
node build-core.mjs
```

### App version badge + auto bump on commit

- `version.json` is the app version source shown in the top-left UI badge.
- `packages/core/package.json` version is kept in sync.
- `scripts/bump-version.mjs` bumps patch version (`x.y.z -> x.y.(z+1)`).
- `.githooks/pre-commit` runs the bump script automatically for every commit.

Enable repo hooks once on your machine:

```bash
git config core.hooksPath .githooks
```

### Quick Node CLI benchmark

Use the runtime CLI script:

```bash
node bench-runtime-cli.mjs --preset 1000000 --bench filtering --current --rounds 3
```

Force sorting benchmark mode (native, timsort, or precomputed):

```bash
node bench-runtime-cli.mjs --preset 1000000 --bench sorting --current --sort-mode precomputed --rounds 3
```

Optional text output:

```bash
node bench-runtime-cli.mjs --preset 1000000 --bench both --current --rounds 3 --out runtime-benchmark.txt
```

Optional worker_threads path in CLI:

```bash
node bench-runtime-cli.mjs --generate-workers 1000000 --workers 4 --chunk-size 10000 --precompute-sort-workers --bench filtering --current --rounds 3
```

This uses the Node worker adapter from `@fasttable/core/generation-workers-node` for generation and sort-index precompute.

### Portability tests

Test files are in `tests/`:

- Node parity/roundtrip/smoke tests: `tests/node/`
- Browser benchmark smoke page: `tests/browser/smoke.html`

See `tests/README.md` for run commands.

## API reference

Stable public API and integration examples are documented in:

- `docs/API.md`

Package release flow:

- `docs/RELEASING_CORE.md`

## Project map (what lives where)

Use this as the quick "which file should I edit?" guide.

- `index.html`: browser app shell + import map + script wiring.
- `styles.css`: browser UI styling.
- `app.js`: main browser controller (DOM, state wiring, rendering flow).
- `generation.js`, `filtering.js`, `sorting.js`, `io.js`: browser adapters that bridge the UI to core package APIs.
- `generation-workers.js`: thin browser adapter that exposes worker APIs on `window.fastTableGenerationWorkers`.
- `filtering-benchmark.js`, `sorting-benchmark.js`: browser benchmark UI wrappers.
- `table-rendering.js`: table render/update helpers used by the browser app.
- `bench-runtime-cli.mjs`: Node CLI benchmark runner (preset load + benchmark output).
- `build-core.mjs`: builds `packages/core/dist` from `packages/core/src`.

Core package:

- `packages/core/src/generation.js`: dataset generation + derived representations.
- `packages/core/src/filtering.js`: filtering controllers and dictionary/planner logic.
- `packages/core/src/filtering-orchestration.js`: shared filtering orchestration/cache logic consumed by browser and CLI/runtime adapters.
- `packages/core/src/filter-runtime-bridge.js`: shared runtime-facing filtering bridge used by browser app and runtime/CLI adapters.
- `packages/core/src/sorting.js`: sort controllers and index/typed comparator paths.
- `packages/core/src/sorting-precomputed-runtime.js`: shared precomputed sorting runtime (typed-array-first) reused by browser/runtime/CLI benchmark paths.
- `packages/core/src/sorting-orchestration.js`: shared sort benchmark orchestration consumed by browser and CLI adapters.
- `packages/core/src/sort-runtime-bridge.js`: shared runtime-facing sorting bridge handling `native`/`timsort`/`precomputed` dispatch.
- `packages/core/src/sort-benchmark-runtime.js`: thin runtime sync bridge used by browser/CLI benchmark adapters.
- `packages/core/src/io.js`: binary codec (format encode/decode + conversion helpers).
- `packages/core/src/io-browser.js`, `packages/core/src/io-node.js`: runtime-specific I/O adapters.
- `packages/core/src/runtime.js`: headless runtime facade for dataset/filter/sort orchestration.
- `packages/core/src/benchmark.js`: filtering and sorting benchmark engines.
- `packages/core/src/generation-worker-protocol.js`: shared worker message protocol.
- `packages/core/src/generation-workers-shared.js`: shared worker orchestration logic used by browser and Node adapters.
- `packages/core/src/generation-workers-browser.js`, `packages/core/src/generation-workers-node.js`: browser/Node worker adapters.
- `packages/core/src/engine.js`: app-agnostic orchestration facade around runtime adapters.
- `packages/core/dist/*`: build output consumed by browser import map and package exports.

Data, docs, tests:

- `tables_presets/`: preset datasets (`.bin` + `.json` metadata).
- `docs/API.md`: stable public API and integration examples.
- `docs/RELEASING_CORE.md`: release/build flow for `@fasttable/core`.
- `tests/node/`: Node parity/IO/benchmark/worker smoke tests.
- `tests/browser/`: browser smoke harness.

## Productivity credits

This project was built with the help of **Codex** and **ChatGPT**, which were used as productivity multipliers for faster iteration, implementation support, and documentation.
I think of this workflow as **Lambda coding**:
Human on top providing direction, ideas, constraints, and feedback (plus the occasional curse word); Codex on the right as the hard(ly) working software engineer; and ChatGPT on the left for critique and adversarial feedback.

## TODO

- Extract remaining orchestration from `app.js` into core runtime/engine adapters so `app.js` is a thin UI layer.
- Finalize filtering result ownership contract (safe owned default + explicit ephemeral mode for max-performance paths).
- Promote selected core experimental entrypoints to stable once API contracts and cross-runtime tests are fully locked.
