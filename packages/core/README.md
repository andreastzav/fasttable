# @fasttable/core

Core ESM modules for FastTable generation, filtering, sorting, I/O, runtime orchestration, and benchmark engines.

## What this package provides

- High-performance table data generation and derived representations
- Filtering controllers (rows/columnar/numeric/binary-columnar)
- Sorting controllers with pluggable sort modes
- Binary table codec + browser/node adapters
- Headless runtime (`createFastTableRuntime`) for browser or Node
- Benchmark engines for filtering/sorting telemetry

## Public entrypoints

- `@fasttable/core`
- `@fasttable/core/generation`
- `@fasttable/core/filtering`
- `@fasttable/core/sorting`
- `@fasttable/core/sort-benchmark-runtime`
- `@fasttable/core/io`
- `@fasttable/core/io-browser`
- `@fasttable/core/io-node`
- `@fasttable/core/benchmark`
- `@fasttable/core/runtime`
- `@fasttable/core/engine` (experimental)
- `@fasttable/core/generation-worker-protocol` (experimental)
- `@fasttable/core/generation-workers` (experimental, conditional browser/node)
- `@fasttable/core/generation-workers-browser` (experimental)
- `@fasttable/core/generation-workers-node` (experimental)
- `@fasttable/core/io-adapter` (conditional browser/node)

`@fasttable/core` is runtime-neutral and does not re-export environment-specific adapters.

## Build

This package is consumed from `dist`.

From repository root:

```bash
node build-core.mjs
```

Or from package directory:

```bash
npm run build
```

## Minimal usage

```js
import { createFastTableRuntime } from "@fasttable/core/runtime";
import { runFilteringBenchmark } from "@fasttable/core/benchmark";

const runtime = createFastTableRuntime();
runtime.generate(100000);

const benchmark = await runFilteringBenchmark({
  api: runtime,
  currentOnly: true,
  rounds: 1,
});

console.log(benchmark.lines.join("\n"));
```

## Node worker adapter (experimental)

For worker-based generation and sort-index precompute in Node/CLI:

```js
import { fastTableGenerationWorkersNodeApi } from "@fasttable/core/generation-workers-node";

const generated = await fastTableGenerationWorkersNodeApi.generateRowsWithWorkers({
  rowCount: 1000000,
  workerCount: 4,
  chunkSize: 10000,
});

const precomputed =
  await fastTableGenerationWorkersNodeApi.buildSortedIndicesWithWorkers({
    numericColumnarData: generated.derivedData.numericColumnarData,
    workerCount: 4,
  });
```

## Browser worker adapter (experimental)

```js
import {
  fastTableGenerationWorkersBrowserApi,
  attachGenerationWorkersBrowserApi,
} from "@fasttable/core/generation-workers-browser";

attachGenerationWorkersBrowserApi(window);

const generated =
  await fastTableGenerationWorkersBrowserApi.generateRowsWithWorkers({
    rowCount: 1000000,
    workerCount: 4,
    chunkSize: 10000,
  });
```

## Versioning and stability

See:

- `../../docs/API.md` for stable surface + compatibility rules
- `CHANGELOG.md` for package release history
- `../../docs/RELEASING_CORE.md` for release steps
