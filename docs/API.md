# FastTable Core API (Stable Surface)

This document defines the **stable public API surface** for `@fasttable/core` as currently exposed by `packages/core/package.json` `exports`.

## Core file map (quick)

Main source files in `packages/core/src`:

- `runtime.js`: headless runtime facade used by browser adapters and Node/CLI.
- `benchmark.js`: filtering and sorting benchmark engines.
- `generation.js`: data generation + object/numeric/columnar derivations.
- `filtering.js`: filtering controllers and dictionary/planner paths.
- `sorting.js`: sort controllers, typed comparators, index sorting.
- `io.js`: binary codec and format conversion utilities.
- `io-browser.js`, `io-node.js`: browser/node I/O adapters.
- `generation-worker-protocol.js`: shared worker message protocol.
- `generation-workers-shared.js`: shared worker orchestration logic reused by browser and Node adapters.
- `generation-workers-browser.js`, `generation-workers-node.js`: browser/Node worker adapters.
- `engine.js`: app-agnostic orchestration facade around runtime adapters.
- `index.js`: package re-export surface.

Build output:

- `packages/core/dist/*`: generated distribution files used by package exports.

## Stability and versioning policy

### Stability levels

- `stable`: safe for external consumers; covered by semver compatibility.
- `experimental`: available, but can change in minor releases if needed.
- `internal`: not exported and not supported for external consumption.

### Current module levels

- `stable`:
  - `@fasttable/core/runtime`
  - `@fasttable/core/benchmark`
  - `@fasttable/core/io`
  - `@fasttable/core/io-adapter`
  - `@fasttable/core/io-node`
  - `@fasttable/core/io-browser`
  - `@fasttable/core/generation`
  - `@fasttable/core/filtering`
  - `@fasttable/core/sorting`
- `experimental`:
  - `@fasttable/core/engine`
  - `@fasttable/core/generation-worker-protocol`
  - `@fasttable/core/generation-workers`
  - `@fasttable/core/generation-workers-browser`
  - `@fasttable/core/generation-workers-node`

### Semver guarantees

- `patch` (`x.y.Z`):
  - bug fixes only
  - no breaking API or shape changes in `stable` modules
- `minor` (`x.Y.z`):
  - additive improvements and new exports
  - no breaking API or shape changes in `stable` modules
  - `experimental` modules may evolve
- `major` (`X.y.z`):
  - breaking changes allowed

### Deprecation policy

- Stable APIs are first marked as deprecated in docs/changelog before removal.
- Removals happen only in a major version unless there is a security/critical correctness reason.

## Public entrypoints

- `@fasttable/core`
- `@fasttable/core/generation`
- `@fasttable/core/filtering`
- `@fasttable/core/sorting`
- `@fasttable/core/io`
- `@fasttable/core/io-adapter`
- `@fasttable/core/io-browser`
- `@fasttable/core/io-node`
- `@fasttable/core/benchmark`
- `@fasttable/core/runtime`
- `@fasttable/core/engine`
- `@fasttable/core/generation-worker-protocol`
- `@fasttable/core/generation-workers`
- `@fasttable/core/generation-workers-browser`
- `@fasttable/core/generation-workers-node`

## Recommended stable modules

For integrations, prefer these:

- `@fasttable/core/runtime`
- `@fasttable/core/benchmark`
- `@fasttable/core/io`
- `@fasttable/core/io-adapter`
- `@fasttable/core/io-node`
- `@fasttable/core/io-browser`

These provide the lowest-friction app/CLI integration path.
`@fasttable/core` itself is runtime-neutral and does not include environment-specific adapters.

## Runtime API (`@fasttable/core/runtime`)

Factory:

- `createFastTableRuntime(options?)`

Returned runtime object:

- `getSchema() -> { columnKeys, columnNames, baseColumnCount, numericCacheOffset, objectCacheKeys }`
- `hasData() -> boolean`
- `getRowCount() -> number`
- `setDataFromRows(rows) -> { rowCount }`
- `setDataFromNumericColumnar(numericColumnarData) -> { rowCount }`
- `generate(rowCount, generationOptions?) -> { rowCount }`
- `getModeOptions() -> object`
- `setModeOptions(options) -> object`
- `getRawFilters() -> object`
- `setRawFilters(rawFilters) -> object`
- `setSingleFilter(columnKey, value) -> object`
- `clearFilters() -> {}`
- `runFilterPass(options?) -> filterResult | null`
- `runSingleFilterPass(columnKey, value, options?) -> filterResult | null`
- `runFilterPassWithRawFilters(rawFilters, options?) -> filterResult | null`
- `getLastFilterResult() -> filterResult | null`
- `getSortModes() -> string[]`
- `getSortMode() -> string`
- `setSortMode(mode) -> string`
- `getSortOptions() -> { useTypedComparator, useIndexSort }`
- `setSortOptions(options) -> { useTypedComparator, useIndexSort }`
- `buildSortRowsSnapshot(rawFilters?) -> { rows, count, filterCoreMs }`
- `runSortSnapshotPass(rowsSnapshot, descriptors, sortMode?) -> sortResult`
- `getNumericColumnarForSave() -> numericColumnarData | null`

Filter result shape:

- `{ modePath, filteredCount, filteredIndices, coreMs, totalMs, dictionaryPrefilter }`

Current ownership semantics (important):

- `filteredIndices` may reference internal scratch buffers reused by subsequent filter calls on the same controller path.
- If you need to retain a result across later filter calls, copy indices first (for example, `Uint32Array.from(result.filteredIndices.buffer.subarray(0, result.filteredIndices.count))` for `{ buffer, count }` results).

Sort result shape:

- `{ sortMs, sortCoreMs, sortPrepMs, sortTotalMs, sortMode, sortedCount, descriptors, dataPath, comparatorMode }`

## Benchmark API (`@fasttable/core/benchmark`)

- `runFilteringBenchmark({ api, currentOnly?, rounds?, benchmarkCases?, now?, delayTick?, onUpdate? })`
- `runSortBenchmark({ api, currentOnly?, rounds?, sortCases?, now?, delayTick?, onUpdate? })`

Return shape (both):

- `{ lines, totals, durationMs, error }`

Important:

- `api` must implement required methods (runtime already does).
- Validation failures throw immediately.
- Runtime execution failures are captured in `error` and also appended in `lines`.

## IO API (`@fasttable/core/io`, `@fasttable/core/io-node`)

Core codec:

- `createColumnarBinaryExportFiles(numericColumnarData, schema) -> { metadata, metadataText, binaryBytes }`
- `parseColumnarBinaryMetadataText(metadataText, schema) -> metadata`
- `decodeColumnarBinaryData(metadata, binaryBuffer, schema) -> numericColumnarData`
- `decodeColumnarBinaryPayload(metadataText, binaryBuffer, schema) -> { metadata, numericColumnarData }`
- `convertNumericColumnarDataToObjectRows(numericColumnarData, schema, includeCache) -> rows[]`

`binaryBuffer` accepts `ArrayBuffer` and `ArrayBuffer` views (`Uint8Array`, Node `Buffer`, and other typed-array views).

Node adapters:

- `saveColumnarBinaryFiles({ numericColumnarData, schema, dirPath?, baseFileName?, rowCount? })`
- `loadColumnarBinaryFiles({ schema, metadataPath, binaryPath })`
- `loadColumnarBinaryPreset({ schema, presetDir?, rowCount?, baseFileName? })`

Node load note:

- `loadColumnarBinaryFiles/loadColumnarBinaryPreset` return `binaryBuffer` as a binary view (currently `Uint8Array`) to avoid an extra full-buffer copy on large preset loads.

## Conditional adapter aliases

- `@fasttable/core/io-adapter`
  - resolves to `@fasttable/core/io-browser` in browser environments
  - resolves to `@fasttable/core/io-node` in Node environments
- `@fasttable/core/generation-workers` (experimental)
  - resolves to `@fasttable/core/generation-workers-browser` in browser environments
  - resolves to `@fasttable/core/generation-workers-node` in Node environments

## Browser worker adapter (`@fasttable/core/generation-workers-browser`, experimental)

- `generateRowsWithWorkers({ rowCount, workerCount?, chunkSize?, onProgress? })`
- `buildSortedIndicesWithWorkers({ numericColumnarData, workerCount?, onProgress? })`
- `attachGenerationWorkersBrowserApi(targetWindow) -> boolean`
- `fastTableGenerationWorkersBrowserApi`
  - `{ generateRowsWithWorkers, buildSortedIndicesWithWorkers }`

## Node worker adapter (`@fasttable/core/generation-workers-node`, experimental)

- `generateRowsWithWorkers({ rowCount, workerCount?, chunkSize?, onProgress? })`
- `buildSortedIndicesWithWorkers({ numericColumnarData, workerCount?, onProgress? })`
- `fastTableGenerationWorkersNodeApi`
  - `{ generateRowsWithWorkers, buildSortedIndicesWithWorkers }`

## Error behavior

Expected error categories:

- Invalid arguments:
  - Example: missing `schema` for IO functions.
  - Example: invalid benchmark `api`.
- Runtime unavailability:
  - Example: browser-only API used in non-browser (`io-browser` functions).
- Data format mismatch:
  - Example: binary metadata byte ranges out of bounds during decode.
- Adapter contract failures:
  - Example: `createFastTableEngine` generate adapter missing and `generate()` called.

Guidance:

- Treat thrown errors as caller-contract issues (wrong inputs or missing dependencies).
- Treat benchmark `error` field as run-time benchmark failure details.

## Minimal integration: Browser app

Use an import map (for static hosting):

```html
<script type="importmap">
{
  "imports": {
    "@fasttable/core/runtime": "./packages/core/dist/runtime.js",
    "@fasttable/core/benchmark": "./packages/core/dist/benchmark.js"
  }
}
</script>
```

```js
import { createFastTableRuntime } from "@fasttable/core/runtime";
import { runFilteringBenchmark } from "@fasttable/core/benchmark";

const runtime = createFastTableRuntime();
runtime.generate(100000);
runtime.setModeOptions({
  useColumnarData: true,
  useBinaryColumnar: true,
  useDictionaryKeySearch: true,
  useDictionaryIntersection: true,
  useSmarterPlanner: true,
});

runtime.setRawFilters({ firstName: "andr" });
const filterResult = runtime.runFilterPass();

const benchmark = await runFilteringBenchmark({
  api: runtime,
  currentOnly: true,
  rounds: 1,
  onUpdate(lines) {
    const last = lines[lines.length - 1];
    if (last) console.log(last);
  },
});
```

Optional browser worker adapter usage:

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

## Minimal integration: Node CLI

```js
import { createFastTableRuntime } from "@fasttable/core/runtime";
import { loadColumnarBinaryPreset } from "@fasttable/core/io-node";
import { runFilteringBenchmark } from "@fasttable/core/benchmark";

const runtime = createFastTableRuntime();
const schema = runtime.getSchema();

const loaded = await loadColumnarBinaryPreset({
  schema,
  presetDir: "./tables_presets",
  rowCount: 1000000,
});

runtime.setDataFromNumericColumnar(loaded.numericColumnarData);

const result = await runFilteringBenchmark({
  api: runtime,
  currentOnly: true,
  rounds: 3,
});

console.log(result.lines.join("\n"));
```

Optional worker-based generation + sort precompute in Node:

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

Existing CLI script in this repo:

- `node bench-runtime-cli.mjs --preset 1000000 --bench filtering --current --rounds 3`
- `node bench-runtime-cli.mjs --preset 1000000 --bench sorting --current --sort-mode precomputed --rounds 3`
