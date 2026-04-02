# FastTable Core API (Stable Surface)

This document defines the **stable public API surface** for `@fasttable/core` as currently exposed by `packages/core/package.json` `exports`.

## Core file map (quick)

Main source files in `packages/core/src`:

- `runtime.js`: headless runtime facade used by browser adapters and Node/CLI.
- `benchmark.js`: filtering and sorting benchmark engines.
- `generation.js`: data generation + object/numeric/columnar derivations.
- `filtering.js`: filtering controllers and dictionary/planner paths.
- `filtering-orchestration.js`: shared filtering orchestration/cache layer for browser and CLI adapters.
- `filtering-runtime-orchestration.js`: shared filter+sort runtime-pass orchestration used by browser wrappers and adapters.
- `filter-sort-runtime-orchestration.js`: shared normal-flow filter/sort core orchestration used by thin browser wrappers.
- `filter-runtime-bridge.js`: shared runtime-facing filtering execution bridge for browser and CLI/runtime adapters.
- `sorting.js`: sort controllers, typed comparators, index sorting.
- `sorting-precomputed-runtime.js`: shared precomputed sorting runtime used by runtime/CLI/browser benchmark adapters.
- `sorting-orchestration.js`: shared sort benchmark orchestration layer for browser and CLI adapters.
- `sort-runtime-bridge.js`: shared runtime-facing sorting bridge handling mode dispatch and precomputed integration.
- `sort-benchmark-runtime.js`: shared sort-benchmark runtime bridge used by thin browser/CLI adapters.
- `benchmark-runtime-adapter.js`: shared benchmark API adapter contract (snapshot/prewarm/run/restore) used by browser and CLI wrappers.
- `io.js`: binary codec and format conversion utilities.
- `io-browser.js`, `io-node.js`: browser/node I/O adapters.
- `generation-worker-protocol.js`: shared worker message protocol.
- `generation-workers-shared.js`: shared worker orchestration logic reused by browser and Node adapters.
- `generation-workers-browser.js`, `generation-workers-node.js`: browser/Node worker adapters.
- `engine.js`: canonical caller-facing core API surface (runtime-backed).
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
  - `@fasttable/core/filtering-orchestration`
  - `@fasttable/core/filtering-runtime-orchestration`
  - `@fasttable/core/filter-sort-runtime-orchestration`
  - `@fasttable/core/filter-runtime-bridge`
  - `@fasttable/core/sorting`
  - `@fasttable/core/sorting-orchestration`
  - `@fasttable/core/sort-benchmark-runtime`
  - `@fasttable/core/benchmark-runtime-adapter`
- `experimental`:
  - `@fasttable/core/sorting-precomputed-runtime`
  - `@fasttable/core/sort-runtime-bridge`
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
- `@fasttable/core/filtering-orchestration`
- `@fasttable/core/filtering-runtime-orchestration`
- `@fasttable/core/filter-sort-runtime-orchestration`
- `@fasttable/core/filter-runtime-bridge`
- `@fasttable/core/sorting`
- `@fasttable/core/sorting-precomputed-runtime`
- `@fasttable/core/sorting-orchestration`
- `@fasttable/core/sort-runtime-bridge`
- `@fasttable/core/sort-benchmark-runtime`
- `@fasttable/core/benchmark-runtime-adapter`
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
- `@fasttable/core/engine`
- `@fasttable/core/benchmark`
- `@fasttable/core/benchmark-runtime-adapter`
- `@fasttable/core/io`
- `@fasttable/core/io-adapter`
- `@fasttable/core/io-node`
- `@fasttable/core/io-browser`

These provide the lowest-friction app/CLI integration path.
For browser static hosting, `@fasttable/core` can be mapped as a single import-map entry and used as the root import surface.
Node-only adapters remain explicit subpaths (for example `@fasttable/core/io-node`, `@fasttable/core/generation-workers-node`).

## Core ownership contract

- `engine` is the canonical caller-facing core API for app/CLI wrappers.
- `runtime` is the execution backend that engine owns internally.
- Wrappers should call engine methods for filter/sort/snapshot/state operations.
- Benchmark wrappers should consume `engine.createBenchmarkApi()` (optionally via `benchmark-runtime-adapter`).

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
- `buildSortRowsSnapshot(rawFilters?) -> { snapshotType, rowIndices, count, filterCoreMs }`
- `runSortSnapshotPass(rowsSnapshot, descriptors, sortMode?) -> sortResult`
- `prewarmPrecomputedSortState() -> boolean`
- `getNumericColumnarForSave() -> numericColumnarData | null`
- `restoreStateCore(statePatch) -> { modeOptions, rawFilters, sortOptions, sortMode }`

## Engine API (`@fasttable/core/engine`, experimental)

Factory:

- `createFastTableEngine({ runtime, adapters? })`

Canonical methods (runtime-backed):

- state/data:
  - `hasData() -> boolean`
  - `getRowCount() -> number`
  - `getModeOptions() -> object`
  - `setModeOptions(options, switchOptions?) -> object`
  - `getRawFilters() -> object`
  - `setRawFilters(rawFilters) -> object`
  - `clearFilters() -> object`
  - `getSortModes() -> string[]`
  - `getSortMode() -> string`
  - `setSortMode(mode) -> string`
  - `getSortOptions() -> object`
  - `setSortOptions(options) -> object`
  - `setDataFromRows(rows) -> number`
  - `setDataFromNumericColumnar(numericColumnarData) -> number`
- core execution:
  - `executeFilterCore(rawFilters, options?) -> filterResult`
  - `buildSortRowsSnapshot(rawFilters?) -> snapshot`
  - `executeSortCore(snapshot, descriptors, sortMode?) -> sortResult`
  - `restoreStateCore(statePatch) -> stateSnapshot`
- filter cache management:
  - `hasTopLevelFilterCacheEntries() -> boolean`
  - `clearTopLevelFilterCache()`
  - `clearTopLevelSmartFilterState()`
  - `clearAllFilterCaches()`
  - `bumpTopLevelFilterCacheRevision()`
- benchmark:
  - `createBenchmarkApi() -> benchmarkApi`

Filter result shape:

- `{ modePath, filteredCount, filteredIndices, coreMs, totalMs, dictionaryPrefilter }`

Current ownership semantics (important):

- `filteredIndices` may reference internal scratch buffers reused by subsequent filter calls on the same controller path.
- If you need to retain a result across later filter calls, copy indices first (for example, `Uint32Array.from(result.filteredIndices.buffer.subarray(0, result.filteredIndices.count))` for `{ buffer, count }` results).

Sort result shape:

- `{ sortMs, sortCoreMs, sortPrepMs, sortTotalMs, sortMode, sortedCount, descriptors, dataPath, comparatorMode, sortedIndices? }`

Sort snapshot contract:

- Runtime snapshots are typed-index-first:
  - `snapshotType: "row-indices-v2"`
  - `rowIndices: Uint32Array` (or compatible index collection)
  - `count: number`
- `runSortSnapshotPass` accepts the above snapshot payload (and keeps legacy array-like support for compatibility).

## Benchmark API (`@fasttable/core/benchmark`)

- `runFilteringBenchmark({ api, currentOnly?, rounds?, benchmarkCases?, now?, delayTick?, onUpdate? })`
- `runSortBenchmark({ api, currentOnly?, rounds?, sortCases?, now?, delayTick?, onUpdate? })`

Sort benchmark prewarm behavior:

- If sort variants include `precomputed` and `api.prewarmPrecomputedSortState` exists, benchmark prewarm is triggered once before measurement in both browser and CLI flows.

Return shape (both):

- `{ lines, totals, durationMs, error }`

Important:

- `api` must implement required methods (runtime already does).
- Filtering benchmark pass execution prefers `executeFilterCore` when available, with `runSingleFilterPass` fallback.
- Sorting benchmark pass execution prefers `executeSortCore` when available, with `runSortSnapshotPass` fallback.
- Required restore contract for both benchmark runners:
  - `api.restoreStateCore(statePatch)`
  - benchmark core restores through this method in `finally`.
- Benchmark core no longer runs legacy direct restore sequences (`setModeOptions` + `setRawFilters` + `runFilterPass`) in `finally`.
- Validation failures throw immediately.
- Runtime execution failures are captured in `error` and also appended in `lines`.

## Benchmark runtime adapter (`@fasttable/core/benchmark-runtime-adapter`)

Factory:

- `createBenchmarkRuntimeAdapter({ api, hooks? }) -> benchmarkApi`

Purpose:

- Wraps a benchmark API with one shared contract for:
  - sort snapshot build
  - sort core execution (`executeSortCore`, with `runSortSnapshotPass` fallback)
  - prewarm
  - state restore
- Used by both browser and CLI wrappers so benchmark orchestration stays portable.

Optional hooks:

- `hooks.beforeBuildSortSnapshot(rawFilters)`
- `hooks.beforeRunSortSnapshotPass(rowsSnapshot, descriptors, sortMode)`
- `hooks.beforePrewarmSortState()`
- `hooks.restoreState(statePatch)`

If no restore hook is provided, adapter requires `api.restoreStateCore(...)` and throws if missing (no legacy fallback restore path).

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
    "@fasttable/core": "./packages/core/dist/index.js"
  }
}
</script>
```

```js
import {
  createFastTableRuntime,
  createFastTableEngine,
  runFilteringBenchmark,
} from "@fasttable/core";

const runtime = createFastTableRuntime();
const engine = createFastTableEngine({ runtime });

runtime.generate(100000);
engine.setModeOptions({
  useColumnarData: true,
  useBinaryColumnar: true,
  useDictionaryKeySearch: true,
  useDictionaryIntersection: true,
  useSmarterPlanner: true,
});

const filterResult = engine.executeFilterCore({ firstName: "andr" });

const benchmark = await runFilteringBenchmark({
  api: engine.createBenchmarkApi(),
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
} from "@fasttable/core";

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
import { createFastTableEngine } from "@fasttable/core/engine";
import { loadColumnarBinaryPreset } from "@fasttable/core/io-node";
import { runFilteringBenchmark } from "@fasttable/core/benchmark";

const runtime = createFastTableRuntime();
const engine = createFastTableEngine({ runtime });
const schema = runtime.getSchema();

const loaded = await loadColumnarBinaryPreset({
  schema,
  presetDir: "./tables_presets",
  rowCount: 1000000,
});

engine.setDataFromNumericColumnar(loaded.numericColumnarData);

const result = await runFilteringBenchmark({
  api: engine.createBenchmarkApi(),
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

- `node benchmark-cli.mjs --preset 1000000 --bench filtering --current --rounds 3`
- `node benchmark-cli.mjs --preset 1000000 --bench sorting --current --sort-mode precomputed --rounds 3`

One-shot normal runtime CLI in this repo:

- `node runtime-cli.mjs --op filter --preset 1000000 --filters firstName=andr`
- `node runtime-cli.mjs --op sort --preset 1000000 --sort firstName:desc,lastName:asc --sort-mode precomputed`
- `node runtime-cli.mjs --op filter-sort --preset 1000000 --filters firstName=andr --sort firstName:desc,lastName:asc --sort-mode precomputed`
