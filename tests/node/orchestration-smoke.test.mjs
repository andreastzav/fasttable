import assert from "node:assert/strict";
import { before, test } from "node:test";
import { ensureCoreDistBuilt } from "./helpers.mjs";

let srcFilteringOrchestration;
let distFilteringOrchestration;
let srcFilteringRuntimeOrchestration;
let distFilteringRuntimeOrchestration;
let srcFilterRuntimeBridge;
let distFilterRuntimeBridge;
let srcSortRuntimeBridge;
let distSortRuntimeBridge;
let srcSortingPrecomputedRuntime;
let distSortingPrecomputedRuntime;
let srcSortingOrchestration;
let distSortingOrchestration;
let srcSortBenchmarkRuntimeBridge;
let distSortBenchmarkRuntimeBridge;
let srcBenchmarkRuntimeAdapter;
let distBenchmarkRuntimeAdapter;
let srcFilterSortRuntimeOrchestration;
let distFilterSortRuntimeOrchestration;
let srcRuntime;
let distRuntime;

before(async () => {
  ensureCoreDistBuilt();

  srcFilteringOrchestration = await import(
    "../../packages/core/src/filtering-orchestration.js"
  );
  distFilteringOrchestration = await import(
    "../../packages/core/dist/filtering-orchestration.js"
  );
  srcFilteringRuntimeOrchestration = await import(
    "../../packages/core/src/filtering-runtime-orchestration.js"
  );
  distFilteringRuntimeOrchestration = await import(
    "../../packages/core/dist/filtering-runtime-orchestration.js"
  );
  srcFilterRuntimeBridge = await import(
    "../../packages/core/src/filter-runtime-bridge.js"
  );
  distFilterRuntimeBridge = await import(
    "../../packages/core/dist/filter-runtime-bridge.js"
  );
  srcSortRuntimeBridge = await import(
    "../../packages/core/src/sort-runtime-bridge.js"
  );
  distSortRuntimeBridge = await import(
    "../../packages/core/dist/sort-runtime-bridge.js"
  );
  srcSortingPrecomputedRuntime = await import(
    "../../packages/core/src/sorting-precomputed-runtime.js"
  );
  distSortingPrecomputedRuntime = await import(
    "../../packages/core/dist/sorting-precomputed-runtime.js"
  );
  srcSortingOrchestration = await import(
    "../../packages/core/src/sorting-orchestration.js"
  );
  distSortingOrchestration = await import(
    "../../packages/core/dist/sorting-orchestration.js"
  );
  srcSortBenchmarkRuntimeBridge = await import(
    "../../packages/core/src/sort-benchmark-runtime.js"
  );
  distSortBenchmarkRuntimeBridge = await import(
    "../../packages/core/dist/sort-benchmark-runtime.js"
  );
  srcBenchmarkRuntimeAdapter = await import(
    "../../packages/core/src/benchmark-runtime-adapter.js"
  );
  distBenchmarkRuntimeAdapter = await import(
    "../../packages/core/dist/benchmark-runtime-adapter.js"
  );
  srcFilterSortRuntimeOrchestration = await import(
    "../../packages/core/src/filter-sort-runtime-orchestration.js"
  );
  distFilterSortRuntimeOrchestration = await import(
    "../../packages/core/dist/filter-sort-runtime-orchestration.js"
  );
  srcRuntime = await import("../../packages/core/src/runtime.js");
  distRuntime = await import("../../packages/core/dist/runtime.js");
});

test("orchestration modules export expected factories in src and dist", () => {
  assert.equal(
    typeof srcFilteringOrchestration.createFilteringOrchestrator,
    "function"
  );
  assert.equal(
    typeof distFilteringOrchestration.createFilteringOrchestrator,
    "function"
  );
  assert.equal(
    typeof srcFilteringRuntimeOrchestration.createFilteringRuntimeOrchestrator,
    "function"
  );
  assert.equal(
    typeof distFilteringRuntimeOrchestration.createFilteringRuntimeOrchestrator,
    "function"
  );
  assert.equal(
    typeof srcFilterRuntimeBridge.createFilterRuntimeBridge,
    "function"
  );
  assert.equal(
    typeof distFilterRuntimeBridge.createFilterRuntimeBridge,
    "function"
  );
  assert.equal(
    typeof srcSortingOrchestration.createSortBenchmarkOrchestrator,
    "function"
  );
  assert.equal(
    typeof distSortingOrchestration.createSortBenchmarkOrchestrator,
    "function"
  );
  assert.equal(
    typeof srcSortBenchmarkRuntimeBridge.createSortBenchmarkRuntimeBridge,
    "function"
  );
  assert.equal(
    typeof distSortBenchmarkRuntimeBridge.createSortBenchmarkRuntimeBridge,
    "function"
  );
  assert.equal(
    typeof srcBenchmarkRuntimeAdapter.createBenchmarkRuntimeAdapter,
    "function"
  );
  assert.equal(
    typeof distBenchmarkRuntimeAdapter.createBenchmarkRuntimeAdapter,
    "function"
  );
  assert.equal(
    typeof srcFilterSortRuntimeOrchestration.createEngineFilterSortOrchestrator,
    "function"
  );
  assert.equal(
    typeof distFilterSortRuntimeOrchestration.createEngineFilterSortOrchestrator,
    "function"
  );
  assert.equal(
    typeof srcSortRuntimeBridge.createSortRuntimeBridge,
    "function"
  );
  assert.equal(
    typeof distSortRuntimeBridge.createSortRuntimeBridge,
    "function"
  );
  assert.equal(
    typeof srcSortingPrecomputedRuntime.createPrecomputedSortRuntime,
    "function"
  );
  assert.equal(
    typeof distSortingPrecomputedRuntime.createPrecomputedSortRuntime,
    "function"
  );
});

test("sorting orchestrator fallback path works", () => {
  const srcFallbackResult = { sortMode: "native", sortCoreMs: 1, sortMs: 1 };
  const src = srcSortingOrchestration.createSortBenchmarkOrchestrator({
    runFallbackSort: () => srcFallbackResult,
    runPrecomputedSort: () => null,
  });

  const result = src.runPrecomputedSortSnapshotPass([], [], 0);
  assert.equal(result, srcFallbackResult);
});

function createFakeFilterController(matchIndices) {
  const out = {
    _count: 0,
    _indices: null,
    _rows: [],
    setRows(rows) {
      this._rows = Array.isArray(rows) ? rows : [];
    },
    setData(rows) {
      this._rows = Array.isArray(rows) ? rows : [];
    },
    apply(rawFilters) {
      const activeFilterCount = Object.keys(rawFilters || {}).length;
      if (activeFilterCount === 0) {
        this._indices = null;
        this._count = this._rows.length;
        return null;
      }

      const values = Array.isArray(matchIndices) ? matchIndices : [];
      const buffer = new Uint32Array(values.length);
      for (let i = 0; i < values.length; i += 1) {
        buffer[i] = values[i] >>> 0;
      }

      this._indices = { buffer, count: buffer.length };
      this._count = buffer.length;
      return this._indices;
    },
    setCurrentIndices(indices) {
      this._indices = indices;
    },
    getCurrentIndices() {
      return this._indices;
    },
    getCurrentCount() {
      return this._count;
    },
    getData() {
      return this._rows;
    },
  };

  return out;
}

function runFilterBridgeSmoke(factory) {
  const rows = [
    { index: 0, firstName: "A" },
    { index: 1, firstName: "B" },
    { index: 2, firstName: "A" },
  ];
  const objectRowController = createFakeFilterController([0, 2]);
  const objectColumnarController = createFakeFilterController([0, 2]);
  const numericRowController = createFakeFilterController([0, 2]);
  const numericColumnarController = createFakeFilterController([0, 2]);

  const bridge = factory({
    getLoadedRowCount: () => rows.length,
    getCurrentFilterModeKey: () => "object-row",
    getFilterOptions: () => ({ enableCaching: true }),
    getRawFilters: () => ({ firstName: "a" }),
    controllers: {
      objectRow: objectRowController,
      objectColumnar: objectColumnarController,
      numericRow: numericRowController,
      numericColumnar: numericColumnarController,
    },
    dataAccessors: {
      getObjectRows: () => rows,
      getObjectColumnarData: () => ({ rowCount: rows.length, columns: [] }),
      getNumericRows: () => rows,
      getNumericColumnarData: () => ({ rowCount: rows.length, columns: [] }),
    },
    syncAllControllerIndices: true,
  });

  const activeResult = bridge.runFilterPassWithRawFilters({ firstName: "a" });
  assert.equal(activeResult.filteredCount, 2);
  assert.equal(activeResult.modePath, "object-row");
  assert.deepEqual(
    Array.from(activeResult.filteredIndices.buffer.subarray(0, activeResult.filteredIndices.count)),
    [0, 2]
  );

  const passiveResult = bridge.runFilterPassWithRawFilters({});
  assert.equal(passiveResult.filteredCount, rows.length);
  assert.equal(passiveResult.filteredIndices, null);

  assert.equal(bridge.hasData(), true);
  assert.equal(typeof bridge.clearTopLevelFilterCache, "function");
  assert.equal(typeof bridge.clearAllFilterCaches, "function");
}

test("filter runtime bridge smoke works for src and dist", () => {
  runFilterBridgeSmoke(srcFilterRuntimeBridge.createFilterRuntimeBridge);
  runFilterBridgeSmoke(distFilterRuntimeBridge.createFilterRuntimeBridge);
});

function runFilteringRuntimeOrchestratorSmoke(factory) {
  const orchestrator = factory({
    runFilterPassWithRawFilters(rawFilters, options) {
      const active = Object.keys(rawFilters || {}).length > 0;
      const dictUsed =
        options &&
        options.filterOptions &&
        options.filterOptions.useDictionaryKeySearch === true &&
        active;
      return {
        modePath: "numeric-columnar",
        filteredCount: active ? 2 : 3,
        filteredIndices: active
          ? { buffer: new Uint32Array([0, 2]), count: 2 }
          : null,
        coreMs: 1.5,
        active,
        topLevelCacheEvent: { enabled: true, hit: false },
        dictionaryPrefilter: dictUsed
          ? {
              used: true,
              durationMs: 0.5,
              searchMs: 0.1,
              searchFullMs: 0.06,
              searchRefinedMs: 0.04,
              mergeMs: 0.2,
              mergeConcatMs: 0.08,
              mergeSortMs: 0.12,
              intersectionMs: 0.2,
            }
          : null,
        selectedBaseCandidateCount: 3,
      };
    },
    runSortForFilterResult(filterResult) {
      return {
        indices: filterResult.filteredIndices
          ? filterResult.filteredIndices.buffer
          : new Uint32Array([0, 1, 2]),
        sortedCount: filterResult.filteredCount,
        result: {
          durationMs: 0.2,
          sortMode: "precomputed",
          comparatorMode: "precomputed",
          dataPath: "indices+precomputed-full-asc",
          effectiveDescriptors: [{ columnKey: "index", direction: "asc" }],
        },
        sortTotalMs: 0.2,
        sortPrepMs: 0,
        rankBuildMs: 0,
      };
    },
  });

  const run = orchestrator.execute(
    { firstName: "a" },
    {
      filterOptions: { useDictionaryKeySearch: true },
      skipRender: false,
      preferPrecomputedFastPath: true,
    }
  );

  assert.ok(run && typeof run === "object");
  assert.equal(run.active, true);
  assert.equal(run.filteredCount, 2);
  assert.ok(run.renderIndices instanceof Uint32Array);
  assert.equal(run.coreMs, 1.5);
  assert.equal(run.reverseIndexMs, 0.5);
  assert.ok(run.sort && typeof run.sort === "object");
}

test("filtering runtime orchestrator smoke works for src and dist", () => {
  runFilteringRuntimeOrchestratorSmoke(
    srcFilteringRuntimeOrchestration.createFilteringRuntimeOrchestrator
  );
  runFilteringRuntimeOrchestratorSmoke(
    distFilteringRuntimeOrchestration.createFilteringRuntimeOrchestrator
  );
});

function runFilterSortRuntimeOrchestratorSmoke(factory) {
  const engine = {
    executeFilterCore(rawFilters) {
      const active = Object.keys(rawFilters || {}).length > 0;
      return {
        modePath: "numeric-columnar",
        filteredCount: active ? 2 : 3,
        filteredIndices: active
          ? { buffer: new Uint32Array([0, 2]), count: 2 }
          : null,
        coreMs: 1.2,
        active,
        dictionaryPrefilter: active
          ? {
              used: true,
              durationMs: 0.4,
              searchMs: 0.1,
              searchFullMs: 0.06,
              searchRefinedMs: 0.04,
              mergeMs: 0.2,
              mergeConcatMs: 0.08,
              mergeSortMs: 0.12,
              intersectionMs: 0.1,
            }
          : null,
        selectedBaseCandidateCount: 3,
      };
    },
    buildSortRowsSnapshot() {
      return {
        snapshotType: "row-indices-v2",
        rowIndices: new Uint32Array([0, 2]),
        count: 2,
      };
    },
    executeSortCore(rowsSnapshot, descriptors, sortMode) {
      return {
        sortedIndices: rowsSnapshot.rowIndices,
        sortedCount: rowsSnapshot.count,
        sortMode: sortMode || "precomputed",
        descriptors,
        sortCoreMs: 0.2,
        sortPrepMs: 0,
        sortTotalMs: 0.2,
      };
    },
  };

  const orchestrator = factory({
    engine,
    getRowCount: () => 3,
    getRawFilters: () => ({ firstName: "a" }),
    getFilterOptions: () => ({ useDictionaryKeySearch: true }),
    getCurrentFilterModeKey: () => "numeric-columnar",
    getSortDescriptors: () => [{ columnKey: "firstName", direction: "desc" }],
    getSortMode: () => "precomputed",
    syncState: () => {},
  });

  const filterRun = orchestrator.runFilterCore(
    { firstName: "a" },
    { skipRender: false, skipStatus: false }
  );
  assert.ok(filterRun && filterRun.kind === "ok");
  assert.equal(filterRun.orchestration.filteredCount, 2);
  assert.ok(filterRun.orchestration.sort && typeof filterRun.orchestration.sort === "object");

  const sortRun = orchestrator.runSortCore(null, {});
  assert.ok(sortRun && sortRun.kind === "ok");
  assert.ok(ArrayBuffer.isView(sortRun.renderIndices));
  assert.equal(sortRun.sortRun.sortedCount, 2);
}

test("filter/sort runtime orchestrator smoke works for src and dist", () => {
  runFilterSortRuntimeOrchestratorSmoke(
    srcFilterSortRuntimeOrchestration.createEngineFilterSortOrchestrator
  );
  runFilterSortRuntimeOrchestratorSmoke(
    distFilterSortRuntimeOrchestration.createEngineFilterSortOrchestrator
  );
});

function runSortBridgeSmoke(
  runtimeFactory,
  sortBridgeFactory,
  precomputedRuntimeFactory
) {
  const runtime = runtimeFactory();
  runtime.generate(2500);
  const rowCount = runtime.getRowCount();
  const schema = runtime.getSchema();
  const numericColumnarData = runtime.getNumericColumnarForSave();
  const columnTypeByKey = Object.create(null);
  const columnIndexByKey = Object.create(null);

  for (let i = 0; i < schema.columnKeys.length; i += 1) {
    const key = schema.columnKeys[i];
    columnIndexByKey[key] = i;
    columnTypeByKey[key] =
      key === "firstName" || key === "lastName" ? "string" : "number";
  }

  const sortBridge = sortBridgeFactory({
    columnKeys: schema.columnKeys,
    columnIndexByKey,
    columnTypeByKey,
    getSortOptions: () => ({
      useTypedComparator: true,
      useIndexSort: true,
    }),
    getSortMode: () => "precomputed",
    getRowCount: () => rowCount,
    getSchema: () => schema,
    getNumericColumnarData: () => numericColumnarData,
  });
  const prewarmed = sortBridge.prewarmPrecomputedSortState();
  assert.equal(typeof prewarmed, "boolean");

  const snapshot = runtime.buildSortRowsSnapshot({});
  const singleResult = sortBridge.runSortSnapshotPass(
    snapshot,
    [{ columnKey: "firstName", direction: "desc" }],
    "precomputed"
  );
  assert.ok(singleResult && typeof singleResult === "object");
  assert.ok(ArrayBuffer.isView(singleResult.sortedIndices));
  assert.equal(singleResult.sortedCount, rowCount);
  assert.ok(String(singleResult.sortMode).includes("precomputed"));

  const tupleResult = sortBridge.runSortSnapshotPass(
    snapshot,
    [
      { columnKey: "firstName", direction: "desc" },
      { columnKey: "lastName", direction: "desc" },
    ],
    "precomputed"
  );
  assert.ok(tupleResult && typeof tupleResult === "object");
  assert.ok(ArrayBuffer.isView(tupleResult.sortedIndices));
  assert.equal(tupleResult.sortedCount, rowCount);
  assert.ok(String(tupleResult.sortMode).includes("precomputed"));

  const precomputedRuntime = precomputedRuntimeFactory({
    getSchema: () => schema,
    getNumericColumnarData: () => numericColumnarData,
  });
  const precomputedResult = precomputedRuntime.runPrecomputedSortSelection({
    descriptorList: [{ columnKey: "index", direction: "desc" }],
    selectedIndices: snapshot.rowIndices,
    rowCount,
    isFullSelection: true,
  });
  assert.ok(precomputedResult && typeof precomputedResult === "object");
  assert.ok(ArrayBuffer.isView(precomputedResult.sortedIndices));
  assert.equal(precomputedResult.sortedCount, rowCount);
  assert.ok(String(precomputedResult.sortMode).includes("precomputed"));
}

test("sort runtime bridge + shared precomputed runtime smoke works for src and dist", () => {
  runSortBridgeSmoke(
    srcRuntime.createFastTableRuntime,
    srcSortRuntimeBridge.createSortRuntimeBridge,
    srcSortingPrecomputedRuntime.createPrecomputedSortRuntime
  );
  runSortBridgeSmoke(
    distRuntime.createFastTableRuntime,
    distSortRuntimeBridge.createSortRuntimeBridge,
    distSortingPrecomputedRuntime.createPrecomputedSortRuntime
  );
});
