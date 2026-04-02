import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { before, test } from "node:test";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { ensureCoreDistBuilt } from "./helpers.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..", "..");
const runtimeCliPath = path.join(repoRoot, "runtime-cli.mjs");

let srcEngine;
let benchmark;
let benchmarkRuntimeAdapter;

before(async () => {
  ensureCoreDistBuilt();
  srcEngine = await import("../../packages/core/src/engine.js");
  benchmark = await import("../../packages/core/src/benchmark.js");
  benchmarkRuntimeAdapter = await import(
    "../../packages/core/src/benchmark-runtime-adapter.js"
  );
});

test("engine core execution is runtime-backed and does not require adapter core methods", () => {
  const calls = {
    runtimeFilter: 0,
    runtimeSingleFilter: 0,
    runtimeSnapshot: 0,
    runtimeSort: 0,
    runtimeRestore: 0,
    adapterFilter: 0,
    adapterSort: 0,
  };

  const runtime = {
    hasData() {
      return true;
    },
    getRowCount() {
      return 10;
    },
    getModeOptions() {
      return {
        useColumnarData: true,
        useBinaryColumnar: true,
        useNumericData: false,
        enableCaching: true,
      };
    },
    getRawFilters() {
      return {};
    },
    getSortMode() {
      return "precomputed";
    },
    getSortOptions() {
      return { useTypedComparator: true, useIndexSort: true };
    },
    runFilterPassWithRawFilters(rawFilters) {
      calls.runtimeFilter += 1;
      return {
        modePath: "numeric-columnar",
        filteredCount: Object.keys(rawFilters || {}).length > 0 ? 4 : 10,
        filteredIndices: null,
        coreMs: 1.25,
        active: Object.keys(rawFilters || {}).length > 0,
      };
    },
    runSingleFilterPass() {
      calls.runtimeSingleFilter += 1;
      return {
        modePath: "numeric-columnar",
        filteredCount: 3,
        filteredIndices: null,
        coreMs: 0.9,
        active: true,
      };
    },
    buildSortRowsSnapshot() {
      calls.runtimeSnapshot += 1;
      return {
        snapshotType: "row-indices-v2",
        rowIndices: new Uint32Array([0, 1, 2, 3]),
        count: 4,
      };
    },
    runSortSnapshotPass(rowsSnapshot, descriptors, sortMode) {
      calls.runtimeSort += 1;
      return {
        sortedIndices: rowsSnapshot.rowIndices,
        sortedCount: rowsSnapshot.count,
        sortMode,
        descriptors,
        sortCoreMs: 0.4,
        sortPrepMs: 0.1,
        sortTotalMs: 0.5,
      };
    },
    setModeOptions() {
      calls.runtimeRestore += 1;
    },
    setRawFilters() {
      calls.runtimeRestore += 1;
    },
    setSingleFilter() {
      calls.runtimeRestore += 1;
    },
    clearFilters() {
      calls.runtimeRestore += 1;
      return {};
    },
    setSortOptions() {
      calls.runtimeRestore += 1;
    },
    setSortMode() {
      calls.runtimeRestore += 1;
      return "native";
    },
    prewarmPrecomputedSortState() {
      return true;
    },
    getSortModes() {
      return ["native", "precomputed"];
    },
  };

  const engine = srcEngine.createFastTableEngine({
    runtime,
    adapters: {
      getRowCount: () => runtime.getRowCount(),
      getModeOptions: () => runtime.getModeOptions(),
      getRawFilters: () => runtime.getRawFilters(),
      getSortMode: () => runtime.getSortMode(),
      getSortOptions: () => runtime.getSortOptions(),
      runFilterPassWithRawFilters() {
        calls.adapterFilter += 1;
        throw new Error("Adapter filter core path should not be called.");
      },
      runSortSnapshotPass() {
        calls.adapterSort += 1;
        throw new Error("Adapter sort core path should not be called.");
      },
    },
  });

  const filterResult = engine.applyFilters({
    rawFilters: { firstName: "a" },
  });
  assert.equal(filterResult.filteredCount, 4);

  const singleFilterResult = engine.applySingleFilter("firstName", "a", {});
  assert.equal(singleFilterResult.filteredCount, 3);

  const sortResult = engine.applySort({
    descriptors: [{ columnKey: "index", direction: "asc" }],
    sortMode: "precomputed",
  });
  assert.equal(sortResult.sortedCount, 4);

  engine.restoreStateCore({
    modeOptions: { enableCaching: false },
    rawFilters: {},
    sortOptions: { useTypedComparator: false, useIndexSort: true },
    sortMode: "native",
  });

  assert.ok(calls.runtimeFilter > 0);
  assert.ok(calls.runtimeSingleFilter > 0);
  assert.ok(calls.runtimeSnapshot > 0);
  assert.ok(calls.runtimeSort > 0);
  assert.ok(calls.runtimeRestore >= 4);
  assert.equal(calls.adapterFilter, 0);
  assert.equal(calls.adapterSort, 0);
});

test("engine state authority is runtime-owned even when adapters disagree", () => {
  const runtimeState = {
    rowCount: 123,
    modeOptions: {
      useColumnarData: true,
      useBinaryColumnar: true,
      useNumericData: false,
      enableCaching: true,
    },
    rawFilters: {
      index: "7",
    },
    sortMode: "precomputed",
    sortOptions: {
      useTypedComparator: true,
      useIndexSort: true,
    },
  };

  const runtimeCalls = {
    setModeOptions: 0,
    setRawFilters: 0,
    setSortOptions: 0,
    setSortMode: 0,
  };

  const runtime = {
    hasData() {
      return runtimeState.rowCount > 0;
    },
    getRowCount() {
      return runtimeState.rowCount;
    },
    getModeOptions() {
      return { ...runtimeState.modeOptions };
    },
    setModeOptions(nextOptions) {
      runtimeCalls.setModeOptions += 1;
      runtimeState.modeOptions = {
        ...runtimeState.modeOptions,
        ...(nextOptions || {}),
      };
      return { ...runtimeState.modeOptions };
    },
    getRawFilters() {
      return { ...runtimeState.rawFilters };
    },
    setRawFilters(nextRawFilters) {
      runtimeCalls.setRawFilters += 1;
      runtimeState.rawFilters = { ...(nextRawFilters || {}) };
      return { ...runtimeState.rawFilters };
    },
    setSingleFilter(columnKey, value) {
      const next = {};
      if (
        typeof columnKey === "string" &&
        columnKey !== "" &&
        String(value ?? "").trim() !== ""
      ) {
        next[columnKey] = String(value);
      }
      runtimeState.rawFilters = next;
      return { ...runtimeState.rawFilters };
    },
    clearFilters() {
      runtimeState.rawFilters = {};
      return {};
    },
    runFilterPassWithRawFilters(rawFilters) {
      return {
        modePath: "numeric-columnar",
        filteredCount:
          rawFilters && Object.keys(rawFilters).length > 0
            ? runtimeState.rowCount - 1
            : runtimeState.rowCount,
        filteredIndices: null,
        coreMs: 1,
        active: !!(rawFilters && Object.keys(rawFilters).length > 0),
      };
    },
    runSingleFilterPass(columnKey, value) {
      return this.runFilterPassWithRawFilters(
        columnKey ? { [columnKey]: String(value ?? "") } : {}
      );
    },
    getSortModes() {
      return ["native", "precomputed"];
    },
    getSortMode() {
      return runtimeState.sortMode;
    },
    setSortMode(nextSortMode) {
      runtimeCalls.setSortMode += 1;
      runtimeState.sortMode = String(nextSortMode || "native");
      return runtimeState.sortMode;
    },
    getSortOptions() {
      return { ...runtimeState.sortOptions };
    },
    setSortOptions(nextSortOptions) {
      runtimeCalls.setSortOptions += 1;
      runtimeState.sortOptions = {
        ...runtimeState.sortOptions,
        ...(nextSortOptions || {}),
      };
      return { ...runtimeState.sortOptions };
    },
    buildSortRowsSnapshot() {
      return {
        snapshotType: "row-indices-v2",
        rowIndices: new Uint32Array([0, 1, 2]),
        count: 3,
      };
    },
    runSortSnapshotPass(rowsSnapshot, descriptors, sortMode) {
      return {
        sortedIndices: rowsSnapshot.rowIndices,
        sortedCount: rowsSnapshot.count,
        sortMode:
          typeof sortMode === "string" && sortMode !== ""
            ? sortMode
            : runtimeState.sortMode,
        descriptors: Array.isArray(descriptors) ? descriptors : [],
        sortCoreMs: 0.2,
        sortPrepMs: 0.1,
        sortTotalMs: 0.3,
      };
    },
    prewarmPrecomputedSortState() {
      return true;
    },
  };

  const engine = srcEngine.createFastTableEngine({
    runtime,
    adapters: {
      // Intentionally disagree with runtime to verify runtime ownership.
      getRowCount: () => 999999,
      getModeOptions: () => ({ useColumnarData: false }),
      getRawFilters: () => ({ index: "adapter" }),
      getSortMode: () => "native",
      getSortOptions: () => ({ useTypedComparator: false, useIndexSort: false }),
    },
  });

  const state = engine.getState();
  assert.equal(state.rowCount, runtimeState.rowCount);
  assert.deepEqual(state.modeOptions, runtimeState.modeOptions);
  assert.deepEqual(state.rawFilters, runtimeState.rawFilters);
  assert.equal(state.sortMode, runtimeState.sortMode);
  assert.deepEqual(state.sortOptions, runtimeState.sortOptions);

  const benchmarkApi = engine.createBenchmarkApi();
  assert.equal(benchmarkApi.getRowCount(), runtimeState.rowCount);
  assert.deepEqual(benchmarkApi.getRawFilters(), runtimeState.rawFilters);
  assert.equal(benchmarkApi.getSortMode(), runtimeState.sortMode);

  benchmarkApi.setModeOptions({ enableCaching: false }, { suppressFilterPass: true });
  benchmarkApi.setRawFilters({ firstName: "and" });
  benchmarkApi.setSortOptions({ useTypedComparator: false });
  benchmarkApi.restoreStateCore({ sortMode: "native" });

  assert.ok(runtimeCalls.setModeOptions >= 1);
  assert.ok(runtimeCalls.setRawFilters >= 1);
  assert.ok(runtimeCalls.setSortOptions >= 1);
  assert.ok(runtimeCalls.setSortMode >= 1);
});

test("browser normal sort path uses engine snapshot builder (no app-local snapshot materializer)", () => {
  const appSource = fs.readFileSync(path.join(repoRoot, "app.js"), "utf8");
  const orchestrationSource = fs.readFileSync(
    path.join(repoRoot, "packages/core/src/filter-sort-runtime-orchestration.js"),
    "utf8"
  );
  const runtimeOperationsSource = fs.readFileSync(
    path.join(repoRoot, "packages/core/src/runtime-operations.js"),
    "utf8"
  );
  assert.ok(
    appSource.includes("fastTableEngine.createRuntimeOperations("),
    "app should use engine-owned runtime operations API."
  );
  assert.ok(
    runtimeOperationsSource.includes("createEngineFilterSortOrchestrator"),
    "runtime operations should delegate to shared filter/sort runtime orchestrator."
  );
  assert.ok(
    orchestrationSource.includes("engine.buildSortRowsSnapshot("),
    "shared orchestrator should build snapshots via engine runtime path."
  );
  assert.ok(
    orchestrationSource.includes("engine.executeSortCore("),
    "shared orchestrator should execute sort through engine core."
  );
  assert.ok(
    !appSource.includes("function buildSortSnapshotFromFilterResult("),
    "legacy app-local sort snapshot builder should be removed from normal path."
  );
});

test("browser runtime state sync writes through engine only", () => {
  const appSource = fs.readFileSync(path.join(repoRoot, "app.js"), "utf8");
  assert.ok(
    appSource.includes("fastTableEngine.restoreStateCore(statePatch);"),
    "app should synchronize runtime state through engine.restoreStateCore."
  );
  assert.ok(
    !appSource.includes("browserCoreRuntime.setModeOptions("),
    "app should not write mode options directly to runtime."
  );
  assert.ok(
    !appSource.includes("browserCoreRuntime.setRawFilters("),
    "app should not write raw filters directly to runtime."
  );
  assert.ok(
    !appSource.includes("browserCoreRuntime.setSortOptions("),
    "app should not write sort options directly to runtime."
  );
  assert.ok(
    !appSource.includes("browserCoreRuntime.setSortMode("),
    "app should not write sort mode directly to runtime."
  );
  assert.ok(
    !appSource.includes("browserCoreRuntime."),
    "app should avoid direct runtime method calls and use engine-owned APIs."
  );
});

test("browser filter->sort handoff uses the same raw filter map per action", () => {
  const orchestrationSource = fs.readFileSync(
    path.join(repoRoot, "packages/core/src/filter-sort-runtime-orchestration.js"),
    "utf8"
  );
  assert.ok(
    orchestrationSource.includes("runFilterCore(sourceRawFilters, {"),
    "shared runSortCore should reuse sourceRawFilters when invoking filter core."
  );
  assert.ok(
    orchestrationSource.includes("runForRawFilters(\n      sourceRawFilters,"),
    "shared runSortCore should pass sourceRawFilters into sort execution."
  );
});

test("runtime one-shot CLI sort path uses shared core orchestrator", () => {
  const runtimeCliSource = fs.readFileSync(runtimeCliPath, "utf8");
  assert.ok(
    runtimeCliSource.includes("engine.createRuntimeOperations("),
    "runtime-cli should use engine-owned runtime operations API."
  );
  assert.ok(
    runtimeCliSource.includes("runSortCore("),
    "runtime-cli sort should run through shared runtime operations core path."
  );
  assert.ok(
    !runtimeCliSource.includes("function materializeFilteredIndices("),
    "runtime-cli should not keep local filtered-index materialization detour."
  );
  assert.ok(
    !runtimeCliSource.includes("createBenchmarkApi().getSortModes"),
    "runtime-cli should not depend on benchmark API for sort-mode discovery."
  );
  assert.ok(
    !runtimeCliSource.includes("createBenchmarkApi().getSortMode"),
    "runtime-cli should read current sort mode from engine core API."
  );
});

test("benchmark restore uses restoreStateCore and does not run legacy filter restore pass", async () => {
  const filterApiCalls = {
    setRawFilters: 0,
    runFilterPass: 0,
    restoreStateCore: 0,
  };
  const filterApi = {
    hasData: () => true,
    getRowCount: () => 100,
    getModeOptions: () => ({
      useColumnarData: true,
      useBinaryColumnar: true,
      useNumericData: false,
      enableCaching: true,
      useDictionaryKeySearch: true,
      useDictionaryIntersection: true,
      useSmarterPlanner: true,
      useSmartFiltering: false,
      useFilterCache: false,
    }),
    setModeOptions: () => ({}),
    getRawFilters: () => ({}),
    setRawFilters: () => {
      filterApiCalls.setRawFilters += 1;
    },
    runFilterPass: () => {
      filterApiCalls.runFilterPass += 1;
      return null;
    },
    runSingleFilterPass: () => ({
      coreMs: 1,
    }),
    restoreStateCore: () => {
      filterApiCalls.restoreStateCore += 1;
      return null;
    },
  };

  const filtering = await benchmark.runFilteringBenchmark({
    api: filterApi,
    currentOnly: true,
    rounds: 1,
    benchmarkCases: [
      {
        key: "index",
        label: "Index",
        values: ["1"],
      },
    ],
  });

  assert.equal(filtering.error, null);
  assert.equal(filterApiCalls.restoreStateCore, 1);
  assert.equal(filterApiCalls.setRawFilters, 0);
  assert.equal(filterApiCalls.runFilterPass, 0);

  const sortApiCalls = {
    setSortOptions: 0,
    restoreStateCore: 0,
  };
  const sortApi = {
    hasData: () => true,
    getRowCount: () => 100,
    buildSortRowsSnapshot: () => ({
      snapshotType: "row-indices-v2",
      rowIndices: new Uint32Array([0, 1, 2]),
      count: 3,
    }),
    runSortSnapshotPass: () => ({
      sortCoreMs: 0.5,
      sortTotalMs: 0.6,
      sortMode: "precomputed",
      sortedCount: 3,
      sortedIndices: new Uint32Array([2, 1, 0]),
    }),
    getSortMode: () => "precomputed",
    getSortModes: () => ["precomputed"],
    getSortOptions: () => ({ useTypedComparator: true, useIndexSort: true }),
    setSortOptions: () => {
      sortApiCalls.setSortOptions += 1;
    },
    restoreStateCore: () => {
      sortApiCalls.restoreStateCore += 1;
      return null;
    },
  };

  const sorting = await benchmark.runSortBenchmark({
    api: sortApi,
    currentOnly: true,
    rounds: 1,
    sortCases: [{ key: "index", label: "Index" }],
  });

  assert.equal(sorting.error, null);
  assert.equal(sortApiCalls.restoreStateCore, 1);
  assert.equal(sortApiCalls.setSortOptions, 0);
});

test("benchmark tick default parity remains micro in browser wrapper and CLI", () => {
  const benchmarkUiSource = fs.readFileSync(
    path.join(repoRoot, "benchmark-ui-browser.js"),
    "utf8"
  );
  const benchCliSource = fs.readFileSync(
    path.join(repoRoot, "benchmark-cli.mjs"),
    "utf8"
  );

  assert.ok(
    benchmarkUiSource.includes('const fallbackPolicy = "micro";'),
    "benchmark-ui should default fallback tick policy to micro."
  );
  assert.ok(
    benchCliSource.includes('tickPolicy: "micro"'),
    "benchmark-cli should default tick policy argument to micro."
  );
  assert.ok(
    benchCliSource.includes('resolveBenchmarkTickPolicy(args.tickPolicy, "micro")'),
    "benchmark-cli should resolve invalid tick policy back to micro."
  );
});

test("benchmark runtime adapter requires strict restore contract", () => {
  assert.throws(
    () =>
      benchmarkRuntimeAdapter.createBenchmarkRuntimeAdapter({
        api: {
          hasData: () => true,
        },
      }),
    /requires api\.restoreStateCore or hooks\.restoreState/
  );

  const calls = {
    hookRestore: 0,
    apiRestore: 0,
  };
  const adapterWithHook = benchmarkRuntimeAdapter.createBenchmarkRuntimeAdapter({
    api: {
      hasData: () => true,
      restoreStateCore: () => {
        calls.apiRestore += 1;
      },
    },
    hooks: {
      restoreState: () => {
        calls.hookRestore += 1;
      },
    },
  });
  adapterWithHook.restoreStateCore({ rawFilters: { index: "1" } });
  assert.equal(calls.hookRestore, 1);
  assert.equal(calls.apiRestore, 0);

  const adapterSortFallback = benchmarkRuntimeAdapter.createBenchmarkRuntimeAdapter({
    api: {
      hasData: () => true,
      restoreStateCore: () => null,
      runSortSnapshotPass: () => ({ sortCoreMs: 1, sortTotalMs: 1 }),
    },
  });
  const fallbackSortResult = adapterSortFallback.executeSortCore(
    { snapshotType: "row-indices-v2", rowIndices: new Uint32Array([0]), count: 1 },
    [{ columnKey: "index", direction: "asc" }],
    "native"
  );
  assert.equal(typeof fallbackSortResult.sortCoreMs, "number");

  const adapterSortDirect = benchmarkRuntimeAdapter.createBenchmarkRuntimeAdapter({
    api: {
      hasData: () => true,
      restoreStateCore: () => null,
      executeSortCore: () => ({ sortCoreMs: 2, sortTotalMs: 2 }),
    },
  });
  const directSortResult = adapterSortDirect.executeSortCore(
    { snapshotType: "row-indices-v2", rowIndices: new Uint32Array([0]), count: 1 },
    [{ columnKey: "index", direction: "asc" }],
    "native"
  );
  assert.equal(directSortResult.sortCoreMs, 2);
});

function runRuntimeCli(args) {
  return spawnSync(process.execPath, [runtimeCliPath, ...args], {
    cwd: repoRoot,
    encoding: "utf8",
    timeout: 120000,
  });
}

function parseJsonStdout(result) {
  const output = String(result.stdout || "").trim();
  if (output === "") {
    return null;
  }
  return JSON.parse(output);
}

test("runtime one-shot CLI smoke: filter/sort/filter-sort succeed and invalid input exits non-zero", (t) => {
  const baseArgs = [
    "--generate-workers",
    "2000",
    "--workers",
    "1",
    "--chunk-size",
    "500",
    "--json",
  ];

  const filterRun = runRuntimeCli([
    "--op",
    "filter",
    "--filters",
    "firstName=andr",
    ...baseArgs,
  ]);
  if (filterRun.error && filterRun.error.code === "EPERM") {
    t.skip("child_process spawn is blocked in this environment (EPERM).");
    return;
  }
  assert.equal(filterRun.status, 0, filterRun.stderr);
  const filterPayload = parseJsonStdout(filterRun);
  assert.equal(filterPayload.operation, "filter");
  assert.equal(typeof filterPayload.filter.filteredCount, "number");
  assert.equal(typeof filterPayload.filter.coreMs, "number");

  const sortRun = runRuntimeCli([
    "--op",
    "sort",
    "--sort",
    "index:asc",
    "--sort-mode",
    "precomputed",
    ...baseArgs,
  ]);
  assert.equal(sortRun.status, 0, sortRun.stderr);
  const sortPayload = parseJsonStdout(sortRun);
  assert.equal(sortPayload.operation, "sort");
  assert.equal(sortPayload.sort.sortMode, "precomputed");
  assert.equal(typeof sortPayload.sort.sortCoreMs, "number");

  const filterSortRun = runRuntimeCli([
    "--op",
    "filter-sort",
    "--filters",
    "firstName=andr",
    "--sort",
    "firstName:desc,lastName:asc",
    "--sort-mode",
    "precomputed",
    ...baseArgs,
  ]);
  assert.equal(filterSortRun.status, 0, filterSortRun.stderr);
  const filterSortPayload = parseJsonStdout(filterSortRun);
  assert.equal(filterSortPayload.operation, "filter-sort");
  assert.equal(typeof filterSortPayload.filter.filteredCount, "number");
  assert.equal(typeof filterSortPayload.sort.sortTotalMs, "number");

  const invalidRun = runRuntimeCli([
    "--op",
    "sort",
    "--sort",
    "index",
    ...baseArgs,
  ]);
  assert.notEqual(invalidRun.status, 0);
  assert.ok(String(invalidRun.stderr || "").includes("CLI failed"));
});
