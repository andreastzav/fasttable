import assert from "node:assert/strict";
import { before, test } from "node:test";
import { ensureCoreDistBuilt } from "./helpers.mjs";

let srcRuntime;
let distRuntime;
let benchmark;
let sortingApi;

function hasLine(lines, pattern) {
  const source = Array.isArray(lines) ? lines : [];
  return source.some((line) => String(line).includes(pattern));
}

before(async () => {
  ensureCoreDistBuilt();

  srcRuntime = await import("../../packages/core/src/runtime.js");
  distRuntime = await import("../../packages/core/dist/runtime.js");
  benchmark = await import("../../packages/core/src/benchmark.js");
  sortingApi = await import("../../packages/core/src/sorting.js");
});

test("benchmark smoke works against src runtime", async () => {
  const runtime = srcRuntime.createFastTableRuntime();
  runtime.generate(5000);

  const availableSortModes = sortingApi.getAvailableSortModes();
  assert.deepEqual(runtime.getSortModes(), availableSortModes);
  assert.ok(runtime.getSortModes().includes("native"));
  assert.equal(runtime.getSortModes().includes("timsort"), false);

  const filtering = await benchmark.runFilteringBenchmark({
    api: runtime,
    currentOnly: true,
    rounds: 1,
  });
  assert.equal(filtering.error, null);
  assert.ok(hasLine(filtering.lines, "Benchmark started"));
  assert.ok(hasLine(filtering.lines, "Benchmark finished"));

  const sorting = await benchmark.runSortBenchmark({
    api: runtime,
    currentOnly: true,
    rounds: 1,
  });
  assert.equal(sorting.error, null);
  assert.ok(hasLine(sorting.lines, "Sort benchmark started"));
  assert.ok(hasLine(sorting.lines, "Sort benchmark finished"));

  const allSortModes = await benchmark.runSortBenchmark({
    api: runtime,
    currentOnly: false,
    rounds: 1,
  });
  assert.equal(allSortModes.error, null);
  assert.ok(hasLine(allSortModes.lines, "Sort benchmark finished"));
});

test("benchmark smoke works against dist runtime", async () => {
  const runtime = distRuntime.createFastTableRuntime();
  runtime.generate(4000);

  const filtering = await benchmark.runFilteringBenchmark({
    api: runtime,
    currentOnly: true,
    rounds: 1,
  });
  assert.equal(filtering.error, null);
  assert.ok(hasLine(filtering.lines, "Benchmark started"));
  assert.ok(hasLine(filtering.lines, "Benchmark finished"));
});
