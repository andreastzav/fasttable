import assert from "node:assert/strict";
import { before, test } from "node:test";
import { ensureCoreDistBuilt } from "./helpers.mjs";

let srcFilteringOrchestration;
let distFilteringOrchestration;
let srcSortingOrchestration;
let distSortingOrchestration;

before(async () => {
  ensureCoreDistBuilt();

  srcFilteringOrchestration = await import(
    "../../packages/core/src/filtering-orchestration.js"
  );
  distFilteringOrchestration = await import(
    "../../packages/core/dist/filtering-orchestration.js"
  );
  srcSortingOrchestration = await import(
    "../../packages/core/src/sorting-orchestration.js"
  );
  distSortingOrchestration = await import(
    "../../packages/core/dist/sorting-orchestration.js"
  );
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
    typeof srcSortingOrchestration.createSortBenchmarkOrchestrator,
    "function"
  );
  assert.equal(
    typeof distSortingOrchestration.createSortBenchmarkOrchestrator,
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

