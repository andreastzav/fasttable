import assert from "node:assert/strict";
import { before, test } from "node:test";
import { ensureCoreDistBuilt, assertArrayLikeEqual } from "./helpers.mjs";

let srcNodeWorkers;
let distNodeWorkers;
let workerProtocol;
let distRuntime;

before(async () => {
  ensureCoreDistBuilt();

  srcNodeWorkers = await import(
    "../../packages/core/src/generation-workers-node.js"
  );
  distNodeWorkers = await import(
    "../../packages/core/dist/generation-workers-node.js"
  );
  workerProtocol = await import(
    "../../packages/core/dist/generation-worker-protocol.js"
  );
  distRuntime = await import("../../packages/core/dist/runtime.js");
});

test("node worker generation smoke works for src and dist builds", async () => {
  const srcResult = await srcNodeWorkers.generateRowsWithWorkers({
    rowCount: 2500,
    workerCount: 2,
    chunkSize: 500,
  });
  assert.equal(srcResult.derivedData.numericColumnarData.rowCount, 2500);
  assert.equal(srcResult.derivedData.objectColumnarData.rowCount, 2500);
  assert.equal(srcResult.completedChunks, srcResult.totalChunks);
  assert.ok(srcResult.wallMs >= 0);

  const distResult = await distNodeWorkers.generateRowsWithWorkers({
    rowCount: 2500,
    workerCount: 2,
    chunkSize: 500,
  });
  assert.equal(distResult.derivedData.numericColumnarData.rowCount, 2500);
  assert.equal(distResult.derivedData.objectColumnarData.rowCount, 2500);
  assert.equal(distResult.completedChunks, distResult.totalChunks);
  assert.ok(distResult.wallMs >= 0);
});

test("node worker sort precompute returns valid sorted columns", async () => {
  const generated = await distNodeWorkers.generateRowsWithWorkers({
    rowCount: 3000,
    workerCount: 2,
    chunkSize: 750,
  });
  const numeric = generated.derivedData.numericColumnarData;
  const sortPrecompute = await distNodeWorkers.buildSortedIndicesWithWorkers({
    numericColumnarData: numeric,
    workerCount: 2,
  });

  assert.equal(sortPrecompute.completedColumns, sortPrecompute.totalColumns);
  assert.ok(sortPrecompute.durationMs >= 0);
  assert.ok(Array.isArray(sortPrecompute.sortedIndexColumns));
  assert.ok(sortPrecompute.sortedIndexByKey);
  assert.ok(sortPrecompute.sortedIndexByKey.index instanceof Uint32Array);

  const expectedIndexSort = workerProtocol.buildSortedIndexColumn(
    numeric.columns[0],
    numeric.columnKinds[0]
  );
  assertArrayLikeEqual(
    sortPrecompute.sortedIndexColumns[0],
    expectedIndexSort,
    "precomputed index column should match deterministic sort helper"
  );
});

test("runtime sort snapshot path consumes precomputed worker ranks", async () => {
  const generated = await distNodeWorkers.generateRowsWithWorkers({
    rowCount: 4000,
    workerCount: 2,
    chunkSize: 1000,
  });
  const numeric = generated.derivedData.numericColumnarData;
  const sortPrecompute = await distNodeWorkers.buildSortedIndicesWithWorkers({
    numericColumnarData: numeric,
    workerCount: 2,
  });

  numeric.sortedIndexColumns = sortPrecompute.sortedIndexColumns;
  numeric.sortedIndexByKey = sortPrecompute.sortedIndexByKey;

  const runtime = distRuntime.createFastTableRuntime();
  runtime.setDataFromNumericColumnar(numeric);
  runtime.setSortOptions({
    useTypedComparator: true,
    useIndexSort: true,
  });

  const snapshot = runtime.buildSortRowsSnapshot({});
  const sortResult = runtime.runSortSnapshotPass(
    snapshot,
    [{ columnKey: "index", direction: "asc" }],
    "native"
  );

  assert.ok(sortResult);
  assert.equal(sortResult.dataPath, "indices+ranks");
  assert.equal(sortResult.comparatorMode, "rank");
});
