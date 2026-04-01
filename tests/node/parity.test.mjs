import assert from "node:assert/strict";
import { before, test } from "node:test";
import {
  ensureCoreDistBuilt,
  createDeterministicRows,
  cloneRows,
  indexResultToArray,
  assertArrayLikeEqual,
  buildColumnTypeByKey,
  applySortDescriptors,
  buildPrecomputedSortKeyColumns,
} from "./helpers.mjs";

let srcGeneration;
let distGeneration;
let srcFiltering;
let distFiltering;
let srcSorting;
let distSorting;

before(async () => {
  ensureCoreDistBuilt();

  srcGeneration = await import("../../packages/core/src/generation.js");
  distGeneration = await import("../../packages/core/dist/generation.js");
  srcFiltering = await import("../../packages/core/src/filtering.js");
  distFiltering = await import("../../packages/core/dist/filtering.js");
  srcSorting = await import("../../packages/core/src/sorting.js");
  distSorting = await import("../../packages/core/dist/sorting.js");
});

test("generation derivation parity between src and dist", () => {
  const rows = createDeterministicRows(512);
  const rowClonesA = cloneRows(rows);
  const rowClonesB = cloneRows(rows);

  const srcObjectColumnar = srcGeneration.deriveColumnarDataFromRows(rowClonesA);
  const distObjectColumnar = distGeneration.deriveColumnarDataFromRows(rowClonesB);

  assert.equal(srcObjectColumnar.rowCount, distObjectColumnar.rowCount);
  for (let i = 0; i < srcGeneration.COLUMN_KEYS.length; i += 1) {
    const key = srcGeneration.COLUMN_KEYS[i];
    assertArrayLikeEqual(
      srcObjectColumnar.columns[key],
      distObjectColumnar.columns[key],
      `column mismatch for ${key}`
    );
    assertArrayLikeEqual(
      srcObjectColumnar.columns[`${key}Cache`],
      distObjectColumnar.columns[`${key}Cache`],
      `cache column mismatch for ${key}`
    );
  }

  const srcNumericRows = srcGeneration.deriveNumericRowsFromRows(cloneRows(rows));
  const distNumericRows = distGeneration.deriveNumericRowsFromRows(cloneRows(rows));
  assert.equal(srcNumericRows.rowCount, distNumericRows.rowCount);
  assert.deepEqual(srcNumericRows.rows, distNumericRows.rows);

  const srcNumericColumnar = srcGeneration.deriveNumericColumnarDataFromNumericRows(
    srcNumericRows.rows
  );
  const distNumericColumnar =
    distGeneration.deriveNumericColumnarDataFromNumericRows(distNumericRows.rows);

  assert.equal(srcNumericColumnar.rowCount, distNumericColumnar.rowCount);
  assert.deepEqual(srcNumericColumnar.columnKinds, distNumericColumnar.columnKinds);
  assert.equal(srcNumericColumnar.columns.length, distNumericColumnar.columns.length);
  for (let i = 0; i < srcNumericColumnar.columns.length; i += 1) {
    assertArrayLikeEqual(
      srcNumericColumnar.columns[i],
      distNumericColumnar.columns[i],
      `numeric column mismatch at ${i}`
    );
  }
  assert.deepEqual(srcNumericColumnar.dictionaries, distNumericColumnar.dictionaries);
  assert.deepEqual(
    srcNumericColumnar.lowerDictionaryValues,
    distNumericColumnar.lowerDictionaryValues
  );
});

test("filtering parity between src and dist controllers", () => {
  const rows = createDeterministicRows(3000);
  const rawFilters = {
    firstName: "a",
    lastName: "o",
    column5: "1",
  };

  const srcObjectRowCtrl = srcFiltering.createRowFilterController(cloneRows(rows));
  const distObjectRowCtrl = distFiltering.createRowFilterController(cloneRows(rows));
  const rowOptions = { enableCaching: true, useSmarterPlanner: true };

  const srcRowIndices = indexResultToArray(
    srcObjectRowCtrl.apply(rawFilters, rowOptions),
    rows.length
  );
  const distRowIndices = indexResultToArray(
    distObjectRowCtrl.apply(rawFilters, rowOptions),
    rows.length
  );
  assert.deepEqual(srcRowIndices, distRowIndices);

  const srcObjectColumnar =
    srcGeneration.deriveColumnarDataFromRows(cloneRows(rows));
  const distObjectColumnar =
    distGeneration.deriveColumnarDataFromRows(cloneRows(rows));

  const srcObjectColumnarCtrl =
    srcFiltering.createColumnarFilterController(srcObjectColumnar);
  const distObjectColumnarCtrl =
    distFiltering.createColumnarFilterController(distObjectColumnar);

  const srcColumnarIndices = indexResultToArray(
    srcObjectColumnarCtrl.apply(rawFilters, rowOptions),
    rows.length
  );
  const distColumnarIndices = indexResultToArray(
    distObjectColumnarCtrl.apply(rawFilters, rowOptions),
    rows.length
  );
  assert.deepEqual(srcColumnarIndices, distColumnarIndices);

  const srcNumericRows = srcGeneration.deriveNumericRowsFromRows(cloneRows(rows));
  const distNumericRows = distGeneration.deriveNumericRowsFromRows(cloneRows(rows));

  const srcNumericRowCtrl = srcFiltering.createNumericRowFilterController(
    srcNumericRows.rows,
    {
      keyToIndex: srcGeneration.COLUMN_INDEX_BY_KEY,
      baseColumnCount: srcGeneration.BASE_COLUMN_COUNT,
      cacheOffset: srcGeneration.NUMERIC_CACHE_OFFSET,
    }
  );
  const distNumericRowCtrl = distFiltering.createNumericRowFilterController(
    distNumericRows.rows,
    {
      keyToIndex: distGeneration.COLUMN_INDEX_BY_KEY,
      baseColumnCount: distGeneration.BASE_COLUMN_COUNT,
      cacheOffset: distGeneration.NUMERIC_CACHE_OFFSET,
    }
  );

  const srcNumericRowIndices = indexResultToArray(
    srcNumericRowCtrl.apply(rawFilters, rowOptions),
    rows.length
  );
  const distNumericRowIndices = indexResultToArray(
    distNumericRowCtrl.apply(rawFilters, rowOptions),
    rows.length
  );
  assert.deepEqual(srcNumericRowIndices, distNumericRowIndices);

  const srcNumericColumnar =
    srcGeneration.deriveNumericColumnarDataFromNumericRows(srcNumericRows.rows);
  const distNumericColumnar =
    distGeneration.deriveNumericColumnarDataFromNumericRows(distNumericRows.rows);

  const srcNumericColumnarCtrl =
    srcFiltering.createNumericColumnarFilterController(srcNumericColumnar, {
      keyToIndex: srcGeneration.COLUMN_INDEX_BY_KEY,
      baseColumnCount: srcGeneration.BASE_COLUMN_COUNT,
      cacheOffset: srcGeneration.NUMERIC_CACHE_OFFSET,
    });
  const distNumericColumnarCtrl =
    distFiltering.createNumericColumnarFilterController(distNumericColumnar, {
      keyToIndex: distGeneration.COLUMN_INDEX_BY_KEY,
      baseColumnCount: distGeneration.BASE_COLUMN_COUNT,
      cacheOffset: distGeneration.NUMERIC_CACHE_OFFSET,
    });

  const numericColumnarOptions = {
    enableCaching: true,
    useDictionaryKeySearch: true,
    useDictionaryIntersection: true,
    useSmarterPlanner: true,
  };
  const srcNumericColumnarIndices = indexResultToArray(
    srcNumericColumnarCtrl.apply(rawFilters, numericColumnarOptions),
    rows.length
  );
  const distNumericColumnarIndices = indexResultToArray(
    distNumericColumnarCtrl.apply(rawFilters, numericColumnarOptions),
    rows.length
  );
  assert.deepEqual(srcNumericColumnarIndices, distNumericColumnarIndices);

  const srcPrefilter = srcFiltering.buildDictionaryKeySearchPrefilter(
    rawFilters,
    srcNumericColumnar,
    {
      useDictionaryKeySearch: true,
      useDictionaryIntersection: true,
      useSmarterPlanner: true,
      keyToIndex: srcGeneration.COLUMN_INDEX_BY_KEY,
    }
  );
  const distPrefilter = distFiltering.buildDictionaryKeySearchPrefilter(
    rawFilters,
    distNumericColumnar,
    {
      useDictionaryKeySearch: true,
      useDictionaryIntersection: true,
      useSmarterPlanner: true,
      keyToIndex: distGeneration.COLUMN_INDEX_BY_KEY,
    }
  );

  assert.equal(srcPrefilter.used, distPrefilter.used);
  assert.equal(srcPrefilter.candidateCount, distPrefilter.candidateCount);
  assert.deepEqual(
    indexResultToArray(srcPrefilter.baseIndices),
    indexResultToArray(distPrefilter.baseIndices)
  );
  assert.deepEqual(srcPrefilter.guaranteedColumnKeys, distPrefilter.guaranteedColumnKeys);
  assert.deepEqual(srcPrefilter.remainingRawFilters, distPrefilter.remainingRawFilters);
});

test("sorting parity between src and dist controllers", () => {
  const rows = createDeterministicRows(1500);
  const columnTypeByKey = buildColumnTypeByKey(srcGeneration.COLUMN_KEYS);
  const descriptors = [
    { columnKey: "firstName", direction: "asc" },
    { columnKey: "age", direction: "desc" },
    { columnKey: "column8", direction: "asc" },
  ];

  const srcRows = cloneRows(rows);
  const distRows = cloneRows(rows);
  const srcRowController = srcSorting.createSortController({
    columnKeys: srcGeneration.COLUMN_KEYS,
    defaultColumnKey: "index",
    columnTypeByKey,
    defaultUseTypedComparator: true,
  });
  const distRowController = distSorting.createSortController({
    columnKeys: distGeneration.COLUMN_KEYS,
    defaultColumnKey: "index",
    columnTypeByKey,
    defaultUseTypedComparator: true,
  });

  applySortDescriptors(srcRowController, descriptors);
  applySortDescriptors(distRowController, descriptors);

  srcRowController.sortRows(srcRows, "native", {
    useTypedComparator: true,
    columnTypeByKey,
  });
  distRowController.sortRows(distRows, "native", {
    useTypedComparator: true,
    columnTypeByKey,
  });

  assert.deepEqual(
    srcRows.map((row) => row.index),
    distRows.map((row) => row.index)
  );

  const srcIndexController = srcSorting.createSortController({
    columnKeys: srcGeneration.COLUMN_KEYS,
    defaultColumnKey: "index",
    columnTypeByKey,
    defaultUseTypedComparator: true,
  });
  const distIndexController = distSorting.createSortController({
    columnKeys: distGeneration.COLUMN_KEYS,
    defaultColumnKey: "index",
    columnTypeByKey,
    defaultUseTypedComparator: true,
  });
  applySortDescriptors(srcIndexController, descriptors);
  applySortDescriptors(distIndexController, descriptors);

  const srcIndices = new Array(rows.length);
  const distIndices = new Array(rows.length);
  for (let i = 0; i < rows.length; i += 1) {
    srcIndices[i] = i;
    distIndices[i] = i;
  }

  const srcKeyColumns = buildPrecomputedSortKeyColumns(
    srcIndices,
    rows,
    srcIndexController.getSortDescriptors(),
    columnTypeByKey
  );
  const distKeyColumns = buildPrecomputedSortKeyColumns(
    distIndices,
    rows,
    distIndexController.getSortDescriptors(),
    columnTypeByKey
  );

  srcIndexController.sortIndices(srcIndices, rows, "native", {
    useTypedComparator: true,
    columnTypeByKey,
    precomputedIndexKeys: srcKeyColumns,
  });
  distIndexController.sortIndices(distIndices, rows, "native", {
    useTypedComparator: true,
    columnTypeByKey,
    precomputedIndexKeys: distKeyColumns,
  });

  assert.deepEqual(srcIndices, distIndices);
});
