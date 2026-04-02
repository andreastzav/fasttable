import { createSortController } from "./sorting.js";
import { ensureNumericColumnarSortedIndices } from "./io.js";
import {
  hasIndexCollection,
  normalizeSortDescriptorList,
  materializeIndexBuffer,
  isSupportedRankArray,
  createPrecomputedSortRuntime,
} from "./sorting-precomputed-runtime.js";

function defaultNow() {
  if (typeof performance !== "undefined" && typeof performance.now === "function") {
    return performance.now();
  }

  return Date.now();
}

function buildRankArrayFromSortedIndices(sortedIndices, rowCount) {
  const useCompact = rowCount <= 65536;
  const rankByRowId = useCompact ? new Uint16Array(rowCount) : new Uint32Array(rowCount);

  for (let rank = 0; rank < rowCount; rank += 1) {
    const rowIndex = Number(sortedIndices[rank]);
    if (!Number.isInteger(rowIndex) || rowIndex < 0 || rowIndex >= rowCount) {
      return null;
    }

    rankByRowId[rowIndex] = rank;
  }

  return rankByRowId;
}

function isValidSnapshotIndices(indices, maxRowCount) {
  if (!hasIndexCollection(indices)) {
    return false;
  }

  const rowCount = Math.max(0, Number(maxRowCount) | 0);
  for (let i = 0; i < indices.length; i += 1) {
    const value = Number(indices[i]);
    if (!Number.isInteger(value) || value < 0 || value >= rowCount) {
      return false;
    }
  }

  return true;
}

function getSnapshotCountHint(snapshot) {
  if (
    snapshot &&
    typeof snapshot === "object" &&
    Number.isFinite(snapshot.count) &&
    Number(snapshot.count) >= 0
  ) {
    return Math.floor(Number(snapshot.count));
  }

  return null;
}

function resolveSnapshotRowIndices(source, totalRows) {
  const maxRows = Math.max(0, Number(totalRows) | 0);
  const snapshot = source && typeof source === "object" ? source : null;
  const directCandidate =
    snapshot && hasIndexCollection(snapshot.rowIndices)
      ? snapshot.rowIndices
      : snapshot && hasIndexCollection(snapshot.indices)
        ? snapshot.indices
        : hasIndexCollection(source)
          ? source
          : null;

  if (isValidSnapshotIndices(directCandidate, maxRows)) {
    return {
      rowIndices: materializeIndexBuffer(directCandidate, maxRows),
      count: directCandidate.length | 0,
    };
  }

  const countHint = getSnapshotCountHint(snapshot);
  if (Number.isInteger(countHint) && countHint >= 0 && countHint <= maxRows) {
    const out = new Uint32Array(countHint);
    for (let i = 0; i < countHint; i += 1) {
      out[i] = i;
    }
    return {
      rowIndices: out,
      count: countHint,
    };
  }

  const fallback = new Uint32Array(maxRows);
  for (let i = 0; i < maxRows; i += 1) {
    fallback[i] = i;
  }
  return {
    rowIndices: fallback,
    count: maxRows,
  };
}

function buildPrecomputedSortKeyColumnsFromNumericData(
  numericData,
  indices,
  descriptors,
  columnTypeByKey,
  columnIndexByKey
) {
  if (!numericData || !Array.isArray(numericData.columns)) {
    return null;
  }

  const descriptorList = Array.isArray(descriptors) ? descriptors : [];
  const keyColumns = new Array(descriptorList.length);
  const columns = numericData.columns;
  const dictionaries = Array.isArray(numericData.dictionaries)
    ? numericData.dictionaries
    : [];

  for (let d = 0; d < descriptorList.length; d += 1) {
    const descriptor = descriptorList[d];
    const columnKey =
      descriptor && typeof descriptor.columnKey === "string"
        ? descriptor.columnKey
        : "";
    const columnIndex = Number(columnIndexByKey[columnKey]);
    if (
      !Number.isInteger(columnIndex) ||
      columnIndex < 0 ||
      columnIndex >= columns.length
    ) {
      return null;
    }

    const columnValues = columns[columnIndex];
    if (!columnValues || typeof columnValues.length !== "number") {
      return null;
    }

    const valueType =
      columnTypeByKey && typeof columnKey === "string"
        ? columnTypeByKey[columnKey]
        : "string";
    const useNumericValues = valueType === "number";
    const values = useNumericValues
      ? new Float64Array(indices.length)
      : new Array(indices.length);
    const dictionary = Array.isArray(dictionaries[columnIndex])
      ? dictionaries[columnIndex]
      : null;

    for (let i = 0; i < indices.length; i += 1) {
      const rowIndex = Number(indices[i]) >>> 0;
      const rawValue = columnValues[rowIndex];
      if (useNumericValues) {
        if (rawValue === undefined || rawValue === null) {
          values[i] = Number.NaN;
        } else {
          const numericValue = Number(rawValue);
          values[i] = Number.isFinite(numericValue) ? numericValue : Number.NaN;
        }
      } else if (dictionary) {
        values[i] = dictionary[rawValue];
      } else {
        values[i] = rawValue;
      }
    }

    keyColumns[d] = values;
  }

  return keyColumns;
}

function normalizeRuntimeSortDescriptorList(descriptors, columnIndexByKey) {
  const normalized = normalizeSortDescriptorList(descriptors);
  const filtered = [];

  for (let i = 0; i < normalized.length; i += 1) {
    const descriptor = normalized[i];
    const columnKey = descriptor && descriptor.columnKey;
    if (
      typeof columnKey === "string" &&
      Object.prototype.hasOwnProperty.call(columnIndexByKey, columnKey)
    ) {
      filtered.push(descriptor);
    }
  }

  if (filtered.length > 0) {
    return filtered;
  }

  return [{ columnKey: "index", direction: "asc" }];
}

function createSortRuntimeBridge(options) {
  const input = options || {};
  const now = typeof input.now === "function" ? input.now : defaultNow;
  const columnKeys = Array.isArray(input.columnKeys) ? input.columnKeys : [];
  const columnIndexByKey =
    input.columnIndexByKey && typeof input.columnIndexByKey === "object"
      ? input.columnIndexByKey
      : Object.create(null);
  const columnTypeByKey =
    input.columnTypeByKey && typeof input.columnTypeByKey === "object"
      ? input.columnTypeByKey
      : Object.create(null);
  const getSortOptions =
    typeof input.getSortOptions === "function" ? input.getSortOptions : () => ({});
  const getSortMode =
    typeof input.getSortMode === "function" ? input.getSortMode : () => "native";
  const getRowCount =
    typeof input.getRowCount === "function" ? input.getRowCount : () => 0;
  const getSchema =
    typeof input.getSchema === "function" ? input.getSchema : () => null;
  const getNumericColumnarData =
    typeof input.getNumericColumnarData === "function"
      ? input.getNumericColumnarData
      : () => null;

  const precomputedSortRuntime = createPrecomputedSortRuntime({
    now,
    getSchema,
    getNumericColumnarData,
    materializeIndices: materializeIndexBuffer,
  });

  function resolvePrecomputedRankColumnsForDescriptors(descriptors, totalRowCount) {
    const descriptorList = Array.isArray(descriptors) ? descriptors : [];
    if (descriptorList.length === 0 || totalRowCount <= 0) {
      return null;
    }

    const numeric = ensureNumericColumnarSortedIndices(
      getNumericColumnarData(),
      getSchema()
    );
    const sortedColumns = Array.isArray(numeric && numeric.sortedIndexColumns)
      ? numeric.sortedIndexColumns
      : null;
    if (!sortedColumns || sortedColumns.length === 0) {
      return null;
    }

    const rankByKey =
      numeric.sortedRankAscByKey &&
      typeof numeric.sortedRankAscByKey === "object" &&
      !Array.isArray(numeric.sortedRankAscByKey)
        ? numeric.sortedRankAscByKey
        : Object.create(null);
    const rankByColumn = Array.isArray(numeric.sortedRankColumns)
      ? numeric.sortedRankColumns
      : new Array(sortedColumns.length);
    const descriptorRankColumns = new Array(descriptorList.length);

    for (let i = 0; i < descriptorList.length; i += 1) {
      const descriptor = descriptorList[i];
      const columnKey =
        descriptor && typeof descriptor.columnKey === "string"
          ? descriptor.columnKey
          : "";
      if (columnKey === "") {
        return null;
      }

      const columnIndex = Number(columnIndexByKey[columnKey]);
      if (
        !Number.isInteger(columnIndex) ||
        columnIndex < 0 ||
        columnIndex >= sortedColumns.length
      ) {
        return null;
      }

      let rankByRowId = rankByKey[columnKey];
      if (!isSupportedRankArray(rankByRowId, totalRowCount)) {
        rankByRowId = rankByColumn[columnIndex];
      }

      if (!isSupportedRankArray(rankByRowId, totalRowCount)) {
        const sortedIndices = sortedColumns[columnIndex];
        if (
          !sortedIndices ||
          (!ArrayBuffer.isView(sortedIndices) && !Array.isArray(sortedIndices)) ||
          sortedIndices.length !== totalRowCount
        ) {
          return null;
        }

        rankByRowId = buildRankArrayFromSortedIndices(sortedIndices, totalRowCount);
        if (!isSupportedRankArray(rankByRowId, totalRowCount)) {
          return null;
        }

        rankByKey[columnKey] = rankByRowId;
        rankByColumn[columnIndex] = rankByRowId;
      }

      descriptorRankColumns[i] = rankByRowId;
    }

    numeric.sortedRankAscByKey = rankByKey;
    numeric.sortedRankColumns = rankByColumn;
    return descriptorRankColumns;
  }

  function runFallbackIndexSort(
    descriptorList,
    runtimeSortMode,
    snapshotRowIndices,
    snapshotCount
  ) {
    const controller = createSortController({
      columnKeys,
      defaultColumnKey: "index",
      columnTypeByKey,
      defaultUseTypedComparator: getSortOptions().useTypedComparator,
    });

    for (let i = 0; i < descriptorList.length; i += 1) {
      const descriptor = descriptorList[i];
      controller.cycle(descriptor.columnKey);
      if (descriptor.direction === "asc") {
        controller.cycle(descriptor.columnKey);
      }
    }

    const indices = snapshotRowIndices.slice();
    const runSortOptions = { ...getSortOptions(), useIndexSort: true };
    const totalRows = getRowCount();
    const precomputedRankColumns = resolvePrecomputedRankColumnsForDescriptors(
      descriptorList,
      totalRows
    );
    if (
      Array.isArray(precomputedRankColumns) &&
      precomputedRankColumns.length === descriptorList.length
    ) {
      runSortOptions.precomputedRankColumns = precomputedRankColumns;
    } else {
      const keyColumns = buildPrecomputedSortKeyColumnsFromNumericData(
        getNumericColumnarData(),
        indices,
        descriptorList,
        columnTypeByKey,
        columnIndexByKey
      );
      if (Array.isArray(keyColumns) && keyColumns.length === descriptorList.length) {
        runSortOptions.precomputedIndexKeys = keyColumns;
      }
    }

    const sortTotalStartMs = now();
    const result = controller.sortIndices(
      indices,
      null,
      runtimeSortMode,
      runSortOptions
    );
    const sortTotalMs = now() - sortTotalStartMs;
    const sortCoreMs = Number(result.durationMs);
    const sortPrepMs = sortTotalMs - sortCoreMs;

    return {
      sortMs: sortCoreMs,
      sortCoreMs,
      sortPrepMs,
      sortTotalMs,
      sortMode: result.sortMode,
      sortedCount: snapshotCount,
      descriptors: result.effectiveDescriptors,
      dataPath: result.dataPath,
      comparatorMode: result.comparatorMode,
      sortedIndices: indices,
    };
  }

  function runSortSnapshotPass(rowsSnapshot, descriptors, sortModeOverride) {
    const descriptorList = normalizeRuntimeSortDescriptorList(
      descriptors,
      columnIndexByKey
    );
    const effectiveSortMode =
      typeof sortModeOverride === "string" && sortModeOverride !== ""
        ? sortModeOverride
        : getSortMode();

    const totalRows = getRowCount();
    const snapshot = resolveSnapshotRowIndices(rowsSnapshot, totalRows);
    const snapshotRowIndices = snapshot.rowIndices;
    const snapshotCount = snapshot.count;
    if (effectiveSortMode === "precomputed") {
      const precomputedRun = precomputedSortRuntime.runPrecomputedSortSelection({
        descriptorList,
        selectedIndices: snapshotRowIndices,
        rowCount: totalRows,
        isFullSelection: snapshotCount === totalRows,
      });
      if (precomputedRun) {
        return precomputedRun;
      }
    }

    const runtimeSortMode =
      effectiveSortMode === "precomputed" ? "native" : effectiveSortMode;
    return runFallbackIndexSort(
      descriptorList,
      runtimeSortMode,
      snapshotRowIndices,
      snapshotCount
    );
  }

  return {
    runSortSnapshotPass,
    prewarmPrecomputedSortState: precomputedSortRuntime.prewarm,
    resetPrecomputedSortState(rowCount) {
      precomputedSortRuntime.reset(rowCount);
    },
  };
}

export { createSortRuntimeBridge };
