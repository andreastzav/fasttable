import {
  ensureNumericColumnarSortedIndices,
  ensureNumericColumnarSortedRanks,
} from "./io.js";

function defaultNow() {
  if (typeof performance !== "undefined" && typeof performance.now === "function") {
    return performance.now();
  }

  return Date.now();
}

function hasIndexCollection(indices) {
  return Array.isArray(indices) || ArrayBuffer.isView(indices);
}

function normalizeSortDescriptorList(descriptors) {
  const source = Array.isArray(descriptors) ? descriptors : [];
  const output = [];

  for (let i = 0; i < source.length; i += 1) {
    const descriptor = source[i];
    const columnKey =
      descriptor && typeof descriptor.columnKey === "string"
        ? descriptor.columnKey
        : "";
    if (columnKey === "") {
      continue;
    }

    output.push({
      columnKey,
      direction: descriptor.direction === "asc" ? "asc" : "desc",
    });
  }

  return output;
}

function materializeIndexBuffer(indices, fallbackCount) {
  const defaultCount = Math.max(0, Number(fallbackCount) | 0);
  if (indices === null || indices === undefined) {
    const out = new Uint32Array(defaultCount);
    for (let i = 0; i < defaultCount; i += 1) {
      out[i] = i;
    }
    return out;
  }

  if (hasIndexCollection(indices)) {
    const out = new Uint32Array(indices.length);
    for (let i = 0; i < indices.length; i += 1) {
      out[i] = Number(indices[i]) >>> 0;
    }
    return out;
  }

  return new Uint32Array(0);
}

function isIdentitySelection(indices, rowCount) {
  if (!hasIndexCollection(indices) || indices.length !== rowCount) {
    return false;
  }

  for (let i = 0; i < indices.length; i += 1) {
    if ((Number(indices[i]) >>> 0) !== i) {
      return false;
    }
  }

  return true;
}

function createPrecomputedSortState() {
  return {
    rowCount: -1,
    sortedByKey: Object.create(null),
    sortedDescByKey: Object.create(null),
    rankByKey: Object.create(null),
    fullOrderByDescriptor: new Map(),
    counts16: null,
    primary: null,
    secondary: null,
  };
}

function ensurePrecomputedSortState(state, rowCount, countHint) {
  const expectedRowCount = Math.max(0, Number(rowCount) | 0);
  const expectedCount = Math.max(0, Number(countHint) | 0);

  if (state.rowCount !== expectedRowCount) {
    state.rowCount = expectedRowCount;
    state.sortedByKey = Object.create(null);
    state.sortedDescByKey = Object.create(null);
    state.rankByKey = Object.create(null);
    state.fullOrderByDescriptor = new Map();
    state.primary = null;
    state.secondary = null;
  }

  if (!(state.counts16 instanceof Uint32Array) || state.counts16.length !== 65536) {
    state.counts16 = new Uint32Array(65536);
  }
  if (!(state.primary instanceof Uint32Array) || state.primary.length < expectedCount) {
    state.primary = new Uint32Array(expectedCount);
  }
  if (!(state.secondary instanceof Uint32Array) || state.secondary.length < expectedCount) {
    state.secondary = new Uint32Array(expectedCount);
  }
}

function reverseUint32Array(source) {
  const length = source.length;
  const out = new Uint32Array(length);
  for (let i = 0; i < length; i += 1) {
    out[i] = source[length - 1 - i];
  }
  return out;
}

function buildDescriptorCacheKey(descriptorList) {
  const source = Array.isArray(descriptorList) ? descriptorList : [];
  let key = "";
  for (let i = 0; i < source.length; i += 1) {
    if (i > 0) {
      key += "|";
    }
    const descriptor = source[i] || {};
    const columnKey =
      typeof descriptor.columnKey === "string" ? descriptor.columnKey : "";
    const direction = descriptor.direction === "asc" ? "asc" : "desc";
    key += `${columnKey}:${direction}`;
  }
  return key;
}

function tryBuildInverseDescriptorList(descriptorList) {
  const source = Array.isArray(descriptorList) ? descriptorList : [];
  if (source.length === 0) {
    return null;
  }

  const firstDirection = source[0].direction === "asc" ? "asc" : "desc";
  for (let i = 1; i < source.length; i += 1) {
    const direction = source[i].direction === "asc" ? "asc" : "desc";
    if (direction !== firstDirection) {
      return null;
    }
  }

  const inverseDirection = firstDirection === "asc" ? "desc" : "asc";
  const inverse = new Array(source.length);
  for (let i = 0; i < source.length; i += 1) {
    const descriptor = source[i];
    inverse[i] = {
      columnKey: descriptor.columnKey,
      direction: inverseDirection,
    };
  }
  return inverse;
}

function getColumnIndexByKey(schema, columnKey) {
  const columnKeys = Array.isArray(schema && schema.columnKeys)
    ? schema.columnKeys
    : [];
  return columnKeys.indexOf(columnKey);
}

function isSupportedRankArray(rankByRowId, expectedLength) {
  if (!(rankByRowId instanceof Uint16Array || rankByRowId instanceof Uint32Array)) {
    return false;
  }

  if (!Number.isFinite(expectedLength)) {
    return true;
  }

  return rankByRowId.length === Math.max(0, Number(expectedLength) | 0);
}

function deriveRankMaxValue(rankByRowId) {
  if (!isSupportedRankArray(rankByRowId)) {
    return 0;
  }

  let maxRank = 0;
  for (let i = 0; i < rankByRowId.length; i += 1) {
    const value = rankByRowId[i] >>> 0;
    if (value > maxRank) {
      maxRank = value;
    }
  }
  return maxRank >>> 0;
}

function resolveRankStateFromNumericColumnarData(
  numericColumnarData,
  columnKey,
  rowCount
) {
  const source =
    numericColumnarData && typeof numericColumnarData === "object"
      ? numericColumnarData
      : null;
  if (!source) {
    return null;
  }

  const rankByKeyPrimary =
    source.sortedRankAscByKey &&
    typeof source.sortedRankAscByKey === "object" &&
    !Array.isArray(source.sortedRankAscByKey)
      ? source.sortedRankAscByKey
      : null;
  const rankByKeyFallback =
    source.sortedRankByKey &&
    typeof source.sortedRankByKey === "object" &&
    !Array.isArray(source.sortedRankByKey)
      ? source.sortedRankByKey
      : null;
  let rankByRowId = rankByKeyPrimary ? rankByKeyPrimary[columnKey] : undefined;
  if (!isSupportedRankArray(rankByRowId, rowCount) && rankByKeyFallback) {
    rankByRowId = rankByKeyFallback[columnKey];
  }
  if (!isSupportedRankArray(rankByRowId, rowCount)) {
    return null;
  }

  const rankMaxByKeyPrimary =
    source.sortedRankAscMaxByKey &&
    typeof source.sortedRankAscMaxByKey === "object" &&
    !Array.isArray(source.sortedRankAscMaxByKey)
      ? source.sortedRankAscMaxByKey
      : null;
  const rankMaxByKeyFallback =
    source.sortedRankMaxByKey &&
    typeof source.sortedRankMaxByKey === "object" &&
    !Array.isArray(source.sortedRankMaxByKey)
      ? source.sortedRankMaxByKey
      : null;
  let maxRank = rankMaxByKeyPrimary ? Number(rankMaxByKeyPrimary[columnKey]) : Number.NaN;
  if (!Number.isFinite(maxRank) && rankMaxByKeyFallback) {
    maxRank = Number(rankMaxByKeyFallback[columnKey]);
  }
  if (!Number.isFinite(maxRank)) {
    maxRank = deriveRankMaxValue(rankByRowId);
  }
  const normalizedMaxRank = Number(maxRank);

  return {
    rankByRowId,
    maxRank: Number.isFinite(normalizedMaxRank)
      ? Math.max(0, normalizedMaxRank) >>> 0
      : 0,
  };
}

function seedPrecomputedStateFromNumericColumnarData(
  state,
  numericColumnarData,
  schema,
  rowCount
) {
  if (!state || typeof state !== "object") {
    return;
  }

  const source =
    numericColumnarData && typeof numericColumnarData === "object"
      ? numericColumnarData
      : null;
  if (!source) {
    return;
  }

  const columnKeys = Array.isArray(schema && schema.columnKeys)
    ? schema.columnKeys
    : [];
  for (let i = 0; i < columnKeys.length; i += 1) {
    const columnKey = columnKeys[i];
    const sortedByKey =
      source.sortedIndexByKey &&
      typeof source.sortedIndexByKey === "object" &&
      !Array.isArray(source.sortedIndexByKey)
        ? source.sortedIndexByKey
        : null;
    const sortedColumn = sortedByKey ? sortedByKey[columnKey] : undefined;
    if (sortedColumn instanceof Uint32Array && sortedColumn.length === rowCount) {
      state.sortedByKey[columnKey] = sortedColumn;
    }

    const rankState = resolveRankStateFromNumericColumnarData(
      source,
      columnKey,
      rowCount
    );
    if (rankState) {
      state.rankByKey[columnKey] = rankState;
    }
  }
}

function applyStableRankRadixPass(
  source,
  target,
  count,
  rankByRowId,
  maxRank,
  descending,
  shift,
  counts
) {
  counts.fill(0);

  for (let i = 0; i < count; i += 1) {
    const rowIndex = source[i];
    let rank = rankByRowId[rowIndex] >>> 0;
    if (descending) {
      rank = (maxRank - rank) >>> 0;
    }
    counts[(rank >>> shift) & 0xffff] += 1;
  }

  let running = 0;
  for (let i = 0; i < counts.length; i += 1) {
    running += counts[i];
    counts[i] = running;
  }

  for (let i = count - 1; i >= 0; i -= 1) {
    const rowIndex = source[i];
    let rank = rankByRowId[rowIndex] >>> 0;
    if (descending) {
      rank = (maxRank - rank) >>> 0;
    }
    const bucket = (rank >>> shift) & 0xffff;
    const pos = --counts[bucket];
    target[pos] = rowIndex;
  }
}

function sortIndicesByPrecomputedRanks(selectedIndices, descriptorList, rankStates, state) {
  const count = selectedIndices.length;
  ensurePrecomputedSortState(state, state.rowCount, count);

  const primary = state.primary;
  const secondary = state.secondary;
  const counts = state.counts16;

  primary.set(selectedIndices);
  let source = primary;
  let target = secondary;

  for (let d = descriptorList.length - 1; d >= 0; d -= 1) {
    const descriptor = descriptorList[d];
    const rankState = rankStates[d];
    if (
      !rankState ||
      !hasIndexCollection(rankState.rankByRowId) ||
      rankState.rankByRowId.length < state.rowCount
    ) {
      return null;
    }
    const rankByRowId = rankState.rankByRowId;
    const maxRank = Number(rankState.maxRank) >>> 0;
    const descending = descriptor.direction === "desc";

    applyStableRankRadixPass(
      source,
      target,
      count,
      rankByRowId,
      maxRank,
      descending,
      0,
      counts
    );
    const afterLow = source;
    source = target;
    target = afterLow;

    applyStableRankRadixPass(
      source,
      target,
      count,
      rankByRowId,
      maxRank,
      descending,
      16,
      counts
    );
    const afterHigh = source;
    source = target;
    target = afterHigh;
  }

  return source.slice(0, count);
}

function createPrecomputedSortRuntime(options) {
  const input = options || {};
  const now = typeof input.now === "function" ? input.now : defaultNow;
  const getSchema =
    typeof input.getSchema === "function" ? input.getSchema : () => null;
  const getNumericColumnarData =
    typeof input.getNumericColumnarData === "function"
      ? input.getNumericColumnarData
      : () => null;
  const materializeIndices =
    typeof input.materializeIndices === "function"
      ? input.materializeIndices
      : materializeIndexBuffer;
  const state =
    input.state && typeof input.state === "object"
      ? input.state
      : createPrecomputedSortState();

  function reset(rowCount) {
    ensurePrecomputedSortState(
      state,
      Number.isFinite(rowCount) ? rowCount : -1,
      0
    );
    state.sortedByKey = Object.create(null);
    state.sortedDescByKey = Object.create(null);
    state.rankByKey = Object.create(null);
    state.fullOrderByDescriptor = new Map();
  }

  function resolvePrecomputedSortedColumn(columnKey, rowCount, schema, numericColumnarData) {
    if (
      state.sortedByKey &&
      state.sortedByKey[columnKey] instanceof Uint32Array &&
      state.sortedByKey[columnKey].length === rowCount
    ) {
      return state.sortedByKey[columnKey];
    }

    const source =
      numericColumnarData && typeof numericColumnarData === "object"
        ? numericColumnarData
        : getNumericColumnarData();
    if (!source || typeof source !== "object") {
      return null;
    }
    ensureNumericColumnarSortedIndices(source, schema);

    const sortedByKey =
      source.sortedIndexByKey &&
      typeof source.sortedIndexByKey === "object" &&
      !Array.isArray(source.sortedIndexByKey)
        ? source.sortedIndexByKey
        : null;
    if (sortedByKey && sortedByKey[columnKey] instanceof Uint32Array) {
      const column = sortedByKey[columnKey];
      if (column.length === rowCount) {
        state.sortedByKey[columnKey] = column;
        return column;
      }
    }

    const columnIndex = getColumnIndexByKey(schema, columnKey);
    if (columnIndex < 0) {
      return null;
    }

    const sortedColumns = Array.isArray(source.sortedIndexColumns)
      ? source.sortedIndexColumns
      : null;
    const fromColumn =
      sortedColumns && columnIndex < sortedColumns.length
        ? sortedColumns[columnIndex]
        : null;
    if (fromColumn instanceof Uint32Array && fromColumn.length === rowCount) {
      state.sortedByKey[columnKey] = fromColumn;
      return fromColumn;
    }
    return null;
  }

  function buildRankStateForColumn(columnKey, rowCount, schema, numericColumnarData) {
    if (
      state.rankByKey &&
      state.rankByKey[columnKey] &&
      isSupportedRankArray(state.rankByKey[columnKey].rankByRowId, rowCount)
    ) {
      return state.rankByKey[columnKey];
    }

    const source =
      numericColumnarData && typeof numericColumnarData === "object"
        ? numericColumnarData
        : getNumericColumnarData();
    if (!source || typeof source !== "object") {
      return null;
    }

    const prewarmed = ensureNumericColumnarSortedRanks(source, schema) || {};
    const sortedNumericColumnarData = prewarmed.numericColumnarData || source;
    seedPrecomputedStateFromNumericColumnarData(
      state,
      sortedNumericColumnarData,
      schema,
      rowCount
    );

    const out = resolveRankStateFromNumericColumnarData(
      sortedNumericColumnarData,
      columnKey,
      rowCount
    );
    if (!out) {
      return null;
    }
    state.rankByKey[columnKey] = out;
    return out;
  }

  function buildPrecomputedSortedSelection(
    selectedIndices,
    sortedColumn,
    direction,
    rowCount,
    fullSelectionHint,
    columnKeyHint
  ) {
    const count = selectedIndices.length;
    if (count === 0) {
      return {
        sortedIndices: new Uint32Array(0),
        dataPath: "indices+precomputed-empty",
      };
    }

    const fullSelection =
      fullSelectionHint === true ||
      (fullSelectionHint !== false && isIdentitySelection(selectedIndices, rowCount));
    if (fullSelection) {
      if (direction === "desc") {
        const columnKey =
          typeof columnKeyHint === "string" && columnKeyHint.trim() !== ""
            ? columnKeyHint
            : "";
        if (
          columnKey &&
          state.sortedDescByKey &&
          state.sortedDescByKey[columnKey] instanceof Uint32Array &&
          state.sortedDescByKey[columnKey].length === rowCount
        ) {
          return {
            sortedIndices: state.sortedDescByKey[columnKey],
            dataPath: "indices+precomputed-full-desc-cached",
          };
        }

        const reversed = reverseUint32Array(sortedColumn);
        if (
          columnKey &&
          state.sortedDescByKey &&
          typeof state.sortedDescByKey === "object"
        ) {
          state.sortedDescByKey[columnKey] = reversed;
        }
        return {
          sortedIndices: reversed,
          dataPath: "indices+precomputed-full-desc",
        };
      }

      return {
        sortedIndices: sortedColumn,
        dataPath: "indices+precomputed-full-asc",
      };
    }

    const selected = new Uint8Array(rowCount);
    for (let i = 0; i < count; i += 1) {
      const rowIndex = Number(selectedIndices[i]) >>> 0;
      if (rowIndex < rowCount) {
        selected[rowIndex] = 1;
      }
    }

    const out = new Uint32Array(count);
    let writeIndex = 0;
    if (direction === "desc") {
      for (let i = sortedColumn.length - 1; i >= 0; i -= 1) {
        const rowIndex = sortedColumn[i];
        if (selected[rowIndex] === 1) {
          out[writeIndex] = rowIndex;
          writeIndex += 1;
        }
      }
    } else {
      for (let i = 0; i < sortedColumn.length; i += 1) {
        const rowIndex = sortedColumn[i];
        if (selected[rowIndex] === 1) {
          out[writeIndex] = rowIndex;
          writeIndex += 1;
        }
      }
    }

    if (writeIndex !== count) {
      return null;
    }

    return {
      sortedIndices: out,
      dataPath: "indices+precomputed-subset-scan",
    };
  }

  function runPrecomputedSortSelection(inputSelection) {
    const payload =
      inputSelection && typeof inputSelection === "object" ? inputSelection : {};
    const descriptorList = normalizeSortDescriptorList(payload.descriptorList);
    const selectedIndices = materializeIndices(
      payload.selectedIndices,
      payload.rowCount
    );
    const rowCount = Math.max(0, Number(payload.rowCount) | 0);
    const isFullSelection = payload.isFullSelection === true;
    if (descriptorList.length === 0 || !hasIndexCollection(selectedIndices)) {
      return null;
    }
    if (rowCount <= 0) {
      return null;
    }

    ensurePrecomputedSortState(state, rowCount, selectedIndices.length);
    const schema = getSchema();
    const numericColumnarData = getNumericColumnarData();
    const coreStartMs = now();
    let sorted = null;

    if (descriptorList.length === 1) {
      const descriptor = descriptorList[0];
      const sortedColumn = resolvePrecomputedSortedColumn(
        descriptor.columnKey,
        rowCount,
        schema,
        numericColumnarData
      );
      if (!(sortedColumn instanceof Uint32Array)) {
        return null;
      }
      sorted = buildPrecomputedSortedSelection(
        selectedIndices,
        sortedColumn,
        descriptor.direction,
        rowCount,
        isFullSelection || isIdentitySelection(selectedIndices, rowCount),
        descriptor.columnKey
      );
    } else {
      const fullSelection =
        isFullSelection || isIdentitySelection(selectedIndices, rowCount);
      if (fullSelection && state.fullOrderByDescriptor instanceof Map) {
        const descriptorKey = buildDescriptorCacheKey(descriptorList);
        const cachedOrder = state.fullOrderByDescriptor.get(descriptorKey);
        if (cachedOrder instanceof Uint32Array && cachedOrder.length === rowCount) {
          const sortCoreMs = now() - coreStartMs;
          return {
            sortMs: sortCoreMs,
            sortCoreMs,
            sortPrepMs: 0,
            sortTotalMs: sortCoreMs,
            sortMode: "precomputed-ranktuple",
            sortedCount: cachedOrder.length,
            descriptors: descriptorList,
            dataPath: "indices+precomputed-ranktuple-cached",
            comparatorMode: "precomputed",
            sortedIndices: cachedOrder,
          };
        }

        const inverseDescriptorList = tryBuildInverseDescriptorList(descriptorList);
        if (inverseDescriptorList) {
          const inverseKey = buildDescriptorCacheKey(inverseDescriptorList);
          const inverseCachedOrder = state.fullOrderByDescriptor.get(inverseKey);
          if (
            inverseCachedOrder instanceof Uint32Array &&
            inverseCachedOrder.length === rowCount
          ) {
            const reversed = reverseUint32Array(inverseCachedOrder);
            state.fullOrderByDescriptor.set(descriptorKey, reversed);
            const sortCoreMs = now() - coreStartMs;
            return {
              sortMs: sortCoreMs,
              sortCoreMs,
              sortPrepMs: 0,
              sortTotalMs: sortCoreMs,
              sortMode: "precomputed-ranktuple",
              sortedCount: reversed.length,
              descriptors: descriptorList,
              dataPath: "indices+precomputed-ranktuple-reverse-cache",
              comparatorMode: "precomputed",
              sortedIndices: reversed,
            };
          }
        }
      }

      const rankStates = new Array(descriptorList.length);
      for (let i = 0; i < descriptorList.length; i += 1) {
        const descriptor = descriptorList[i];
        rankStates[i] = buildRankStateForColumn(
          descriptor.columnKey,
          rowCount,
          schema,
          numericColumnarData
        );
        if (!rankStates[i]) {
          return null;
        }
      }

      const rankSorted = sortIndicesByPrecomputedRanks(
        selectedIndices,
        descriptorList,
        rankStates,
        state
      );
      if (!rankSorted) {
        return null;
      }

      sorted = {
        sortedIndices: rankSorted,
        dataPath: "indices+precomputed-ranktuple",
      };

      if (
        fullSelection &&
        state &&
        state.fullOrderByDescriptor instanceof Map
      ) {
        state.fullOrderByDescriptor.set(
          buildDescriptorCacheKey(descriptorList),
          rankSorted
        );
      }
    }

    if (!sorted || !hasIndexCollection(sorted.sortedIndices)) {
      return null;
    }

    const sortCoreMs = now() - coreStartMs;
    const sortModeLabel =
      descriptorList.length === 1 ? "precomputed" : "precomputed-ranktuple";
    return {
      sortMs: sortCoreMs,
      sortCoreMs,
      sortPrepMs: 0,
      sortTotalMs: sortCoreMs,
      sortMode: sortModeLabel,
      sortedCount: sorted.sortedIndices.length,
      descriptors: descriptorList,
      dataPath: sorted.dataPath,
      comparatorMode: "precomputed",
      sortedIndices: sorted.sortedIndices,
    };
  }

  function prewarm() {
    const schema = getSchema();
    const numericColumnarData = getNumericColumnarData();
    const rowCount = Math.max(
      0,
      Number(numericColumnarData && numericColumnarData.rowCount) | 0
    );
    if (!numericColumnarData || typeof numericColumnarData !== "object" || rowCount <= 0) {
      return false;
    }

    ensurePrecomputedSortState(state, rowCount, rowCount);
    const prewarmed = ensureNumericColumnarSortedRanks(numericColumnarData, schema) || {};
    const sortedNumericColumnarData = prewarmed.numericColumnarData || numericColumnarData;
    seedPrecomputedStateFromNumericColumnarData(
      state,
      sortedNumericColumnarData,
      schema,
      rowCount
    );
    return Object.keys(state.rankByKey).length > 0;
  }

  return {
    runPrecomputedSortSelection,
    prewarm,
    reset,
    getState() {
      return state;
    },
  };
}

export {
  hasIndexCollection,
  normalizeSortDescriptorList,
  materializeIndexBuffer,
  isSupportedRankArray,
  createPrecomputedSortState,
  createPrecomputedSortRuntime,
};
