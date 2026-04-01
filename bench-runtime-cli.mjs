import { promises as fs } from "node:fs";
import path from "node:path";
import { createFastTableRuntime } from "./packages/core/dist/runtime.js";
import { loadColumnarBinaryPreset } from "./packages/core/dist/io-node.js";
import {
  ensureNumericColumnarSortedIndices,
  ensureNumericColumnarSortedRanks,
} from "./packages/core/dist/io.js";
import { fastTableGenerationWorkersNodeApi } from "./packages/core/dist/generation-workers-node.js";
import {
  runFilteringBenchmark,
  runSortBenchmark,
} from "./packages/core/dist/benchmark.js";
import { createSortBenchmarkOrchestrator } from "./packages/core/dist/sorting-orchestration.js";

function parseArgs(argv) {
  const args = {
    preset: 1000000,
    presetDir: "./tables_presets",
    bench: "filtering",
    currentOnly: false,
    rounds: 3,
    out: "",
    workers: 4,
    chunkSize: 10000,
    generateWorkers: 0,
    precomputeSortWorkers: false,
    sortMode: "",
  };

  for (let i = 0; i < argv.length; i += 1) {
    const token = String(argv[i] || "");
    if (token === "--preset" && i + 1 < argv.length) {
      args.preset = Number.parseInt(String(argv[i + 1]), 10);
      i += 1;
      continue;
    }
    if (token === "--preset-dir" && i + 1 < argv.length) {
      args.presetDir = String(argv[i + 1]);
      i += 1;
      continue;
    }
    if (token === "--bench" && i + 1 < argv.length) {
      args.bench = String(argv[i + 1]).toLowerCase();
      i += 1;
      continue;
    }
    if (token === "--current") {
      args.currentOnly = true;
      continue;
    }
    if (token === "--rounds" && i + 1 < argv.length) {
      args.rounds = Math.max(1, Number.parseInt(String(argv[i + 1]), 10) || 1);
      i += 1;
      continue;
    }
    if (token === "--out" && i + 1 < argv.length) {
      args.out = String(argv[i + 1]);
      i += 1;
      continue;
    }
    if (token === "--workers" && i + 1 < argv.length) {
      args.workers = Math.max(1, Number.parseInt(String(argv[i + 1]), 10) || 1);
      i += 1;
      continue;
    }
    if (token === "--chunk-size" && i + 1 < argv.length) {
      args.chunkSize = Math.max(
        1,
        Number.parseInt(String(argv[i + 1]), 10) || 1
      );
      i += 1;
      continue;
    }
    if (token === "--generate-workers" && i + 1 < argv.length) {
      args.generateWorkers = Math.max(
        0,
        Number.parseInt(String(argv[i + 1]), 10) || 0
      );
      i += 1;
      continue;
    }
    if (token === "--precompute-sort-workers") {
      args.precomputeSortWorkers = true;
      continue;
    }
    if (token === "--sort-mode" && i + 1 < argv.length) {
      args.sortMode = String(argv[i + 1]).trim().toLowerCase();
      i += 1;
      continue;
    }
    if (token === "--help" || token === "-h") {
      args.help = true;
      continue;
    }
  }

  return args;
}

function printHelp() {
  console.log("FastTable runtime benchmark CLI");
  console.log("");
  console.log("Usage:");
  console.log("  node bench-runtime-cli.mjs [options]");
  console.log("");
  console.log("Options:");
  console.log("  --preset <rows>        Preset row count (default: 1000000)");
  console.log("  --preset-dir <path>    Preset directory (default: ./tables_presets)");
  console.log("  --bench <name>         filtering | sorting | both (default: filtering)");
  console.log("  --current              Benchmark current combo only");
  console.log("  --rounds <n>           Rounds per benchmark value (default: 3)");
  console.log("  --out <file>           Write final benchmark text output");
  console.log("  --generate-workers <n> Generate n rows with worker_threads (skip preset load)");
  console.log("  --workers <n>          Worker count for generation/precompute (default: 4)");
  console.log("  --chunk-size <n>       Worker generation chunk size (default: 10000)");
  console.log("  --sort-mode <name>     native | timsort | precomputed (force current sorting mode)");
  console.log("  --precompute-sort-workers");
  console.log("                         Precompute sorted index columns using worker_threads");
  console.log("  -h, --help             Show this help");
}

function createLinePrinter(prefix) {
  let emittedCount = 0;
  return function onUpdate(lines) {
    const allLines = Array.isArray(lines) ? lines : [];
    for (let i = emittedCount; i < allLines.length; i += 1) {
      const line = String(allLines[i]);
      if (prefix) {
        console.log(`[${prefix}] ${line}`);
      } else {
        console.log(line);
      }
    }
    emittedCount = allLines.length;
  };
}

async function maybeWriteOutput(outPath, sections) {
  if (!outPath) {
    return;
  }

  const absolutePath = path.resolve(outPath);
  const content = sections.join("\n\n");
  await fs.writeFile(absolutePath, content, "utf8");
  console.log(`Saved benchmark output to: ${absolutePath}`);
}

function hasIndexCollection(indices) {
  return Array.isArray(indices) || ArrayBuffer.isView(indices);
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

function getColumnIndexByKey(schema, columnKey) {
  const columnKeys = Array.isArray(schema && schema.columnKeys)
    ? schema.columnKeys
    : [];
  return columnKeys.indexOf(columnKey);
}

function createCliPrecomputedState() {
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

function ensureCliPrecomputedState(state, rowCount, countHint) {
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

function resolvePrecomputedSortedColumn(
  runtime,
  schema,
  columnKey,
  rowCount,
  precomputedState
) {
  if (
    precomputedState &&
    precomputedState.sortedByKey &&
    precomputedState.sortedByKey[columnKey] instanceof Uint32Array &&
    precomputedState.sortedByKey[columnKey].length === rowCount
  ) {
    return precomputedState.sortedByKey[columnKey];
  }

  const numericColumnarData = runtime.getNumericColumnarForSave();
  if (!numericColumnarData || typeof numericColumnarData !== "object") {
    return null;
  }
  ensureNumericColumnarSortedIndices(numericColumnarData, schema);

  const sortedByKey =
    numericColumnarData.sortedIndexByKey &&
    typeof numericColumnarData.sortedIndexByKey === "object" &&
    !Array.isArray(numericColumnarData.sortedIndexByKey)
      ? numericColumnarData.sortedIndexByKey
      : null;
  if (sortedByKey && sortedByKey[columnKey] instanceof Uint32Array) {
    const column = sortedByKey[columnKey];
    if (column.length === rowCount) {
      if (precomputedState && precomputedState.sortedByKey) {
        precomputedState.sortedByKey[columnKey] = column;
      }
      return column;
    }
  }

  const columnIndex = getColumnIndexByKey(schema, columnKey);
  if (columnIndex < 0) {
    return null;
  }

  const sortedColumns = Array.isArray(numericColumnarData.sortedIndexColumns)
    ? numericColumnarData.sortedIndexColumns
    : null;
  const fromColumn =
    sortedColumns && columnIndex >= 0 && columnIndex < sortedColumns.length
      ? sortedColumns[columnIndex]
      : null;
  if (fromColumn instanceof Uint32Array && fromColumn.length === rowCount) {
    if (precomputedState && precomputedState.sortedByKey) {
      precomputedState.sortedByKey[columnKey] = fromColumn;
    }
    return fromColumn;
  }
  return null;
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
  precomputedState,
  numericColumnarData,
  schema,
  rowCount
) {
  if (!precomputedState || typeof precomputedState !== "object") {
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
      precomputedState.sortedByKey[columnKey] = sortedColumn;
    }

    const rankState = resolveRankStateFromNumericColumnarData(
      source,
      columnKey,
      rowCount
    );
    if (rankState) {
      precomputedState.rankByKey[columnKey] = rankState;
    }
  }
}

function buildRankStateForColumn(
  runtime,
  schema,
  columnKey,
  rowCount,
  precomputedState
) {
  if (
    precomputedState &&
    precomputedState.rankByKey &&
    precomputedState.rankByKey[columnKey] &&
    isSupportedRankArray(precomputedState.rankByKey[columnKey].rankByRowId, rowCount)
  ) {
    return precomputedState.rankByKey[columnKey];
  }

  const numericColumnarData = runtime.getNumericColumnarForSave();
  if (!numericColumnarData || typeof numericColumnarData !== "object") {
    return null;
  }
  const prewarmed =
    ensureNumericColumnarSortedRanks(numericColumnarData, schema) || {};
  const sortedNumericColumnarData =
    prewarmed.numericColumnarData || numericColumnarData;
  seedPrecomputedStateFromNumericColumnarData(
    precomputedState,
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
  if (precomputedState && precomputedState.rankByKey) {
    precomputedState.rankByKey[columnKey] = out;
  }
  return out;
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

function sortIndicesByPrecomputedRanks(
  selectedIndices,
  descriptorList,
  rankStates,
  precomputedState
) {
  const count = selectedIndices.length;
  ensureCliPrecomputedState(precomputedState, precomputedState.rowCount, count);

  const primary = precomputedState.primary;
  const secondary = precomputedState.secondary;
  const counts = precomputedState.counts16;

  primary.set(selectedIndices);
  let source = primary;
  let target = secondary;

  for (let d = descriptorList.length - 1; d >= 0; d -= 1) {
    const descriptor = descriptorList[d];
    const rankState = rankStates[d];
    if (
      !rankState ||
      !hasIndexCollection(rankState.rankByRowId) ||
      rankState.rankByRowId.length < precomputedState.rowCount
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

function buildPrecomputedSortedSelection(
  selectedIndices,
  sortedColumn,
  direction,
  rowCount,
  precomputedState,
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
        precomputedState &&
        precomputedState.sortedDescByKey &&
        precomputedState.sortedDescByKey[columnKey] instanceof Uint32Array &&
        precomputedState.sortedDescByKey[columnKey].length === rowCount
      ) {
        return {
          sortedIndices: precomputedState.sortedDescByKey[columnKey],
          dataPath: "indices+precomputed-full-desc-cached",
        };
      }

      const reversed = reverseUint32Array(sortedColumn);
      if (
        columnKey &&
        precomputedState &&
        precomputedState.sortedDescByKey &&
        typeof precomputedState.sortedDescByKey === "object"
      ) {
        precomputedState.sortedDescByKey[columnKey] = reversed;
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

function runCliPrecomputedSortSelection(
  runtime,
  schema,
  descriptorList,
  selectedIndices,
  rowCount,
  isFullSelection,
  precomputedState
) {
  const normalizedDescriptors = Array.isArray(descriptorList) ? descriptorList : [];
  if (!hasIndexCollection(selectedIndices) || normalizedDescriptors.length === 0) {
    return null;
  }

  const totalRows = Math.max(0, Number(rowCount) | 0);
  if (totalRows <= 0) {
    return null;
  }

  const selectionIsFull =
    isFullSelection === true || isIdentitySelection(selectedIndices, totalRows);
  ensureCliPrecomputedState(
    precomputedState,
    totalRows,
    selectedIndices.length
  );

  const coreStartMs = performance.now();
  let sorted = null;

  if (normalizedDescriptors.length === 1) {
    const descriptor = normalizedDescriptors[0];
    const sortedColumn = resolvePrecomputedSortedColumn(
      runtime,
      schema,
      descriptor.columnKey,
      totalRows,
      precomputedState
    );
    if (!(sortedColumn instanceof Uint32Array)) {
      return null;
    }
    sorted = buildPrecomputedSortedSelection(
      selectedIndices,
      sortedColumn,
      descriptor.direction,
      totalRows,
      precomputedState,
      selectionIsFull,
      descriptor.columnKey
    );
  } else {
    if (
      selectionIsFull &&
      precomputedState &&
      precomputedState.fullOrderByDescriptor instanceof Map
    ) {
      const descriptorKey = buildDescriptorCacheKey(normalizedDescriptors);
      const cachedOrder = precomputedState.fullOrderByDescriptor.get(descriptorKey);
      if (cachedOrder instanceof Uint32Array && cachedOrder.length === totalRows) {
        const sortCoreMs = performance.now() - coreStartMs;
        return {
          sortMs: sortCoreMs,
          sortCoreMs,
          sortPrepMs: 0,
          sortTotalMs: sortCoreMs,
          sortMode: "precomputed-ranktuple",
          sortedCount: cachedOrder.length,
          descriptors: normalizedDescriptors,
          dataPath: "indices+precomputed-ranktuple-cli-cached",
          comparatorMode: "precomputed",
        };
      }

      const inverseDescriptorList = tryBuildInverseDescriptorList(
        normalizedDescriptors
      );
      if (inverseDescriptorList) {
        const inverseKey = buildDescriptorCacheKey(inverseDescriptorList);
        const inverseCachedOrder =
          precomputedState.fullOrderByDescriptor.get(inverseKey);
        if (
          inverseCachedOrder instanceof Uint32Array &&
          inverseCachedOrder.length === totalRows
        ) {
          const reversed = reverseUint32Array(inverseCachedOrder);
          precomputedState.fullOrderByDescriptor.set(descriptorKey, reversed);
          const sortCoreMs = performance.now() - coreStartMs;
          return {
            sortMs: sortCoreMs,
            sortCoreMs,
            sortPrepMs: 0,
            sortTotalMs: sortCoreMs,
            sortMode: "precomputed-ranktuple",
            sortedCount: reversed.length,
            descriptors: normalizedDescriptors,
            dataPath: "indices+precomputed-ranktuple-cli-reverse-cache",
            comparatorMode: "precomputed",
          };
        }
      }
    }

    const rankStates = new Array(normalizedDescriptors.length);
    for (let i = 0; i < normalizedDescriptors.length; i += 1) {
      const descriptor = normalizedDescriptors[i];
      rankStates[i] = buildRankStateForColumn(
        runtime,
        schema,
        descriptor.columnKey,
        totalRows,
        precomputedState
      );
      if (!rankStates[i]) {
        return null;
      }
    }

    const rankSorted = sortIndicesByPrecomputedRanks(
      selectedIndices,
      normalizedDescriptors,
      rankStates,
      precomputedState
    );
    if (rankSorted) {
      sorted = {
        sortedIndices: rankSorted,
        dataPath: "indices+precomputed-ranktuple-cli",
      };
      if (
        selectionIsFull &&
        precomputedState &&
        precomputedState.fullOrderByDescriptor instanceof Map
      ) {
        precomputedState.fullOrderByDescriptor.set(
          buildDescriptorCacheKey(normalizedDescriptors),
          rankSorted
        );
      }
    }
  }

  if (!sorted || !hasIndexCollection(sorted.sortedIndices)) {
    return null;
  }

  const sortCoreMs = performance.now() - coreStartMs;
  const sortModeLabel =
    normalizedDescriptors.length === 1 ? "precomputed" : "precomputed-ranktuple";

  return {
    sortMs: sortCoreMs,
    sortCoreMs,
    sortPrepMs: 0,
    sortTotalMs: sortCoreMs,
    sortMode: sortModeLabel,
    sortedCount: sorted.sortedIndices.length,
    descriptors: normalizedDescriptors,
    dataPath: sorted.dataPath,
    comparatorMode: "precomputed",
  };
}

function buildCliSortModes(runtime) {
  const baseModes =
    runtime && typeof runtime.getSortModes === "function"
      ? runtime.getSortModes()
      : ["native"];
  const modeSet = Object.create(null);
  const modes = [];

  for (let i = 0; i < baseModes.length; i += 1) {
    const mode = typeof baseModes[i] === "string" ? baseModes[i].trim() : "";
    if (mode === "" || modeSet[mode] === true) {
      continue;
    }
    modeSet[mode] = true;
    modes.push(mode);
  }

  if (modeSet.precomputed !== true) {
    modeSet.precomputed = true;
    modes.push("precomputed");
  }

  if (modes.length === 0) {
    modes.push("native");
  }

  return modes;
}

function createCliBenchmarkApi(runtime, schema, forcedSortMode) {
  const availableSortModes = buildCliSortModes(runtime);
  const precomputedState = createCliPrecomputedState();
  const normalizedForcedMode =
    typeof forcedSortMode === "string" && forcedSortMode.trim() !== ""
      ? forcedSortMode.trim().toLowerCase()
      : "";
  const effectiveForcedMode =
    normalizedForcedMode !== "" &&
    availableSortModes.includes(normalizedForcedMode)
      ? normalizedForcedMode
      : "";
  const sortBenchmarkOrchestrator = createSortBenchmarkOrchestrator({
    now: () => performance.now(),
    materializeIndices: materializeIndexBuffer,
    runFallbackSort: (rowsSnapshot, descriptorList) =>
      runtime.runSortSnapshotPass(rowsSnapshot, descriptorList, "native"),
    runPrecomputedSort: ({ descriptorList, snapshotIndices, rowCount }) =>
      runCliPrecomputedSortSelection(
        runtime,
        schema,
        descriptorList,
        snapshotIndices,
        rowCount,
        isIdentitySelection(snapshotIndices, rowCount),
        precomputedState
      ),
  });

  return {
    hasData: () => runtime.hasData(),
    getRowCount: () => runtime.getRowCount(),
    getModeOptions: () => runtime.getModeOptions(),
    setModeOptions: (nextOptions, switchOptions) =>
      runtime.setModeOptions(nextOptions, switchOptions),
    getRawFilters: () => runtime.getRawFilters(),
    setRawFilters: (rawFilters) => runtime.setRawFilters(rawFilters),
    setSingleFilter: (columnKey, value) => runtime.setSingleFilter(columnKey, value),
    clearFilters: () => runtime.clearFilters(),
    runFilterPass: (options) => runtime.runFilterPass(options),
    runSingleFilterPass: (columnKey, value, options) =>
      runtime.runSingleFilterPass(columnKey, value, options),
    runFilterPassWithRawFilters: (rawFilters, options) =>
      runtime.runFilterPassWithRawFilters(rawFilters, options),
    getSortModes: () =>
      effectiveForcedMode !== ""
        ? [effectiveForcedMode]
        : availableSortModes.slice(),
    getSortMode: () =>
      effectiveForcedMode !== "" ? effectiveForcedMode : runtime.getSortMode(),
    getSortOptions: () => runtime.getSortOptions(),
    setSortOptions: (nextSortOptions) => runtime.setSortOptions(nextSortOptions),
    buildSortRowsSnapshot: (rawFilters) => runtime.buildSortRowsSnapshot(rawFilters),
    runSortSnapshotPass: (rowsSnapshot, descriptors, sortMode) => {
      const requestedMode =
        typeof sortMode === "string" && sortMode.trim() !== ""
          ? sortMode.trim().toLowerCase()
          : effectiveForcedMode !== ""
            ? effectiveForcedMode
            : runtime.getSortMode();
      if (requestedMode === "precomputed") {
        return sortBenchmarkOrchestrator.runPrecomputedSortSnapshotPass(
          rowsSnapshot,
          descriptors,
          runtime.getRowCount()
        );
      }

      return runtime.runSortSnapshotPass(rowsSnapshot, descriptors, requestedMode);
    },
    prewarmPrecomputedSortState: () => {
      const rowCount = Math.max(0, Number(runtime.getRowCount()) | 0);
      ensureCliPrecomputedState(precomputedState, rowCount, rowCount);
      precomputedState.sortedByKey = Object.create(null);
      precomputedState.sortedDescByKey = Object.create(null);
      precomputedState.rankByKey = Object.create(null);
      precomputedState.fullOrderByDescriptor = new Map();
      const numericColumnarData = runtime.getNumericColumnarForSave();
      if (!numericColumnarData || typeof numericColumnarData !== "object") {
        return false;
      }
      const prewarmed =
        ensureNumericColumnarSortedRanks(numericColumnarData, schema) || {};
      const sortedNumericColumnarData =
        prewarmed.numericColumnarData || numericColumnarData;
      seedPrecomputedStateFromNumericColumnarData(
        precomputedState,
        sortedNumericColumnarData,
        schema,
        rowCount
      );
      return Object.keys(precomputedState.rankByKey).length > 0;
    },
    isTimSortAvailable: () => availableSortModes.includes("timsort"),
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printHelp();
    return;
  }

  if (
    (!Number.isInteger(args.preset) || args.preset <= 0) &&
    args.generateWorkers <= 0
  ) {
    throw new Error("Invalid --preset value.");
  }
  if (
    args.bench !== "filtering" &&
    args.bench !== "sorting" &&
    args.bench !== "both"
  ) {
    throw new Error("Invalid --bench value. Use filtering, sorting, or both.");
  }

  const runtime = createFastTableRuntime();
  const schema = runtime.getSchema();
  if (args.generateWorkers > 0) {
    console.log(
      `Generating ${args.generateWorkers.toLocaleString(
        "en-US"
      )} rows with worker_threads...`
    );
    const generated = await fastTableGenerationWorkersNodeApi.generateRowsWithWorkers(
      {
        rowCount: args.generateWorkers,
        workerCount: args.workers,
        chunkSize: args.chunkSize,
      }
    );
    runtime.setDataFromNumericColumnar(generated.derivedData.numericColumnarData);
    console.log(
      `Generated ${runtime
        .getRowCount()
        .toLocaleString("en-US")} rows in ${generated.wallMs.toFixed(2)} ms.`
    );
  } else {
    const presetDir = path.resolve(args.presetDir);
    console.log(`Loading preset ${args.preset} from ${presetDir}...`);
    const loaded = await loadColumnarBinaryPreset({
      schema,
      presetDir,
      rowCount: args.preset,
    });
    runtime.setDataFromNumericColumnar(loaded.numericColumnarData);
    console.log(`Loaded ${runtime.getRowCount().toLocaleString("en-US")} rows.`);
  }

  if (args.precomputeSortWorkers) {
    const numericColumnarData = runtime.getNumericColumnarForSave();
    if (!numericColumnarData) {
      throw new Error("No numeric columnar data available for sort precompute.");
    }

    console.log(
      `Precomputing sorted indices with ${args.workers} worker(s)...`
    );
    const sortPrecompute =
      await fastTableGenerationWorkersNodeApi.buildSortedIndicesWithWorkers({
        numericColumnarData,
        workerCount: args.workers,
      });

    numericColumnarData.sortedIndexColumns = sortPrecompute.sortedIndexColumns;
    numericColumnarData.sortedIndexByKey = sortPrecompute.sortedIndexByKey;
    runtime.setDataFromNumericColumnar(numericColumnarData);

    console.log(
      `Precomputed ${sortPrecompute.completedColumns}/${
        sortPrecompute.totalColumns
      } sortable columns in ${sortPrecompute.durationMs.toFixed(2)} ms.`
    );
  }

  const availableSortModes = buildCliSortModes(runtime);
  if (
    typeof args.sortMode === "string" &&
    args.sortMode.trim() !== "" &&
    !availableSortModes.includes(args.sortMode)
  ) {
    throw new Error(
      `Invalid --sort-mode value: ${args.sortMode}. Available: ${availableSortModes.join(
        ", "
      )}.`
    );
  }
  const benchmarkApi = createCliBenchmarkApi(runtime, schema, args.sortMode);
  if (
    (args.bench === "sorting" || args.bench === "both") &&
    benchmarkApi &&
    typeof benchmarkApi.getSortModes === "function" &&
    benchmarkApi.getSortModes().includes("precomputed") &&
    typeof benchmarkApi.prewarmPrecomputedSortState === "function"
  ) {
    benchmarkApi.prewarmPrecomputedSortState();
  }

  const outputSections = [];

  if (args.bench === "filtering" || args.bench === "both") {
    console.log("");
    console.log("Running filtering benchmark...");
    const filtering = await runFilteringBenchmark({
      api: benchmarkApi,
      currentOnly: args.currentOnly,
      rounds: args.rounds,
      onUpdate: createLinePrinter("filter"),
    });
    outputSections.push(filtering.lines.join("\n"));
    if (filtering.error) {
      throw new Error(
        `Filtering benchmark failed: ${String(
          filtering.error && filtering.error.message
            ? filtering.error.message
            : filtering.error
        )}`
      );
    }
  }

  if (args.bench === "sorting" || args.bench === "both") {
    console.log("");
    console.log("Running sorting benchmark...");
    const sorting = await runSortBenchmark({
      api: benchmarkApi,
      currentOnly: args.currentOnly,
      rounds: args.rounds,
      onUpdate: createLinePrinter("sort"),
    });
    outputSections.push(sorting.lines.join("\n"));
    if (sorting.error) {
      throw new Error(
        `Sorting benchmark failed: ${String(
          sorting.error && sorting.error.message
            ? sorting.error.message
            : sorting.error
        )}`
      );
    }
  }

  await maybeWriteOutput(args.out, outputSections);
}

main().catch((error) => {
  console.error(
    `CLI failed: ${String(error && error.message ? error.message : error)}`
  );
  process.exitCode = 1;
});
