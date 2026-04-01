import { promises as fs } from "node:fs";
import path from "node:path";
import { createFastTableRuntime } from "./packages/core/dist/runtime.js";
import { loadColumnarBinaryPreset } from "./packages/core/dist/io-node.js";
import { ensureNumericColumnarSortedIndices } from "./packages/core/dist/io.js";
import { fastTableGenerationWorkersNodeApi } from "./packages/core/dist/generation-workers-node.js";
import {
  runFilteringBenchmark,
  runSortBenchmark,
} from "./packages/core/dist/benchmark.js";

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

function normalizeSortDescriptors(descriptors) {
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

function isNumericSortColumnKey(columnKey) {
  return columnKey !== "firstName" && columnKey !== "lastName";
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
    rankByKey: Object.create(null),
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
    state.rankByKey = Object.create(null);
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
    hasIndexCollection(precomputedState.rankByKey[columnKey].rankByRowId) &&
    precomputedState.rankByKey[columnKey].rankByRowId.length === rowCount
  ) {
    return precomputedState.rankByKey[columnKey];
  }

  const numericColumnarData = runtime.getNumericColumnarForSave();
  if (!numericColumnarData || typeof numericColumnarData !== "object") {
    return null;
  }

  const columnIndex = getColumnIndexByKey(schema, columnKey);
  if (columnIndex < 0) {
    return null;
  }

  const sortedIndices = resolvePrecomputedSortedColumn(
    runtime,
    schema,
    columnKey,
    rowCount,
    precomputedState
  );
  if (!(sortedIndices instanceof Uint32Array) || sortedIndices.length !== rowCount) {
    return null;
  }

  const columns = Array.isArray(numericColumnarData.columns)
    ? numericColumnarData.columns
    : [];
  const dictionaries = Array.isArray(numericColumnarData.dictionaries)
    ? numericColumnarData.dictionaries
    : [];
  const values = columns[columnIndex];
  const hasDictionary =
    Array.isArray(dictionaries[columnIndex]) && dictionaries[columnIndex].length > 0;
  const useNumeric = isNumericSortColumnKey(columnKey) && !hasDictionary;

  const rank32 = new Uint32Array(rowCount);
  if (rowCount === 0) {
    const empty = { rankByRowId: rank32, maxRank: 0 };
    if (precomputedState && precomputedState.rankByKey) {
      precomputedState.rankByKey[columnKey] = empty;
    }
    return empty;
  }

  const firstRowIndex = Number(sortedIndices[0]) >>> 0;
  let previousToken = values ? values[firstRowIndex] : undefined;
  let currentRank = 0;
  rank32[firstRowIndex] = currentRank;

  for (let i = 1; i < rowCount; i += 1) {
    const rowIndex = Number(sortedIndices[i]) >>> 0;
    const nextToken = values ? values[rowIndex] : undefined;
    let isSame = false;

    if (hasDictionary) {
      isSame = nextToken === previousToken;
    } else if (useNumeric) {
      const previousNumber = Number(previousToken);
      const nextNumber = Number(nextToken);
      isSame =
        (Number.isNaN(previousNumber) && Number.isNaN(nextNumber)) ||
        previousNumber === nextNumber;
    } else {
      isSame = nextToken === previousToken;
    }

    if (!isSame) {
      currentRank += 1;
      previousToken = nextToken;
    }

    rank32[rowIndex] = currentRank;
  }

  const rankByRowId =
    currentRank <= 0xffff ? new Uint16Array(rank32) : rank32;
  const out = { rankByRowId, maxRank: currentRank >>> 0 };
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
  rowCount
) {
  const count = selectedIndices.length;
  if (count === 0) {
    return {
      sortedIndices: new Uint32Array(0),
      dataPath: "indices+precomputed-empty",
    };
  }

  const fullSelection = isIdentitySelection(selectedIndices, rowCount);
  if (fullSelection) {
    if (direction === "desc") {
      const reversed = new Uint32Array(rowCount);
      for (let i = 0; i < rowCount; i += 1) {
        reversed[i] = sortedColumn[rowCount - 1 - i];
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

function runCliPrecomputedSortSnapshotPass(
  runtime,
  schema,
  rowsSnapshot,
  descriptors,
  precomputedState
) {
  const descriptorList = normalizeSortDescriptors(descriptors);
  const fallback = () =>
    runtime.runSortSnapshotPass(rowsSnapshot, descriptorList, "native");
  if (descriptorList.length === 0) {
    return fallback();
  }

  const rowCount = runtime.getRowCount();
  ensureCliPrecomputedState(precomputedState, rowCount, rowCount);

  const sourceIndices =
    Array.isArray(rowsSnapshot) && hasIndexCollection(rowsSnapshot.__rowIndices)
      ? rowsSnapshot.__rowIndices
      : rowsSnapshot &&
          typeof rowsSnapshot === "object" &&
          hasIndexCollection(rowsSnapshot.rowIndices)
        ? rowsSnapshot.rowIndices
        : null;
  if (!hasIndexCollection(sourceIndices)) {
    return fallback();
  }

  const selectedIndices = materializeIndexBuffer(sourceIndices, rowCount);
  const coreStartMs = performance.now();
  let sorted = null;

  if (descriptorList.length === 1) {
    const descriptor = descriptorList[0];
    const sortedColumn = resolvePrecomputedSortedColumn(
      runtime,
      schema,
      descriptor.columnKey,
      rowCount,
      precomputedState
    );
    if (!(sortedColumn instanceof Uint32Array)) {
      return fallback();
    }
    sorted = buildPrecomputedSortedSelection(
      selectedIndices,
      sortedColumn,
      descriptor.direction,
      rowCount
    );
  } else {
    const rankStates = new Array(descriptorList.length);
    for (let i = 0; i < descriptorList.length; i += 1) {
      const descriptor = descriptorList[i];
      rankStates[i] = buildRankStateForColumn(
        runtime,
        schema,
        descriptor.columnKey,
        rowCount,
        precomputedState
      );
      if (!rankStates[i]) {
        return fallback();
      }
    }

    const rankSorted = sortIndicesByPrecomputedRanks(
      selectedIndices,
      descriptorList,
      rankStates,
      precomputedState
    );
    if (rankSorted) {
      sorted = {
        sortedIndices: rankSorted,
        dataPath: "indices+precomputed-ranktuple-cli",
      };
    }
  }

  if (!sorted || !hasIndexCollection(sorted.sortedIndices)) {
    return fallback();
  }
  const sortCoreMs = performance.now() - coreStartMs;
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
        return runCliPrecomputedSortSnapshotPass(
          runtime,
          schema,
          rowsSnapshot,
          descriptors,
          precomputedState
        );
      }

      return runtime.runSortSnapshotPass(rowsSnapshot, descriptors, requestedMode);
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
