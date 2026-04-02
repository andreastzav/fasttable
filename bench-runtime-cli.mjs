import { promises as fs } from "node:fs";
import path from "node:path";
import { createFastTableRuntime } from "./packages/core/dist/runtime.js";
import { createFastTableEngine } from "./packages/core/dist/engine.js";
import { loadColumnarBinaryPreset } from "./packages/core/dist/io-node.js";
import { fastTableGenerationWorkersNodeApi } from "./packages/core/dist/generation-workers-node.js";
import {
  createBenchmarkDelayTick,
  resolveBenchmarkTickPolicy,
  runFilteringBenchmark,
  runSortBenchmark,
} from "./packages/core/dist/benchmark.js";
import { createSortBenchmarkRuntimeBridge } from "./packages/core/dist/sort-benchmark-runtime.js";

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
    tickPolicy: "micro",
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
    if (token === "--tick" && i + 1 < argv.length) {
      args.tickPolicy = String(argv[i + 1]).trim().toLowerCase();
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
  console.log("  --tick <policy>        micro | macro (benchmark delay tick policy, default: micro)");
  console.log("  --precompute-sort-workers");
  console.log("                         Precompute sorted indices using worker_threads");
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

  if (modes.length === 0) {
    modes.push("native");
  }

  return modes;
}

function isValidNumericColumnarData(data) {
  return (
    data &&
    typeof data === "object" &&
    Number.isInteger(data.rowCount) &&
    data.rowCount >= 0 &&
    Array.isArray(data.columns)
  );
}

function createCliBenchmarkApi(runtime, forcedSortMode) {
  const availableSortModes = buildCliSortModes(runtime);
  const normalizedForcedMode =
    typeof forcedSortMode === "string" && forcedSortMode.trim() !== ""
      ? forcedSortMode.trim().toLowerCase()
      : "";
  const effectiveForcedMode =
    normalizedForcedMode !== "" &&
    availableSortModes.includes(normalizedForcedMode)
      ? normalizedForcedMode
      : "";

  const sortBenchmarkRuntimeBridge = createSortBenchmarkRuntimeBridge({
    runtime,
    readRawFilters: () => runtime.getRawFilters(),
    getRowCount: () => runtime.getRowCount(),
    getModeOptions: () => runtime.getModeOptions(),
    getSortOptions: () => runtime.getSortOptions(),
    getSortMode: () =>
      effectiveForcedMode !== "" ? effectiveForcedMode : runtime.getSortMode(),
    getNumericColumnarData: () => runtime.getNumericColumnarForSave(),
    isValidNumericColumnarData,
  });
  const engine = createFastTableEngine({
    adapters: {
      hasData: () => runtime.hasData(),
      getRowCount: () => runtime.getRowCount(),
      getModeOptions: () => runtime.getModeOptions(),
      setModeOptions: (nextOptions, switchOptions) =>
        runtime.setModeOptions(nextOptions, switchOptions),
      getRawFilters: () => runtime.getRawFilters(),
      setRawFilters: (rawFilters) => runtime.setRawFilters(rawFilters),
      setSingleFilter: (columnKey, value) =>
        runtime.setSingleFilter(columnKey, value),
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
      setSortOptions: (nextSortOptions) =>
        runtime.setSortOptions(nextSortOptions),
      buildSortRowsSnapshot: (rawFilters) =>
        sortBenchmarkRuntimeBridge.buildSortRowsSnapshot(rawFilters),
      runSortSnapshotPass: (rowsSnapshot, descriptors, sortMode) => {
        const requestedMode =
          typeof sortMode === "string" && sortMode.trim() !== ""
            ? sortMode.trim().toLowerCase()
            : effectiveForcedMode !== ""
              ? effectiveForcedMode
              : runtime.getSortMode();
        return sortBenchmarkRuntimeBridge.runSortSnapshotPass(
          rowsSnapshot,
          descriptors,
          requestedMode
        );
      },
      prewarmPrecomputedSortState: () =>
        sortBenchmarkRuntimeBridge.prewarmPrecomputedSortState(),
      isTimSortAvailable: () => availableSortModes.includes("timsort"),
    },
  });

  return engine.createBenchmarkApi();
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

  const benchmarkApi = createCliBenchmarkApi(runtime, args.sortMode);
  const delayTick = createBenchmarkDelayTick(
    resolveBenchmarkTickPolicy(args.tickPolicy, "micro")
  );

  const outputSections = [];

  if (args.bench === "filtering" || args.bench === "both") {
    console.log("");
    console.log("Running filtering benchmark...");
    const filtering = await runFilteringBenchmark({
      api: benchmarkApi,
      currentOnly: args.currentOnly,
      rounds: args.rounds,
      delayTick,
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
      delayTick,
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
