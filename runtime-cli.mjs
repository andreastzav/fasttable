import path from "node:path";
import { createFastTableRuntime } from "./packages/core/dist/runtime.js";
import { createFastTableEngine } from "./packages/core/dist/engine.js";
import { loadColumnarBinaryPreset } from "./packages/core/dist/io-node.js";
import { fastTableGenerationWorkersNodeApi } from "./packages/core/dist/generation-workers-node.js";

function parseArgs(argv) {
  const args = {
    op: "",
    preset: 1000000,
    presetDir: "./tables_presets",
    filters: "",
    sort: "",
    sortMode: "",
    workers: 4,
    chunkSize: 10000,
    generateWorkers: 0,
    precomputeSortWorkers: false,
    preferPrecomputedFastPath: true,
    json: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const token = String(argv[i] || "");
    if (token === "--op" && i + 1 < argv.length) {
      args.op = String(argv[i + 1]).trim().toLowerCase();
      i += 1;
      continue;
    }
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
    if (token === "--filters" && i + 1 < argv.length) {
      args.filters = String(argv[i + 1]);
      i += 1;
      continue;
    }
    if (token === "--sort" && i + 1 < argv.length) {
      args.sort = String(argv[i + 1]);
      i += 1;
      continue;
    }
    if (token === "--sort-mode" && i + 1 < argv.length) {
      args.sortMode = String(argv[i + 1]).trim().toLowerCase();
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
    if (token === "--no-prefer-precomputed-fast-path") {
      args.preferPrecomputedFastPath = false;
      continue;
    }
    if (token === "--prefer-precomputed-fast-path") {
      args.preferPrecomputedFastPath = true;
      continue;
    }
    if (token === "--json") {
      args.json = true;
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
  console.log("FastTable runtime one-shot CLI");
  console.log("");
  console.log("Usage:");
  console.log("  node runtime-cli.mjs --op <filter|sort|filter-sort> [options]");
  console.log("");
  console.log("Options:");
  console.log("  --op <name>             filter | sort | filter-sort");
  console.log("  --filters <k=v,...>     Filter map for filter/filter-sort operations");
  console.log("  --sort <k:dir,...>      Sort descriptors for sort/filter-sort (dir: asc|desc)");
  console.log("  --sort-mode <name>      native | timsort | precomputed");
  console.log("  --preset <rows>         Preset row count (default: 1000000)");
  console.log("  --preset-dir <path>     Preset directory (default: ./tables_presets)");
  console.log("  --generate-workers <n>  Generate n rows with worker_threads (skip preset load)");
  console.log("  --workers <n>           Worker count for generation/precompute (default: 4)");
  console.log("  --chunk-size <n>        Worker generation chunk size (default: 10000)");
  console.log("  --precompute-sort-workers");
  console.log("                          Precompute sorted indices using worker_threads");
  console.log("  --no-prefer-precomputed-fast-path");
  console.log("                          Disable precomputed-fast-path preference policy");
  console.log("  --json                  Output JSON payload instead of human text");
  console.log("  -h, --help              Show this help");
}

function formatCount(value) {
  return Number(value || 0).toLocaleString("en-US");
}

function formatMs(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "0.00";
  }
  return numeric.toFixed(2);
}

function parseFilterMap(value) {
  const text = typeof value === "string" ? value.trim() : "";
  if (text === "") {
    return {};
  }

  const out = {};
  const parts = text.split(",");
  for (let i = 0; i < parts.length; i += 1) {
    const token = String(parts[i] || "").trim();
    if (token === "") {
      continue;
    }
    const equalsIndex = token.indexOf("=");
    if (equalsIndex <= 0) {
      throw new Error(`Invalid --filters entry: ${token}`);
    }
    const key = token.slice(0, equalsIndex).trim();
    const rawValue = token.slice(equalsIndex + 1);
    if (key === "") {
      throw new Error(`Invalid --filters entry: ${token}`);
    }
    out[key] = rawValue;
  }

  return out;
}

function parseSortDescriptors(value) {
  const text = typeof value === "string" ? value.trim() : "";
  if (text === "") {
    return [];
  }

  const out = [];
  const parts = text.split(",");
  for (let i = 0; i < parts.length; i += 1) {
    const token = String(parts[i] || "").trim();
    if (token === "") {
      continue;
    }
    const colonIndex = token.indexOf(":");
    if (colonIndex <= 0) {
      throw new Error(`Invalid --sort entry: ${token}`);
    }
    const columnKey = token.slice(0, colonIndex).trim();
    const direction = token.slice(colonIndex + 1).trim().toLowerCase();
    if (columnKey === "") {
      throw new Error(`Invalid --sort entry: ${token}`);
    }
    if (direction !== "asc" && direction !== "desc") {
      throw new Error(
        `Invalid sort direction for ${columnKey}: ${direction} (use asc|desc)`
      );
    }
    out.push({ columnKey, direction });
  }

  return out;
}

function stringifyDescriptors(descriptors) {
  const source = Array.isArray(descriptors) ? descriptors : [];
  if (source.length === 0) {
    return "(none)";
  }
  return source
    .map((item) => `${item.columnKey}:${item.direction}`)
    .join(", ");
}

function createCliEngine(runtime) {
  return createFastTableEngine({ runtime });
}

async function loadRuntimeData(engine, runtime, args, log) {
  const schema = runtime.getSchema();
  if (args.generateWorkers > 0) {
    log(
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
    engine.setDataFromNumericColumnar(generated.derivedData.numericColumnarData);
    log(
      `Generated ${engine
        .getRowCount()
        .toLocaleString("en-US")} rows in ${generated.wallMs.toFixed(2)} ms.`
    );
    return;
  }

  const presetDir = path.resolve(args.presetDir);
  log(`Loading preset ${args.preset} from ${presetDir}...`);
  const loaded = await loadColumnarBinaryPreset({
    schema,
    presetDir,
    rowCount: args.preset,
  });
  engine.setDataFromNumericColumnar(loaded.numericColumnarData);
  log(`Loaded ${engine.getRowCount().toLocaleString("en-US")} rows.`);
}

async function maybePrecomputeSortWorkers(engine, args, log) {
  if (!args.precomputeSortWorkers) {
    return;
  }

  const numericColumnarData = engine.getNumericColumnarForSave();
  if (!numericColumnarData) {
    throw new Error("No numeric columnar data available for sort precompute.");
  }

  log(`Precomputing sorted indices with ${args.workers} worker(s)...`);
  const sortPrecompute =
    await fastTableGenerationWorkersNodeApi.buildSortedIndicesWithWorkers({
      numericColumnarData,
      workerCount: args.workers,
    });

  numericColumnarData.sortedIndexColumns = sortPrecompute.sortedIndexColumns;
  numericColumnarData.sortedIndexByKey = sortPrecompute.sortedIndexByKey;
  engine.setDataFromNumericColumnar(numericColumnarData);
  log(
    `Precomputed ${sortPrecompute.completedColumns}/${sortPrecompute.totalColumns} sortable columns in ${sortPrecompute.durationMs.toFixed(2)} ms.`
  );
}

function buildHumanOutput(payload) {
  const lines = [];
  lines.push(`Operation: ${payload.operation}`);
  lines.push(`Rows loaded: ${formatCount(payload.rowCount)}`);

  if (payload.operation === "filter" || payload.operation === "filter-sort") {
    lines.push(`Filters: ${Object.keys(payload.filters).length}`);
    lines.push(`Filtered rows: ${formatCount(payload.filter.filteredCount)}`);
    lines.push(`Filter core: ${formatMs(payload.filter.coreMs)} ms`);
  }

  if (payload.operation === "sort" || payload.operation === "filter-sort") {
    lines.push(`Sort mode: ${payload.sort.sortMode}`);
    lines.push(`Descriptors: ${stringifyDescriptors(payload.sort.descriptors)}`);
    lines.push(`Sorted rows: ${formatCount(payload.sort.sortedCount)}`);
    lines.push(`Sort core: ${formatMs(payload.sort.sortCoreMs)} ms`);
    lines.push(`Sort prep: ${formatMs(payload.sort.sortPrepMs)} ms`);
    lines.push(`Sort total: ${formatMs(payload.sort.sortTotalMs)} ms`);
  }

  return lines.join("\n");
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printHelp();
    return;
  }

  if (args.op !== "filter" && args.op !== "sort" && args.op !== "filter-sort") {
    throw new Error("Invalid --op. Use filter, sort, or filter-sort.");
  }

  if (
    (!Number.isInteger(args.preset) || args.preset <= 0) &&
    args.generateWorkers <= 0
  ) {
    throw new Error("Invalid --preset value.");
  }

  if ((args.op === "filter" || args.op === "filter-sort") && args.filters.trim() === "") {
    throw new Error("Missing --filters for filter/filter-sort operation.");
  }

  if ((args.op === "sort" || args.op === "filter-sort") && args.sort.trim() === "") {
    throw new Error("Missing --sort for sort/filter-sort operation.");
  }

  const quiet = args.json === true;
  const log = quiet ? () => {} : (line) => console.log(line);

  const runtime = createFastTableRuntime();
  const engine = createCliEngine(runtime);
  await loadRuntimeData(engine, runtime, args, log);
  await maybePrecomputeSortWorkers(engine, args, log);

  const runtimeOperations = engine.createRuntimeOperations({
    getRowCount() {
      return engine.getRowCount();
    },
    getRawFilters() {
      return {};
    },
    defaultPreferPrecomputedFastPath: args.preferPrecomputedFastPath === true,
  });

  const descriptors = parseSortDescriptors(args.sort);
  const filters = parseFilterMap(args.filters);
  const effectiveSortMode = runtimeOperations.ensureSortMode(args.sortMode);
  const rowCount = engine.getRowCount();

  let payload = null;

  if (args.op === "filter") {
    const filterCoreRun = runtimeOperations.runFilterCore(filters, {
      rawFilters: filters,
      skipRender: true,
      skipStatus: true,
      keepScroll: false,
      preferPrecomputedFastPath: args.preferPrecomputedFastPath === true,
    });
    if (
      !filterCoreRun ||
      filterCoreRun.kind !== "ok" ||
      !filterCoreRun.orchestration ||
      !filterCoreRun.orchestration.filterResult
    ) {
      throw new Error("Filter core execution failed.");
    }
    const filterRun = filterCoreRun.orchestration.filterResult;

    payload = {
      operation: "filter",
      rowCount,
      filters,
      filter: {
        filteredCount: Number(filterRun.filteredCount) || 0,
        coreMs: Number(filterRun.coreMs) || 0,
        modePath:
          typeof filterRun.modePath === "string" ? filterRun.modePath : "",
      },
    };
  } else if (args.op === "sort") {
    const sortCoreRun = runtimeOperations.runSortCore(null, {
      rawFilters: {},
      descriptors,
      sortMode: effectiveSortMode,
      preferPrecomputedFastPath: args.preferPrecomputedFastPath === true,
      skipRender: true,
      skipStatus: true,
    });
    if (
      !sortCoreRun ||
      sortCoreRun.kind !== "ok" ||
      !sortCoreRun.sortRun ||
      !sortCoreRun.sortRun.result
    ) {
      throw new Error("Sort core execution failed.");
    }
    const sortRun = sortCoreRun.sortRun;

    payload = {
      operation: "sort",
      rowCount,
      sort: {
        sortMode:
          sortRun.result &&
          typeof sortRun.result.sortMode === "string" &&
          sortRun.result.sortMode !== ""
            ? sortRun.result.sortMode
            : effectiveSortMode,
        descriptors: Array.isArray(descriptors) ? descriptors : [],
        sortedCount: Number(sortRun.sortedCount) || 0,
        sortCoreMs: Number(sortRun.result ? sortRun.result.durationMs : 0) || 0,
        sortPrepMs: Number(sortRun.sortPrepMs) || 0,
        sortTotalMs: Number(sortRun.sortTotalMs) || 0,
      },
    };
  } else {
    const filterSortRun = runtimeOperations.runFilterSortCore(filters, {
      rawFilters: filters,
      descriptors,
      sortMode: effectiveSortMode,
      preferPrecomputedFastPath: args.preferPrecomputedFastPath === true,
      skipRender: true,
      skipStatus: true,
      keepScroll: false,
    });
    if (
      !filterSortRun ||
      !filterSortRun.filterRun ||
      filterSortRun.filterRun.kind !== "ok" ||
      !filterSortRun.filterRun.orchestration ||
      !filterSortRun.filterRun.orchestration.filterResult ||
      !filterSortRun.sortRun ||
      filterSortRun.sortRun.kind !== "ok" ||
      !filterSortRun.sortRun.sortRun ||
      !filterSortRun.sortRun.sortRun.result
    ) {
      if (
        filterSortRun &&
        filterSortRun.filterRun &&
        filterSortRun.filterRun.kind !== "ok"
      ) {
        throw new Error("Filter core execution failed.");
      }
      if (
        filterSortRun &&
        filterSortRun.sortRun &&
        filterSortRun.sortRun.kind !== "ok"
      ) {
        throw new Error("Sort core execution failed.");
      }
      throw new Error("Filter/sort core execution failed.");
    }
    const filterRun = filterSortRun.filterRun.orchestration.filterResult;
    const sortRun = filterSortRun.sortRun.sortRun;

    payload = {
      operation: "filter-sort",
      rowCount,
      filters,
      filter: {
        filteredCount: Number(filterRun.filteredCount) || 0,
        coreMs: Number(filterRun.coreMs) || 0,
        modePath:
          typeof filterRun.modePath === "string" ? filterRun.modePath : "",
      },
      sort: {
        sortMode:
          sortRun.result &&
          typeof sortRun.result.sortMode === "string" &&
          sortRun.result.sortMode !== ""
            ? sortRun.result.sortMode
            : effectiveSortMode,
        descriptors: Array.isArray(descriptors) ? descriptors : [],
        sortedCount: Number(sortRun.sortedCount) || 0,
        sortCoreMs: Number(sortRun.result ? sortRun.result.durationMs : 0) || 0,
        sortPrepMs: Number(sortRun.sortPrepMs) || 0,
        sortTotalMs: Number(sortRun.sortTotalMs) || 0,
      },
    };
  }

  if (args.json) {
    console.log(JSON.stringify(payload, null, 2));
    return;
  }

  console.log("");
  console.log(buildHumanOutput(payload));
}

main().catch((error) => {
  console.error(
    `CLI failed: ${String(error && error.message ? error.message : error)}`
  );
  process.exitCode = 1;
});
