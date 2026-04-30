import path from "node:path";
import { loadColumnarBinaryPreset } from "./packages/core/dist/io-node.js";
import { fastTableGenerationWorkersNodeApi } from "./packages/core/dist/generation-workers-node.js";

function consumeSharedDataOption(argv, index, args) {
  if (!Array.isArray(argv) || !args || typeof args !== "object") {
    return null;
  }

  const token = String(argv[index] || "");
  if (token === "--preset" && index + 1 < argv.length) {
    args.preset = Number.parseInt(String(argv[index + 1]), 10);
    return index + 1;
  }
  if (token === "--preset-dir" && index + 1 < argv.length) {
    args.presetDir = String(argv[index + 1]);
    return index + 1;
  }
  if (token === "--workers" && index + 1 < argv.length) {
    args.workers = Math.max(1, Number.parseInt(String(argv[index + 1]), 10) || 1);
    return index + 1;
  }
  if (token === "--chunk-size" && index + 1 < argv.length) {
    args.chunkSize = Math.max(
      1,
      Number.parseInt(String(argv[index + 1]), 10) || 1
    );
    return index + 1;
  }
  if (token === "--generate-workers" && index + 1 < argv.length) {
    args.generateWorkers = Math.max(
      0,
      Number.parseInt(String(argv[index + 1]), 10) || 0
    );
    return index + 1;
  }
  if (token === "--precompute-sort-workers") {
    args.precomputeSortWorkers = true;
    return index;
  }

  return null;
}

async function loadRuntimeDataFromArgs(engine, runtime, args, log) {
  const writeLine = typeof log === "function" ? log : () => {};
  const schema = runtime.getSchema();
  if (args.generateWorkers > 0) {
    writeLine(
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
    writeLine(
      `Generated ${engine
        .getRowCount()
        .toLocaleString("en-US")} rows in ${generated.wallMs.toFixed(2)} ms.`
    );
    return;
  }

  const presetDir = path.resolve(args.presetDir);
  writeLine(`Loading preset ${args.preset} from ${presetDir}...`);
  const loaded = await loadColumnarBinaryPreset({
    schema,
    presetDir,
    rowCount: args.preset,
  });
  engine.setDataFromNumericColumnar(loaded.numericColumnarData);
  writeLine(`Loaded ${engine.getRowCount().toLocaleString("en-US")} rows.`);
}

async function precomputeSortWorkersFromArgs(engine, args, log) {
  if (!args.precomputeSortWorkers) {
    return;
  }

  const writeLine = typeof log === "function" ? log : () => {};
  const numericColumnarData = engine.getNumericColumnarForSave();
  if (!numericColumnarData) {
    throw new Error("No numeric columnar data available for sort precompute.");
  }

  writeLine(`Precomputing sorted indices with ${args.workers} worker(s)...`);
  const sortPrecompute =
    await fastTableGenerationWorkersNodeApi.buildSortedIndicesWithWorkers({
      numericColumnarData,
      workerCount: args.workers,
    });

  numericColumnarData.sortedIndexColumns = sortPrecompute.sortedIndexColumns;
  numericColumnarData.sortedIndexByKey = sortPrecompute.sortedIndexByKey;
  engine.setDataFromNumericColumnar(numericColumnarData);
  writeLine(
    `Precomputed ${sortPrecompute.completedColumns}/${sortPrecompute.totalColumns} sortable columns in ${sortPrecompute.durationMs.toFixed(2)} ms.`
  );
}

export {
  consumeSharedDataOption,
  loadRuntimeDataFromArgs,
  precomputeSortWorkersFromArgs,
};
