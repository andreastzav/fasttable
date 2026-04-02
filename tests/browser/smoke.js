import {
  createFastTableRuntime,
  runFilteringBenchmark,
  runSortBenchmark,
} from "@fasttable/core";

const statusEl = document.getElementById("status");
const outputEl = document.getElementById("output");

function log(line) {
  outputEl.textContent += `${line}\n`;
}

async function run() {
  const runtime = createFastTableRuntime();
  runtime.generate(8000);

  log(`Generated rows: ${runtime.getRowCount().toLocaleString("en-US")}`);
  log("Running filtering benchmark smoke...");
  const filtering = await runFilteringBenchmark({
    api: runtime,
    currentOnly: true,
    rounds: 1,
    onUpdate(lines) {
      const latest = lines && lines.length > 0 ? lines[lines.length - 1] : "";
      if (latest) {
        log(`[filter] ${latest}`);
      }
    },
  });
  if (filtering.error) {
    throw filtering.error;
  }

  log("");
  log("Running sorting benchmark smoke...");
  const sorting = await runSortBenchmark({
    api: runtime,
    currentOnly: true,
    rounds: 1,
    onUpdate(lines) {
      const latest = lines && lines.length > 0 ? lines[lines.length - 1] : "";
      if (latest) {
        log(`[sort] ${latest}`);
      }
    },
  });
  if (sorting.error) {
    throw sorting.error;
  }

  return {
    filteringLines: filtering.lines.length,
    sortingLines: sorting.lines.length,
  };
}

run()
  .then((result) => {
    statusEl.textContent = "PASS";
    statusEl.style.color = "#0a7a0a";
    log("");
    log(
      `Browser smoke passed. Filter lines: ${result.filteringLines}, sort lines: ${result.sortingLines}.`
    );
    window.fastTableBrowserSmokeResult = {
      ok: true,
      result,
    };
  })
  .catch((error) => {
    statusEl.textContent = "FAIL";
    statusEl.style.color = "#b30000";
    log("");
    log(`Browser smoke failed: ${String(error && error.message ? error.message : error)}`);
    window.fastTableBrowserSmokeResult = {
      ok: false,
      error: String(error && error.message ? error.message : error),
    };
  });
