import { runFilteringBenchmark } from "@fasttable/core";
import { bindBenchmarkUi } from "./benchmark-ui-browser.js";

(function () {
  if (typeof document === "undefined") {
    return;
  }

  const benchmarkBtnEl = document.getElementById("benchmarkBtn");
  const benchmarkCurrentBtnEl = document.getElementById("benchmarkCurrentBtn");
  const benchmarkStatusEl = document.getElementById("benchmarkStatus");

  bindBenchmarkUi({
    primaryBtnEl: benchmarkBtnEl,
    currentBtnEl: benchmarkCurrentBtnEl,
    statusEl: benchmarkStatusEl,
    linePrefix: "filter",
    emptyMessage: "Benchmark API not available.",
    runBenchmark({ api, currentOnly, delayTick, now, onUpdate }) {
      return runFilteringBenchmark({
        api,
        currentOnly,
        delayTick,
        now,
        onUpdate,
      });
    },
  });
})();
