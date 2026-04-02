import { runSortBenchmark } from "@fasttable/core";
import { bindBenchmarkUi } from "./benchmark-ui-browser.js";

(function () {
  if (typeof document === "undefined") {
    return;
  }

  const sortBenchmarkBtnEl = document.getElementById("sortBenchmarkBtn");
  const sortBenchmarkCurrentBtnEl = document.getElementById(
    "sortBenchmarkCurrentBtn"
  );
  const benchmarkStatusEl = document.getElementById("benchmarkStatus");

  bindBenchmarkUi({
    primaryBtnEl: sortBenchmarkBtnEl,
    currentBtnEl: sortBenchmarkCurrentBtnEl,
    statusEl: benchmarkStatusEl,
    linePrefix: "sort",
    emptyMessage: "Sort benchmark API not available.",
    runBenchmark({ api, currentOnly, delayTick, now, onUpdate }) {
      return runSortBenchmark({
        api,
        currentOnly,
        delayTick,
        now,
        onUpdate,
      });
    },
  });
})();
