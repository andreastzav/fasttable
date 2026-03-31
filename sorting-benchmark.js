(function () {
  const sortBenchmarkBtnEl = document.getElementById("sortBenchmarkBtn");
  const sortBenchmarkCurrentBtnEl = document.getElementById(
    "sortBenchmarkCurrentBtn"
  );
  const benchmarkStatusEl = document.getElementById("benchmarkStatus");

  if (!sortBenchmarkBtnEl || !benchmarkStatusEl) {
    return;
  }

  function getBenchmarkApi() {
    return window.fastTableBenchmarkApi;
  }

  function setAllActionButtonsDisabled(disabled) {
    if (typeof window.fastTableSetActionButtonsDisabled === "function") {
      window.fastTableSetActionButtonsDisabled(disabled);
      return;
    }

    sortBenchmarkBtnEl.disabled = disabled;
    if (sortBenchmarkCurrentBtnEl) {
      sortBenchmarkCurrentBtnEl.disabled = disabled;
    }
  }

  function formatMs(value) {
    if (!Number.isFinite(value)) {
      return String(value);
    }

    return value.toLocaleString("en-US", {
      useGrouping: false,
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }

  function delayTick() {
    return new Promise((resolve) => {
      setTimeout(resolve, 0);
    });
  }

  function formatCount(value) {
    return value.toLocaleString("de-DE");
  }

  function percentile(values, p) {
    if (!Array.isArray(values) || values.length === 0) {
      return 0;
    }

    const sorted = values.slice().sort((a, b) => a - b);
    const lastIndex = sorted.length - 1;
    const index = lastIndex * p;
    const lower = Math.floor(index);
    const upper = Math.ceil(index);

    if (lower === upper) {
      return sorted[lower];
    }

    const weight = index - lower;
    return sorted[lower] + (sorted[upper] - sorted[lower]) * weight;
  }

  const SORT_BENCHMARK_ROUNDS = 3;
  const SORT_BENCHMARK_CASES = [
    { key: "index", label: "Index" },
    { key: "column5", label: "Column5" },
    { key: "column6", label: "Column6" },
    { key: "column7", label: "Column7" },
    { key: "column8", label: "Column8" },
    { key: "column9", label: "Column9" },
    { key: "column10", label: "Column10" },
    { key: "age", label: "Age" },
    { key: "firstName", label: "First Name" },
    { key: "lastName", label: "Last Name" },
    { keys: ["firstName", "lastName"], label: "First Name & Last Name" },
    { keys: ["lastName", "firstName"], label: "Last Name & First Name" },
    { keys: ["age", "lastName"], label: "Age & Last Name" },
  ];

  function buildSortDescriptorsForCase(benchCase, direction) {
    if (Array.isArray(benchCase.keys) && benchCase.keys.length > 0) {
      return benchCase.keys.map((columnKey) => ({
        columnKey,
        direction,
      }));
    }

    return [
      {
        columnKey: benchCase.key,
        direction,
      },
    ];
  }

  function appendBenchmarkLine(lines, text) {
    lines.push(text);
    benchmarkStatusEl.innerHTML = lines.join("<br>");
  }

  function normalizeSortOptionFlags(options) {
    const input = options || {};
    return {
      useTypedComparator: input.useTypedComparator === true,
      useIndexSort: input.useIndexSort === true,
    };
  }

  function formatSortOptionFlags(options) {
    const normalized = normalizeSortOptionFlags(options);
    return `typed:${normalized.useTypedComparator ? "on" : "off"}, index:${
      normalized.useIndexSort ? "on" : "off"
    }`;
  }

  function buildSortModeVariants(sortMode, baseSortOptions) {
    const normalizedBase = normalizeSortOptionFlags(baseSortOptions);
    if (sortMode !== "timsort" && sortMode !== "native") {
      return [
        {
          mode: sortMode,
          options: normalizedBase,
          label: `mode:${sortMode}, ${formatSortOptionFlags(normalizedBase)}`,
        },
      ];
    }

    const variants = [
      { useTypedComparator: false, useIndexSort: false },
      { useTypedComparator: false, useIndexSort: true },
      { useTypedComparator: true, useIndexSort: false },
      { useTypedComparator: true, useIndexSort: true },
    ];

    return variants.map((options) => ({
      mode: sortMode,
      options,
      label: `mode:${sortMode}, ${formatSortOptionFlags(options)}`,
    }));
  }

  function resolveCurrentSortMode(api, fallbackMode) {
    if (api && typeof api.getSortMode === "function") {
      const mode = api.getSortMode();
      if (typeof mode === "string" && mode !== "") {
        return mode;
      }
    }

    return fallbackMode;
  }

  function buildSortVariantsForRun(api, currentOnly, baseSortOptions) {
    if (currentOnly) {
      const mode = resolveCurrentSortMode(api, "native");
      const options = normalizeSortOptionFlags(baseSortOptions);
      return [
        {
          mode,
          options,
          label: `mode:${mode}, ${formatSortOptionFlags(options)}`,
        },
      ];
    }

    const sortModes =
      api && typeof api.getSortModes === "function" ? api.getSortModes() : ["native"];
    const sortModeVariants = [];
    for (let i = 0; i < sortModes.length; i += 1) {
      const variants = buildSortModeVariants(sortModes[i], baseSortOptions);
      for (let j = 0; j < variants.length; j += 1) {
        sortModeVariants.push(variants[j]);
      }
    }
    return sortModeVariants;
  }

  async function runSortBenchmark(options) {
    const benchmarkOptions = options || {};
    const currentOnly = benchmarkOptions.currentOnly === true;
    const api = getBenchmarkApi();
    if (!api) {
      benchmarkStatusEl.textContent = "Benchmark API not available.";
      return;
    }

    if (!api.hasData()) {
      benchmarkStatusEl.textContent = "Load or generate data before benchmark.";
      return;
    }

    if (
      typeof api.buildSortRowsSnapshot !== "function" ||
      typeof api.runSortSnapshotPass !== "function"
    ) {
      benchmarkStatusEl.textContent = "Sort benchmark API not available.";
      return;
    }

    const originalFilters = api.getRawFilters();
    const originalSortOptions =
      typeof api.getSortOptions === "function"
        ? normalizeSortOptionFlags(api.getSortOptions())
        : { useTypedComparator: true, useIndexSort: false };
    const sortModeVariants = buildSortVariantsForRun(
      api,
      currentOnly,
      originalSortOptions
    );
    const lines = [];
    const benchmarkStartMs = performance.now();
    const rawFilters = Object.assign({}, originalFilters);
    const snapshot = api.buildSortRowsSnapshot(rawFilters);
    const snapshotRows =
      snapshot && Array.isArray(snapshot.rows) ? snapshot.rows : [];
    const snapshotCount =
      snapshot && Number.isFinite(snapshot.count)
        ? snapshot.count
        : snapshotRows.length;

    setAllActionButtonsDisabled(true);

    try {
      appendBenchmarkLine(
        lines,
        `Sort benchmark started on ${formatCount(api.getRowCount())} rows.`
      );
      appendBenchmarkLine(
        lines,
        `Current filtered snapshot size: ${formatCount(snapshotCount)} rows.`
      );
      appendBenchmarkLine(
        lines,
        `Runs: ${SORT_BENCHMARK_ROUNDS} per sort case per direction (desc + asc).`
      );
      appendBenchmarkLine(
        lines,
        "Reported timings: core + total (total includes prep)."
      );
      if (currentOnly) {
        appendBenchmarkLine(lines, "Scope: current sorting combination only.");
      }

      const totalsByMode = [];
      const directions = ["desc", "asc"];

      for (
        let modeIndex = 0;
        modeIndex < sortModeVariants.length;
        modeIndex += 1
      ) {
        const variant = sortModeVariants[modeIndex];
        const sortMode = variant.mode;
        if (typeof api.setSortOptions === "function") {
          api.setSortOptions(variant.options);
          await delayTick();
        }

        appendBenchmarkLine(lines, "");
        appendBenchmarkLine(
          lines,
          `[${modeIndex + 1}/${sortModeVariants.length}] ${variant.label}`
        );

        let modeTotalCoreMs = 0;
        let modeTotalMs = 0;
        let modeRuns = 0;
        const modeSamples = [];

        for (
          let caseIndex = 0;
          caseIndex < SORT_BENCHMARK_CASES.length;
          caseIndex += 1
        ) {
          const benchCase = SORT_BENCHMARK_CASES[caseIndex];
          let caseTotalCoreMs = 0;
          let caseTotalMs = 0;
          let caseRuns = 0;
          const caseSamples = [];

          for (let dirIndex = 0; dirIndex < directions.length; dirIndex += 1) {
            const direction = directions[dirIndex];
            for (let round = 0; round < SORT_BENCHMARK_ROUNDS; round += 1) {
              const descriptors = buildSortDescriptorsForCase(
                benchCase,
                direction
              );
              const result = api.runSortSnapshotPass(
                snapshotRows,
                descriptors,
                sortMode
              );
              if (!result) {
                throw new Error("Failed to run sort benchmark pass.");
              }

              const coreMs = Number.isFinite(result.sortCoreMs)
                ? result.sortCoreMs
                : result.sortMs;
              const totalMs = Number.isFinite(result.sortTotalMs)
                ? result.sortTotalMs
                : coreMs;

              caseTotalCoreMs += coreMs;
              caseTotalMs += totalMs;
              modeTotalCoreMs += coreMs;
              modeTotalMs += totalMs;
              caseRuns += 1;
              modeRuns += 1;
              caseSamples.push(totalMs);
              modeSamples.push(totalMs);

              await delayTick();
            }
          }

          const caseAverageCoreMs = caseRuns > 0 ? caseTotalCoreMs / caseRuns : 0;
          const caseAverageMs = caseRuns > 0 ? caseTotalMs / caseRuns : 0;
          const caseP50Ms = percentile(caseSamples, 0.5);
          const caseP75Ms = percentile(caseSamples, 0.75);
          const caseP95Ms = percentile(caseSamples, 0.95);
          appendBenchmarkLine(
            lines,
            `${benchCase.label} core avg: ${formatMs(caseAverageCoreMs)} ms, total avg: ${formatMs(caseAverageMs)} ms, median/p50 total: ${formatMs(caseP50Ms)} ms, p75 total: ${formatMs(caseP75Ms)} ms, p95 total: ${formatMs(caseP95Ms)} ms (${caseRuns} runs)`
          );
          await delayTick();
        }

        const modeAverageCoreMs =
          modeRuns > 0 ? modeTotalCoreMs / modeRuns : 0;
        const modeAverageMs = modeRuns > 0 ? modeTotalMs / modeRuns : 0;
        const modeP50Ms = percentile(modeSamples, 0.5);
        const modeP75Ms = percentile(modeSamples, 0.75);
        const modeP95Ms = percentile(modeSamples, 0.95);
        totalsByMode.push({
          mode: sortMode,
          label: variant.label,
          options: normalizeSortOptionFlags(variant.options),
          averageCoreMs: modeAverageCoreMs,
          averageMs: modeAverageMs,
          p50Ms: modeP50Ms,
          p75Ms: modeP75Ms,
          p95Ms: modeP95Ms,
          runs: modeRuns,
        });

        appendBenchmarkLine(
          lines,
          `Total avg (all columns): core ${formatMs(modeAverageCoreMs)} ms, total ${formatMs(modeAverageMs)} ms, median/p50 total ${formatMs(modeP50Ms)} ms, p75 total ${formatMs(modeP75Ms)} ms, p95 total ${formatMs(modeP95Ms)} ms (${modeRuns} runs)`
        );
        await delayTick();
      }

      appendBenchmarkLine(lines, "");
      appendBenchmarkLine(lines, "Final totals per sort mode/options:");
      for (let i = 0; i < totalsByMode.length; i += 1) {
        const item = totalsByMode[i];
        appendBenchmarkLine(
          lines,
          `${item.label} -> core avg: ${formatMs(item.averageCoreMs)} ms, total avg: ${formatMs(item.averageMs)} ms, median/p50 total: ${formatMs(item.p50Ms)} ms, p75 total: ${formatMs(item.p75Ms)} ms, p95 total: ${formatMs(item.p95Ms)} ms (${item.runs} runs)`
        );
      }

      const benchmarkEndMs = performance.now();
      appendBenchmarkLine(
        lines,
        `Sort benchmark finished in ${formatMs(benchmarkEndMs - benchmarkStartMs)} ms.`
      );
    } catch (error) {
      appendBenchmarkLine(
        lines,
        `Sort benchmark failed: ${String(error && error.message ? error.message : error)}`
      );
    } finally {
      if (typeof api.setSortOptions === "function") {
        api.setSortOptions(originalSortOptions);
      }
      api.setRawFilters(originalFilters);
      await delayTick();
      api.runFilterPass();
      setAllActionButtonsDisabled(false);
    }
  }

  sortBenchmarkBtnEl.addEventListener("click", () => {
    runSortBenchmark({ currentOnly: false });
  });

  if (sortBenchmarkCurrentBtnEl) {
    sortBenchmarkCurrentBtnEl.addEventListener("click", () => {
      runSortBenchmark({ currentOnly: true });
    });
  }
})();
