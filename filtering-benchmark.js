(function () {
  const benchmarkBtnEl = document.getElementById("benchmarkBtn");
  const benchmarkCurrentBtnEl = document.getElementById("benchmarkCurrentBtn");
  const benchmarkStatusEl = document.getElementById("benchmarkStatus");

  if (!benchmarkBtnEl || !benchmarkStatusEl) {
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

    benchmarkBtnEl.disabled = disabled;
    if (benchmarkCurrentBtnEl) {
      benchmarkCurrentBtnEl.disabled = disabled;
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

  const BENCHMARK_ROUNDS = 3;
  const BENCHMARK_CASES = [
    {
      key: "index",
      label: "Index",
      values: ["50", "1", "99", "255", "6000"],
    },
    {
      key: "column5",
      label: "Column5",
      values: ["50", "1", "99", "255", "6000"],
    },
    {
      key: "column6",
      label: "Column6",
      values: ["50", "1", "99", "255", "6000"],
    },
    {
      key: "column7",
      label: "Column7",
      values: ["50", "1", "99", "255", "6000"],
    },
    {
      key: "column8",
      label: "Column8",
      values: ["50", "1", "99", "255", "6000"],
    },
    {
      key: "age",
      label: "Age",
      values: ["18", "99", "40", "50", "69"],
    },
    {
      key: "firstName",
      label: "First Name",
      values: ["andr", "a", "phani", "ab", "olet"],
    },
    {
      key: "lastName",
      label: "Last Name",
      values: ["i", "y", "lli", "righ", "derson"],
    },
  ];

  function buildCombinations() {
    const combinations = [];
    const boolValues = [false, true];

    for (let i = 0; i < boolValues.length; i += 1) {
      for (let j = 0; j < boolValues.length; j += 1) {
        for (let m = 0; m < boolValues.length; m += 1) {
          for (let d = 0; d < boolValues.length; d += 1) {
            for (let x = 0; x < boolValues.length; x += 1) {
              for (let p = 0; p < boolValues.length; p += 1) {
                const useColumnarData = boolValues[i];
                const useBinaryColumnar = boolValues[j];
                const enableCaching = boolValues[m];
                const useDictionaryKeySearch = boolValues[d];
                const useDictionaryIntersection = boolValues[x];
                const useSmarterPlanner = boolValues[p];

                if (!useColumnarData && useBinaryColumnar) {
                  continue;
                }

                if (!useDictionaryKeySearch && useDictionaryIntersection) {
                  continue;
                }

                combinations.push({
                  useColumnarData,
                  useBinaryColumnar,
                  useNumericData: false,
                  enableCaching,
                  useDictionaryKeySearch,
                  useDictionaryIntersection,
                  useSmarterPlanner,
                });
                if (useColumnarData) {
                  continue;
                }

                combinations.push({
                  useColumnarData,
                  useBinaryColumnar,
                  useNumericData: true,
                  enableCaching,
                  useDictionaryKeySearch,
                  useDictionaryIntersection,
                  useSmarterPlanner,
                });
              }
            }
          }
        }
      }
    }

    return combinations;
  }

  function normalizeCombination(input) {
    const normalized = {
      useColumnarData: input && input.useColumnarData === true,
      useBinaryColumnar: input && input.useBinaryColumnar === true,
      useNumericData: input && input.useNumericData === true,
      enableCaching: !input || input.enableCaching !== false,
      useDictionaryKeySearch:
        !input || input.useDictionaryKeySearch !== false,
      useDictionaryIntersection:
        input && input.useDictionaryIntersection === true,
      useSmarterPlanner: input && input.useSmarterPlanner === true,
      useSmartFiltering: input && input.useSmartFiltering === true,
      useFilterCache: input && input.useFilterCache === true,
    };

    if (!normalized.useColumnarData) {
      normalized.useBinaryColumnar = false;
    }

    if (normalized.useColumnarData) {
      normalized.useNumericData = false;
    }

    if (!normalized.useDictionaryKeySearch) {
      normalized.useDictionaryIntersection = false;
    }

    return normalized;
  }

  function buildCurrentCombination(api) {
    const modeOptions = api && typeof api.getModeOptions === "function"
      ? api.getModeOptions()
      : {};
    return normalizeCombination(modeOptions);
  }

  function formatCombinationLabel(combination) {
    const columnar = combination.useColumnarData ? "on" : "off";
    const binaryColumnar = combination.useBinaryColumnar ? "on" : "off";
    const numeric = combination.useColumnarData
      ? "n/a"
      : combination.useNumericData
        ? "on"
        : "off";
    const normalized = combination.enableCaching ? "on" : "off";
    const dict = combination.useDictionaryKeySearch ? "on" : "off";
    const dictIntersect = combination.useDictionaryIntersection ? "on" : "off";
    const smarterPlanner = combination.useSmarterPlanner ? "on" : "off";
    return `columnar:${columnar}, binary:${binaryColumnar}, numeric:${numeric}, normalized:${normalized}, dict:${dict}, dictIntersect:${dictIntersect}, smarterPlanner:${smarterPlanner}`;
  }

  function appendBenchmarkLine(lines, text) {
    lines.push(text);
    benchmarkStatusEl.innerHTML = lines.join("<br>");
  }

  async function runBenchmark(options) {
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

    const originalModeOptions = api.getModeOptions();
    const originalFilters = api.getRawFilters();
    const combinations = currentOnly
      ? [buildCurrentCombination(api)]
      : buildCombinations();
    const forcedBenchmarkOptions = currentOnly
      ? null
      : {
          useSmartFiltering: false,
          useFilterCache: false,
        };
    const lines = [];
    const benchmarkStartMs = performance.now();

    setAllActionButtonsDisabled(true);

    try {
      appendBenchmarkLine(
        lines,
        `Benchmark started on ${formatCount(api.getRowCount())} rows.`
      );
      appendBenchmarkLine(
        lines,
        `Runs: ${BENCHMARK_ROUNDS} per value, one active filter at a time.`
      );
      if (currentOnly) {
        appendBenchmarkLine(lines, "Scope: current filtering combination only.");
      } else {
        appendBenchmarkLine(
          lines,
          "Forced options for benchmark: smart filtering off, filter cache off."
        );
      }

      if (forcedBenchmarkOptions) {
        api.setModeOptions(forcedBenchmarkOptions, { suppressFilterPass: true });
        await delayTick();
      }

      const totalByCombination = [];

      for (let comboIndex = 0; comboIndex < combinations.length; comboIndex += 1) {
        const combination = normalizeCombination(combinations[comboIndex]);
        const appliedCombination = forcedBenchmarkOptions
          ? Object.assign({}, combination, forcedBenchmarkOptions)
          : combination;
        appendBenchmarkLine(lines, "");
        appendBenchmarkLine(
          lines,
          `[${comboIndex + 1}/${combinations.length}] ${formatCombinationLabel(
            appliedCombination
          )}`
        );

        api.setModeOptions(appliedCombination, { suppressFilterPass: true });
        await delayTick();

        let combinationTotalMs = 0;
        let combinationRuns = 0;
        const combinationSamples = [];

        for (let caseIndex = 0; caseIndex < BENCHMARK_CASES.length; caseIndex += 1) {
          const benchCase = BENCHMARK_CASES[caseIndex];
          let caseTotalMs = 0;
          let caseRuns = 0;
          const caseSamples = [];

          for (let round = 0; round < BENCHMARK_ROUNDS; round += 1) {
            for (
              let valueIndex = 0;
              valueIndex < benchCase.values.length;
              valueIndex += 1
            ) {
              const value = benchCase.values[valueIndex];
              const result = api.runSingleFilterPass(benchCase.key, value, {
                skipRender: true,
                skipStatus: true,
              });
              if (!result) {
                throw new Error("Failed to run benchmark filter pass.");
              }

              caseTotalMs += result.coreMs;
              combinationTotalMs += result.coreMs;
              caseRuns += 1;
              combinationRuns += 1;
              caseSamples.push(result.coreMs);
              combinationSamples.push(result.coreMs);

              await delayTick();
            }
          }

          const caseAverageMs = caseRuns > 0 ? caseTotalMs / caseRuns : 0;
          const caseP50Ms = percentile(caseSamples, 0.5);
          const caseP75Ms = percentile(caseSamples, 0.75);
          const caseP95Ms = percentile(caseSamples, 0.95);
          appendBenchmarkLine(
            lines,
            `${benchCase.label} avg: ${formatMs(caseAverageMs)} ms, median/p50: ${formatMs(caseP50Ms)} ms, p75: ${formatMs(caseP75Ms)} ms, p95: ${formatMs(caseP95Ms)} ms (${caseRuns} runs)`
          );
          await delayTick();
        }

        const combinationAverageMs =
          combinationRuns > 0 ? combinationTotalMs / combinationRuns : 0;
        const combinationP50Ms = percentile(combinationSamples, 0.5);
        const combinationP75Ms = percentile(combinationSamples, 0.75);
        const combinationP95Ms = percentile(combinationSamples, 0.95);
        totalByCombination.push({
          label: formatCombinationLabel(appliedCombination),
          averageMs: combinationAverageMs,
          p50Ms: combinationP50Ms,
          p75Ms: combinationP75Ms,
          p95Ms: combinationP95Ms,
          runs: combinationRuns,
        });

        appendBenchmarkLine(
          lines,
          `Total avg (all columns): ${formatMs(combinationAverageMs)} ms, median/p50: ${formatMs(combinationP50Ms)} ms, p75: ${formatMs(combinationP75Ms)} ms, p95: ${formatMs(combinationP95Ms)} ms (${combinationRuns} runs)`
        );
        await delayTick();
      }

      appendBenchmarkLine(lines, "");
      appendBenchmarkLine(lines, "Final totals per combination:");
      for (let i = 0; i < totalByCombination.length; i += 1) {
        const item = totalByCombination[i];
        appendBenchmarkLine(
          lines,
          `${item.label} -> avg: ${formatMs(item.averageMs)} ms, median/p50: ${formatMs(item.p50Ms)} ms, p75: ${formatMs(item.p75Ms)} ms, p95: ${formatMs(item.p95Ms)} ms (${item.runs} runs)`
        );
      }

      const benchmarkEndMs = performance.now();
      appendBenchmarkLine(
        lines,
        `Benchmark finished in ${formatMs(benchmarkEndMs - benchmarkStartMs)} ms.`
      );
    } catch (error) {
      appendBenchmarkLine(
        lines,
        `Benchmark failed: ${String(error && error.message ? error.message : error)}`
      );
    } finally {
      api.setModeOptions(originalModeOptions, { suppressFilterPass: true });
      api.setRawFilters(originalFilters);
      await delayTick();
      api.runFilterPass();
      setAllActionButtonsDisabled(false);
    }
  }

  benchmarkBtnEl.addEventListener("click", () => {
    runBenchmark({ currentOnly: false });
  });

  if (benchmarkCurrentBtnEl) {
    benchmarkCurrentBtnEl.addEventListener("click", () => {
      runBenchmark({ currentOnly: true });
    });
  }
})();
