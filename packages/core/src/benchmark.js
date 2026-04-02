const DEFAULT_FILTER_BENCHMARK_ROUNDS = 3;
const DEFAULT_FILTER_BENCHMARK_CASES = [
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

const DEFAULT_SORT_BENCHMARK_ROUNDS = 3;
const DEFAULT_SORT_BENCHMARK_CASES = [
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

function defaultNow() {
  if (typeof performance !== "undefined" && typeof performance.now === "function") {
    return performance.now();
  }

  return Date.now();
}

const BENCHMARK_TICK_POLICY_MICRO = "micro";
const BENCHMARK_TICK_POLICY_MACRO = "macro";

function resolveBenchmarkTickPolicy(policy, fallbackPolicy) {
  const fallback =
    fallbackPolicy === BENCHMARK_TICK_POLICY_MACRO
      ? BENCHMARK_TICK_POLICY_MACRO
      : BENCHMARK_TICK_POLICY_MICRO;
  const normalized =
    typeof policy === "string" ? policy.trim().toLowerCase() : "";
  if (
    normalized === BENCHMARK_TICK_POLICY_MICRO ||
    normalized === BENCHMARK_TICK_POLICY_MACRO
  ) {
    return normalized;
  }

  return fallback;
}

function createBenchmarkDelayTick(policy) {
  const resolvedPolicy = resolveBenchmarkTickPolicy(
    policy,
    BENCHMARK_TICK_POLICY_MICRO
  );
  if (resolvedPolicy === BENCHMARK_TICK_POLICY_MACRO) {
    return function benchmarkDelayTickMacro() {
      return new Promise((resolve) => {
        setTimeout(resolve, 0);
      });
    };
  }

  return function benchmarkDelayTickMicro() {
    return Promise.resolve();
  };
}

const defaultDelayTickFn = createBenchmarkDelayTick(BENCHMARK_TICK_POLICY_MICRO);

function defaultDelayTick() {
  return defaultDelayTickFn();
}

function createLineReporter(onUpdate) {
  const lines = [];
  const update = typeof onUpdate === "function" ? onUpdate : null;

  function append(text) {
    lines.push(String(text));
    if (update) {
      update(lines);
    }
  }

  return {
    lines,
    append,
  };
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

function buildFilteringCombinations() {
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

function buildCurrentFilteringCombination(api) {
  const modeOptions = api && typeof api.getModeOptions === "function"
    ? api.getModeOptions()
    : {};
  return normalizeCombination(modeOptions);
}

function formatFilteringCombinationLabel(combination) {
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

function validateFilteringApi(api) {
  if (!api) {
    throw new Error("Benchmark API not available.");
  }

  if (typeof api.hasData !== "function" || !api.hasData()) {
    throw new Error("Load or generate data before benchmark.");
  }

  if (
    typeof api.getModeOptions !== "function" ||
    typeof api.setModeOptions !== "function" ||
    typeof api.getRawFilters !== "function" ||
    typeof api.setRawFilters !== "function" ||
    (typeof api.runFilterCore !== "function" &&
      typeof api.runFilterPass !== "function" &&
      typeof api.executeFilterCore !== "function" &&
      typeof api.runSingleFilterPass !== "function") ||
    typeof api.restoreStateCore !== "function" ||
    typeof api.getRowCount !== "function"
  ) {
    throw new Error("Benchmark API not available.");
  }
}

function executeFilteringBenchmarkPass(api, columnKey, value) {
  const rawFilters = {};
  rawFilters[columnKey] = value;

  if (typeof api.runFilterCore === "function") {
    const coreRun = api.runFilterCore(rawFilters, {
      rawFilters,
      skipRender: true,
      skipStatus: true,
      keepScroll: false,
      preferPrecomputedFastPath: false,
    });
    if (
      !coreRun ||
      coreRun.kind !== "ok" ||
      !coreRun.orchestration ||
      !coreRun.orchestration.filterResult
    ) {
      return null;
    }

    return coreRun.orchestration.filterResult;
  }

  if (typeof api.executeFilterCore === "function") {
    return api.executeFilterCore(rawFilters, {
      skipRender: true,
      skipStatus: true,
    });
  }

  return api.runSingleFilterPass(columnKey, value, {
    skipRender: true,
    skipStatus: true,
  });
}

async function runFilteringBenchmark(options) {
  const input = options || {};
  const api = input.api;
  validateFilteringApi(api);

  const currentOnly = input.currentOnly === true;
  const rounds = Number.isFinite(input.rounds)
    ? Math.max(1, Number(input.rounds) | 0)
    : DEFAULT_FILTER_BENCHMARK_ROUNDS;
  const benchmarkCases =
    Array.isArray(input.benchmarkCases) && input.benchmarkCases.length > 0
      ? input.benchmarkCases
      : DEFAULT_FILTER_BENCHMARK_CASES;
  const delayTick =
    typeof input.delayTick === "function" ? input.delayTick : defaultDelayTick;
  const now = typeof input.now === "function" ? input.now : defaultNow;
  const reporter = createLineReporter(input.onUpdate);

  const originalModeOptions = api.getModeOptions();
  const originalFilters = api.getRawFilters();
  const combinations = currentOnly
    ? [buildCurrentFilteringCombination(api)]
    : buildFilteringCombinations();
  const forcedBenchmarkOptions = {
    useSmartFiltering: false,
    useFilterCache: false,
  };
  let benchmarkStartMs = now();
  const totalByCombination = [];
  let error = null;

  try {
    reporter.append(`Benchmark started on ${formatCount(api.getRowCount())} rows.`);
    reporter.append(
      `Runs: ${rounds} per value, one active filter at a time.`
    );
    if (currentOnly) {
      reporter.append("Scope: current filtering combination only.");
    }
    reporter.append(
      "Forced options for benchmark: smart filtering off, filter cache off."
    );

    api.setModeOptions(forcedBenchmarkOptions, { suppressFilterPass: true });
    await delayTick();

    for (let comboIndex = 0; comboIndex < combinations.length; comboIndex += 1) {
      const combination = normalizeCombination(combinations[comboIndex]);
      const appliedCombination = Object.assign(
        {},
        combination,
        forcedBenchmarkOptions
      );
      reporter.append("");
      reporter.append(
        `[${comboIndex + 1}/${combinations.length}] ${formatFilteringCombinationLabel(
          appliedCombination
        )}`
      );

      api.setModeOptions(appliedCombination, { suppressFilterPass: true });
      await delayTick();

      let combinationTotalMs = 0;
      let combinationRuns = 0;
      const combinationSamples = [];

      for (let caseIndex = 0; caseIndex < benchmarkCases.length; caseIndex += 1) {
        const benchCase = benchmarkCases[caseIndex];
        let caseTotalMs = 0;
        let caseRuns = 0;
        const caseSamples = [];

        for (let round = 0; round < rounds; round += 1) {
          for (
            let valueIndex = 0;
            valueIndex < benchCase.values.length;
            valueIndex += 1
          ) {
            const value = benchCase.values[valueIndex];
            const result = executeFilteringBenchmarkPass(
              api,
              benchCase.key,
              value
            );
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
        reporter.append(
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
        label: formatFilteringCombinationLabel(appliedCombination),
        averageMs: combinationAverageMs,
        p50Ms: combinationP50Ms,
        p75Ms: combinationP75Ms,
        p95Ms: combinationP95Ms,
        runs: combinationRuns,
      });

      reporter.append(
        `Total avg (all columns): ${formatMs(combinationAverageMs)} ms, median/p50: ${formatMs(combinationP50Ms)} ms, p75: ${formatMs(combinationP75Ms)} ms, p95: ${formatMs(combinationP95Ms)} ms (${combinationRuns} runs)`
      );
      await delayTick();
    }

    reporter.append("");
    reporter.append("Final totals per combination:");
    for (let i = 0; i < totalByCombination.length; i += 1) {
      const item = totalByCombination[i];
      reporter.append(
        `${item.label} -> avg: ${formatMs(item.averageMs)} ms, median/p50: ${formatMs(item.p50Ms)} ms, p75: ${formatMs(item.p75Ms)} ms, p95: ${formatMs(item.p95Ms)} ms (${item.runs} runs)`
      );
    }

    const benchmarkEndMs = now();
    reporter.append(
      `Benchmark finished in ${formatMs(benchmarkEndMs - benchmarkStartMs)} ms.`
    );
  } catch (err) {
    error = err;
    reporter.append(
      `Benchmark failed: ${String(err && err.message ? err.message : err)}`
    );
  } finally {
    try {
      await api.restoreStateCore({
        modeOptions: originalModeOptions,
        rawFilters: originalFilters,
      });
      await delayTick();
    } catch (restoreError) {
      if (!error) {
        error = restoreError;
      }
      reporter.append(
        `Benchmark restore failed: ${String(
          restoreError && restoreError.message
            ? restoreError.message
            : restoreError
        )}`
      );
    }
  }

  return {
    lines: reporter.lines,
    totals: totalByCombination,
    durationMs: now() - benchmarkStartMs,
    error,
  };
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

function sortModeUsesComparatorFlags(sortMode) {
  return sortMode === "native" || sortMode === "timsort";
}

function formatSortOptionFlagsForMode(sortMode, options) {
  if (!sortModeUsesComparatorFlags(sortMode)) {
    return "typed:n/a, index:n/a";
  }

  return formatSortOptionFlags(options);
}

function buildSortModeVariants(sortMode, baseSortOptions) {
  const normalizedBase = normalizeSortOptionFlags(baseSortOptions);
  if (sortMode !== "timsort" && sortMode !== "native") {
    return [
      {
        mode: sortMode,
        options: normalizedBase,
        label: `mode:${sortMode}, ${formatSortOptionFlagsForMode(
          sortMode,
          normalizedBase
        )}`,
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
        label: `mode:${mode}, ${formatSortOptionFlagsForMode(mode, options)}`,
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

function validateSortApi(api) {
  if (!api) {
    throw new Error("Benchmark API not available.");
  }

  if (typeof api.hasData !== "function" || !api.hasData()) {
    throw new Error("Load or generate data before benchmark.");
  }

  if (
    typeof api.getRowCount !== "function" ||
    typeof api.buildSortRowsSnapshot !== "function" ||
    (typeof api.runSortSnapshotCore !== "function" &&
      typeof api.runSortCore !== "function" &&
      typeof api.executeSortCore !== "function" &&
      typeof api.runSortSnapshotPass !== "function") ||
    typeof api.restoreStateCore !== "function"
  ) {
    throw new Error("Sort benchmark API not available.");
  }
}

function executeSortBenchmarkPass(api, snapshotPayload, descriptors, sortMode) {
  if (typeof api.runSortSnapshotCore === "function") {
    const coreRun = api.runSortSnapshotCore(snapshotPayload, descriptors, sortMode, {
      rawFilters: {},
      skipRender: true,
      skipStatus: true,
      keepScroll: false,
      preferPrecomputedFastPath: false,
    });
    if (
      !coreRun ||
      !coreRun.result
    ) {
      return null;
    }

    return {
      sortCoreMs: Number(coreRun.result.durationMs) || 0,
      sortTotalMs: Number(coreRun.sortTotalMs) || Number(coreRun.result.durationMs) || 0,
      sortPrepMs: Number(coreRun.sortPrepMs) || 0,
      sortMode:
        coreRun.result && typeof coreRun.result.sortMode === "string"
          ? coreRun.result.sortMode
          : sortMode,
      sortedCount: Number(coreRun.sortedCount) || 0,
      sortedIndices: coreRun.indices,
    };
  }

  if (typeof api.runSortCore === "function") {
    const coreRun = api.runSortCore(null, descriptors, sortMode, {
      rawFilters: {},
      skipRender: true,
      skipStatus: true,
      keepScroll: false,
      preferPrecomputedFastPath: false,
    });
    if (
      !coreRun ||
      coreRun.kind !== "ok" ||
      !coreRun.sortRun ||
      !coreRun.sortRun.result
    ) {
      return null;
    }

    return {
      sortCoreMs: Number(coreRun.sortRun.result.durationMs) || 0,
      sortTotalMs:
        Number(coreRun.sortRun.sortTotalMs) ||
        Number(coreRun.sortRun.result.durationMs) ||
        0,
      sortPrepMs: Number(coreRun.sortRun.sortPrepMs) || 0,
      sortMode:
        coreRun.sortRun.result &&
        typeof coreRun.sortRun.result.sortMode === "string"
          ? coreRun.sortRun.result.sortMode
          : sortMode,
      sortedCount: Number(coreRun.sortRun.sortedCount) || 0,
      sortedIndices: coreRun.sortRun.indices,
    };
  }

  if (typeof api.executeSortCore === "function") {
    return api.executeSortCore(snapshotPayload, descriptors, sortMode);
  }

  return api.runSortSnapshotPass(snapshotPayload, descriptors, sortMode);
}

async function runSortBenchmark(options) {
  const input = options || {};
  const api = input.api;
  validateSortApi(api);

  const currentOnly = input.currentOnly === true;
  const rounds = Number.isFinite(input.rounds)
    ? Math.max(1, Number(input.rounds) | 0)
    : DEFAULT_SORT_BENCHMARK_ROUNDS;
  const sortCases =
    Array.isArray(input.sortCases) && input.sortCases.length > 0
      ? input.sortCases
      : DEFAULT_SORT_BENCHMARK_CASES;
  const delayTick =
    typeof input.delayTick === "function" ? input.delayTick : defaultDelayTick;
  const now = typeof input.now === "function" ? input.now : defaultNow;
  const reporter = createLineReporter(input.onUpdate);

  const originalSortOptions =
    typeof api.getSortOptions === "function"
      ? normalizeSortOptionFlags(api.getSortOptions())
      : { useTypedComparator: true, useIndexSort: false };
  const sortModeVariants = buildSortVariantsForRun(
    api,
    currentOnly,
    originalSortOptions
  );
  const includesPrecomputedMode = sortModeVariants.some(
    (variant) => variant && variant.mode === "precomputed"
  );
  let benchmarkStartMs = now();
  const totalsByMode = [];
  let error = null;

  try {
    if (typeof api.resetPrecomputedSortState === "function") {
      api.resetPrecomputedSortState(api.getRowCount());
      await delayTick();
    }

    // Sorting benchmark always runs on the full table snapshot.
    // Active UI filters are intentionally ignored here.
    const snapshot = api.buildSortRowsSnapshot({});
    const snapshotPayload =
      snapshot && typeof snapshot === "object" && !Array.isArray(snapshot)
        ? snapshot
        : Array.isArray(snapshot)
          ? {
              snapshotType: "legacy-row-array-v1",
              rowIndices: snapshot,
              count: snapshot.length,
            }
          : {
              snapshotType: "empty-v1",
              rowIndices: [],
              count: 0,
            };
    const snapshotCount =
      Number.isFinite(snapshotPayload.count) && Number(snapshotPayload.count) >= 0
        ? Math.floor(Number(snapshotPayload.count))
        : Array.isArray(snapshotPayload.rows)
          ? snapshotPayload.rows.length
          : Array.isArray(snapshotPayload.rowIndices) ||
              ArrayBuffer.isView(snapshotPayload.rowIndices)
            ? snapshotPayload.rowIndices.length
            : 0;

    if (
      includesPrecomputedMode &&
      typeof api.prewarmPrecomputedSortState === "function"
    ) {
      // Prewarm must happen after snapshot sync so precomputed state is not
      // reset right before timed runs.
      api.prewarmPrecomputedSortState();
      await delayTick();
    }
    benchmarkStartMs = now();

    reporter.append(
      `Sort benchmark started on ${formatCount(api.getRowCount())} rows.`
    );
    reporter.append(
      `Benchmark snapshot size: ${formatCount(snapshotCount)} rows (full table).`
    );
    reporter.append(
      `Runs: ${rounds} per sort case per direction (desc + asc).`
    );
    reporter.append(
      "Reported timings: core + total (total includes prep)."
    );
    if (currentOnly) {
      reporter.append("Scope: current sorting combination only.");
    }

    const directions = ["desc", "asc"];

    for (
      let modeIndex = 0;
      modeIndex < sortModeVariants.length;
      modeIndex += 1
    ) {
      const variant = sortModeVariants[modeIndex];
      const sortMode = variant.mode;
      if (sortModeUsesComparatorFlags(sortMode) && typeof api.setSortOptions === "function") {
        api.setSortOptions(variant.options);
        await delayTick();
      }

      reporter.append("");
      reporter.append(
        `[${modeIndex + 1}/${sortModeVariants.length}] ${variant.label}`
      );

      let modeTotalCoreMs = 0;
      let modeTotalMs = 0;
      let modeRuns = 0;
      const modeSamples = [];

      for (let caseIndex = 0; caseIndex < sortCases.length; caseIndex += 1) {
        const benchCase = sortCases[caseIndex];
        let caseTotalCoreMs = 0;
        let caseTotalMs = 0;
        let caseRuns = 0;
        const caseSamples = [];

        for (let dirIndex = 0; dirIndex < directions.length; dirIndex += 1) {
          const direction = directions[dirIndex];
          for (let round = 0; round < rounds; round += 1) {
            const descriptors = buildSortDescriptorsForCase(
              benchCase,
              direction
            );
            const result = executeSortBenchmarkPass(
              api,
              snapshotPayload,
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
        reporter.append(
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

      reporter.append(
        `Total avg (all columns): core ${formatMs(modeAverageCoreMs)} ms, total ${formatMs(modeAverageMs)} ms, median/p50 total ${formatMs(modeP50Ms)} ms, p75 total ${formatMs(modeP75Ms)} ms, p95 total ${formatMs(modeP95Ms)} ms (${modeRuns} runs)`
      );
      await delayTick();
    }

    reporter.append("");
    reporter.append("Final totals per sort mode/options:");
    for (let i = 0; i < totalsByMode.length; i += 1) {
      const item = totalsByMode[i];
      reporter.append(
        `${item.label} -> core avg: ${formatMs(item.averageCoreMs)} ms, total avg: ${formatMs(item.averageMs)} ms, median/p50 total: ${formatMs(item.p50Ms)} ms, p75 total: ${formatMs(item.p75Ms)} ms, p95 total: ${formatMs(item.p95Ms)} ms (${item.runs} runs)`
      );
    }

    const benchmarkEndMs = now();
    reporter.append(
      `Sort benchmark finished in ${formatMs(benchmarkEndMs - benchmarkStartMs)} ms.`
    );
  } catch (err) {
    error = err;
    reporter.append(
      `Sort benchmark failed: ${String(err && err.message ? err.message : err)}`
    );
  } finally {
    try {
      if (typeof api.resetPrecomputedSortState === "function") {
        api.resetPrecomputedSortState(api.getRowCount());
      }
      await api.restoreStateCore({
        sortOptions: originalSortOptions,
      });
    } catch (restoreError) {
      if (!error) {
        error = restoreError;
      }
      reporter.append(
        `Sort benchmark restore failed: ${String(
          restoreError && restoreError.message
            ? restoreError.message
            : restoreError
        )}`
      );
    }
  }

  return {
    lines: reporter.lines,
    totals: totalsByMode,
    durationMs: now() - benchmarkStartMs,
    error,
  };
}

export {
  DEFAULT_FILTER_BENCHMARK_ROUNDS,
  DEFAULT_FILTER_BENCHMARK_CASES,
  DEFAULT_SORT_BENCHMARK_ROUNDS,
  DEFAULT_SORT_BENCHMARK_CASES,
  BENCHMARK_TICK_POLICY_MICRO,
  BENCHMARK_TICK_POLICY_MACRO,
  resolveBenchmarkTickPolicy,
  createBenchmarkDelayTick,
  formatMs,
  formatCount,
  percentile,
  normalizeCombination,
  buildFilteringCombinations,
  buildCurrentFilteringCombination,
  formatFilteringCombinationLabel,
  normalizeSortOptionFlags,
  formatSortOptionFlags,
  buildSortModeVariants,
  buildSortVariantsForRun,
  buildSortDescriptorsForCase,
  runFilteringBenchmark,
  runSortBenchmark,
};
