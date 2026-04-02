import { createFastTableEngine } from "@fasttable/core/engine";
import { createFilterRuntimeBridge } from "@fasttable/core/filter-runtime-bridge";
import { createSortRuntimeBridge } from "@fasttable/core/sort-runtime-bridge";
import {
  ensureNumericColumnarSortedIndices as ensureCoreNumericColumnarSortedIndices,
  ensureNumericColumnarSortedRanks as ensureCoreNumericColumnarSortedRanks,
} from "@fasttable/core/io";

const {
  COLUMN_NAMES: columnNames,
  COLUMN_KEYS: columnKeys,
  COLUMN_INDEX_BY_KEY: columnIndexByKey,
  BASE_COLUMN_COUNT: baseColumnCount,
  NUMERIC_CACHE_OFFSET: numericCacheOffset,
  generateRowsWithoutCache: runGenerateRowsWithoutCache,
  attachCachesToRows: runAttachCachesToRows,
  deriveColumnarDataFromRows: runDeriveColumnarDataFromRows,
  deriveNumericRowsFromRows: runDeriveNumericRowsFromRows,
  deriveNumericColumnarDataFromNumericRows: runDeriveNumericColumnarDataFromNumericRows,
  deriveObjectAndNumericColumnarFromNumericRows:
    runDeriveObjectAndNumericColumnarFromNumericRows,
  formatCount: formatRowCount,
} = window.fastTableGeneration;
const tableRenderingApi = window.fastTableRendering || null;
const generationWorkersApi = window.fastTableGenerationWorkers || null;
const sortingApi = window.fastTableSorting || null;
const {
  createRowFilterController,
  createColumnarFilterController,
  createNumericRowFilterController,
  createNumericColumnarFilterController,
  buildDictionaryKeySearchPrefilter,
  precomputeDictionaryKeySearchState,
} = window.fastTableFiltering;

const rowCountEl = document.getElementById("rowCount");
const useWorkerGenerationEl = document.getElementById("useWorkerGeneration");
const workerGenerationOptionsEl = document.getElementById("workerGenerationOptions");
const workerCountEl = document.getElementById("workerCount");
const workerChunkSizeEl = document.getElementById("workerChunkSize");
const workerProgressEl = document.getElementById("workerProgress");
const workerProgressTextEl = document.getElementById("workerProgressText");
const sortModeEl = document.getElementById("sortMode");
const useTypedSortComparatorEl = document.getElementById(
  "useTypedSortComparator"
);
const useIndexSortEl = document.getElementById("useIndexSort");
const resetSortBtnEl = document.getElementById("resetSortBtn");
const useColumnarDataEl = document.getElementById("useColumnarData");
const useBinaryColumnarEl = document.getElementById("useBinaryColumnar");
const useNumericDataEl = document.getElementById("useNumericData");
const enableCachingEl = document.getElementById("enableCaching");
const useDictionaryKeySearchEl = document.getElementById("useDictionaryKeySearch");
const useDictionaryIntersectionEl = document.getElementById(
  "useDictionaryIntersection"
);
const useSmarterPlannerEl = document.getElementById("useSmarterPlanner");
const useSmartFilteringEl = document.getElementById("useSmartFiltering");
const useFilterCacheEl = document.getElementById("useFilterCache");
const clearFilterCacheBtnEl = document.getElementById("clearFilterCacheBtn");
const generateBtnEl = document.getElementById("generateBtn");
const saveObjectBtnEl = document.getElementById("saveObjectBtn");
const loadPregeneratedBtnEl = document.getElementById("loadPregeneratedBtn");
const loadPregeneratedPresetEl = document.getElementById("loadPregeneratedPreset");
const benchmarkBtnEl = document.getElementById("benchmarkBtn");
const benchmarkCurrentBtnEl = document.getElementById("benchmarkCurrentBtn");
const sortBenchmarkBtnEl = document.getElementById("sortBenchmarkBtn");
const sortBenchmarkCurrentBtnEl = document.getElementById(
  "sortBenchmarkCurrentBtn"
);
const generationStatusEl = document.getElementById("generationStatus");
const filterStatusEl = document.getElementById("filterStatus");
const sortTelemetryStatusEl = document.getElementById("sortTelemetryStatus");
const benchmarkStatusEl = document.getElementById("benchmarkStatus");
const generationStatusPanelEl = document.getElementById("generationStatusPanel");
const benchmarkStatusPanelEl = document.getElementById("benchmarkStatusPanel");
const generationStatusClearBtnEl = document.getElementById(
  "generationStatusClearBtn"
);
const generationStatusToggleBtnEl = document.getElementById(
  "generationStatusToggleBtn"
);
const benchmarkStatusClearBtnEl = document.getElementById("benchmarkStatusClearBtn");
const benchmarkStatusToggleBtnEl = document.getElementById(
  "benchmarkStatusToggleBtn"
);
const previewHeadEl = document.getElementById("previewHead");
const previewBodyEl = document.getElementById("previewBody");
const previewTableWrapEl = document.querySelector(".tableWrap");
const appVersionBadgeEl = document.getElementById("appVersionBadge");

const objectRowFilterController = createRowFilterController([]);
const objectColumnarFilterController = createColumnarFilterController(null);
const numericRowFilterController = createNumericRowFilterController([], {
  keyToIndex: columnIndexByKey,
  baseColumnCount,
  cacheOffset: numericCacheOffset,
});
const numericColumnarFilterController = createNumericColumnarFilterController(
  null,
  {
    keyToIndex: columnIndexByKey,
    baseColumnCount,
    cacheOffset: numericCacheOffset,
  }
);
const sortColumnTypeByKey = buildSortColumnTypeByKey(columnKeys);
const sortController =
  sortingApi &&
  typeof sortingApi.createSortController === "function"
    ? sortingApi.createSortController({
        columnKeys,
        defaultColumnKey: "index",
        columnTypeByKey: sortColumnTypeByKey,
        defaultUseTypedComparator: shouldUseTypedSortComparator(),
      })
    : null;

let filterInputs = [];
let sortHeaderButtons = [];
let objectRows = [];

let cachedObjectColumnarData = null;

let cachedNumericRows = null;

let cachedNumericColumnarData = null;
let fastTableEngine = null;

let currentRepresentation = "numeric";
let currentLayout = "row";
let currentColumnarMode = "binary";
let actionButtonsAreDisabled = false;
let actionButtonsDisableCounter = 0;

const MAX_VIRTUAL_VISIBLE_ROWS = 15;
const DEFAULT_WORKER_COUNT = 4;
const DEFAULT_WORKER_CHUNK_SIZE = 10000;
const PRECOMPUTED_GROUP_MIN_DESCRIPTOR_COUNT = 2;
const PRECOMPUTED_FULL_GROUP_MAX_GROUPS = 250000;
const TOP_LEVEL_FILTER_CACHE_MIN_INSERT_MS = 4;
const FILTER_PASS_PREFER_PRECOMPUTED_SORT = true;
const TOP_LEVEL_FILTER_CACHE_SMALL_MAX_RESULTS = 50000;
const TOP_LEVEL_FILTER_CACHE_MEDIUM_MAX_RESULTS = 500000;
const TOP_LEVEL_FILTER_CACHE_SMALL_CAPACITY = 100;
const TOP_LEVEL_FILTER_CACHE_MEDIUM_CAPACITY = 50;
const objectCacheKeys = columnKeys.map((key) => `${key}Cache`);
const filterRuntimeBridge = createFilterRuntimeBridge({
  now: () => performance.now(),
  getLoadedRowCount,
  getCurrentFilterModeKey,
  getFilterOptions,
  getRawFilters: readRawFilters,
  normalizeRawFilters: (rawFilters) =>
    rawFilters && typeof rawFilters === "object" ? rawFilters : {},
  controllers: {
    objectRow: objectRowFilterController,
    objectColumnar: objectColumnarFilterController,
    numericRow: numericRowFilterController,
    numericColumnar: numericColumnarFilterController,
  },
  dataAccessors: {
    getObjectRows: ensureObjectRowsAvailable,
    getObjectColumnarData: () =>
      cachedObjectColumnarData !== null
        ? cachedObjectColumnarData
        : getOrBuildObjectColumnarData().columnarData,
    getNumericRows: () => getOrBuildNumericRows().numericRows,
    getNumericColumnarData: getNumericColumnarDataForDictionaryKeySearch,
  },
  buildDictionaryKeySearchPrefilter,
  keyToIndex: columnIndexByKey,
  isValidNumericColumnarData,
  modePathByKey: {
    "binary-columnar": "numeric-columnar",
    "object-columnar": "object-columnar",
    "numeric-row": "numeric-rows",
    "object-row": "object-rows",
  },
  syncAllControllerIndices: true,
  onCacheStateChange: syncClearFilterCacheButtonState,
  topLevelFilterCacheMinInsertMs: TOP_LEVEL_FILTER_CACHE_MIN_INSERT_MS,
  topLevelFilterCacheSmallMaxResults: TOP_LEVEL_FILTER_CACHE_SMALL_MAX_RESULTS,
  topLevelFilterCacheMediumMaxResults: TOP_LEVEL_FILTER_CACHE_MEDIUM_MAX_RESULTS,
  topLevelFilterCacheSmallCapacity: TOP_LEVEL_FILTER_CACHE_SMALL_CAPACITY,
  topLevelFilterCacheMediumCapacity: TOP_LEVEL_FILTER_CACHE_MEDIUM_CAPACITY,
});
const virtualPreviewRenderer =
  tableRenderingApi &&
  typeof tableRenderingApi.createVirtualTableRenderer === "function" &&
  previewTableWrapEl &&
  previewBodyEl
    ? tableRenderingApi.createVirtualTableRenderer({
        containerEl: previewTableWrapEl,
        bodyEl: previewBodyEl,
        columnCount: columnKeys.length,
        maxRenderRows: MAX_VIRTUAL_VISIBLE_ROWS,
        rowHeight: 34,
      })
    : null;
const appSortRuntimeBridge = createSortRuntimeBridge({
  now: () => performance.now(),
  columnKeys,
  columnIndexByKey,
  columnTypeByKey: sortColumnTypeByKey,
  getSortOptions: getSortRuntimeOptions,
  getSortMode,
  getRowCount: getLoadedRowCount,
  getSchema: getFastTableSchema,
  getNumericColumnarData: getNumericColumnarForSave,
});

function formatMs(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return String(value);
  }

  return numeric.toFixed(2);
}

function formatMsFixed3(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return String(value);
  }

  return numeric.toFixed(3);
}

function formatPercent(value) {
  return Number(value).toFixed(2);
}

async function loadAndRenderAppVersion() {
  if (!appVersionBadgeEl) {
    return;
  }

  try {
    const cacheBuster = Date.now();
    const response = await fetch(`./version.json?v=${cacheBuster}`, {
      cache: "no-store",
    });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const payload = await response.json();
    const version =
      payload && typeof payload.version === "string" ? payload.version.trim() : "";
    if (!version) {
      throw new Error("Missing version field");
    }

    appVersionBadgeEl.textContent = `v${version}`;
  } catch (error) {
    appVersionBadgeEl.textContent = "vunknown";
  }
}

function formatKbFromBytes(bytes) {
  const numeric = Number(bytes);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "0.000";
  }

  return (numeric / 1024).toFixed(3);
}

function formatPregeneratedLoadStatus(
  rowCount,
  loadDurationMs,
  loadTimingDetails
) {
  const details =
    loadTimingDetails && typeof loadTimingDetails === "object"
      ? loadTimingDetails
      : null;
  const detailsTotalMs = details ? Number(details.totalMs) : NaN;
  const totalMs = Number.isFinite(detailsTotalMs)
    ? detailsTotalMs
    : Number(loadDurationMs);
  const baseStatus = `Loaded pregenerated columnar-binary dataset (${formatRowCount(
    rowCount
  )} rows) in ${formatMs(totalMs)} ms.`;

  if (!details) {
    return baseStatus;
  }

  const jsonReadMs = Number(details.jsonReadMs);
  const jsonParseMs = Number(details.jsonParseMs);
  const binReadMs = Number(details.binReadMs);
  const decodeMs = Number(details.decodeMs);
  const downloadMs = Number(details.downloadMs);
  const hasBreakdown =
    Number.isFinite(jsonReadMs) &&
    Number.isFinite(jsonParseMs) &&
    Number.isFinite(binReadMs) &&
    Number.isFinite(decodeMs);
  if (!hasBreakdown) {
    return baseStatus;
  }

  const downloadText = Number.isFinite(downloadMs)
    ? ` Download ${formatMs(downloadMs)} ms.`
    : "";
  return `${baseStatus}${downloadText} JSON read ${formatMs(jsonReadMs)} ms, JSON parse ${formatMs(
    jsonParseMs
  )} ms, BIN read ${formatMs(binReadMs)} ms, decode ${formatMs(decodeMs)} ms.`;
}

function toPositiveInt(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
}

function buildSortColumnTypeByKey(keys) {
  const typeByKey = Object.create(null);
  const inputKeys = Array.isArray(keys) ? keys : [];

  for (let i = 0; i < inputKeys.length; i += 1) {
    const key = inputKeys[i];
    typeByKey[key] =
      key === "firstName" || key === "lastName" ? "string" : "number";
  }

  return typeByKey;
}

function shouldUseTypedSortComparator() {
  if (!useTypedSortComparatorEl) {
    return true;
  }

  return useTypedSortComparatorEl.checked === true;
}

function shouldUseIndexSort() {
  return useIndexSortEl && useIndexSortEl.checked === true;
}

function shouldDisableComparatorSortOptions() {
  return getSortMode() === "precomputed";
}

function syncSortOptionToggleAvailability() {
  const busyDisabled = actionButtonsDisableCounter > 0;
  const modeDisabled = shouldDisableComparatorSortOptions();
  const disabled = busyDisabled || modeDisabled;

  if (useTypedSortComparatorEl) {
    useTypedSortComparatorEl.disabled = disabled;
  }

  if (useIndexSortEl) {
    useIndexSortEl.disabled = disabled;
  }
}

function getSortRuntimeOptions() {
  return {
    useTypedComparator: shouldUseTypedSortComparator(),
    useIndexSort: shouldUseIndexSort(),
  };
}

function setSortRuntimeOptions(nextOptions) {
  const options = nextOptions || {};

  if (
    useTypedSortComparatorEl &&
    typeof options.useTypedComparator === "boolean"
  ) {
    useTypedSortComparatorEl.checked = options.useTypedComparator;
  }

  if (useIndexSortEl && typeof options.useIndexSort === "boolean") {
    useIndexSortEl.checked = options.useIndexSort;
  }
}

function createZeroGenerationMetrics() {
  return {
    rowGenerationMs: 0,
    rowCacheGenerationMs: 0,
    workerRowGenerationMs: 0,
    workerRowCacheGenerationMs: 0,
    workerNumericTransformMs: 0,
    workerNumericCacheGenerationMs: 0,
    workerColumnarDerivationMs: 0,
    workerColumnarCacheGenerationMs: 0,
    workerWallByIndex: [],
    numericTransformMs: 0,
    numericCacheGenerationMs: 0,
    columnarDerivationMs: 0,
    columnarCacheGenerationMs: 0,
    workerPhaseWallMs: 0,
    usedWorkerMode: false,
    sortedIndexPrecomputeMs: 0,
    sortedRankPrecomputeMs: 0,
    finalizeMs: 0,
    totalMs: 0,
  };
}

function setGenerationStatus(rowCount, metrics) {
  const lines = [];
  const workerWallByIndex = Array.isArray(metrics.workerWallByIndex)
    ? metrics.workerWallByIndex
    : [];
  const workerWallSuffix =
    workerWallByIndex.length > 0
      ? ` (worker wall: ${workerWallByIndex
          .map((value, idx) => `w${idx + 1} ${formatMs(value)} ms`)
          .join(", ")})`
      : "";

  if (metrics.usedWorkerMode) {
    const workerComputeParts = [];
    if (metrics.workerRowGenerationMs > 0) {
      workerComputeParts.push(
        `rows ${formatMs(metrics.workerRowGenerationMs)} ms`
      );
    }
    if (metrics.workerColumnarDerivationMs > 0) {
      workerComputeParts.push(
        `fused columnar (object+numeric+cache) ${formatMs(
          metrics.workerColumnarDerivationMs
        )} ms`
      );
    }
    if (metrics.workerRowCacheGenerationMs > 0) {
      workerComputeParts.push(
        `row cache ${formatMs(metrics.workerRowCacheGenerationMs)} ms`
      );
    }
    if (metrics.workerNumericTransformMs > 0) {
      workerComputeParts.push(
        `numeric transform ${formatMs(metrics.workerNumericTransformMs)} ms`
      );
    }
    if (metrics.workerNumericCacheGenerationMs > 0) {
      workerComputeParts.push(
        `numeric cache ${formatMs(metrics.workerNumericCacheGenerationMs)} ms`
      );
    }
    if (metrics.workerColumnarCacheGenerationMs > 0) {
      workerComputeParts.push(
        `columnar cache ${formatMs(metrics.workerColumnarCacheGenerationMs)} ms`
      );
    }

    const workerSubtotalMs = metrics.workerPhaseWallMs + metrics.finalizeMs;
    lines.push(
      `Generated ${formatRowCount(rowCount)} rows x ${columnNames.length} columns.`,
      `Worker chunk phase (wall): ${formatMs(metrics.workerPhaseWallMs)} ms.`,
      workerComputeParts.length > 0
        ? `Worker compute (non-additive): ${workerComputeParts.join(", ")}${workerWallSuffix}.`
        : `Worker compute (non-additive): none${workerWallSuffix}.`,
      `Finalize step: ${formatMs(metrics.finalizeMs)} ms.`,
      `Subtotal (chunk wall + finalize): ${formatMs(workerSubtotalMs)} ms.`,
      `Sorted indices precompute: ${formatMs(metrics.sortedIndexPrecomputeMs)} ms.`,
      `Sorted rank arrays precompute: ${formatMs(metrics.sortedRankPrecomputeMs)} ms.`,
      `Total time: ${formatMs(metrics.totalMs)} ms.`
    );
  } else {
    const rowBuildMs = metrics.rowGenerationMs + metrics.rowCacheGenerationMs;
    const finalizeWorkParts = [];
    if (metrics.numericTransformMs > 0) {
      finalizeWorkParts.push(
        `numeric rows (+cache) ${formatMs(metrics.numericTransformMs)} ms`
      );
    }
    if (metrics.columnarDerivationMs > 0) {
      finalizeWorkParts.push(
        `fused columnar (object+numeric+cache) ${formatMs(
          metrics.columnarDerivationMs
        )} ms`
      );
    }
    const mainSubtotalMs = rowBuildMs + metrics.finalizeMs;

    lines.push(
      `Generated ${formatRowCount(rowCount)} rows x ${columnNames.length} columns.`,
      `Main build: rows ${formatMs(metrics.rowGenerationMs)} ms + row cache ${formatMs(
        metrics.rowCacheGenerationMs
      )} ms = ${formatMs(rowBuildMs)} ms.`,
      finalizeWorkParts.length > 0
        ? `Finalize work (non-additive): ${finalizeWorkParts.join(", ")}.`
        : "Finalize work (non-additive): none.",
      `Finalize step: ${formatMs(metrics.finalizeMs)} ms.`,
      `Subtotal (build + finalize): ${formatMs(mainSubtotalMs)} ms.`,
      `Sorted indices precompute: ${formatMs(metrics.sortedIndexPrecomputeMs)} ms.`,
      `Sorted rank arrays precompute: ${formatMs(metrics.sortedRankPrecomputeMs)} ms.`,
      `Total time: ${formatMs(metrics.totalMs)} ms.`
    );
  }

  generationStatusEl.innerHTML = lines.join("<br>");
}

function setupTelemetryPanelControls(
  panelEl,
  statusBodyEl,
  clearBtnEl,
  toggleBtnEl
) {
  if (!panelEl || !statusBodyEl) {
    return;
  }

  function updateToggleUi() {
    if (!toggleBtnEl) {
      return;
    }

    const isCollapsed = panelEl.classList.contains("collapsed");
    toggleBtnEl.dataset.glyph = isCollapsed ? "\u25BC" : "\u25B2";
    toggleBtnEl.title = isCollapsed
      ? "Maximize telemetry panel"
      : "Minimize telemetry panel";
  }

  if (clearBtnEl) {
    clearBtnEl.addEventListener("click", () => {
      statusBodyEl.textContent = "";
    });
  }

  if (toggleBtnEl) {
    toggleBtnEl.addEventListener("click", () => {
      panelEl.classList.toggle("collapsed");
      updateToggleUi();
    });
  }

  updateToggleUi();
}

function initializeTelemetryPanelControls() {
  setupTelemetryPanelControls(
    generationStatusPanelEl,
    generationStatusEl,
    generationStatusClearBtnEl,
    generationStatusToggleBtnEl
  );
  setupTelemetryPanelControls(
    benchmarkStatusPanelEl,
    benchmarkStatusEl,
    benchmarkStatusClearBtnEl,
    benchmarkStatusToggleBtnEl
  );
}

function resetWorkerProgressUI() {
  if (!workerProgressEl || !workerProgressTextEl) {
    return;
  }

  workerProgressEl.value = 0;
  workerProgressTextEl.textContent = "Worker progress: idle.";
}

function setWorkerProgressText(text) {
  if (!workerProgressTextEl) {
    return;
  }

  workerProgressTextEl.textContent = text;
}

function updateWorkerProgressUI(progress) {
  if (!workerProgressEl || !workerProgressTextEl) {
    return;
  }

  const completedRows = progress && Number(progress.completedRows) ? Number(progress.completedRows) : 0;
  const totalRows = progress && Number(progress.totalRows) ? Number(progress.totalRows) : 0;
  const percentRaw = progress && Number(progress.percent) ? Number(progress.percent) : 0;
  const percent = Math.max(0, Math.min(100, percentRaw));
  const avgChunkMs = progress && Number(progress.avgChunkMs) ? Number(progress.avgChunkMs) : 0;
  const lastChunkMs = progress && Number(progress.lastChunkMs) ? Number(progress.lastChunkMs) : 0;

  workerProgressEl.value = percent;
  workerProgressTextEl.textContent =
    `Worker progress: ${formatPercent(percent)}% (${formatRowCount(completedRows)} / ${formatRowCount(totalRows)} rows). ` +
    `Avg chunk worker total: ${formatMsFixed3(avgChunkMs)} ms. Last chunk: ${formatMsFixed3(lastChunkMs)} ms.`;
}

function syncWorkerGenerationControls() {
  if (!useWorkerGenerationEl) {
    return;
  }

  const workersEnabled = useWorkerGenerationEl.checked === true;
  if (workerGenerationOptionsEl) {
    workerGenerationOptionsEl.style.display = workersEnabled ? "inline-flex" : "none";
  }

  if (!workersEnabled) {
    if (workerProgressEl) {
      workerProgressEl.value = 0;
    }

    setWorkerProgressText("Worker generation disabled.");
    return;
  }

  resetWorkerProgressUI();
}

function hasTopLevelFilterCacheEntries() {
  return filterRuntimeBridge.hasTopLevelFilterCacheEntries();
}

function syncClearFilterCacheButtonState() {
  if (!clearFilterCacheBtnEl) {
    return;
  }

  const hasCacheEntries = hasTopLevelFilterCacheEntries();
  clearFilterCacheBtnEl.disabled = actionButtonsAreDisabled || !hasCacheEntries;
}

function clearTopLevelFilterCache() {
  filterRuntimeBridge.clearTopLevelFilterCache();
}

function clearTopLevelSmartFilterState() {
  filterRuntimeBridge.clearTopLevelSmartFilterState();
}

function bumpTopLevelFilterCacheRevision() {
  filterRuntimeBridge.bumpTopLevelFilterCacheRevision();
}

function getCurrentFilterModeKey() {
  if (currentLayout === "columnar" && currentColumnarMode === "binary") {
    return "binary-columnar";
  }

  if (currentLayout === "columnar") {
    return "object-columnar";
  }

  if (currentRepresentation === "numeric") {
    return "numeric-row";
  }

  return "object-row";
}

function clearAllFilterCaches() {
  filterRuntimeBridge.clearAllFilterCaches();
}

function setSortTelemetryStatus(text) {
  if (!sortTelemetryStatusEl) {
    return;
  }

  sortTelemetryStatusEl.textContent = text;
}

function getSortMode() {
  if (sortModeEl && sortModeEl.value) {
    return sortModeEl.value;
  }

  if (sortingApi && sortingApi.DEFAULT_SORT_MODE) {
    return sortingApi.DEFAULT_SORT_MODE;
  }

  return "native";
}

function getAvailableSortModes() {
  if (!sortModeEl) {
    return ["native"];
  }

  const modes = [];
  for (let i = 0; i < sortModeEl.options.length; i += 1) {
    const option = sortModeEl.options[i];
    if (option.disabled) {
      continue;
    }
    const optionValue = option.value;
    if (optionValue) {
      modes.push(optionValue);
    }
  }

  return modes.length > 0 ? modes : ["native"];
}

function isTimSortAvailable() {
  return (
    typeof window !== "undefined" && typeof window.FastTimSort === "function"
  );
}

function syncSortModeAvailability() {
  if (!sortModeEl) {
    syncSortOptionToggleAvailability();
    return;
  }

  const timSortAvailable = isTimSortAvailable();
  let timsortOption = null;
  for (let i = 0; i < sortModeEl.options.length; i += 1) {
    const opt = sortModeEl.options[i];
    if (opt.value === "timsort") {
      timsortOption = opt;
    }
  }

  if (timsortOption && !timSortAvailable) {
    timsortOption.disabled = true;
    timsortOption.textContent = "timsort (unavailable)";
    if (sortModeEl.value === "timsort") {
      sortModeEl.value = "native";
    }
  } else if (timsortOption) {
    timsortOption.disabled = false;
    timsortOption.textContent = "timsort";
  }

  syncSortOptionToggleAvailability();
}

function getColumnLabelByKey(columnKey) {
  const columnIndex = columnIndexByKey[columnKey];
  if (typeof columnIndex === "number" && columnNames[columnIndex]) {
    return columnNames[columnIndex];
  }

  return columnKey;
}

function formatSortDescriptorList(descriptors) {
  if (!Array.isArray(descriptors) || descriptors.length === 0) {
    return "none";
  }

  return descriptors
    .map((descriptor, index) => {
      const columnLabel = getColumnLabelByKey(descriptor.columnKey);
      return `${index + 1}) ${columnLabel} ${descriptor.direction}`;
    })
    .join(" -> ");
}

function setSortTelemetryFromResult(sortResult, sortedCount, extraMetrics) {
  if (!sortResult) {
    setSortTelemetryStatus("No sort telemetry yet.");
    return;
  }

  const metrics = extraMetrics || {};
  const sortedRowsText =
    typeof sortedCount === "number"
      ? `Rows sorted: ${formatRowCount(sortedCount)}. `
      : "";
  const dataPathText =
    typeof sortResult.dataPath === "string" && sortResult.dataPath !== ""
      ? sortResult.dataPath
      : null;
  const timingParts = [];
  const sortCoreMs = Number(sortResult.durationMs);
  const rankBuildMs = Number(metrics.rankBuildMs);
  const hasRankBuildMs = Number.isFinite(rankBuildMs) && rankBuildMs > 0;
  const otherPrepMs = hasRankBuildMs
    ? Math.max(0, Number(metrics.sortPrepMs) - rankBuildMs)
    : null;
  const sortModeWithComparator = sortResult.comparatorMode
    ? `${sortResult.sortMode}, ${sortResult.comparatorMode} comparator${
        dataPathText ? `, ${dataPathText} path` : ""
      }`
    : sortResult.sortMode;
  if (Number.isFinite(metrics.sortTotalMs) && Number.isFinite(metrics.sortPrepMs)) {
    const prepText = hasRankBuildMs
      ? `prep ${formatMs(metrics.sortPrepMs)} ms (rank arrays ${formatMs(
          rankBuildMs
        )} + other ${formatMs(otherPrepMs)} ms)`
      : `prep ${formatMs(metrics.sortPrepMs)} ms`;
    timingParts.push(
      `Sort core ${formatMs(sortCoreMs)} ms (${sortModeWithComparator}) + ${prepText} = total ${formatMs(
        metrics.sortTotalMs
      )} ms`
    );
  } else {
    timingParts.push(
      `Sort core ${formatMs(sortCoreMs)} ms (${sortModeWithComparator})`
    );
    if (Number.isFinite(metrics.sortPrepMs)) {
      if (hasRankBuildMs) {
        timingParts.push(
          `prep ${formatMs(metrics.sortPrepMs)} ms (rank arrays ${formatMs(
            rankBuildMs
          )} + other ${formatMs(otherPrepMs)} ms)`
        );
      } else {
        timingParts.push(`prep ${formatMs(metrics.sortPrepMs)} ms`);
      }
    }
    if (Number.isFinite(metrics.sortTotalMs)) {
      timingParts.push(`total ${formatMs(metrics.sortTotalMs)} ms`);
    }
  }
  if (Number.isFinite(metrics.filterCoreMs)) {
    timingParts.push(`filter core ${formatMs(metrics.filterCoreMs)} ms`);
  }
  if (Number.isFinite(metrics.renderMs)) {
    timingParts.push(`render ${formatMs(metrics.renderMs)} ms`);
  }
  if (Number.isFinite(metrics.wallMs)) {
    timingParts.push(`wall ${formatMs(metrics.wallMs)} ms`);
  }
  const timingText =
    timingParts.length > 0 ? ` ${timingParts.join(", ")}.` : "";

  if (sortResult.restoredDefault) {
    setSortTelemetryStatus(
      `${sortedRowsText} Sort plan: ${formatSortDescriptorList(
        sortResult.descriptors
      )}. Effective order: ${formatSortDescriptorList(
        sortResult.effectiveDescriptors
      )}.${timingText}`
    );
  } else {
    setSortTelemetryStatus(
      `${sortedRowsText} Sorted by: ${formatSortDescriptorList(
        sortResult.effectiveDescriptors
      )}.${timingText}`
    );
  }
}

function normalizeSortDirection(direction) {
  if (direction === "asc") {
    return "asc";
  }

  return "desc";
}

function normalizeSortDescriptorList(descriptors) {
  if (!Array.isArray(descriptors)) {
    return [];
  }

  const out = [];
  for (let i = 0; i < descriptors.length; i += 1) {
    const descriptor = descriptors[i];
    if (!descriptor || typeof descriptor.columnKey !== "string") {
      continue;
    }

    out.push({
      columnKey: descriptor.columnKey,
      direction: normalizeSortDirection(descriptor.direction),
    });
  }

  return out;
}

function isSupportedRankArray(rankByRowId) {
  return (
    rankByRowId instanceof Uint32Array || rankByRowId instanceof Uint16Array
  );
}

function areSortDescriptorsEqual(left, right) {
  return (
    !!left &&
    !!right &&
    left.columnKey === right.columnKey &&
    normalizeSortDirection(left.direction) === normalizeSortDirection(right.direction)
  );
}

function getMatchingSortDescriptorPrefixLength(leftList, rightList) {
  const left = Array.isArray(leftList) ? leftList : [];
  const right = Array.isArray(rightList) ? rightList : [];
  const limit = Math.min(left.length, right.length);
  let index = 0;
  for (; index < limit; index += 1) {
    if (!areSortDescriptorsEqual(left[index], right[index])) {
      break;
    }
  }
  return index;
}

function areSortDescriptorListsEqual(leftList, rightList) {
  const left = Array.isArray(leftList) ? leftList : [];
  const right = Array.isArray(rightList) ? rightList : [];
  if (left.length !== right.length) {
    return false;
  }

  for (let i = 0; i < left.length; i += 1) {
    if (!areSortDescriptorsEqual(left[i], right[i])) {
      return false;
    }
  }

  return true;
}

function buildRowsSnapshotFromRawFilters(rawFilters) {
  const sourceRawFilters =
    rawFilters && typeof rawFilters === "object" ? rawFilters : readRawFilters();
  const filterResult =
    fastTableEngine && typeof fastTableEngine.executeFilterCore === "function"
      ? fastTableEngine.executeFilterCore(sourceRawFilters, {
          filterOptions: getFilterOptions(),
        })
      : filterRuntimeBridge.runFilterPassWithRawFilters(sourceRawFilters, {
          filterOptions: getFilterOptions(),
        });
  const loadedRowCount = getLoadedRowCount();
  const snapshotIndices = materializeFilteredIndexArray(
    filterResult ? filterResult.filteredIndices : null,
    loadedRowCount
  );

  return {
    snapshotType: "row-indices-v2",
    rowIndices: snapshotIndices,
    count: snapshotIndices.length,
    filterCoreMs: Number(filterResult && filterResult.coreMs) || 0,
  };
}

function runSortSnapshotPass(rowsSnapshot, descriptors, sortMode) {
  return appSortRuntimeBridge.runSortSnapshotPass(rowsSnapshot, descriptors, sortMode);
}

function getSortSymbolForState(state) {
  if (sortingApi && typeof sortingApi.getSortSymbol === "function") {
    return sortingApi.getSortSymbol(state);
  }

  if (state === "desc") {
    return "\u25BC";
  }

  if (state === "asc") {
    return "\u25B2";
  }

  return "\u25B6";
}

function updateSortHeaderIndicators() {
  if (!sortController) {
    return;
  }

  for (let i = 0; i < sortHeaderButtons.length; i += 1) {
    const item = sortHeaderButtons[i];
    const state = sortController.getStateForKey(item.key);
    item.iconEl.textContent = getSortSymbolForState(state);
    item.buttonEl.classList.toggle("active", state !== "none");
  }
}

function resetSortState(updateStatus) {
  if (!sortController) {
    return;
  }

  sortController.reset();
  updateSortHeaderIndicators();
  if (updateStatus !== false) {
    setSortTelemetryStatus("No sort telemetry yet.");
  }
}

function runSortCore(filterResult, options) {
  const executionOptions = options || {};
  if (!sortController) {
    return {
      kind: "no-sort-controller",
    };
  }

  if (getLoadedRowCount() === 0) {
    return {
      kind: "no-data",
    };
  }

  const sourceFilterResult =
    filterResult && typeof filterResult === "object"
      ? filterResult
      : getCurrentFilterResultSnapshot();
  const sortRun = buildSortedIndicesForCurrentResult(
    sourceFilterResult,
    executionOptions
  );
  const renderIndices =
    sortRun && hasIndexCollection(sortRun.indices)
      ? sortRun.indices
      : sourceFilterResult.filteredIndices;

  return {
    kind: "ok",
    filterResult: sourceFilterResult,
    sortRun,
    renderIndices,
  };
}

function renderSortUi(coreRun, options) {
  const executionOptions = options || {};
  const skipRender = executionOptions.skipRender === true;
  const skipStatus = executionOptions.skipStatus === true;
  const keepScroll = executionOptions.keepScroll !== false;
  const wallStartMs = Number.isFinite(executionOptions.wallStartMs)
    ? Number(executionOptions.wallStartMs)
    : performance.now();

  if (!coreRun || coreRun.kind === "no-sort-controller") {
    return null;
  }

  if (coreRun.kind === "no-data") {
    if (!skipStatus) {
      setSortTelemetryStatus("No data loaded yet.");
    }
    return null;
  }

  const filterResult = coreRun.filterResult;
  const sortRun = coreRun.sortRun;
  const renderIndices = coreRun.renderIndices;

  let renderMs = 0;
  if (!skipRender) {
    const renderStartMs = performance.now();
    renderFilterResultByCurrentMode(filterResult, renderIndices, keepScroll);
    renderMs = performance.now() - renderStartMs;
    window.fastTableFilteredRows = null;
    window.fastTableFilteredRowIndices = renderIndices;
  }

  const wallMs = performance.now() - wallStartMs;
  updateSortHeaderIndicators();

  if (!sortRun || !sortRun.result) {
    if (!skipStatus) {
      setSortTelemetryStatus("No sort telemetry yet.");
    }
    return null;
  }

  if (!skipStatus) {
    setSortTelemetryFromResult(sortRun.result, sortRun.sortedCount, {
      sortTotalMs: sortRun.sortTotalMs,
      sortPrepMs: sortRun.sortPrepMs,
      rankBuildMs: sortRun.rankBuildMs,
      renderMs,
      wallMs,
    });
  }
  return sortRun.result;
}

function runSortFromCurrentUi(options) {
  const executionOptions = options || {};
  const wallStartMs = performance.now();
  const sourceFilterResult =
    executionOptions.filterResult || getCurrentFilterResultSnapshot();
  const coreRun = runSortCore(sourceFilterResult, executionOptions);
  return renderSortUi(
    coreRun,
    Object.assign({}, executionOptions, {
      wallStartMs,
    })
  );
}

function applySortAndRender(options) {
  return runSortFromCurrentUi(options);
}

function runSortInteraction(action, options) {
  if (typeof action !== "function") {
    return;
  }

  const interactionOptions = options || {};
  const disableButtons = interactionOptions.disableButtons !== false;

  if (disableButtons) {
    setActionButtonsDisabled(true);
  }
  setTimeout(() => {
    try {
      action();
    } catch (error) {
      const message =
        error && error.message ? error.message : String(error);
      setSortTelemetryStatus(`Sort failed: ${message}`);
      if (typeof console !== "undefined" && console.error) {
        console.error(error);
      }
    } finally {
      if (disableButtons) {
        setActionButtonsDisabled(false);
      }
    }
  }, 0);
}

function setActionButtonsDisabled(disabled) {
  if (disabled) {
    actionButtonsDisableCounter += 1;
  } else {
    actionButtonsDisableCounter = Math.max(0, actionButtonsDisableCounter - 1);
  }

  const nextDisabled = actionButtonsDisableCounter > 0;
  if (actionButtonsAreDisabled !== nextDisabled) {
    setGlobalBusyState(nextDisabled);
    actionButtonsAreDisabled = nextDisabled;
  }

  generateBtnEl.disabled = nextDisabled;
  if (saveObjectBtnEl) {
    saveObjectBtnEl.disabled = nextDisabled;
  }

  if (loadPregeneratedBtnEl) {
    loadPregeneratedBtnEl.disabled = nextDisabled;
  }

  if (loadPregeneratedPresetEl) {
    loadPregeneratedPresetEl.disabled = nextDisabled;
  }

  if (benchmarkBtnEl) {
    benchmarkBtnEl.disabled = nextDisabled;
  }

  if (benchmarkCurrentBtnEl) {
    benchmarkCurrentBtnEl.disabled = nextDisabled;
  }

  if (sortBenchmarkBtnEl) {
    sortBenchmarkBtnEl.disabled = nextDisabled;
  }

  if (sortBenchmarkCurrentBtnEl) {
    sortBenchmarkCurrentBtnEl.disabled = nextDisabled;
  }

  if (sortModeEl) {
    sortModeEl.disabled = nextDisabled;
  }

  syncSortOptionToggleAvailability();

  if (resetSortBtnEl) {
    resetSortBtnEl.disabled = nextDisabled;
  }

  for (let i = 0; i < sortHeaderButtons.length; i += 1) {
    sortHeaderButtons[i].buttonEl.disabled = nextDisabled;
  }

  syncClearFilterCacheButtonState();
}

function setGlobalBusyState(isBusy) {
  if (typeof window === "undefined") {
    return;
  }

  const counterKey = "__fastTableBusyCounter";
  const current = Number(window[counterKey]) || 0;
  const next = Math.max(0, current + (isBusy ? 1 : -1));
  window[counterKey] = next;

  if (document && document.body) {
    document.body.classList.toggle("busyCursor", next > 0);
  }
}

window.fastTableSetBusyState = setGlobalBusyState;
window.fastTableSetActionButtonsDisabled = setActionButtonsDisabled;

function shouldUseWorkerGeneration() {
  return (
    useWorkerGenerationEl &&
    useWorkerGenerationEl.checked === true &&
    generationWorkersApi &&
    typeof generationWorkersApi.generateRowsWithWorkers === "function"
  );
}

function generateRowsOnMainThread(rowCount) {
  const rowGenerationStart = performance.now();
  const rows = runGenerateRowsWithoutCache(rowCount);
  const rowGenerationEnd = performance.now();
  const rowCacheGenerationStart = performance.now();
  runAttachCachesToRows(rows);
  const rowCacheGenerationEnd = performance.now();

  return {
    rows,
    metrics: {
      rowGenerationMs: rowGenerationEnd - rowGenerationStart,
      rowCacheGenerationMs: rowCacheGenerationEnd - rowCacheGenerationStart,
      numericTransformMs: 0,
      numericCacheGenerationMs: 0,
      columnarDerivationMs: 0,
      columnarCacheGenerationMs: 0,
      workerWallByIndex: [],
      workerPhaseWallMs: rowCacheGenerationEnd - rowGenerationStart,
      totalMs: 0,
    },
  };
}

function clearCurrentDatasetBeforeGeneration() {
  if (getLoadedRowCount() > 0) {
    setObjectRowsDataset([]);
  }

  resetSortState();
  clearPreviewBody();
  filterStatusEl.textContent = "No data loaded yet.";
}

async function generateRowsWithWorkers(rowCount) {
  const workerCount = toPositiveInt(
    workerCountEl ? workerCountEl.value : "",
    DEFAULT_WORKER_COUNT
  );
  const chunkSize = toPositiveInt(
    workerChunkSizeEl ? workerChunkSizeEl.value : "",
    DEFAULT_WORKER_CHUNK_SIZE
  );

  const workerResult = await generationWorkersApi.generateRowsWithWorkers({
    rowCount,
    workerCount,
    chunkSize,
    onProgress(progress) {
      updateWorkerProgressUI(progress);
    },
  });

  return {
    rows: Array.isArray(workerResult.rows) ? workerResult.rows : [],
    derivedData: workerResult.derivedData || null,
    metrics: {
      rowGenerationMs: workerResult.workerMetrics.rowGenerationMs,
      rowCacheGenerationMs: workerResult.workerMetrics.rowCacheGenerationMs,
      numericTransformMs: workerResult.workerMetrics.numericTransformMs,
      numericCacheGenerationMs: workerResult.workerMetrics.numericCacheGenerationMs,
      columnarDerivationMs: workerResult.workerMetrics.columnarDerivationMs,
      columnarCacheGenerationMs: workerResult.workerMetrics.columnarCacheGenerationMs,
      workerWallByIndex: workerResult.perWorkerWallMs || [],
      workerPhaseWallMs: workerResult.wallMs,
      totalMs: 0,
    },
    avgChunkMs: workerResult.avgChunkMs,
    totalChunks: workerResult.totalChunks,
    completedChunks: workerResult.completedChunks,
  };
}

async function precomputeSortedIndicesWithWorkersForCurrentData() {
  if (!isValidNumericColumnarData(cachedNumericColumnarData)) {
    return {
      durationMs: 0,
      sortedIndexPrecomputeMs: 0,
      sortedRankPrecomputeMs: 0,
      totalColumns: 0,
      completedColumns: 0,
    };
  }

  const numericColumnarData = ensureNumericColumnarCacheColumns(
    cachedNumericColumnarData
  );
  const canUseSortWorkers =
    generationWorkersApi &&
    typeof generationWorkersApi.buildSortedIndicesWithWorkers === "function";

  if (!canUseSortWorkers) {
    const sortedStartMs = performance.now();
    cachedNumericColumnarData = ensureNumericColumnarSortedIndices(
      numericColumnarData
    );
    const sortedIndexPrecomputeMs = performance.now() - sortedStartMs;
    const rankPrecompute = ensureNumericColumnarSortedRanks(
      cachedNumericColumnarData
    );
    cachedNumericColumnarData = rankPrecompute.numericColumnarData;
    const sortedRankPrecomputeMs = Number(rankPrecompute.durationMs) || 0;
    window.fastTableNumericColumnarData = cachedNumericColumnarData;
    return {
      durationMs: sortedIndexPrecomputeMs + sortedRankPrecomputeMs,
      sortedIndexPrecomputeMs,
      sortedRankPrecomputeMs,
      totalColumns: 0,
      completedColumns: 0,
    };
  }

  const workerCount = toPositiveInt(
    workerCountEl ? workerCountEl.value : "",
    DEFAULT_WORKER_COUNT
  );
  const result = await generationWorkersApi.buildSortedIndicesWithWorkers({
    numericColumnarData,
    workerCount,
    onProgress(progress) {
      if (!progress) {
        return;
      }
      const completed = Math.max(0, Number(progress.completedColumns) || 0);
      const total = Math.max(0, Number(progress.totalColumns) || 0);
      if (total > 0) {
        setWorkerProgressText(
          `Workers precompute sorted indices: ${formatRowCount(completed)} / ${formatRowCount(total)} columns.`
        );
      }
    },
  });

  if (result && Array.isArray(result.sortedIndexColumns)) {
    numericColumnarData.sortedIndexColumns = result.sortedIndexColumns;
    numericColumnarData.sortedIndexByKey =
      result.sortedIndexByKey &&
      typeof result.sortedIndexByKey === "object" &&
      !Array.isArray(result.sortedIndexByKey)
        ? result.sortedIndexByKey
        : Object.create(null);
  } else {
    numericColumnarData.sortedIndexColumns = [];
    numericColumnarData.sortedIndexByKey = Object.create(null);
  }

  const dictionaryFillStartMs = performance.now();
  const completedNumericColumnarData = ensureNumericColumnarSortedIndices(
    numericColumnarData
  );
  const sortedIndexPrecomputeMs = performance.now() - dictionaryFillStartMs;
  const rankPrecompute = ensureNumericColumnarSortedRanks(
    completedNumericColumnarData
  );
  const numericWithRanks = rankPrecompute.numericColumnarData;
  const sortedRankPrecomputeMs = Number(rankPrecompute.durationMs) || 0;

  cachedNumericColumnarData = numericWithRanks;
  window.fastTableNumericColumnarData = numericWithRanks;

  const baseResult = result || {
    durationMs: 0,
    totalColumns: 0,
    completedColumns: 0,
  };
  const workerDurationMs = Number(baseResult.durationMs) || 0;
  return Object.assign({}, baseResult, {
    durationMs:
      workerDurationMs + sortedIndexPrecomputeMs + sortedRankPrecomputeMs,
    sortedIndexPrecomputeMs: workerDurationMs + sortedIndexPrecomputeMs,
    sortedRankPrecomputeMs,
    dictionarySortedFillMs: sortedIndexPrecomputeMs,
  });
}

function invalidateDerivedData() {
  cachedObjectColumnarData = null;
  objectColumnarFilterController.setData(null);

  cachedNumericRows = null;
  numericRowFilterController.setRows([]);

  cachedNumericColumnarData = null;
  numericColumnarFilterController.setData(null);

  window.fastTableColumnarData = null;
  window.fastTableNumericRows = null;
  window.fastTableNumericColumnarData = null;

  if (
    appSortRuntimeBridge &&
    typeof appSortRuntimeBridge.resetPrecomputedSortState === "function"
  ) {
    appSortRuntimeBridge.resetPrecomputedSortState(getLoadedRowCount());
  }
}

function getLoadedRowCount() {
  if (Array.isArray(objectRows) && objectRows.length > 0) {
    return objectRows.length;
  }

  if (
    cachedNumericColumnarData &&
    typeof cachedNumericColumnarData.rowCount === "number"
  ) {
    return cachedNumericColumnarData.rowCount;
  }

  if (
    cachedObjectColumnarData &&
    typeof cachedObjectColumnarData.rowCount === "number"
  ) {
    return cachedObjectColumnarData.rowCount;
  }

  return 0;
}

function setObjectRowsDataset(rows) {
  objectRows = Array.isArray(rows) ? rows : [];
  objectRowFilterController.setRows(objectRows);
  bumpTopLevelFilterCacheRevision();
  invalidateDerivedData();

  window.fastTableRows = objectRows;
  window.fastTableColumns = columnNames;
  window.fastTableFilteredRows = null;
  window.fastTableFilteredRowIndices = null;
  window.fastTableLastFilterMode = null;
}

function materializeObjectRowsFromObjectColumnar(columnarData) {
  const rowCount =
    columnarData && typeof columnarData.rowCount === "number"
      ? columnarData.rowCount
      : 0;
  const columns = (columnarData && columnarData.columns) || {};
  const rows = new Array(rowCount);

  for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
    const row = {};

    for (let colIndex = 0; colIndex < columnKeys.length; colIndex += 1) {
      const key = columnKeys[colIndex];
      const cacheKey = objectCacheKeys[colIndex];
      const valueColumn = columns[key];
      const cacheColumn = columns[cacheKey];
      const value = valueColumn ? valueColumn[rowIndex] : "";

      row[key] = value;
      row[cacheKey] =
        cacheColumn && cacheColumn[rowIndex] !== undefined
          ? cacheColumn[rowIndex]
          : String(value).toLowerCase();
    }

    rows[rowIndex] = row;
  }

  return rows;
}

function materializeObjectRowsFromNumericColumnar(numericColumnarData) {
  const rowCount =
    numericColumnarData && typeof numericColumnarData.rowCount === "number"
      ? numericColumnarData.rowCount
      : 0;
  const rows = new Array(rowCount);
  const columnKinds = numericColumnarData.columnKinds || [];
  const columns = numericColumnarData.columns || [];
  const dictionaries = numericColumnarData.dictionaries || [];
  const cacheColumns = numericColumnarData.cacheColumns;
  const hasCacheColumns =
    Array.isArray(cacheColumns) && cacheColumns.length >= columnKeys.length;

  for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
    const row = {};

    for (let colIndex = 0; colIndex < columnKeys.length; colIndex += 1) {
      const kind = columnKinds[colIndex];
      const key = columnKeys[colIndex];
      const cacheKey = objectCacheKeys[colIndex];
      let value = "";

      if (kind === "stringId") {
        const ids = columns[colIndex];
        const dict = dictionaries[colIndex] || [];
        value = dict[ids[rowIndex]];
      } else {
        value = columns[colIndex][rowIndex];
      }

      row[key] = value;
      if (hasCacheColumns) {
        row[cacheKey] = cacheColumns[colIndex][rowIndex];
      } else {
        row[cacheKey] = String(value).toLowerCase();
      }
    }

    rows[rowIndex] = row;
  }

  return rows;
}

function ensureObjectRowsAvailable() {
  if (Array.isArray(objectRows) && objectRows.length > 0) {
    return objectRows;
  }

  if (!isValidObjectColumnarData(cachedObjectColumnarData)) {
    if (!isValidNumericColumnarData(cachedNumericColumnarData)) {
      return objectRows;
    }

    objectRows = materializeObjectRowsFromNumericColumnar(
      cachedNumericColumnarData
    );
    objectRowFilterController.setRows(objectRows);
    window.fastTableRows = objectRows;
    return objectRows;
  }

  objectRows = materializeObjectRowsFromObjectColumnar(cachedObjectColumnarData);
  objectRowFilterController.setRows(objectRows);
  window.fastTableRows = objectRows;
  return objectRows;
}

function getOrBuildObjectColumnarData() {
  if (cachedObjectColumnarData !== null) {
    return {
      columnarData: cachedObjectColumnarData,
      columnarDerivationMs: 0,
      columnarCacheGenerationMs: 0,
    };
  }

  const sourceRows = ensureObjectRowsAvailable();
  const metrics = { cacheGenerationMs: 0 };
  const start = performance.now();
  const columnarData = runDeriveColumnarDataFromRows(sourceRows, {
    enableCaching: true,
    metrics,
  });
  const end = performance.now();

  cachedObjectColumnarData = columnarData;
  return {
    columnarData,
    columnarDerivationMs: end - start,
    columnarCacheGenerationMs: metrics.cacheGenerationMs,
  };
}

function getOrBuildNumericRows() {
  if (cachedNumericRows !== null) {
    return {
      numericRows: cachedNumericRows,
      numericTransformMs: 0,
      numericCacheGenerationMs: 0,
    };
  }

  const sourceRows = ensureObjectRowsAvailable();

  const metrics = { cacheGenerationMs: 0 };
  const start = performance.now();
  const transformed = runDeriveNumericRowsFromRows(sourceRows, {
    enableCaching: true,
    metrics,
  });
  const end = performance.now();

  cachedNumericRows = transformed.rows;

  cachedNumericColumnarData = null;
  numericColumnarFilterController.setData(null);
  window.fastTableNumericColumnarData = null;

  return {
    numericRows: cachedNumericRows,
    numericTransformMs: end - start,
    numericCacheGenerationMs: metrics.cacheGenerationMs,
  };
}

function getOrBuildNumericColumnarData(numericRows) {
  if (cachedNumericColumnarData !== null) {
    cachedNumericColumnarData = ensureNumericColumnarCacheColumns(
      cachedNumericColumnarData
    );
    prewarmDictionaryKeySearchForNumericData(cachedNumericColumnarData);
    const sortedStartMs = performance.now();
    cachedNumericColumnarData = ensureNumericColumnarSortedIndices(
      cachedNumericColumnarData
    );
    const sortedIndexPrecomputeMs = performance.now() - sortedStartMs;
    const rankPrecompute = ensureNumericColumnarSortedRanks(
      cachedNumericColumnarData
    );
    cachedNumericColumnarData = rankPrecompute.numericColumnarData;
    const sortedRankPrecomputeMs = Number(rankPrecompute.durationMs) || 0;
    return {
      columnarData: cachedNumericColumnarData,
      columnarDerivationMs: 0,
      columnarCacheGenerationMs: 0,
      sortedIndexPrecomputeMs,
      sortedRankPrecomputeMs,
    };
  }

  const start = performance.now();
  const columnarData = runDeriveNumericColumnarDataFromNumericRows(numericRows);
  const end = performance.now();

  cachedNumericColumnarData = ensureNumericColumnarCacheColumns(columnarData);
  prewarmDictionaryKeySearchForNumericData(cachedNumericColumnarData);
  const sortedStartMs = performance.now();
  cachedNumericColumnarData = ensureNumericColumnarSortedIndices(
    cachedNumericColumnarData
  );
  const sortedIndexPrecomputeMs = performance.now() - sortedStartMs;
  const rankPrecompute = ensureNumericColumnarSortedRanks(
    cachedNumericColumnarData
  );
  cachedNumericColumnarData = rankPrecompute.numericColumnarData;
  const sortedRankPrecomputeMs = Number(rankPrecompute.durationMs) || 0;

  return {
    columnarData: cachedNumericColumnarData,
    columnarDerivationMs: end - start,
    columnarCacheGenerationMs: 0,
    sortedIndexPrecomputeMs,
    sortedRankPrecomputeMs,
  };
}

function buildAllDerivedData() {
  const metrics = createZeroGenerationMetrics();
  const numericRowsBuild = getOrBuildNumericRows();
  metrics.numericTransformMs = numericRowsBuild.numericTransformMs;
  metrics.numericCacheGenerationMs = numericRowsBuild.numericCacheGenerationMs;

  const canUseCombinedColumnarBuild =
    cachedObjectColumnarData === null &&
    cachedNumericColumnarData === null &&
    typeof runDeriveObjectAndNumericColumnarFromNumericRows === "function";

  if (canUseCombinedColumnarBuild) {
    const combinedStartMs = performance.now();
    const combinedBuild = runDeriveObjectAndNumericColumnarFromNumericRows(
      numericRowsBuild.numericRows
    );
    const combinedEndMs = performance.now();
    const combinedDurationMs = combinedEndMs - combinedStartMs;

    cachedObjectColumnarData = combinedBuild.objectColumnarData;
    objectColumnarFilterController.setData(cachedObjectColumnarData);

    const sortedStartMs = performance.now();
    cachedNumericColumnarData = ensureNumericColumnarCacheColumns(
      combinedBuild.numericColumnarData
    );
    prewarmDictionaryKeySearchForNumericData(cachedNumericColumnarData);
    cachedNumericColumnarData = ensureNumericColumnarSortedIndices(
      cachedNumericColumnarData
    );
    metrics.sortedIndexPrecomputeMs = performance.now() - sortedStartMs;
    const rankPrecompute = ensureNumericColumnarSortedRanks(
      cachedNumericColumnarData
    );
    cachedNumericColumnarData = rankPrecompute.numericColumnarData;
    metrics.sortedRankPrecomputeMs = Number(rankPrecompute.durationMs) || 0;
    numericColumnarFilterController.setData(cachedNumericColumnarData);

    metrics.columnarDerivationMs = combinedDurationMs;
    metrics.columnarCacheGenerationMs = 0;
    return metrics;
  }

  const objectColumnarBuild = getOrBuildObjectColumnarData();
  metrics.columnarDerivationMs += objectColumnarBuild.columnarDerivationMs;
  metrics.columnarCacheGenerationMs += objectColumnarBuild.columnarCacheGenerationMs;

  const numericColumnarBuild = getOrBuildNumericColumnarData(
    numericRowsBuild.numericRows
  );
  metrics.columnarDerivationMs += numericColumnarBuild.columnarDerivationMs;
  metrics.columnarCacheGenerationMs +=
    numericColumnarBuild.columnarCacheGenerationMs;
  metrics.sortedIndexPrecomputeMs +=
    Number(numericColumnarBuild.sortedIndexPrecomputeMs) || 0;
  metrics.sortedRankPrecomputeMs +=
    Number(numericColumnarBuild.sortedRankPrecomputeMs) || 0;

  return metrics;
}

function isValidObjectColumnarData(data) {
  return (
    data &&
    typeof data.rowCount === "number" &&
    data.columns &&
    typeof data.columns === "object"
  );
}

function isValidNumericColumnarData(data) {
  return (
    data &&
    typeof data.rowCount === "number" &&
    Array.isArray(data.columns) &&
    Array.isArray(data.columnKinds)
  );
}

function prewarmDictionaryKeySearchForNumericData(numericColumnarData) {
  if (
    !isValidNumericColumnarData(numericColumnarData) ||
    typeof precomputeDictionaryKeySearchState !== "function"
  ) {
    return;
  }

  precomputeDictionaryKeySearchState(numericColumnarData);
}

function ensureNumericColumnarCacheColumns(numericColumnarData) {
  if (!isValidNumericColumnarData(numericColumnarData)) {
    return numericColumnarData;
  }

  const hasCacheColumns =
    numericColumnarData.hasCacheColumns === true &&
    Array.isArray(numericColumnarData.cacheColumns) &&
    numericColumnarData.cacheColumns.length >= columnKeys.length;
  const hasLowerDictionaries =
    Array.isArray(numericColumnarData.lowerDictionaries) &&
    numericColumnarData.lowerDictionaries.length >= columnKeys.length;
  const hasLowerDictionaryValues =
    Array.isArray(numericColumnarData.lowerDictionaryValues) &&
    numericColumnarData.lowerDictionaryValues.length >= columnKeys.length;

  if (hasCacheColumns && hasLowerDictionaries && hasLowerDictionaryValues) {
    return numericColumnarData;
  }

  const rowCount = numericColumnarData.rowCount;
  const baseCount = Math.min(columnKeys.length, numericColumnarData.columns.length);
  const cacheColumns = new Array(baseCount);
  const columnKinds = numericColumnarData.columnKinds || [];
  const columns = numericColumnarData.columns || [];
  const dictionaries = numericColumnarData.dictionaries || [];
  const lowerDictionaries = Array.isArray(numericColumnarData.lowerDictionaries)
    ? numericColumnarData.lowerDictionaries
    : [];
  const lowerDictionaryValues = Array.isArray(numericColumnarData.lowerDictionaryValues)
    ? numericColumnarData.lowerDictionaryValues
    : new Array(baseCount);

  for (let colIndex = 0; colIndex < baseCount; colIndex += 1) {
    const kind = columnKinds[colIndex];
    const values = columns[colIndex];
    const cacheCol = new Array(rowCount);

    if (kind === "stringId") {
      const ids = values;
      const rawDict = dictionaries[colIndex] || [];
      let lowerValues = Array.isArray(lowerDictionaryValues[colIndex])
        ? lowerDictionaryValues[colIndex]
        : null;

      if (lowerValues === null) {
        lowerValues = new Array(rawDict.length);
        for (let i = 0; i < rawDict.length; i += 1) {
          lowerValues[i] = String(rawDict[i]).toLowerCase();
        }
      }

      for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
        const id = ids[rowIndex];
        const lowerValue = lowerValues[id];
        cacheCol[rowIndex] =
          lowerValue !== undefined
            ? lowerValue
            : String(rawDict[id] === undefined ? "" : rawDict[id]).toLowerCase();
      }

      lowerDictionaryValues[colIndex] = lowerValues;

      const postings = Object.create(null);
      for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
        const lowerValue = cacheCol[rowIndex];
        let bucket = postings[lowerValue];
        if (bucket === undefined) {
          bucket = [];
          postings[lowerValue] = bucket;
        }
        bucket.push(rowIndex);
      }
      lowerDictionaries[colIndex] = postings;
    } else {
      for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
        cacheCol[rowIndex] = String(values[rowIndex]).toLowerCase();
      }

      lowerDictionaryValues[colIndex] = [];
      lowerDictionaries[colIndex] = Object.create(null);
    }

    cacheColumns[colIndex] = cacheCol;
  }

  numericColumnarData.lowerDictionaries = lowerDictionaries;
  numericColumnarData.lowerDictionaryValues = lowerDictionaryValues;
  numericColumnarData.cacheColumns = cacheColumns;
  numericColumnarData.hasCacheColumns = true;
  return numericColumnarData;
}

function ensureNumericColumnarSortedIndices(numericColumnarData) {
  return ensureCoreNumericColumnarSortedIndices(
    numericColumnarData,
    getFastTableSchema()
  );
}

function ensureNumericColumnarSortedRanks(numericColumnarData) {
  return ensureCoreNumericColumnarSortedRanks(
    numericColumnarData,
    getFastTableSchema()
  );
}

function applyWorkerPrebuiltDerivedData(derivedData) {
  const nextDerived = derivedData || {};
  const objectColumnarData = nextDerived.objectColumnarData;
  const numericColumnarData = nextDerived.numericColumnarData;

  if (
    !isValidObjectColumnarData(objectColumnarData) ||
    !isValidNumericColumnarData(numericColumnarData)
  ) {
    return false;
  }

  cachedObjectColumnarData = objectColumnarData;
  objectColumnarFilterController.setData(objectColumnarData);

  const preparedNumericColumnarData = ensureNumericColumnarCacheColumns(
    numericColumnarData
  );
  prewarmDictionaryKeySearchForNumericData(preparedNumericColumnarData);

  cachedNumericRows = null;
  cachedNumericColumnarData = preparedNumericColumnarData;
  numericRowFilterController.setRows([]);
  numericColumnarFilterController.setData(preparedNumericColumnarData);

  window.fastTableColumnarData = null;
  window.fastTableNumericRows = null;
  window.fastTableNumericColumnarData = preparedNumericColumnarData;
  return true;
}

function clearPreviewBody() {
  if (virtualPreviewRenderer) {
    virtualPreviewRenderer.clear();
    return;
  }

  previewBodyEl.replaceChildren();
}

function renderPreviewVirtual(totalRows, getCellValue, keepScroll) {
  if (virtualPreviewRenderer) {
    virtualPreviewRenderer.setMaxRenderRows(MAX_VIRTUAL_VISIBLE_ROWS);
    virtualPreviewRenderer.render({
      rowCount: totalRows,
      getCellValue,
      keepScroll: keepScroll === true,
    });
    return;
  }

  const limit = Math.min(MAX_VIRTUAL_VISIBLE_ROWS, totalRows);
  const fragment = document.createDocumentFragment();
  for (let i = 0; i < limit; i += 1) {
    const tr = document.createElement("tr");
    for (let j = 0; j < columnKeys.length; j += 1) {
      const td = document.createElement("td");
      const rawValue = getCellValue(i, j);
      td.textContent = rawValue === undefined || rawValue === null ? "" : String(rawValue);
      tr.appendChild(td);
    }

    fragment.appendChild(tr);
  }

  previewBodyEl.replaceChildren(fragment);
}

function renderHeaderAndFilters() {
  const titleRow = document.createElement("tr");
  const inputRow = document.createElement("tr");
  const nextInputs = [];
  const nextSortButtons = [];

  for (let i = 0; i < columnNames.length; i += 1) {
    const titleCell = document.createElement("th");
    if (sortController) {
      const sortButton = document.createElement("button");
      sortButton.type = "button";
      sortButton.className = "sortHeaderButton";
      sortButton.dataset.key = columnKeys[i];

      const labelSpan = document.createElement("span");
      labelSpan.className = "sortHeaderLabel";
      labelSpan.textContent = columnNames[i];

      const iconSpan = document.createElement("span");
      iconSpan.className = "sortHeaderIcon";
      iconSpan.textContent = getSortSymbolForState("none");

      sortButton.appendChild(labelSpan);
      sortButton.appendChild(iconSpan);
      sortButton.addEventListener("click", () => {
        runSortInteraction(() => {
          sortController.cycle(columnKeys[i]);
          runSortFromCurrentUi();
        }, { disableButtons: false });
      });

      titleCell.appendChild(sortButton);
      nextSortButtons.push({
        key: columnKeys[i],
        buttonEl: sortButton,
        iconEl: iconSpan,
      });
    } else {
      titleCell.textContent = columnNames[i];
    }

    titleRow.appendChild(titleCell);

    const inputCell = document.createElement("th");
    const input = document.createElement("input");
    input.type = "text";
    input.className = "filterInput";
    input.placeholder = "Filter";
    input.dataset.key = columnKeys[i];
    inputCell.appendChild(input);
    inputRow.appendChild(inputCell);
    nextInputs.push(input);
  }

  previewHeadEl.replaceChildren(titleRow, inputRow);
  filterInputs = nextInputs;
  sortHeaderButtons = nextSortButtons;
  updateSortHeaderIndicators();
}

function getFilteredIndicesCount(filteredIndices, fallbackCount) {
  if (filteredIndices === null || filteredIndices === undefined) {
    return fallbackCount;
  }

  if (typeof filteredIndices.count === "number") {
    return filteredIndices.count;
  }

  if (Array.isArray(filteredIndices) || ArrayBuffer.isView(filteredIndices)) {
    return filteredIndices.length;
  }

  return fallbackCount;
}

function getFilteredIndexAt(filteredIndices, rowOffset) {
  if (filteredIndices === null || filteredIndices === undefined) {
    return rowOffset;
  }

  if (
    filteredIndices &&
    ArrayBuffer.isView(filteredIndices.buffer) &&
    typeof filteredIndices.count === "number"
  ) {
    return filteredIndices.buffer[rowOffset];
  }

  return filteredIndices[rowOffset];
}

function materializeFilteredIndexArray(filteredIndices, fallbackCount) {
  const count = getFilteredIndicesCount(filteredIndices, fallbackCount);
  const out = new Uint32Array(count);

  for (let i = 0; i < count; i += 1) {
    out[i] = Number(getFilteredIndexAt(filteredIndices, i)) >>> 0;
  }

  return out;
}

function hasIndexCollection(indices) {
  return Array.isArray(indices) || ArrayBuffer.isView(indices);
}

function buildSortSnapshotFromFilterResult(filterResult) {
  const loadedRowCount = getLoadedRowCount();
  const snapshotIndices = materializeFilteredIndexArray(
    filterResult ? filterResult.filteredIndices : null,
    loadedRowCount
  );
  return {
    snapshotType: "row-indices-v2",
    rowIndices: snapshotIndices,
    count: snapshotIndices.length,
    filterCoreMs: Number(filterResult && filterResult.coreMs) || 0,
  };
}

function buildSortedIndicesForCurrentResult(filterResult, options) {
  if (!sortController || !fastTableEngine) {
    return null;
  }

  const sortBuildOptions = options || {};
  const activeDescriptors = normalizeSortDescriptorList(
    sortController.getSortDescriptors()
  );
  if (activeDescriptors.length === 0) {
    return null;
  }

  const loadedRowCount = getLoadedRowCount();
  if (loadedRowCount <= 0) {
    return null;
  }

  const rowsSnapshot = buildSortSnapshotFromFilterResult(filterResult);
  const selectedSortMode = getSortMode();
  const shouldPreferPrecomputedFastPath =
    sortBuildOptions.preferPrecomputedFastPath === true &&
    selectedSortMode !== "precomputed";

  function normalizeRuntimeSortRun(runtimeSortRun, fallbackMode) {
    if (!runtimeSortRun || !hasIndexCollection(runtimeSortRun.sortedIndices)) {
      return null;
    }

    const indices = runtimeSortRun.sortedIndices;
    const sortCoreMs = Number.isFinite(runtimeSortRun.sortCoreMs)
      ? Number(runtimeSortRun.sortCoreMs)
      : Number.isFinite(runtimeSortRun.sortMs)
        ? Number(runtimeSortRun.sortMs)
        : Number(runtimeSortRun.durationMs) || 0;
    const sortTotalMs = Number.isFinite(runtimeSortRun.sortTotalMs)
      ? Number(runtimeSortRun.sortTotalMs)
      : sortCoreMs;
    const sortPrepMs = Number.isFinite(runtimeSortRun.sortPrepMs)
      ? Number(runtimeSortRun.sortPrepMs)
      : sortTotalMs - sortCoreMs;
    const telemetryDescriptors =
      Array.isArray(runtimeSortRun.descriptors) &&
      runtimeSortRun.descriptors.length > 0
        ? normalizeSortDescriptorList(runtimeSortRun.descriptors)
        : activeDescriptors;
    const telemetrySortMode =
      typeof runtimeSortRun.sortMode === "string" && runtimeSortRun.sortMode !== ""
        ? runtimeSortRun.sortMode
        : fallbackMode;

    return {
      indices,
      sortedCount: Number.isFinite(runtimeSortRun.sortedCount)
        ? Math.max(0, Number(runtimeSortRun.sortedCount) | 0)
        : indices.length,
      result: {
        changedOrder: indices.length > 1,
        durationMs: sortCoreMs,
        sortMode: telemetrySortMode,
        dataPath:
          typeof runtimeSortRun.dataPath === "string" && runtimeSortRun.dataPath !== ""
            ? runtimeSortRun.dataPath
            : "indices",
        comparatorMode:
          typeof runtimeSortRun.comparatorMode === "string"
            ? runtimeSortRun.comparatorMode
            : "",
        descriptors: telemetryDescriptors,
        effectiveDescriptors: telemetryDescriptors,
        restoredDefault: runtimeSortRun.restoredDefault === true,
      },
      sortTotalMs,
      sortPrepMs,
      rankBuildMs: Number(runtimeSortRun.rankBuildMs) || 0,
    };
  }

  function runRuntimeSortPass(requestedMode) {
    const mode =
      typeof requestedMode === "string" && requestedMode !== ""
        ? requestedMode
        : selectedSortMode;
    const runtimeSortRun =
      fastTableEngine && typeof fastTableEngine.executeSortCore === "function"
        ? fastTableEngine.executeSortCore(rowsSnapshot, activeDescriptors, mode)
        : fastTableEngine.runSortSnapshotPass(
            rowsSnapshot,
            activeDescriptors,
            mode
          );
    return normalizeRuntimeSortRun(runtimeSortRun, mode);
  }

  if (selectedSortMode === "precomputed") {
    return runRuntimeSortPass("precomputed");
  }

  if (shouldPreferPrecomputedFastPath) {
    const precomputedRun = runRuntimeSortPass("precomputed");
    const precomputedMode =
      precomputedRun &&
      precomputedRun.result &&
      typeof precomputedRun.result.sortMode === "string"
        ? precomputedRun.result.sortMode
        : "";
    if (precomputedRun && precomputedMode.startsWith("precomputed")) {
      return precomputedRun;
    }
  }

  return runRuntimeSortPass(selectedSortMode);
}
function renderPreviewFromObjectRowIndices(rows, filteredIndices, keepScroll) {
  const totalRows = getFilteredIndicesCount(filteredIndices, rows.length);
  renderPreviewVirtual(
    totalRows,
    (rowOffset, colIndex) => {
      const rowIndex = getFilteredIndexAt(filteredIndices, rowOffset);
      const row = rows[rowIndex];
      return row[columnKeys[colIndex]];
    },
    keepScroll
  );
}

function renderPreviewFromObjectColumnar(
  columnarData,
  filteredIndices,
  keepScroll
) {
  const totalRows = getFilteredIndicesCount(filteredIndices, columnarData.rowCount);
  const columns = columnarData.columns;
  renderPreviewVirtual(
    totalRows,
    (rowOffset, colIndex) => {
      const rowIndex = getFilteredIndexAt(filteredIndices, rowOffset);
      const key = columnKeys[colIndex];
      return columns[key][rowIndex];
    },
    keepScroll
  );
}

function renderPreviewFromNumericRowIndices(
  dataOrRows,
  filteredIndices,
  keepScroll
) {
  const isRowArray = Array.isArray(dataOrRows);
  const totalRows = getFilteredIndicesCount(
    filteredIndices,
    isRowArray ? dataOrRows.length : dataOrRows.rowCount
  );
  renderPreviewVirtual(
    totalRows,
    (rowOffset, colIndex) => {
      const rowIndex = getFilteredIndexAt(filteredIndices, rowOffset);
      if (isRowArray) {
        const row = dataOrRows[rowIndex];
        return row[colIndex];
      }

      return getNumericColumnDisplayValue(dataOrRows, colIndex, rowIndex);
    },
    keepScroll
  );
}

function getNumericColumnDisplayValue(columnarData, columnIndex, rowIndex) {
  const kind =
    columnarData.columnKinds && columnarData.columnKinds[columnIndex];
  if (kind === "stringId") {
    const ids = columnarData.columns[columnIndex];
    const dict = columnarData.dictionaries[columnIndex];
    return dict[ids[rowIndex]];
  }

  return columnarData.columns[columnIndex][rowIndex];
}

function renderPreviewFromNumericColumnar(
  columnarData,
  filteredIndices,
  keepScroll
) {
  const totalRows = getFilteredIndicesCount(filteredIndices, columnarData.rowCount);
  renderPreviewVirtual(
    totalRows,
    (rowOffset, colIndex) => {
      const rowIndex = getFilteredIndexAt(filteredIndices, rowOffset);
      return getNumericColumnDisplayValue(columnarData, colIndex, rowIndex);
    },
    keepScroll
  );
}

function readRawFilters() {
  const rawFilters = {};

  for (let i = 0; i < filterInputs.length; i += 1) {
    const input = filterInputs[i];
    rawFilters[input.dataset.key] = input.value;
  }

  return rawFilters;
}

function clearFiltersUI() {
  for (let i = 0; i < filterInputs.length; i += 1) {
    filterInputs[i].value = "";
  }
}

function getFilterOptions() {
  const dictionaryKeySearchEnabled =
    useDictionaryKeySearchEl && useDictionaryKeySearchEl.checked === true;
  const dictionaryIntersectionEnabled =
    useDictionaryIntersectionEl && useDictionaryIntersectionEl.checked === true;
  const smarterPlannerEnabled =
    useSmarterPlannerEl && useSmarterPlannerEl.checked === true;
  const smartFilteringEnabled =
    useSmartFilteringEl && useSmartFilteringEl.checked === true;
  const filterCacheEnabled =
    useFilterCacheEl && useFilterCacheEl.checked === true;

  return {
    enableCaching: enableCachingEl.checked,
    useDictionaryKeySearch: dictionaryKeySearchEnabled,
    useDictionaryIntersection:
      dictionaryKeySearchEnabled && dictionaryIntersectionEnabled,
    useSmarterPlanner: smarterPlannerEnabled,
    useSmartFiltering: smartFilteringEnabled,
    useFilterCache: filterCacheEnabled,
  };
}

function getNumericColumnarDataForDictionaryKeySearch() {
  if (
    cachedNumericColumnarData !== null &&
    isValidNumericColumnarData(cachedNumericColumnarData)
  ) {
    return ensureNumericColumnarCacheColumns(cachedNumericColumnarData);
  }

  if (getLoadedRowCount() === 0) {
    return null;
  }

  const numericRowsBuild = getOrBuildNumericRows();
  const numericColumnarBuild = getOrBuildNumericColumnarData(
    numericRowsBuild.numericRows
  );
  return ensureNumericColumnarCacheColumns(numericColumnarBuild.columnarData);
}

function getCurrentFilterResultSnapshot() {
  if (currentLayout === "columnar" && currentColumnarMode === "binary") {
    return {
      filteredCount: numericColumnarFilterController.getCurrentCount(),
      filteredIndices: numericColumnarFilterController.getCurrentIndices(),
      columnarData: numericColumnarFilterController.getData(),
    };
  }

  if (currentLayout === "columnar") {
    return {
      filteredCount: objectColumnarFilterController.getCurrentCount(),
      filteredIndices: objectColumnarFilterController.getCurrentIndices(),
      columnarData: objectColumnarFilterController.getData(),
    };
  }

  if (currentRepresentation === "numeric") {
    return {
      filteredCount: numericRowFilterController.getCurrentCount(),
      filteredIndices: numericRowFilterController.getCurrentIndices(),
      numericData: numericRowFilterController.getData(),
    };
  }

  const rows = ensureObjectRowsAvailable();
  return {
    filteredCount: objectRowFilterController.getCurrentCount(),
    filteredIndices: objectRowFilterController.getCurrentIndices(),
    rows,
  };
}

function renderFilterResultByCurrentMode(filterResult, renderIndices, keepScroll) {
  if (currentLayout === "columnar" && currentColumnarMode === "binary") {
    renderPreviewFromNumericColumnar(
      filterResult.columnarData,
      renderIndices,
      keepScroll
    );
    return;
  }

  if (currentLayout === "columnar") {
    renderPreviewFromObjectColumnar(
      filterResult.columnarData,
      renderIndices,
      keepScroll
    );
    return;
  }

  if (currentRepresentation === "numeric") {
    renderPreviewFromNumericRowIndices(
      filterResult.numericData,
      renderIndices,
      keepScroll
    );
    return;
  }

  renderPreviewFromObjectRowIndices(filterResult.rows, renderIndices, keepScroll);
}

function runFilterCore(rawFilters, options) {
  const executionOptions = options || {};
  const skipRender = executionOptions.skipRender === true;
  const skipStatus = executionOptions.skipStatus === true;
  const keepScroll = executionOptions.keepScroll === true;
  const preferPrecomputedFastPath =
    executionOptions.preferPrecomputedFastPath !== false &&
    FILTER_PASS_PREFER_PRECOMPUTED_SORT;
  const filterOptions = executionOptions.filterOptions || getFilterOptions();
  const sourceRawFilters =
    rawFilters && typeof rawFilters === "object" ? rawFilters : {};

  if (getLoadedRowCount() === 0) {
    return {
      kind: "no-data",
      skipRender,
      skipStatus,
    };
  }

  const filterResult =
    fastTableEngine && typeof fastTableEngine.executeFilterCore === "function"
      ? fastTableEngine.executeFilterCore(sourceRawFilters, {
          filterOptions,
        })
      : filterRuntimeBridge.runFilterPassWithRawFilters(sourceRawFilters, {
          filterOptions,
        });
  if (!filterResult) {
    return null;
  }

  let sortRun = null;
  let renderIndices = filterResult.filteredIndices;
  if (!skipRender) {
    sortRun = buildSortedIndicesForCurrentResult(filterResult, {
      preferPrecomputedFastPath,
      rawFilters: sourceRawFilters,
    });
    if (sortRun && hasIndexCollection(sortRun.indices)) {
      renderIndices = sortRun.indices;
    }
  }

  const dictionaryKeySearchPlan = filterResult.dictionaryPrefilter || null;
  const reverseIndexMs =
    dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
      ? dictionaryKeySearchPlan.durationMs
      : 0;
  const reverseIndexSearchMs =
    dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
      ? dictionaryKeySearchPlan.searchMs || 0
      : 0;
  const reverseIndexSearchFullMs =
    dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
      ? dictionaryKeySearchPlan.searchFullMs || 0
      : 0;
  const reverseIndexSearchRefinedMs =
    dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
      ? dictionaryKeySearchPlan.searchRefinedMs || 0
      : 0;
  const reverseIndexMergeMs =
    dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
      ? dictionaryKeySearchPlan.mergeMs || 0
      : 0;
  const reverseIndexMergeConcatMs =
    dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
      ? dictionaryKeySearchPlan.mergeConcatMs || 0
      : 0;
  const reverseIndexMergeSortMs =
    dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
      ? dictionaryKeySearchPlan.mergeSortMs || 0
      : 0;
  const reverseIndexIntersectMs =
    dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
      ? dictionaryKeySearchPlan.intersectionMs || 0
      : 0;

  const orchestration = {
    filterResult,
    filterModePath:
      typeof filterResult.modePath === "string" ? filterResult.modePath : "",
    filteredCount: filterResult.filteredCount,
    filteredIndices: filterResult.filteredIndices,
    renderIndices,
    coreMs: Number(filterResult.coreMs) || 0,
    reverseIndexMs,
    reverseIndexSearchMs,
    reverseIndexSearchFullMs,
    reverseIndexSearchRefinedMs,
    reverseIndexMergeMs,
    reverseIndexMergeConcatMs,
    reverseIndexMergeSortMs,
    reverseIndexIntersectMs,
    active: filterResult.active === true,
    sort: sortRun,
    topLevelFilterCacheEvent: filterResult.topLevelCacheEvent || null,
    reverseIndex:
      dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
        ? dictionaryKeySearchPlan
        : null,
    selectedBaseCandidateCount: Number.isFinite(
      filterResult.selectedBaseCandidateCount
    )
      ? Number(filterResult.selectedBaseCandidateCount)
      : -1,
    filterOptions,
    dictionaryKeySearchPlan,
  };
  window.fastTableLastFilterMode =
    orchestration.filterModePath || getCurrentFilterModeKey();

  return {
    kind: "ok",
    skipRender,
    skipStatus,
    keepScroll,
    filterOptions,
    orchestration,
  };
}

function renderFilterUi(coreRun) {
  if (!coreRun) {
    return null;
  }

  if (coreRun.kind === "no-data") {
    if (!coreRun.skipRender) {
      clearPreviewBody();
    }

    if (!coreRun.skipStatus) {
      filterStatusEl.textContent = "No data loaded yet.";
    }

    syncClearFilterCacheButtonState();
    return null;
  }

  const skipRender = coreRun.skipRender === true;
  const skipStatus = coreRun.skipStatus === true;
  const keepScroll = coreRun.keepScroll === true;
  const filterOptions = coreRun.filterOptions || getFilterOptions();
  const orchestration = coreRun.orchestration;
  const filterResult = orchestration.filterResult;
  const active = orchestration.active;
  const selectedBaseCandidateCount = orchestration.selectedBaseCandidateCount;
  const topLevelCacheEvent = orchestration.topLevelFilterCacheEvent;
  const dictionaryKeySearchPlan = orchestration.dictionaryKeySearchPlan;
  const filterCoreDurationMs = orchestration.coreMs;
  const sortRun = orchestration.sort;
  const renderIndices = orchestration.renderIndices;

  let renderDurationMs = 0;
  let renderStart = 0;
  if (!skipRender) {
    renderStart = performance.now();
  }
  if (!skipRender) {
    renderFilterResultByCurrentMode(filterResult, renderIndices, keepScroll);
  }
  let renderEnd = 0;
  if (!skipRender) {
    renderEnd = performance.now();
  }

  if (!skipRender) {
    renderDurationMs = renderEnd - renderStart;
  }
  const totalDurationMs = skipRender
    ? filterCoreDurationMs
    : filterCoreDurationMs + renderDurationMs;
  const output = {
    filteredCount: orchestration.filteredCount,
    filteredIndices: orchestration.filteredIndices,
    renderIndices,
    coreMs: filterCoreDurationMs,
    reverseIndexMs: orchestration.reverseIndexMs,
    reverseIndexSearchMs: orchestration.reverseIndexSearchMs,
    reverseIndexSearchFullMs: orchestration.reverseIndexSearchFullMs,
    reverseIndexSearchRefinedMs: orchestration.reverseIndexSearchRefinedMs,
    reverseIndexMergeMs: orchestration.reverseIndexMergeMs,
    reverseIndexMergeConcatMs: orchestration.reverseIndexMergeConcatMs,
    reverseIndexMergeSortMs: orchestration.reverseIndexMergeSortMs,
    reverseIndexIntersectMs: orchestration.reverseIndexIntersectMs,
    renderMs: renderDurationMs,
    totalMs: totalDurationMs,
    active,
    sort: sortRun,
    topLevelFilterCacheEvent: topLevelCacheEvent,
    reverseIndex: orchestration.reverseIndex,
  };

  if (!skipRender) {
    window.fastTableFilteredRows = null;
    window.fastTableFilteredRowIndices = renderIndices;
  }

  if (!skipStatus && sortRun && sortRun.result) {
    setSortTelemetryFromResult(sortRun.result, sortRun.sortedCount, {
      sortTotalMs: sortRun.sortTotalMs,
      sortPrepMs: sortRun.sortPrepMs,
      rankBuildMs: sortRun.rankBuildMs,
      filterCoreMs: filterCoreDurationMs,
      renderMs: renderDurationMs,
      wallMs: totalDurationMs,
    });
  }

  if (!skipStatus) {
    let cacheLineHtml = "";
    if (topLevelCacheEvent && topLevelCacheEvent.enabled) {
      const smallSizeKb = formatKbFromBytes(topLevelCacheEvent.smallSizeBytes);
      const mediumSizeKb = formatKbFromBytes(topLevelCacheEvent.mediumSizeBytes);
      const smallKeysText = `${Number(topLevelCacheEvent.smallEntryCount) || 0}/${Number(topLevelCacheEvent.smallCapacity) || 0}`;
      const mediumKeysText = `${Number(topLevelCacheEvent.mediumEntryCount) || 0}/${Number(topLevelCacheEvent.mediumCapacity) || 0}`;
      const tiersText = `Small ${smallSizeKb} KB (keys ${smallKeysText}), Medium ${mediumSizeKb} KB (keys ${mediumKeysText})`;
      const tierText =
        topLevelCacheEvent.tier === "small"
          ? "small tier"
          : topLevelCacheEvent.tier === "medium"
            ? "medium tier"
            : "tier n/a";
      if (topLevelCacheEvent.hit) {
        cacheLineHtml =
          `<br><span class="filterCacheHit">Top-level filter cache hit: reused ${formatRowCount(
            topLevelCacheEvent.resultCount
          )} indices in ${formatMsFixed3(
            topLevelCacheEvent.lookupMs
          )} ms (${tierText}). ${tiersText}.</span>`;
      } else if (topLevelCacheEvent.inserted) {
        cacheLineHtml =
          `<br><span class="filterCacheMiss">Top-level filter cache miss: inserted ${formatRowCount(
            topLevelCacheEvent.resultCount
          )} indices in ${formatMsFixed3(
            topLevelCacheEvent.insertMs
          )} ms (lookup ${formatMsFixed3(
            topLevelCacheEvent.lookupMs
          )} ms, ${tierText}). ${tiersText}.</span>`;
      } else {
        const skippedReason =
          topLevelCacheEvent.skippedReason === "core"
            ? "insert skipped (core <= 4 ms)"
            : topLevelCacheEvent.skippedReason === "size"
              ? "insert skipped (> 500.000 indices)"
              : "insert skipped";
        cacheLineHtml =
          `<br><span class="filterCacheMiss">Top-level filter cache miss: ${skippedReason} (lookup ${formatMsFixed3(
            topLevelCacheEvent.lookupMs
          )} ms). ${tiersText}.</span>`;
      }
    }
    const reverseIndexLineHtml =
      filterOptions && filterOptions.useDictionaryKeySearch === true && active
        ? dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
          ? `<br><span class="filterReverseIndexTiming">Dict key search: ${formatMsFixed3(
              dictionaryKeySearchPlan.durationMs
            )} ms (search ${formatMsFixed3(
              dictionaryKeySearchPlan.searchMs || 0
            )} ms (full ${formatMsFixed3(
              dictionaryKeySearchPlan.searchFullMs || 0
            )} ms, refined ${formatMsFixed3(
              dictionaryKeySearchPlan.searchRefinedMs || 0
            )} ms), merge ${formatMsFixed3(
              dictionaryKeySearchPlan.mergeMs || 0
            )} ms (${
              dictionaryKeySearchPlan.mergeFirstStageLabel || "concat"
            } ${formatMsFixed3(
              dictionaryKeySearchPlan.mergeConcatMs || 0
            )} ms, sort ${formatMsFixed3(
              dictionaryKeySearchPlan.mergeSortMs || 0
            )} ms), intersect ${formatMsFixed3(
              dictionaryKeySearchPlan.intersectionMs || 0
            )} ms, ${dictionaryKeySearchPlan.handledColumnCount} columns (${formatRowCount(
              dictionaryKeySearchPlan.searchFullColumnCount || 0
            )} full scans, ${formatRowCount(
              dictionaryKeySearchPlan.searchRefinedColumnCount || 0
            )} refined scans), ${formatRowCount(
              dictionaryKeySearchPlan.searchFullKeyCount || 0
            )} full keys, ${formatRowCount(
              dictionaryKeySearchPlan.searchRefinedKeyCount || 0
            )} refined keys, ${formatRowCount(
              dictionaryKeySearchPlan.candidateKeyCount || 0
            )} candidate keys, ${formatRowCount(
              dictionaryKeySearchPlan.candidateCount
            )} candidate rows).</span>`
          : topLevelCacheEvent && topLevelCacheEvent.hit
            ? `<br><span class="filterReverseIndexTiming">Dict key search: cached by top-level filter cache.</span>`
            : `<br><span class="filterReverseIndexTiming">Dict key search: 0.000 ms (search 0.000 ms, merge 0.000 ms (concat 0.000 ms, sort 0.000 ms), intersect 0.000 ms, no dictionary-backed active filters).</span>`
        : "";

    if (active) {
      const searchedRowCount =
        selectedBaseCandidateCount >= 0
          ? selectedBaseCandidateCount
          : getLoadedRowCount();
      filterStatusEl.innerHTML = `Searched ${formatRowCount(searchedRowCount)} rows. Filtered to ${formatRowCount(filterResult.filteredCount)} rows. core ${formatMs(filterCoreDurationMs)} ms, render ${formatMs(renderDurationMs)} ms, total ${formatMs(totalDurationMs)} ms.${cacheLineHtml}${reverseIndexLineHtml}`;
    } else {
      filterStatusEl.innerHTML = `No active filters. core ${formatMs(filterCoreDurationMs)} ms, render ${formatMs(renderDurationMs)} ms, total ${formatMs(totalDurationMs)} ms.${cacheLineHtml}${reverseIndexLineHtml}`;
    }
  }

  syncClearFilterCacheButtonState();
  return output;
}

function runFiltersWithRawFilters(rawFilters, options) {
  const coreRun = runFilterCore(rawFilters, options);
  return renderFilterUi(coreRun);
}

function runFilterFromCurrentUi(options) {
  const executionOptions = options || {};
  const rawFilters = executionOptions.rawFilters || readRawFilters();
  const coreRun = runFilterCore(rawFilters, executionOptions);
  return renderFilterUi(coreRun);
}

function applyFiltersAndRender(options) {
  return runFilterFromCurrentUi(options);
}

function attachFilterListeners() {
  for (let i = 0; i < filterInputs.length; i += 1) {
    filterInputs[i].addEventListener("input", () => {
      runFilterFromCurrentUi();
    });
  }
}

function getRequestedRepresentation() {
  return useNumericDataEl && useNumericDataEl.checked ? "numeric" : "object";
}

function getRequestedLayout() {
  return useColumnarDataEl.checked ? "columnar" : "row";
}

function getRequestedColumnarMode() {
  return useBinaryColumnarEl && useBinaryColumnarEl.checked ? "binary" : "object";
}

function activateRequestedMode(shouldClearFilters, options) {
  const modeOptions = options || {};
  const skipInitialRender = modeOptions.skipInitialRender === true;
  const metrics = createZeroGenerationMetrics();
  currentRepresentation = getRequestedRepresentation();
  currentLayout = getRequestedLayout();
  currentColumnarMode = getRequestedColumnarMode();

  if (shouldClearFilters) {
    clearFiltersUI();
  }

  if (objectRows.length > 0) {
    objectRowFilterController.setRows(objectRows);
  }

  if (currentLayout === "columnar" && currentColumnarMode === "object") {
    const objectColumnarData =
      cachedObjectColumnarData !== null
        ? cachedObjectColumnarData
        : getOrBuildObjectColumnarData().columnarData;
    objectColumnarFilterController.setData(objectColumnarData);
    if (!skipInitialRender) {
      renderPreviewFromObjectColumnar(objectColumnarData, null);
    }
    window.fastTableColumnarData = objectColumnarData;
    window.fastTableNumericRows = null;
    window.fastTableNumericColumnarData = null;
    return metrics;
  }

  if (currentLayout === "columnar" && currentColumnarMode === "binary") {
    let numericColumnarData = null;
    if (cachedNumericColumnarData !== null) {
      numericColumnarData = cachedNumericColumnarData;
    } else {
      const numericRowsBuild = getOrBuildNumericRows();
      numericColumnarData = getOrBuildNumericColumnarData(
        numericRowsBuild.numericRows
      ).columnarData;
    }

    numericColumnarFilterController.setData(numericColumnarData);
    if (!skipInitialRender) {
      renderPreviewFromNumericColumnar(numericColumnarData, null);
    }
    window.fastTableColumnarData = numericColumnarData;
    window.fastTableNumericColumnarData = numericColumnarData;
    window.fastTableNumericRows = cachedNumericRows;
    return metrics;
  }

  if (currentRepresentation === "numeric") {
    const numericRowsBuild = getOrBuildNumericRows();
    const numericRows = numericRowsBuild.numericRows;

    numericRowFilterController.setRows(numericRows);
    if (!skipInitialRender) {
      renderPreviewFromNumericRowIndices(numericRows, null);
    }

    window.fastTableColumnarData = null;
    window.fastTableNumericRows = numericRows;
    window.fastTableNumericColumnarData = null;
    return metrics;
  }

  const rows = ensureObjectRowsAvailable();
  if (!skipInitialRender) {
    renderPreviewFromObjectRowIndices(rows, null);
  }
  window.fastTableColumnarData = null;
  window.fastTableNumericRows = null;
  window.fastTableNumericColumnarData = null;
  return metrics;
}

function applyLoadedColumnarBinaryDataset(
  loadedRows,
  loadDurationMs,
  loadTimingDetails
) {
  const totalStartMs = performance.now();
  const finalizeStartMs = performance.now();
  setObjectRowsDataset(loadedRows);
  resetSortState();
  const derivedMetrics = buildAllDerivedData();
  derivedMetrics.usedWorkerMode = false;
  derivedMetrics.workerPhaseWallMs = 0;
  activateRequestedMode(true);
  derivedMetrics.finalizeMs = performance.now() - finalizeStartMs;
  derivedMetrics.totalMs = performance.now() - totalStartMs;
  setGenerationStatus(loadedRows.length, derivedMetrics);

  filterStatusEl.textContent = formatPregeneratedLoadStatus(
    loadedRows.length,
    loadDurationMs,
    loadTimingDetails
  );
}

function applyLoadedNumericColumnarDataset(
  loadedNumericColumnarData,
  loadDurationMs,
  loadTimingDetails
) {
  if (!isValidNumericColumnarData(loadedNumericColumnarData)) {
    generationStatusEl.textContent = "Failed to load pregenerated data: invalid numeric columnar payload.";
    return;
  }

  const sortedStartMs = performance.now();
  let numericColumnarData = ensureNumericColumnarCacheColumns(
    loadedNumericColumnarData
  );
  prewarmDictionaryKeySearchForNumericData(numericColumnarData);
  numericColumnarData = ensureNumericColumnarSortedIndices(numericColumnarData);
  const sortedIndexPrecomputeMs = performance.now() - sortedStartMs;
  const rankPrecompute = ensureNumericColumnarSortedRanks(numericColumnarData);
  numericColumnarData = rankPrecompute.numericColumnarData;
  const sortedRankPrecomputeMs = Number(rankPrecompute.durationMs) || 0;

  const totalStartMs = performance.now();
  const finalizeStartMs = performance.now();
  setObjectRowsDataset([]);
  resetSortState();
  cachedNumericRows = null;
  cachedNumericColumnarData = numericColumnarData;
  numericRowFilterController.setRows([]);
  numericColumnarFilterController.setData(numericColumnarData);

  const rowCount = numericColumnarData.rowCount;
  const metrics = createZeroGenerationMetrics();
  metrics.usedWorkerMode = false;
  metrics.workerPhaseWallMs = 0;
  metrics.sortedIndexPrecomputeMs = sortedIndexPrecomputeMs;
  metrics.sortedRankPrecomputeMs = sortedRankPrecomputeMs;
  activateRequestedMode(true);
  metrics.finalizeMs = performance.now() - finalizeStartMs;
  metrics.totalMs = performance.now() - totalStartMs;
  setGenerationStatus(rowCount, metrics);

  filterStatusEl.textContent = formatPregeneratedLoadStatus(
    rowCount,
    loadDurationMs,
    loadTimingDetails
  );
}

function trySwitchModeUsingExistingRows(options) {
  if (getLoadedRowCount() === 0) {
    return;
  }

  const switchOptions = options || {};
  activateRequestedMode(false, { skipInitialRender: true });
  if (switchOptions.suppressFilterPass === true) {
    return;
  }

  runFilterFromCurrentUi();
}

function setRawFiltersUI(rawFilters) {
  const nextFilters = rawFilters || {};

  for (let i = 0; i < filterInputs.length; i += 1) {
    const input = filterInputs[i];
    const value = nextFilters[input.dataset.key];
    input.value = value === undefined ? "" : String(value);
  }
}

function setSingleFilterUI(columnKey, value) {
  for (let i = 0; i < filterInputs.length; i += 1) {
    const input = filterInputs[i];
    input.value = input.dataset.key === columnKey ? String(value) : "";
  }
}

function getModeOptions() {
  const dictionaryKeySearchEnabled =
    useDictionaryKeySearchEl && useDictionaryKeySearchEl.checked === true;
  const dictionaryIntersectionEnabled =
    useDictionaryIntersectionEl && useDictionaryIntersectionEl.checked === true;
  const smarterPlannerEnabled =
    useSmarterPlannerEl && useSmarterPlannerEl.checked === true;
  const smartFilteringEnabled =
    useSmartFilteringEl && useSmartFilteringEl.checked === true;
  const filterCacheEnabled =
    useFilterCacheEl && useFilterCacheEl.checked === true;

  return {
    useColumnarData: useColumnarDataEl.checked,
    useBinaryColumnar: useBinaryColumnarEl && useBinaryColumnarEl.checked,
    useNumericData: useNumericDataEl && useNumericDataEl.checked,
    enableCaching: enableCachingEl.checked,
    useDictionaryKeySearch: dictionaryKeySearchEnabled,
    useDictionaryIntersection: dictionaryIntersectionEnabled,
    useSmarterPlanner: smarterPlannerEnabled,
    useSmartFiltering: smartFilteringEnabled,
    useFilterCache: filterCacheEnabled,
  };
}

function setModeOptions(nextOptions, switchOptions) {
  const modeOptions = nextOptions || {};

  if (typeof modeOptions.useColumnarData === "boolean") {
    useColumnarDataEl.checked = modeOptions.useColumnarData;
  }

  if (typeof modeOptions.useBinaryColumnar === "boolean") {
    useBinaryColumnarEl.checked = modeOptions.useBinaryColumnar;
  }

  if (typeof modeOptions.useNumericData === "boolean" && useNumericDataEl) {
    useNumericDataEl.checked = modeOptions.useNumericData;
  }

  if (typeof modeOptions.enableCaching === "boolean") {
    enableCachingEl.checked = modeOptions.enableCaching;
  }

  if (
    typeof modeOptions.useDictionaryKeySearch === "boolean" &&
    useDictionaryKeySearchEl
  ) {
    useDictionaryKeySearchEl.checked = modeOptions.useDictionaryKeySearch;
  }

  if (
    typeof modeOptions.useDictionaryIntersection === "boolean" &&
    useDictionaryIntersectionEl
  ) {
    useDictionaryIntersectionEl.checked = modeOptions.useDictionaryIntersection;
  }

  if (
    typeof modeOptions.useSmarterPlanner === "boolean" &&
    useSmarterPlannerEl
  ) {
    useSmarterPlannerEl.checked = modeOptions.useSmarterPlanner;
  }

  if (typeof modeOptions.useSmartFiltering === "boolean") {
    useSmartFilteringEl.checked = modeOptions.useSmartFiltering;
  }

  if (typeof modeOptions.useFilterCache === "boolean" && useFilterCacheEl) {
    useFilterCacheEl.checked = modeOptions.useFilterCache;
  }

  trySwitchModeUsingExistingRows(switchOptions);
}

function getFastTableSchema() {
  return {
    columnKeys: columnKeys.slice(),
    columnNames: columnNames.slice(),
    baseColumnCount,
    numericCacheOffset,
    objectCacheKeys: objectCacheKeys.slice(),
  };
}

function getNumericColumnarForSave() {
  if (getLoadedRowCount() === 0) {
    return null;
  }

  if (cachedNumericColumnarData !== null) {
    cachedNumericColumnarData = ensureNumericColumnarSortedIndices(
      cachedNumericColumnarData
    );
    return cachedNumericColumnarData;
  }

  const numericRowsBuild = getOrBuildNumericRows();
  return getOrBuildNumericColumnarData(numericRowsBuild.numericRows).columnarData;
}

async function runGenerationAction(options) {
  const input = options || {};
  const rowCount = Number.parseInt(String(input.rowCount || "0"), 10);
  const useWorkerForRun = input.useWorkerGeneration === true;

  generationStatusEl.textContent = "Generating...";
  clearCurrentDatasetBeforeGeneration();
  if (useWorkerForRun) {
    resetWorkerProgressUI();
  }

  const totalStartMs = performance.now();
  const generationMetrics = createZeroGenerationMetrics();
  const usedWorkerPath = useWorkerForRun;
  generationMetrics.usedWorkerMode = usedWorkerPath;

  try {
    let generationResult = null;
    if (useWorkerForRun) {
      generationResult = await generateRowsWithWorkers(rowCount);
      setWorkerProgressText(
        `Workers finished chunk generation. Avg chunk worker total: ${formatMsFixed3(
          generationResult.avgChunkMs
        )} ms. Finalizing...`
      );
    } else {
      generationResult = generateRowsOnMainThread(rowCount);
    }

    generationMetrics.workerPhaseWallMs =
      generationResult.metrics.workerPhaseWallMs;
    generationMetrics.workerWallByIndex =
      generationResult.metrics.workerWallByIndex || [];
    if (usedWorkerPath) {
      generationMetrics.workerRowGenerationMs =
        generationResult.metrics.rowGenerationMs;
      generationMetrics.workerRowCacheGenerationMs =
        generationResult.metrics.rowCacheGenerationMs;
      generationMetrics.workerNumericTransformMs =
        generationResult.metrics.numericTransformMs;
      generationMetrics.workerNumericCacheGenerationMs =
        generationResult.metrics.numericCacheGenerationMs;
      generationMetrics.workerColumnarDerivationMs =
        generationResult.metrics.columnarDerivationMs;
      generationMetrics.workerColumnarCacheGenerationMs =
        generationResult.metrics.columnarCacheGenerationMs;
    } else {
      generationMetrics.rowGenerationMs = generationResult.metrics.rowGenerationMs;
      generationMetrics.rowCacheGenerationMs =
        generationResult.metrics.rowCacheGenerationMs;
    }

    const finalizeStartMs = performance.now();
    if (usedWorkerPath) {
      const usedPrebuiltWorkerDerived = applyWorkerPrebuiltDerivedData(
        generationResult.derivedData
      );
      if (!usedPrebuiltWorkerDerived) {
        throw new Error("Worker returned invalid derived data.");
      }

      ensureObjectRowsAvailable();
      setWorkerProgressText("Workers are precomputing sorted indices...");
      const sortedPrecomputeResult =
        await precomputeSortedIndicesWithWorkersForCurrentData();
      generationMetrics.sortedIndexPrecomputeMs =
        Number(
          sortedPrecomputeResult &&
            typeof sortedPrecomputeResult.sortedIndexPrecomputeMs === "number"
            ? sortedPrecomputeResult.sortedIndexPrecomputeMs
            : 0
        ) || 0;
      generationMetrics.sortedRankPrecomputeMs =
        Number(
          sortedPrecomputeResult &&
            typeof sortedPrecomputeResult.sortedRankPrecomputeMs === "number"
            ? sortedPrecomputeResult.sortedRankPrecomputeMs
            : 0
        ) || 0;

      generationMetrics.numericTransformMs = 0;
      generationMetrics.numericCacheGenerationMs = 0;
      generationMetrics.columnarDerivationMs = 0;
      generationMetrics.columnarCacheGenerationMs = 0;
    } else {
      setObjectRowsDataset(generationResult.rows);
      const derivedMetrics = buildAllDerivedData();
      generationMetrics.numericTransformMs = derivedMetrics.numericTransformMs;
      generationMetrics.numericCacheGenerationMs =
        derivedMetrics.numericCacheGenerationMs;
      generationMetrics.columnarDerivationMs = derivedMetrics.columnarDerivationMs;
      generationMetrics.columnarCacheGenerationMs =
        derivedMetrics.columnarCacheGenerationMs;
      generationMetrics.sortedIndexPrecomputeMs =
        Number(derivedMetrics.sortedIndexPrecomputeMs) || 0;
      generationMetrics.sortedRankPrecomputeMs =
        Number(derivedMetrics.sortedRankPrecomputeMs) || 0;
    }

    activateRequestedMode(true);
    generationMetrics.finalizeMs = performance.now() - finalizeStartMs;
    generationMetrics.totalMs = performance.now() - totalStartMs;

    setGenerationStatus(rowCount, generationMetrics);

    filterStatusEl.textContent = "No active filters yet.";
    if (usedWorkerPath) {
      updateWorkerProgressUI({
        completedRows: rowCount,
        totalRows: rowCount,
        completedChunks: generationResult.completedChunks || 0,
        totalChunks: generationResult.totalChunks || 0,
        percent: 100,
        lastChunkMs: generationResult.avgChunkMs || 0,
        avgChunkMs: generationResult.avgChunkMs || 0,
      });
    }

    return {
      rowCount,
      usedWorkerPath,
      metrics: generationMetrics,
      generationResult,
    };
  } catch (error) {
    const message = String(error && error.message ? error.message : error);
    generationStatusEl.textContent = `Generation failed: ${message}`;
    if (usedWorkerPath) {
      setWorkerProgressText(`Worker generation is not possible: ${message}`);
    }
    throw error;
  }
}

fastTableEngine = createFastTableEngine({
  adapters: {
    hasData() {
      return getLoadedRowCount() > 0;
    },
    getRowCount() {
      return getLoadedRowCount();
    },
    getModeOptions,
    setModeOptions,
    getRawFilters: readRawFilters,
    setRawFilters: setRawFiltersUI,
    setSingleFilter: setSingleFilterUI,
    clearFilters: clearFiltersUI,
    runFilterPass: (options) => applyFiltersAndRender(options),
    runSingleFilterPass: (columnKey, value, options) =>
      filterRuntimeBridge.runSingleFilterPass(columnKey, value, options),
    runFilterPassWithRawFilters(rawFilters, options) {
      return filterRuntimeBridge.runFilterPassWithRawFilters(
        rawFilters || readRawFilters(),
        options
      );
    },
    getSortModes: getAvailableSortModes,
    getSortMode,
    getSortOptions: getSortRuntimeOptions,
    setSortOptions: setSortRuntimeOptions,
    buildSortRowsSnapshot: buildRowsSnapshotFromRawFilters,
    runSortSnapshotPass,
    prewarmPrecomputedSortState: () =>
      appSortRuntimeBridge.prewarmPrecomputedSortState(),
    isTimSortAvailable,
    getNumericColumnarForSave,
    applyLoadedColumnarBinaryDataset,
    applyLoadedNumericColumnarDataset,
    setGenerationError(message) {
      generationStatusEl.textContent = message;
    },
    generate(generateOptions) {
      return runGenerationAction(generateOptions);
    },
  },
});

window.fastTableEngine = fastTableEngine;
window.fastTableIOBridge = fastTableEngine.createIOBridge({
  getSchema: getFastTableSchema,
});
window.fastTableBenchmarkApi = fastTableEngine.createBenchmarkApi();

renderHeaderAndFilters();
attachFilterListeners();
syncWorkerGenerationControls();
syncSortModeAvailability();
initializeTelemetryPanelControls();
syncClearFilterCacheButtonState();
void loadAndRenderAppVersion();

if (sortModeEl) {
  sortModeEl.addEventListener("change", () => {
    syncSortModeAvailability();
    runSortInteraction(() => {
      runSortFromCurrentUi();
    }, { disableButtons: false });
  });
}

if (useTypedSortComparatorEl) {
  useTypedSortComparatorEl.addEventListener("change", () => {
    runSortInteraction(() => {
      runSortFromCurrentUi();
    }, { disableButtons: false });
  });
}

if (useIndexSortEl) {
  useIndexSortEl.addEventListener("change", () => {
    runSortInteraction(() => {
      runSortFromCurrentUi();
    }, { disableButtons: false });
  });
}

if (resetSortBtnEl) {
  resetSortBtnEl.addEventListener("click", () => {
    if (!sortController) {
      return;
    }

    runSortInteraction(() => {
      resetSortState(false);
      runSortFromCurrentUi();
    }, { disableButtons: false });
  });
}

enableCachingEl.addEventListener("change", () => {
  runFilterFromCurrentUi();
});

if (useDictionaryKeySearchEl) {
  useDictionaryKeySearchEl.addEventListener("change", () => {
    runFilterFromCurrentUi();
  });
}

if (useDictionaryIntersectionEl) {
  useDictionaryIntersectionEl.addEventListener("change", () => {
    runFilterFromCurrentUi();
  });
}

if (useSmarterPlannerEl) {
  useSmarterPlannerEl.addEventListener("change", () => {
    runFilterFromCurrentUi();
  });
}

useSmartFilteringEl.addEventListener("change", () => {
  runFilterFromCurrentUi();
});

if (useFilterCacheEl) {
  useFilterCacheEl.addEventListener("change", () => {
    runFilterFromCurrentUi();
  });
}

if (clearFilterCacheBtnEl) {
  clearFilterCacheBtnEl.addEventListener("click", () => {
    clearAllFilterCaches();
    filterStatusEl.innerHTML = "Filter cache cleared.";
  });
}

useColumnarDataEl.addEventListener("change", () => {
  trySwitchModeUsingExistingRows();
});

if (useBinaryColumnarEl) {
  useBinaryColumnarEl.addEventListener("change", () => {
    trySwitchModeUsingExistingRows();
  });
}

if (useNumericDataEl) {
  useNumericDataEl.addEventListener("change", () => {
    trySwitchModeUsingExistingRows();
  });
}

if (useWorkerGenerationEl) {
  useWorkerGenerationEl.addEventListener("change", () => {
    syncWorkerGenerationControls();
  });
}

generateBtnEl.addEventListener("click", () => {
  const rowCount = Number.parseInt(rowCountEl.value, 10);
  const useWorkerForRun = shouldUseWorkerGeneration();

  setActionButtonsDisabled(true);
  requestAnimationFrame(async () => {
    try {
      await fastTableEngine.generate({
        rowCount,
        useWorkerGeneration: useWorkerForRun,
      });
    } catch (error) {
      // runGenerationAction already handles user-visible error messages.
    } finally {
      setActionButtonsDisabled(false);
    }
  });
});
