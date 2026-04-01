import { createFastTableEngine } from "@fasttable/core/engine";
import { createFastTableRuntime } from "@fasttable/core/runtime";
import { ensureNumericColumnarSortedIndices as ensureCoreNumericColumnarSortedIndices } from "@fasttable/core/io";

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
let topLevelFilterCacheRevision = 0;
const topLevelFilterCaches = {
  small: {
    capacity: TOP_LEVEL_FILTER_CACHE_SMALL_CAPACITY,
    entries: new Map(),
    totalBytes: 0,
  },
  medium: {
    capacity: TOP_LEVEL_FILTER_CACHE_MEDIUM_CAPACITY,
    entries: new Map(),
    totalBytes: 0,
  },
};
const topLevelSmartFilterStates = new Map();
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
const precomputedSortScratch = {
  rowCount: 0,
  marks: null,
  output: null,
  work: null,
  counts: null,
  epoch: 1,
};
const precomputedGroupedSortScratch = {
  rowCount: 0,
  primary: null,
  secondary: null,
  counts: null,
};
const precomputedGroupedSortCache = {
  rowCount: 0,
  descriptors: [],
  sortedOrder: null,
  prefixStartsByLength: Object.create(null),
  prefixEndsByLength: Object.create(null),
};
const precomputedSubsetSortCache = {
  rowCount: 0,
  descriptors: [],
  sortedOrder: null,
  sortedCount: 0,
};
const PRECOMPUTED_ORDER_CHECK_MAX_GROUP_SIZE = 16384;
const benchmarkSortRuntime = createFastTableRuntime();

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
  return (
    topLevelFilterCaches.small.entries.size > 0 ||
    topLevelFilterCaches.medium.entries.size > 0
  );
}

function syncClearFilterCacheButtonState() {
  if (!clearFilterCacheBtnEl) {
    return;
  }

  const hasCacheEntries = hasTopLevelFilterCacheEntries();
  clearFilterCacheBtnEl.disabled = actionButtonsAreDisabled || !hasCacheEntries;
}

function clearTopLevelFilterCache() {
  topLevelFilterCaches.small.entries.clear();
  topLevelFilterCaches.small.totalBytes = 0;
  topLevelFilterCaches.medium.entries.clear();
  topLevelFilterCaches.medium.totalBytes = 0;
  syncClearFilterCacheButtonState();
}

function clearTopLevelSmartFilterState() {
  topLevelSmartFilterStates.clear();
}

function bumpTopLevelFilterCacheRevision() {
  topLevelFilterCacheRevision += 1;
  clearTopLevelFilterCache();
  clearTopLevelSmartFilterState();
}

function normalizeFilterValueForTopLevelCache(value) {
  return String(value).trim().toLowerCase();
}

function getActiveFiltersForOrchestration(rawFilters) {
  const sourceFilters =
    rawFilters && typeof rawFilters === "object" ? rawFilters : {};
  const activeFilters = Object.create(null);
  const keys = Object.keys(sourceFilters);

  for (let i = 0; i < keys.length; i += 1) {
    const key = keys[i];
    const normalized = normalizeFilterValueForTopLevelCache(sourceFilters[key]);
    if (normalized !== "") {
      activeFilters[key] = normalized;
    }
  }

  return activeFilters;
}

function cloneActiveFilters(activeFilters) {
  const out = Object.create(null);
  if (!activeFilters || typeof activeFilters !== "object") {
    return out;
  }

  const keys = Object.keys(activeFilters);
  for (let i = 0; i < keys.length; i += 1) {
    const key = keys[i];
    out[key] = activeFilters[key];
  }

  return out;
}

function areActiveFiltersStricter(nextFilters, previousFilters) {
  const prev = previousFilters || Object.create(null);
  const next = nextFilters || Object.create(null);
  const prevKeys = Object.keys(prev);
  const nextKeys = Object.keys(next);

  if (nextKeys.length < prevKeys.length) {
    return false;
  }

  let changed = false;

  for (let i = 0; i < prevKeys.length; i += 1) {
    const key = prevKeys[i];
    const prevValue = prev[key];
    const nextValue = next[key];

    if (nextValue === undefined) {
      return false;
    }

    if (!nextValue.startsWith(prevValue)) {
      return false;
    }

    if (nextValue !== prevValue) {
      changed = true;
    }
  }

  if (nextKeys.length > prevKeys.length) {
    changed = true;
  }

  return changed;
}

function buildRawFiltersWithoutGuaranteed(
  sourceRawFilters,
  fullActiveFilters,
  guaranteedActiveFilters
) {
  const source =
    sourceRawFilters && typeof sourceRawFilters === "object"
      ? sourceRawFilters
      : {};
  const fullActive =
    fullActiveFilters && typeof fullActiveFilters === "object"
      ? fullActiveFilters
      : Object.create(null);
  const guaranteed =
    guaranteedActiveFilters && typeof guaranteedActiveFilters === "object"
      ? guaranteedActiveFilters
      : Object.create(null);
  const out = {};
  const keys = Object.keys(source);

  for (let i = 0; i < keys.length; i += 1) {
    const key = keys[i];
    const activeValue = fullActive[key];
    const guaranteedValue = guaranteed[key];
    const isGuaranteed =
      activeValue !== undefined &&
      guaranteedValue !== undefined &&
      activeValue === guaranteedValue;
    if (!isGuaranteed) {
      out[key] = source[key];
    }
  }

  return out;
}

function buildTopLevelSmartFilterStateKey(filterOptions) {
  return [
    `mode:${getCurrentFilterModeKey()}`,
    "match:includes",
    `norm:${filterOptions && filterOptions.enableCaching === true ? 1 : 0}`,
    `dict:${filterOptions && filterOptions.useDictionaryKeySearch === true ? 1 : 0}`,
    `dictIntersect:${filterOptions && filterOptions.useDictionaryIntersection === true ? 1 : 0}`,
    `smartPlanner:${filterOptions && filterOptions.useSmarterPlanner === true ? 1 : 0}`,
  ].join("\u0001");
}

function getTopLevelSmartFilterStateEntry(stateKey) {
  if (!stateKey) {
    return null;
  }

  return topLevelSmartFilterStates.get(stateKey) || null;
}

function setTopLevelSmartFilterStateEntry(
  stateKey,
  fullActiveFilters,
  filteredIndices,
  filteredCount,
  indicesAlreadyStable
) {
  if (!stateKey) {
    return;
  }

  topLevelSmartFilterStates.set(stateKey, {
    fullActiveFilters: cloneActiveFilters(fullActiveFilters),
    filteredIndices:
      indicesAlreadyStable === true
        ? filteredIndices
        : cloneFilteredIndicesForTopLevelCache(filteredIndices),
    filteredCount: Math.max(0, Number(filteredCount) || 0),
  });
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

function buildTopLevelFilterCacheKey(rawFilters, filterOptions) {
  const normalizedEntries = [];
  const sourceFilters =
    rawFilters && typeof rawFilters === "object" ? rawFilters : {};
  const keys = Object.keys(sourceFilters);
  for (let i = 0; i < keys.length; i += 1) {
    const key = keys[i];
    const normalized = normalizeFilterValueForTopLevelCache(sourceFilters[key]);
    if (normalized !== "") {
      normalizedEntries.push(`${key}=${normalized}`);
    }
  }
  normalizedEntries.sort();

  return [
    `rev:${topLevelFilterCacheRevision}`,
    `mode:${getCurrentFilterModeKey()}`,
    "match:includes",
    `norm:${filterOptions && filterOptions.enableCaching === true ? 1 : 0}`,
    `dict:${filterOptions && filterOptions.useDictionaryKeySearch === true ? 1 : 0}`,
    `dictIntersect:${filterOptions && filterOptions.useDictionaryIntersection === true ? 1 : 0}`,
    `smartPlanner:${filterOptions && filterOptions.useSmarterPlanner === true ? 1 : 0}`,
    `filters:${normalizedEntries.join("\u0002")}`,
  ].join("\u0001");
}

function cloneFilteredIndicesForTopLevelCache(filteredIndices) {
  if (filteredIndices === null || filteredIndices === undefined) {
    return null;
  }

  if (
    filteredIndices &&
    ArrayBuffer.isView(filteredIndices.buffer) &&
    typeof filteredIndices.count === "number"
  ) {
    const count = Math.max(
      0,
      Math.min(filteredIndices.count | 0, filteredIndices.buffer.length)
    );
    const copy = new Uint32Array(count);
    if (count > 0) {
      copy.set(filteredIndices.buffer.subarray(0, count));
    }
    return {
      buffer: copy,
      count,
    };
  }

  if (Array.isArray(filteredIndices)) {
    return filteredIndices.slice();
  }

  if (ArrayBuffer.isView(filteredIndices)) {
    const copy = new filteredIndices.constructor(filteredIndices.length);
    copy.set(filteredIndices);
    return copy;
  }

  return null;
}

function getTopLevelFilterCacheEntry(cache, cacheKey) {
  const existing = cache.entries.get(cacheKey);
  if (!existing) {
    return null;
  }

  cache.entries.delete(cacheKey);
  cache.entries.set(cacheKey, existing);
  return existing;
}

function getTopLevelFilterCacheResultBytes(filteredIndices) {
  if (
    filteredIndices &&
    ArrayBuffer.isView(filteredIndices.buffer) &&
    typeof filteredIndices.count === "number"
  ) {
    return Math.max(0, filteredIndices.count | 0) * 4;
  }

  if (Array.isArray(filteredIndices) || ArrayBuffer.isView(filteredIndices)) {
    return Math.max(0, filteredIndices.length | 0) * 4;
  }

  return 0;
}

function getTopLevelFilterCacheEntryFromTiers(cacheKey) {
  const fromSmall = getTopLevelFilterCacheEntry(
    topLevelFilterCaches.small,
    cacheKey
  );
  if (fromSmall) {
    return fromSmall;
  }

  return getTopLevelFilterCacheEntry(topLevelFilterCaches.medium, cacheKey);
}

function chooseTopLevelFilterCacheTier(resultCount) {
  if (resultCount <= TOP_LEVEL_FILTER_CACHE_SMALL_MAX_RESULTS) {
    return "small";
  }

  if (resultCount <= TOP_LEVEL_FILTER_CACHE_MEDIUM_MAX_RESULTS) {
    return "medium";
  }

  return "";
}

function setTopLevelFilterCacheEntryInTier(
  tier,
  cacheKey,
  filteredIndices,
  filteredCount,
  searchedCount
) {
  const cache =
    tier === "small"
      ? topLevelFilterCaches.small
      : topLevelFilterCaches.medium;

  const entry = {
    filteredIndices: cloneFilteredIndicesForTopLevelCache(filteredIndices),
    filteredCount: Number(filteredCount) || 0,
    searchedCount:
      Number.isFinite(Number(searchedCount)) && Number(searchedCount) >= 0
        ? Number(searchedCount)
        : -1,
    tier,
  };
  const bytes = getTopLevelFilterCacheResultBytes(entry.filteredIndices);

  const previous = cache.entries.get(cacheKey);
  if (previous) {
    cache.totalBytes -= previous.bytes || 0;
    cache.entries.delete(cacheKey);
  }
  entry.bytes = bytes;
  cache.entries.set(cacheKey, entry);
  cache.totalBytes += bytes;

  while (cache.entries.size > cache.capacity) {
    const oldestKey = cache.entries.keys().next().value;
    const oldestEntry = cache.entries.get(oldestKey);
    if (oldestEntry) {
      cache.totalBytes -= oldestEntry.bytes || 0;
    }
    cache.entries.delete(oldestKey);
  }

  return entry;
}

function clearAllFilterCaches() {
  clearTopLevelFilterCache();
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

function clearPrecomputedGroupedSortCache() {
  precomputedGroupedSortCache.rowCount = 0;
  precomputedGroupedSortCache.descriptors = [];
  precomputedGroupedSortCache.sortedOrder = null;
  precomputedGroupedSortCache.prefixStartsByLength = Object.create(null);
  precomputedGroupedSortCache.prefixEndsByLength = Object.create(null);
  precomputedSubsetSortCache.rowCount = 0;
  precomputedSubsetSortCache.descriptors = [];
  precomputedSubsetSortCache.sortedOrder = null;
  precomputedSubsetSortCache.sortedCount = 0;
}

function syncBenchmarkSortRuntime(rawFilters) {
  const runtimeRawFilters =
    rawFilters && typeof rawFilters === "object" ? rawFilters : readRawFilters();
  const rowCount = getLoadedRowCount();

  if (rowCount > 0) {
    const numericColumnarData = getNumericColumnarForSave();
    if (
      numericColumnarData &&
      isValidNumericColumnarData(numericColumnarData)
    ) {
      benchmarkSortRuntime.setDataFromNumericColumnar(numericColumnarData);
    } else {
      benchmarkSortRuntime.setDataFromRows(ensureObjectRowsAvailable());
    }
  } else {
    benchmarkSortRuntime.setDataFromRows([]);
  }

  benchmarkSortRuntime.setModeOptions(getModeOptions());
  benchmarkSortRuntime.setRawFilters(runtimeRawFilters);
  benchmarkSortRuntime.setSortOptions(getSortRuntimeOptions());

  const selectedSortMode = getSortMode();
  if (selectedSortMode !== "precomputed") {
    benchmarkSortRuntime.setSortMode(selectedSortMode);
  }
  return runtimeRawFilters;
}

function buildRowsSnapshotFromRawFilters(rawFilters) {
  const runtimeRawFilters = syncBenchmarkSortRuntime(rawFilters);
  return benchmarkSortRuntime.buildSortRowsSnapshot(runtimeRawFilters);
}

function resolveBenchmarkSnapshotIndices(rowsSnapshot, totalRowCount) {
  if (Array.isArray(rowsSnapshot) && hasIndexCollection(rowsSnapshot.__rowIndices)) {
    return materializeFilteredIndexArray(rowsSnapshot.__rowIndices, totalRowCount);
  }

  if (
    rowsSnapshot &&
    typeof rowsSnapshot === "object" &&
    hasIndexCollection(rowsSnapshot.rowIndices)
  ) {
    return materializeFilteredIndexArray(rowsSnapshot.rowIndices, totalRowCount);
  }

  return null;
}

function runPrecomputedSortSnapshotBenchmarkPass(rowsSnapshot, descriptors) {
  const descriptorList = normalizeSortDescriptorList(descriptors);
  if (!Array.isArray(descriptorList) || descriptorList.length === 0) {
    return benchmarkSortRuntime.runSortSnapshotPass(
      rowsSnapshot,
      descriptorList,
      "native"
    );
  }

  const loadedRowCount = getLoadedRowCount();
  const snapshotIndices = resolveBenchmarkSnapshotIndices(
    rowsSnapshot,
    loadedRowCount
  );
  if (!hasIndexCollection(snapshotIndices)) {
    return benchmarkSortRuntime.runSortSnapshotPass(
      rowsSnapshot,
      descriptorList,
      "native"
    );
  }

  const snapshotCount = snapshotIndices.length;
  const totalStartMs = performance.now();
  const sortedRun = buildSortedIndicesViaPrecomputedMode(
    snapshotIndices,
    snapshotCount,
    descriptorList,
    loadedRowCount
  );
  if (
    !sortedRun ||
    !sortedRun.sortResult ||
    !hasIndexCollection(sortedRun.sortedIndices)
  ) {
    return benchmarkSortRuntime.runSortSnapshotPass(
      rowsSnapshot,
      descriptorList,
      "native"
    );
  }

  const sortTotalMs = performance.now() - totalStartMs;
  const sortCoreMs = Number(sortedRun.sortResult.durationMs) || 0;
  return {
    sortMs: sortCoreMs,
    sortCoreMs,
    sortPrepMs: sortTotalMs - sortCoreMs,
    sortTotalMs,
    sortMode: "precomputed",
    sortedCount: sortedRun.sortedIndices.length,
    descriptors:
      sortedRun.sortResult.effectiveDescriptors ||
      sortedRun.sortResult.descriptors ||
      descriptorList,
    dataPath: sortedRun.sortResult.dataPath,
    comparatorMode: sortedRun.sortResult.comparatorMode || "precomputed",
  };
}

function runSortSnapshotPass(rowsSnapshot, descriptors, sortMode) {
  syncBenchmarkSortRuntime();

  const requestedSortMode =
    typeof sortMode === "string" && sortMode.trim() !== ""
      ? sortMode
      : undefined;
  if (requestedSortMode === "precomputed") {
    return runPrecomputedSortSnapshotBenchmarkPass(rowsSnapshot, descriptors);
  }

  return benchmarkSortRuntime.runSortSnapshotPass(
    rowsSnapshot,
    descriptors,
    requestedSortMode
  );
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

function applySortAndRender() {
  if (!sortController) {
    return null;
  }

  if (getLoadedRowCount() === 0) {
    setSortTelemetryStatus("No data loaded yet.");
    return null;
  }

  const wallStartMs = performance.now();
  const filterResult = getCurrentFilterResultSnapshot();
  let sortRun = null;
  let renderIndices = filterResult.filteredIndices;

  sortRun = buildSortedIndicesForCurrentResult(filterResult);
  if (sortRun && hasIndexCollection(sortRun.indices)) {
    renderIndices = sortRun.indices;
  }

  const renderStartMs = performance.now();
  renderFilterResultByCurrentMode(filterResult, renderIndices, true);
  const renderMs = performance.now() - renderStartMs;
  const wallMs = performance.now() - wallStartMs;
  window.fastTableFilteredRows = null;
  window.fastTableFilteredRowIndices = renderIndices;
  updateSortHeaderIndicators();
  if (!sortRun || !sortRun.result) {
    setSortTelemetryStatus("No sort telemetry yet.");
    return null;
  }

  setSortTelemetryFromResult(
    sortRun.result,
    sortRun.sortedCount,
    {
      sortTotalMs: sortRun.sortTotalMs,
      sortPrepMs: sortRun.sortPrepMs,
      rankBuildMs: sortRun.rankBuildMs,
      renderMs,
      wallMs,
    }
  );
  return sortRun.result;
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
  clearPrecomputedGroupedSortCache();
  cachedObjectColumnarData = null;
  objectColumnarFilterController.setData(null);

  cachedNumericRows = null;
  numericRowFilterController.setRows([]);

  cachedNumericColumnarData = null;
  numericColumnarFilterController.setData(null);

  window.fastTableColumnarData = null;
  window.fastTableNumericRows = null;
  window.fastTableNumericColumnarData = null;
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

function buildRankArrayForColumnFromSortedIndices(
  numericColumnarData,
  columnIndex,
  sortedIndices,
  rowCount
) {
  if (
    !isValidNumericColumnarData(numericColumnarData) ||
    !(sortedIndices instanceof Uint32Array) ||
    sortedIndices.length !== rowCount
  ) {
    return null;
  }

  const columnKinds = numericColumnarData.columnKinds || [];
  const columns = numericColumnarData.columns || [];
  const columnKind = columnKinds[columnIndex];
  const values = columns[columnIndex];
  const rank32 = new Uint32Array(rowCount);
  if (rowCount <= 0) {
    return {
      rankByRowId: rank32,
      maxRank: 0,
    };
  }

  let currentRank = 0;
  const firstRowIndex = sortedIndices[0];
  let previousToken = values ? values[firstRowIndex] : undefined;
  rank32[firstRowIndex] = currentRank;

  for (let i = 1; i < rowCount; i += 1) {
    const rowIndex = sortedIndices[i];
    const nextToken = values ? values[rowIndex] : undefined;
    let isSame = false;

    if (columnKind === "float") {
      const previousNumber = Number(previousToken);
      const nextNumber = Number(nextToken);
      if (
        (Number.isNaN(previousNumber) && Number.isNaN(nextNumber)) ||
        previousNumber === nextNumber
      ) {
        isSame = true;
      }
    } else {
      isSame = nextToken === previousToken;
    }

    if (!isSame) {
      currentRank += 1;
      previousToken = nextToken;
    }

    rank32[rowIndex] = currentRank;
  }

  if (currentRank <= 0xffff) {
    const rank16 = new Uint16Array(rowCount);
    rank16.set(rank32);
    return {
      rankByRowId: rank16,
      maxRank: currentRank >>> 0,
    };
  }

  return {
    rankByRowId: rank32,
    maxRank: currentRank >>> 0,
  };
}

function buildDescendingRankArrayFromAscending(rankByRowId, maxRank, rowCount) {
  if (
    !isSupportedRankArray(rankByRowId) ||
    rankByRowId.length !== rowCount
  ) {
    return null;
  }

  const maxRankValue = Number(maxRank) >>> 0;
  if (maxRankValue <= 0xffff) {
    const out16 = new Uint16Array(rowCount);
    for (let i = 0; i < rowCount; i += 1) {
      out16[i] = (maxRankValue - (rankByRowId[i] >>> 0)) >>> 0;
    }
    return out16;
  }

  const out32 = new Uint32Array(rowCount);
  for (let i = 0; i < rowCount; i += 1) {
    out32[i] = (maxRankValue - (rankByRowId[i] >>> 0)) >>> 0;
  }
  return out32;
}

function ensureNumericColumnarSortedRanks(numericColumnarData) {
  if (!isValidNumericColumnarData(numericColumnarData)) {
    return {
      numericColumnarData,
      durationMs: 0,
      computedColumns: 0,
    };
  }

  const rowCount = Math.max(0, Number(numericColumnarData.rowCount) || 0);
  const sortedColumnarData = ensureNumericColumnarSortedIndices(numericColumnarData);
  const sortedColumns = Array.isArray(sortedColumnarData.sortedIndexColumns)
    ? sortedColumnarData.sortedIndexColumns
    : [];
  const baseCount = Math.min(columnKeys.length, sortedColumns.length);
  const existingAscRankColumns = Array.isArray(sortedColumnarData.sortedRankAscColumns)
    ? sortedColumnarData.sortedRankAscColumns
    : Array.isArray(sortedColumnarData.sortedRankColumns)
      ? sortedColumnarData.sortedRankColumns
      : [];
  const existingAscRankMaxColumns = Array.isArray(
    sortedColumnarData.sortedRankAscMaxColumns
  )
    ? sortedColumnarData.sortedRankAscMaxColumns
    : Array.isArray(sortedColumnarData.sortedRankMaxColumns)
      ? sortedColumnarData.sortedRankMaxColumns
      : [];
  const existingDescRankColumns = Array.isArray(sortedColumnarData.sortedRankDescColumns)
    ? sortedColumnarData.sortedRankDescColumns
    : [];
  const existingDescRankMaxColumns = Array.isArray(
    sortedColumnarData.sortedRankDescMaxColumns
  )
    ? sortedColumnarData.sortedRankDescMaxColumns
    : [];
  const ascRankColumns = new Array(baseCount).fill(null);
  const ascRankMaxColumns = new Array(baseCount).fill(0);
  const ascRankByKey = Object.create(null);
  const ascRankMaxByKey = Object.create(null);
  const descRankColumns = new Array(baseCount).fill(null);
  const descRankMaxColumns = new Array(baseCount).fill(0);
  const descRankByKey = Object.create(null);
  const descRankMaxByKey = Object.create(null);
  const startMs = performance.now();
  let computedColumns = 0;

  for (let colIndex = 0; colIndex < baseCount; colIndex += 1) {
    const sortedIndices = sortedColumns[colIndex];
    if (!(sortedIndices instanceof Uint32Array) || sortedIndices.length !== rowCount) {
      continue;
    }

    const existingAscRank = existingAscRankColumns[colIndex];
    const existingAscMax = Number(existingAscRankMaxColumns[colIndex]);
    let ascRankByRowId = null;
    let ascMaxRank = 0;
    if (isSupportedRankArray(existingAscRank) && existingAscRank.length === rowCount) {
      ascRankByRowId = existingAscRank;
      ascMaxRank = Number.isFinite(existingAscMax)
        ? Math.max(0, existingAscMax) >>> 0
        : rowCount > 0
          ? existingAscRank[sortedIndices[rowCount - 1]] >>> 0
          : 0;
    } else {
      const builtRank = buildRankArrayForColumnFromSortedIndices(
        sortedColumnarData,
        colIndex,
        sortedIndices,
        rowCount
      );
      if (!builtRank || !isSupportedRankArray(builtRank.rankByRowId)) {
        continue;
      }

      ascRankByRowId = builtRank.rankByRowId;
      ascMaxRank = Number(builtRank.maxRank) >>> 0;
      computedColumns += 1;
    }

    ascRankColumns[colIndex] = ascRankByRowId;
    ascRankMaxColumns[colIndex] = ascMaxRank;

    const existingDescRank = existingDescRankColumns[colIndex];
    const existingDescMax = Number(existingDescRankMaxColumns[colIndex]);
    let descRankByRowId = null;
    if (isSupportedRankArray(existingDescRank) && existingDescRank.length === rowCount) {
      descRankByRowId = existingDescRank;
      descRankMaxColumns[colIndex] = Number.isFinite(existingDescMax)
        ? Math.max(0, existingDescMax) >>> 0
        : ascMaxRank;
    } else {
      const builtDescRank = buildDescendingRankArrayFromAscending(
        ascRankByRowId,
        ascMaxRank,
        rowCount
      );
      if (!isSupportedRankArray(builtDescRank)) {
        continue;
      }
      descRankByRowId = builtDescRank;
      descRankMaxColumns[colIndex] = ascMaxRank;
      computedColumns += 1;
    }
    descRankColumns[colIndex] = descRankByRowId;

    const columnKey = columnKeys[colIndex];
    ascRankByKey[columnKey] = ascRankByRowId;
    ascRankMaxByKey[columnKey] = ascMaxRank;
    descRankByKey[columnKey] = descRankByRowId;
    descRankMaxByKey[columnKey] = descRankMaxColumns[colIndex];
  }

  sortedColumnarData.sortedRankAscColumns = ascRankColumns;
  sortedColumnarData.sortedRankAscByKey = ascRankByKey;
  sortedColumnarData.sortedRankAscMaxColumns = ascRankMaxColumns;
  sortedColumnarData.sortedRankAscMaxByKey = ascRankMaxByKey;
  sortedColumnarData.sortedRankDescColumns = descRankColumns;
  sortedColumnarData.sortedRankDescByKey = descRankByKey;
  sortedColumnarData.sortedRankDescMaxColumns = descRankMaxColumns;
  sortedColumnarData.sortedRankDescMaxByKey = descRankMaxByKey;
  // Back-compat aliases for existing call sites: default rank = ascending.
  sortedColumnarData.sortedRankColumns = ascRankColumns;
  sortedColumnarData.sortedRankByKey = ascRankByKey;
  sortedColumnarData.sortedRankMaxColumns = ascRankMaxColumns;
  sortedColumnarData.sortedRankMaxByKey = ascRankMaxByKey;

  return {
    numericColumnarData: sortedColumnarData,
    durationMs: performance.now() - startMs,
    computedColumns,
  };
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
          applySortAndRender();
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

function buildPrecomputedSortKeyColumns(indices, rowsByIndex, descriptors) {
  const descriptorList = Array.isArray(descriptors) ? descriptors : [];
  const keyColumns = new Array(descriptorList.length);

  for (let d = 0; d < descriptorList.length; d += 1) {
    const descriptor = descriptorList[d];
    const columnKey = descriptor && descriptor.columnKey;
    const valueType =
      sortColumnTypeByKey && typeof columnKey === "string"
        ? sortColumnTypeByKey[columnKey]
        : "string";
    const useNumericValues = valueType === "number";
    const values = useNumericValues
      ? new Float64Array(indices.length)
      : new Array(indices.length);

    for (let i = 0; i < indices.length; i += 1) {
      const rowIndex = indices[i];
      const row = rowsByIndex[rowIndex];
      const rawValue = row && columnKey ? row[columnKey] : undefined;
      if (useNumericValues) {
        if (rawValue === undefined || rawValue === null) {
          values[i] = Number.NaN;
        } else {
          const numericValue = Number(rawValue);
          values[i] = Number.isFinite(numericValue) ? numericValue : Number.NaN;
        }
      } else {
        values[i] = rawValue;
      }
    }

    keyColumns[d] = values;
  }

  return keyColumns;
}

function buildPrecomputedSortRankColumns(descriptors, rowCount) {
  const descriptorList = Array.isArray(descriptors) ? descriptors : [];
  const expectedRowCount = Math.max(0, Number(rowCount) || 0);
  if (descriptorList.length === 0 || expectedRowCount <= 0) {
    return null;
  }

  let numericColumnarData =
    isValidNumericColumnarData(cachedNumericColumnarData)
      ? cachedNumericColumnarData
      : null;
  const hasReadyAscRanks =
    numericColumnarData &&
    ((numericColumnarData.sortedRankAscByKey &&
      typeof numericColumnarData.sortedRankAscByKey === "object" &&
      !Array.isArray(numericColumnarData.sortedRankAscByKey)) ||
      (numericColumnarData.sortedRankByKey &&
        typeof numericColumnarData.sortedRankByKey === "object" &&
        !Array.isArray(numericColumnarData.sortedRankByKey)));
  if (!hasReadyAscRanks) {
    numericColumnarData = getNumericColumnarDataForPrecomputedSort();
  }
  if (!isValidNumericColumnarData(numericColumnarData)) {
    return null;
  }

  const rankByKey =
    numericColumnarData.sortedRankAscByKey &&
    typeof numericColumnarData.sortedRankAscByKey === "object" &&
    !Array.isArray(numericColumnarData.sortedRankAscByKey)
      ? numericColumnarData.sortedRankAscByKey
      : numericColumnarData.sortedRankByKey &&
          typeof numericColumnarData.sortedRankByKey === "object" &&
          !Array.isArray(numericColumnarData.sortedRankByKey)
        ? numericColumnarData.sortedRankByKey
        : Object.create(null);
  const rankColumnsByColumn = Array.isArray(numericColumnarData.sortedRankAscColumns)
    ? numericColumnarData.sortedRankAscColumns
    : Array.isArray(numericColumnarData.sortedRankColumns)
      ? numericColumnarData.sortedRankColumns
      : [];

  const descriptorRankColumns = new Array(descriptorList.length);
  for (let i = 0; i < descriptorList.length; i += 1) {
    const descriptor = descriptorList[i];
    if (!descriptor || typeof descriptor.columnKey !== "string") {
      return null;
    }
    let rankByRowId = rankByKey[descriptor.columnKey];
    if (!isSupportedRankArray(rankByRowId)) {
      const columnIndex = columnIndexByKey[descriptor.columnKey];
      if (typeof columnIndex === "number" && columnIndex >= 0) {
        rankByRowId = rankColumnsByColumn[columnIndex];
      }
    }
    if (
      !isSupportedRankArray(rankByRowId) ||
      rankByRowId.length !== expectedRowCount
    ) {
      return null;
    }

    descriptorRankColumns[i] = rankByRowId;
  }

  return descriptorRankColumns;
}

function resolveSortModeForRun(sortModeOverride) {
  if (typeof sortModeOverride === "string" && sortModeOverride !== "") {
    return sortModeOverride;
  }

  return getSortMode();
}

function getNumericColumnarDataForPrecomputedSort() {
  if (
    cachedNumericColumnarData !== null &&
    isValidNumericColumnarData(cachedNumericColumnarData)
  ) {
    cachedNumericColumnarData = ensureNumericColumnarSortedIndices(
      cachedNumericColumnarData
    );
    const rankPrecompute = ensureNumericColumnarSortedRanks(
      cachedNumericColumnarData
    );
    cachedNumericColumnarData = rankPrecompute.numericColumnarData;
    return cachedNumericColumnarData;
  }

  if (getLoadedRowCount() === 0) {
    return null;
  }

  const numericRowsBuild = getOrBuildNumericRows();
  const numericColumnarBuild = getOrBuildNumericColumnarData(
    numericRowsBuild.numericRows
  );
  cachedNumericColumnarData = ensureNumericColumnarSortedIndices(
    numericColumnarBuild.columnarData
  );
  const rankPrecompute = ensureNumericColumnarSortedRanks(
    cachedNumericColumnarData
  );
  cachedNumericColumnarData = rankPrecompute.numericColumnarData;
  return cachedNumericColumnarData;
}

function getPrecomputedSortedIndexForColumn(columnKey, rowCount) {
  if (typeof columnKey !== "string" || columnKey === "") {
    return null;
  }

  const numericColumnarData = getNumericColumnarDataForPrecomputedSort();
  if (!isValidNumericColumnarData(numericColumnarData)) {
    return null;
  }

  const expectedRowCount = Math.max(0, Number(rowCount) || 0);
  const byKey = numericColumnarData.sortedIndexByKey;
  if (byKey && typeof byKey === "object" && !Array.isArray(byKey)) {
    const direct = byKey[columnKey];
    if (direct instanceof Uint32Array && direct.length === expectedRowCount) {
      return direct;
    }
  }

  const colIndex = columnIndexByKey[columnKey];
  if (typeof colIndex !== "number") {
    return null;
  }

  const sortedColumns = Array.isArray(numericColumnarData.sortedIndexColumns)
    ? numericColumnarData.sortedIndexColumns
    : [];
  const fromColumn = sortedColumns[colIndex];
  if (fromColumn instanceof Uint32Array && fromColumn.length === expectedRowCount) {
    return fromColumn;
  }

  return null;
}

function ensurePrecomputedSortScratch(rowCount) {
  const count = Math.max(0, Number(rowCount) || 0);
  if (
    precomputedSortScratch.marks instanceof Uint32Array &&
    precomputedSortScratch.output instanceof Uint32Array &&
    precomputedSortScratch.work instanceof Uint32Array &&
    precomputedSortScratch.counts instanceof Uint32Array &&
    precomputedSortScratch.counts.length >= 256 &&
    precomputedSortScratch.rowCount === count
  ) {
    return precomputedSortScratch;
  }

  precomputedSortScratch.rowCount = count;
  precomputedSortScratch.marks = new Uint32Array(count);
  precomputedSortScratch.output = new Uint32Array(count);
  precomputedSortScratch.work = new Uint32Array(count);
  precomputedSortScratch.counts = new Uint32Array(256);
  precomputedSortScratch.epoch = 1;
  return precomputedSortScratch;
}

function ensurePrecomputedGroupedSortScratch(rowCount) {
  const count = Math.max(0, Number(rowCount) || 0);
  if (
    precomputedGroupedSortScratch.primary instanceof Uint32Array &&
    precomputedGroupedSortScratch.secondary instanceof Uint32Array &&
    precomputedGroupedSortScratch.counts instanceof Uint32Array &&
    precomputedGroupedSortScratch.counts.length >= 256 &&
    precomputedGroupedSortScratch.rowCount === count
  ) {
    return precomputedGroupedSortScratch;
  }

  precomputedGroupedSortScratch.rowCount = count;
  precomputedGroupedSortScratch.primary = new Uint32Array(count);
  precomputedGroupedSortScratch.secondary = new Uint32Array(count);
  precomputedGroupedSortScratch.counts = new Uint32Array(256);
  return precomputedGroupedSortScratch;
}

function nextPrecomputedSortEpoch(scratch) {
  if (!scratch) {
    return 1;
  }

  let nextEpoch = (scratch.epoch + 1) >>> 0;
  if (nextEpoch === 0) {
    scratch.marks.fill(0);
    nextEpoch = 1;
  }
  scratch.epoch = nextEpoch;
  return nextEpoch;
}

function getResolvedFilteredCount(filteredIndices, filteredCount, rowCount) {
  const explicitCount = Number(filteredCount);
  if (Number.isFinite(explicitCount) && explicitCount >= 0) {
    return explicitCount | 0;
  }

  return Number(getFilteredIndicesCount(filteredIndices, rowCount)) | 0;
}

function materializeFilteredIndicesIntoUint32(
  filteredIndices,
  filteredCount,
  rowCount,
  target
) {
  const count = Math.max(0, Number(filteredCount) || 0);
  if (!(target instanceof Uint32Array) || target.length < count) {
    return null;
  }

  if (filteredIndices === null || filteredIndices === undefined) {
    for (let i = 0; i < count; i += 1) {
      target[i] = i;
    }
    return target.subarray(0, count);
  }

  if (
    filteredIndices &&
    ArrayBuffer.isView(filteredIndices.buffer) &&
    typeof filteredIndices.count === "number"
  ) {
    const bufferView = filteredIndices.buffer;
    for (let i = 0; i < count; i += 1) {
      target[i] = bufferView[i] >>> 0;
    }
    return target.subarray(0, count);
  }

  if (Array.isArray(filteredIndices) || ArrayBuffer.isView(filteredIndices)) {
    for (let i = 0; i < count; i += 1) {
      target[i] = filteredIndices[i] >>> 0;
    }
    return target.subarray(0, count);
  }

  for (let i = 0; i < count; i += 1) {
    target[i] = getFilteredIndexAt(filteredIndices, i) >>> 0;
  }
  return target.subarray(0, count);
}

function isFullSelectionForPrecomputedSort(
  filteredIndices,
  filteredCount,
  rowCount
) {
  if (filteredIndices === null || filteredIndices === undefined) {
    return true;
  }

  return getResolvedFilteredCount(filteredIndices, filteredCount, rowCount) === rowCount;
}

function shouldUseRankSortForPrecomputedSubset(filteredCount, rowCount) {
  const subsetCount = Math.max(0, Number(filteredCount) || 0);
  const totalRows = Math.max(0, Number(rowCount) || 0);
  if (subsetCount <= 1 || totalRows <= 1 || subsetCount >= totalRows) {
    return false;
  }

  const subsetLogCost = subsetCount * Math.log2(Math.max(2, subsetCount));
  const scanCost = totalRows;
  return subsetLogCost <= scanCost * 0.75;
}

function buildPrecomputedRankForColumn(columnKey, rowCount, direction) {
  const expectedRowCount = Math.max(0, Number(rowCount) || 0);
  const normalizedDirection = normalizeSortDirection(direction);
  const numericColumnarData = getNumericColumnarDataForPrecomputedSort();
  if (!isValidNumericColumnarData(numericColumnarData)) {
    return null;
  }

  const rankByKey =
    normalizedDirection === "asc"
      ? numericColumnarData.sortedRankAscByKey &&
        typeof numericColumnarData.sortedRankAscByKey === "object" &&
        !Array.isArray(numericColumnarData.sortedRankAscByKey)
        ? numericColumnarData.sortedRankAscByKey
        : numericColumnarData.sortedRankByKey &&
          typeof numericColumnarData.sortedRankByKey === "object" &&
          !Array.isArray(numericColumnarData.sortedRankByKey)
          ? numericColumnarData.sortedRankByKey
          : Object.create(null)
      : numericColumnarData.sortedRankDescByKey &&
        typeof numericColumnarData.sortedRankDescByKey === "object" &&
        !Array.isArray(numericColumnarData.sortedRankDescByKey)
        ? numericColumnarData.sortedRankDescByKey
      : Object.create(null);
  const rankMaxByKey =
    normalizedDirection === "asc"
      ? numericColumnarData.sortedRankAscMaxByKey &&
        typeof numericColumnarData.sortedRankAscMaxByKey === "object" &&
        !Array.isArray(numericColumnarData.sortedRankAscMaxByKey)
        ? numericColumnarData.sortedRankAscMaxByKey
        : numericColumnarData.sortedRankMaxByKey &&
          typeof numericColumnarData.sortedRankMaxByKey === "object" &&
          !Array.isArray(numericColumnarData.sortedRankMaxByKey)
          ? numericColumnarData.sortedRankMaxByKey
          : Object.create(null)
      : numericColumnarData.sortedRankDescMaxByKey &&
        typeof numericColumnarData.sortedRankDescMaxByKey === "object" &&
        !Array.isArray(numericColumnarData.sortedRankDescMaxByKey)
        ? numericColumnarData.sortedRankDescMaxByKey
      : Object.create(null);
  const existingByKey = rankByKey[columnKey];
  if (isSupportedRankArray(existingByKey) && existingByKey.length === expectedRowCount) {
    return {
      rankByRowId: existingByKey,
      durationMs: 0,
      maxRank: Number(rankMaxByKey[columnKey]) >>> 0,
    };
  }

  const columnIndex = columnIndexByKey[columnKey];
  if (typeof columnIndex !== "number" || columnIndex < 0) {
    return null;
  }

  const existingRankColumns =
    normalizedDirection === "asc"
      ? Array.isArray(numericColumnarData.sortedRankAscColumns)
        ? numericColumnarData.sortedRankAscColumns
        : Array.isArray(numericColumnarData.sortedRankColumns)
          ? numericColumnarData.sortedRankColumns
          : []
      : Array.isArray(numericColumnarData.sortedRankDescColumns)
        ? numericColumnarData.sortedRankDescColumns
    : [];
  const existingRankMaxColumns =
    normalizedDirection === "asc"
      ? Array.isArray(numericColumnarData.sortedRankAscMaxColumns)
        ? numericColumnarData.sortedRankAscMaxColumns
        : Array.isArray(numericColumnarData.sortedRankMaxColumns)
          ? numericColumnarData.sortedRankMaxColumns
          : []
      : Array.isArray(numericColumnarData.sortedRankDescMaxColumns)
        ? numericColumnarData.sortedRankDescMaxColumns
    : [];
  const existingByColumn = existingRankColumns[columnIndex];
  if (
    isSupportedRankArray(existingByColumn) &&
    existingByColumn.length === expectedRowCount
  ) {
    const maxRank = Number(existingRankMaxColumns[columnIndex]) >>> 0;
    rankByKey[columnKey] = existingByColumn;
    rankMaxByKey[columnKey] = maxRank;
    if (normalizedDirection === "asc") {
      numericColumnarData.sortedRankAscByKey = rankByKey;
      numericColumnarData.sortedRankAscMaxByKey = rankMaxByKey;
    } else {
      numericColumnarData.sortedRankDescByKey = rankByKey;
      numericColumnarData.sortedRankDescMaxByKey = rankMaxByKey;
    }
    return {
      rankByRowId: existingByColumn,
      durationMs: 0,
      maxRank,
    };
  }
  return null;
}

function buildPrecomputedDescAllIndices(sortedColumnIndices, rowCount) {
  if (!(sortedColumnIndices instanceof Uint32Array)) {
    return null;
  }

  const totalRows = Math.max(0, Number(rowCount) || 0);
  if (sortedColumnIndices.length !== totalRows) {
    return null;
  }

  const scratch = ensurePrecomputedSortScratch(totalRows);
  const output = scratch.output;
  for (let i = 0; i < totalRows; i += 1) {
    output[i] = sortedColumnIndices[totalRows - 1 - i];
  }

  return output.subarray(0, totalRows);
}

function buildSortedIndicesFromPrecomputedScan(
  filteredIndices,
  filteredCount,
  sortedColumnIndices,
  direction,
  rowCount
) {
  if (!(sortedColumnIndices instanceof Uint32Array)) {
    return null;
  }

  const totalRows = Math.max(0, Number(rowCount) || 0);
  if (sortedColumnIndices.length !== totalRows) {
    return null;
  }

  const count = getResolvedFilteredCount(filteredIndices, filteredCount, totalRows);
  if (count <= 0) {
    return new Uint32Array(0);
  }

  if (isFullSelectionForPrecomputedSort(filteredIndices, count, totalRows)) {
    if (direction === "desc") {
      return buildPrecomputedDescAllIndices(sortedColumnIndices, totalRows);
    }

    return sortedColumnIndices;
  }

  const scratch = ensurePrecomputedSortScratch(totalRows);
  const marks = scratch.marks;
  const output = scratch.output;
  const epoch = nextPrecomputedSortEpoch(scratch);
  const subset = materializeFilteredIndicesIntoUint32(
    filteredIndices,
    count,
    totalRows,
    scratch.work
  );
  if (!(subset instanceof Uint32Array)) {
    return null;
  }

  for (let i = 0; i < subset.length; i += 1) {
    marks[subset[i]] = epoch;
  }

  let writeIndex = 0;
  if (direction === "desc") {
    for (let i = totalRows - 1; i >= 0; i -= 1) {
      const rowIndex = sortedColumnIndices[i];
      if (marks[rowIndex] === epoch) {
        output[writeIndex] = rowIndex;
        writeIndex += 1;
        if (writeIndex === subset.length) {
          break;
        }
      }
    }
  } else {
    for (let i = 0; i < totalRows; i += 1) {
      const rowIndex = sortedColumnIndices[i];
      if (marks[rowIndex] === epoch) {
        output[writeIndex] = rowIndex;
        writeIndex += 1;
        if (writeIndex === subset.length) {
          break;
        }
      }
    }
  }

  return output.subarray(0, writeIndex);
}

function stableRadixSortRowIdsByUint32Key(
  rowIds,
  keyByRowId,
  descending,
  tempBuffer,
  countsBuffer,
  keyMaxValueOverride
) {
  if (!(rowIds instanceof Uint32Array)) {
    return false;
  }

  const length = rowIds.length;
  if (length <= 1) {
    return true;
  }

  if (!(tempBuffer instanceof Uint32Array) || tempBuffer.length < length) {
    return false;
  }

  const counts =
    countsBuffer instanceof Uint32Array && countsBuffer.length >= 256
      ? countsBuffer
      : new Uint32Array(256);
  const hasKeyByRowId =
    Array.isArray(keyByRowId) || ArrayBuffer.isView(keyByRowId);
  let keyMaxValue = Number.isFinite(keyMaxValueOverride)
    ? Math.max(0, keyMaxValueOverride >>> 0)
    : 0;
  if (!Number.isFinite(keyMaxValueOverride)) {
    if (hasKeyByRowId) {
      for (let i = 0; i < length; i += 1) {
        const rowId = rowIds[i] >>> 0;
        const key = keyByRowId[rowId] >>> 0;
        if (key > keyMaxValue) {
          keyMaxValue = key;
        }
      }
    } else {
      for (let i = 0; i < length; i += 1) {
        const rowId = rowIds[i] >>> 0;
        if (rowId > keyMaxValue) {
          keyMaxValue = rowId;
        }
      }
    }
  }
  let passCount = 1;
  if (keyMaxValue > 0xff) {
    passCount = 2;
  }
  if (keyMaxValue > 0xffff) {
    passCount = 3;
  }
  if (keyMaxValue > 0xffffff) {
    passCount = 4;
  }
  let source = rowIds;
  let target = tempBuffer.subarray(0, length);

  if (hasKeyByRowId) {
    if (descending) {
      for (let pass = 0; pass < passCount; pass += 1) {
        const shift = pass * 8;
        counts.fill(0);

        for (let i = 0; i < length; i += 1) {
          const rowId = source[i] >>> 0;
          const key = keyByRowId[rowId] >>> 0;
          const digit = (((keyMaxValue - key) >>> 0) >>> shift) & 0xff;
          counts[digit] += 1;
        }

        let prefix = 0;
        for (let d = 0; d < 256; d += 1) {
          const bucketSize = counts[d];
          counts[d] = prefix;
          prefix += bucketSize;
        }

        for (let i = 0; i < length; i += 1) {
          const rowId = source[i] >>> 0;
          const key = keyByRowId[rowId] >>> 0;
          const digit = (((keyMaxValue - key) >>> 0) >>> shift) & 0xff;
          const outIndex = counts[digit];
          target[outIndex] = rowId;
          counts[digit] = outIndex + 1;
        }

        const previousSource = source;
        source = target;
        target = previousSource;
      }
    } else {
      for (let pass = 0; pass < passCount; pass += 1) {
        const shift = pass * 8;
        counts.fill(0);

        for (let i = 0; i < length; i += 1) {
          const rowId = source[i] >>> 0;
          const key = keyByRowId[rowId] >>> 0;
          const digit = (key >>> shift) & 0xff;
          counts[digit] += 1;
        }

        let prefix = 0;
        for (let d = 0; d < 256; d += 1) {
          const bucketSize = counts[d];
          counts[d] = prefix;
          prefix += bucketSize;
        }

        for (let i = 0; i < length; i += 1) {
          const rowId = source[i] >>> 0;
          const key = keyByRowId[rowId] >>> 0;
          const digit = (key >>> shift) & 0xff;
          const outIndex = counts[digit];
          target[outIndex] = rowId;
          counts[digit] = outIndex + 1;
        }

        const previousSource = source;
        source = target;
        target = previousSource;
      }
    }
  } else if (descending) {
    for (let pass = 0; pass < passCount; pass += 1) {
      const shift = pass * 8;
      counts.fill(0);

      for (let i = 0; i < length; i += 1) {
        const rowId = source[i] >>> 0;
        const digit = (((keyMaxValue - rowId) >>> 0) >>> shift) & 0xff;
        counts[digit] += 1;
      }

      let prefix = 0;
      for (let d = 0; d < 256; d += 1) {
        const bucketSize = counts[d];
        counts[d] = prefix;
        prefix += bucketSize;
      }

      for (let i = 0; i < length; i += 1) {
        const rowId = source[i] >>> 0;
        const digit = (((keyMaxValue - rowId) >>> 0) >>> shift) & 0xff;
        const outIndex = counts[digit];
        target[outIndex] = rowId;
        counts[digit] = outIndex + 1;
      }

      const previousSource = source;
      source = target;
      target = previousSource;
    }
  } else {
    for (let pass = 0; pass < passCount; pass += 1) {
      const shift = pass * 8;
      counts.fill(0);

      for (let i = 0; i < length; i += 1) {
        const rowId = source[i] >>> 0;
        const digit = (rowId >>> shift) & 0xff;
        counts[digit] += 1;
      }

      let prefix = 0;
      for (let d = 0; d < 256; d += 1) {
        const bucketSize = counts[d];
        counts[d] = prefix;
        prefix += bucketSize;
      }

      for (let i = 0; i < length; i += 1) {
        const rowId = source[i] >>> 0;
        const digit = (rowId >>> shift) & 0xff;
        const outIndex = counts[digit];
        target[outIndex] = rowId;
        counts[digit] = outIndex + 1;
      }

      const previousSource = source;
      source = target;
      target = previousSource;
    }
  }

  if (source !== rowIds) {
    rowIds.set(source);
  }

  return true;
}

function stableRadixSortRowIdsRangeByUint32Key(
  rowIds,
  start,
  end,
  keyByRowId,
  descending,
  tempBuffer,
  countsBuffer,
  keyMaxValueOverride
) {
  if (!(rowIds instanceof Uint32Array) || !(tempBuffer instanceof Uint32Array)) {
    return false;
  }

  const startIndex = Math.max(0, Number(start) | 0);
  const endIndex = Math.min(rowIds.length, Number(end) | 0);
  const length = endIndex - startIndex;
  if (length <= 1) {
    return true;
  }

  if (tempBuffer.length < rowIds.length) {
    return false;
  }

  const counts =
    countsBuffer instanceof Uint32Array && countsBuffer.length >= 256
      ? countsBuffer
      : new Uint32Array(256);
  const hasKeyByRowId =
    Array.isArray(keyByRowId) || ArrayBuffer.isView(keyByRowId);
  let keyMaxValue = Number.isFinite(keyMaxValueOverride)
    ? Math.max(0, keyMaxValueOverride >>> 0)
    : 0;
  if (!Number.isFinite(keyMaxValueOverride)) {
    if (hasKeyByRowId) {
      for (let i = startIndex; i < endIndex; i += 1) {
        const rowId = rowIds[i] >>> 0;
        const key = keyByRowId[rowId] >>> 0;
        if (key > keyMaxValue) {
          keyMaxValue = key;
        }
      }
    } else {
      for (let i = startIndex; i < endIndex; i += 1) {
        const rowId = rowIds[i] >>> 0;
        if (rowId > keyMaxValue) {
          keyMaxValue = rowId;
        }
      }
    }
  }

  let passCount = 1;
  if (keyMaxValue > 0xff) {
    passCount = 2;
  }
  if (keyMaxValue > 0xffff) {
    passCount = 3;
  }
  if (keyMaxValue > 0xffffff) {
    passCount = 4;
  }

  let source = rowIds;
  let target = tempBuffer;

  if (hasKeyByRowId) {
    if (descending) {
      for (let pass = 0; pass < passCount; pass += 1) {
        const shift = pass * 8;
        counts.fill(0);

        for (let i = startIndex; i < endIndex; i += 1) {
          const rowId = source[i] >>> 0;
          const key = keyByRowId[rowId] >>> 0;
          const digit = (((keyMaxValue - key) >>> 0) >>> shift) & 0xff;
          counts[digit] += 1;
        }

        let prefix = startIndex;
        for (let d = 0; d < 256; d += 1) {
          const bucketSize = counts[d];
          counts[d] = prefix;
          prefix += bucketSize;
        }

        for (let i = startIndex; i < endIndex; i += 1) {
          const rowId = source[i] >>> 0;
          const key = keyByRowId[rowId] >>> 0;
          const digit = (((keyMaxValue - key) >>> 0) >>> shift) & 0xff;
          const outIndex = counts[digit];
          target[outIndex] = rowId;
          counts[digit] = outIndex + 1;
        }

        const previousSource = source;
        source = target;
        target = previousSource;
      }
    } else {
      for (let pass = 0; pass < passCount; pass += 1) {
        const shift = pass * 8;
        counts.fill(0);

        for (let i = startIndex; i < endIndex; i += 1) {
          const rowId = source[i] >>> 0;
          const key = keyByRowId[rowId] >>> 0;
          const digit = (key >>> shift) & 0xff;
          counts[digit] += 1;
        }

        let prefix = startIndex;
        for (let d = 0; d < 256; d += 1) {
          const bucketSize = counts[d];
          counts[d] = prefix;
          prefix += bucketSize;
        }

        for (let i = startIndex; i < endIndex; i += 1) {
          const rowId = source[i] >>> 0;
          const key = keyByRowId[rowId] >>> 0;
          const digit = (key >>> shift) & 0xff;
          const outIndex = counts[digit];
          target[outIndex] = rowId;
          counts[digit] = outIndex + 1;
        }

        const previousSource = source;
        source = target;
        target = previousSource;
      }
    }
  } else if (descending) {
    for (let pass = 0; pass < passCount; pass += 1) {
      const shift = pass * 8;
      counts.fill(0);

      for (let i = startIndex; i < endIndex; i += 1) {
        const rowId = source[i] >>> 0;
        const digit = (((keyMaxValue - rowId) >>> 0) >>> shift) & 0xff;
        counts[digit] += 1;
      }

      let prefix = startIndex;
      for (let d = 0; d < 256; d += 1) {
        const bucketSize = counts[d];
        counts[d] = prefix;
        prefix += bucketSize;
      }

      for (let i = startIndex; i < endIndex; i += 1) {
        const rowId = source[i] >>> 0;
        const digit = (((keyMaxValue - rowId) >>> 0) >>> shift) & 0xff;
        const outIndex = counts[digit];
        target[outIndex] = rowId;
        counts[digit] = outIndex + 1;
      }

      const previousSource = source;
      source = target;
      target = previousSource;
    }
  } else {
    for (let pass = 0; pass < passCount; pass += 1) {
      const shift = pass * 8;
      counts.fill(0);

      for (let i = startIndex; i < endIndex; i += 1) {
        const rowId = source[i] >>> 0;
        const digit = (rowId >>> shift) & 0xff;
        counts[digit] += 1;
      }

      let prefix = startIndex;
      for (let d = 0; d < 256; d += 1) {
        const bucketSize = counts[d];
        counts[d] = prefix;
        prefix += bucketSize;
      }

      for (let i = startIndex; i < endIndex; i += 1) {
        const rowId = source[i] >>> 0;
        const digit = (rowId >>> shift) & 0xff;
        const outIndex = counts[digit];
        target[outIndex] = rowId;
        counts[digit] = outIndex + 1;
      }

      const previousSource = source;
      source = target;
      target = previousSource;
    }
  }

  if (source !== rowIds) {
    rowIds.set(source.subarray(startIndex, endIndex), startIndex);
  }

  return true;
}

function isMonotonicRowIdsForDirection(rowIds, descending) {
  if (!(rowIds instanceof Uint32Array)) {
    return false;
  }

  const length = rowIds.length;
  if (length <= 1) {
    return true;
  }

  if (descending) {
    for (let i = 1; i < length; i += 1) {
      if (rowIds[i - 1] < rowIds[i]) {
        return false;
      }
    }
    return true;
  }

  for (let i = 1; i < length; i += 1) {
    if (rowIds[i - 1] > rowIds[i]) {
      return false;
    }
  }
  return true;
}

function isRowIdRangeOrderedByUint32Key(
  rowIds,
  start,
  end,
  keyByRowId,
  descending
) {
  if (!(rowIds instanceof Uint32Array)) {
    return false;
  }

  const startIndex = Math.max(0, Number(start) | 0);
  const endIndex = Math.min(rowIds.length, Number(end) | 0);
  if (endIndex - startIndex <= 1) {
    return true;
  }

  let previousRowId = rowIds[startIndex] >>> 0;
  let previousKey = keyByRowId
    ? keyByRowId[previousRowId] >>> 0
    : previousRowId;
  for (let i = startIndex + 1; i < endIndex; i += 1) {
    const rowId = rowIds[i] >>> 0;
    const nextKey = keyByRowId ? keyByRowId[rowId] >>> 0 : rowId;
    if (descending ? previousKey < nextKey : previousKey > nextKey) {
      return false;
    }
    previousKey = nextKey;
  }

  return true;
}

function collectEqualRankSubgroups(
  rowIds,
  start,
  end,
  rankByRowId,
  outStarts,
  outEnds
) {
  if (
    !(rowIds instanceof Uint32Array) ||
    !isSupportedRankArray(rankByRowId) ||
    !Array.isArray(outStarts) ||
    !Array.isArray(outEnds)
  ) {
    return;
  }

  const startIndex = Math.max(0, Number(start) | 0);
  const endIndex = Math.min(rowIds.length, Number(end) | 0);
  if (endIndex - startIndex <= 1) {
    return;
  }

  let segmentStart = startIndex;
  let segmentRank = rankByRowId[rowIds[startIndex] >>> 0] >>> 0;
  for (let i = startIndex + 1; i <= endIndex; i += 1) {
    const atEnd = i === endIndex;
    const nextRank = atEnd ? -1 : rankByRowId[rowIds[i] >>> 0] >>> 0;
    if (atEnd || nextRank !== segmentRank) {
      if (i - segmentStart > 1) {
        outStarts.push(segmentStart);
        outEnds.push(i);
      }
      segmentStart = i;
      segmentRank = nextRank;
    }
  }
}

function getCachedPrecomputedGroupedPrefixState(descriptorList, rowCount) {
  const totalRows = Math.max(0, Number(rowCount) || 0);
  const cache = precomputedGroupedSortCache;
  if (
    !cache ||
    cache.rowCount !== totalRows ||
    !(cache.sortedOrder instanceof Uint32Array) ||
    cache.sortedOrder.length !== totalRows
  ) {
    return null;
  }

  const cachedDescriptors = Array.isArray(cache.descriptors) ? cache.descriptors : [];
  const matchLength = getMatchingSortDescriptorPrefixLength(
    descriptorList,
    cachedDescriptors
  );
  if (matchLength <= 0) {
    return null;
  }

  let groupStarts = null;
  let groupEnds = null;
  if (matchLength < descriptorList.length) {
    const startsByLength =
      cache.prefixStartsByLength &&
      typeof cache.prefixStartsByLength === "object" &&
      !Array.isArray(cache.prefixStartsByLength)
        ? cache.prefixStartsByLength
        : null;
    const endsByLength =
      cache.prefixEndsByLength &&
      typeof cache.prefixEndsByLength === "object" &&
      !Array.isArray(cache.prefixEndsByLength)
        ? cache.prefixEndsByLength
        : null;
    const starts = startsByLength ? startsByLength[matchLength] : null;
    const ends = endsByLength ? endsByLength[matchLength] : null;
    if (!Array.isArray(starts) || !Array.isArray(ends) || starts.length !== ends.length) {
      return null;
    }
    groupStarts = starts.slice();
    groupEnds = ends.slice();
  }

  return {
    sortedOrder: cache.sortedOrder,
    matchLength,
    groupStarts,
    groupEnds,
  };
}

function hasReusablePrecomputedGroupedPrefix(descriptorList, rowCount) {
  const cachedState = getCachedPrecomputedGroupedPrefixState(
    descriptorList,
    rowCount
  );
  return !!(cachedState && cachedState.matchLength > 0);
}

function updatePrecomputedGroupedSortCache(
  rowCount,
  descriptorList,
  sortedIndices,
  prefixStartsByLength,
  prefixEndsByLength
) {
  const totalRows = Math.max(0, Number(rowCount) || 0);
  if (!(sortedIndices instanceof Uint32Array) || sortedIndices.length !== totalRows) {
    clearPrecomputedGroupedSortCache();
    return;
  }

  const normalizedDescriptors = normalizeSortDescriptorList(descriptorList);

  const startsByLength = Object.create(null);
  const endsByLength = Object.create(null);
  for (let len = 1; len <= normalizedDescriptors.length; len += 1) {
    const starts = prefixStartsByLength ? prefixStartsByLength[len] : null;
    const ends = prefixEndsByLength ? prefixEndsByLength[len] : null;
    if (!Array.isArray(starts) || !Array.isArray(ends) || starts.length !== ends.length) {
      continue;
    }

    startsByLength[len] = starts.slice();
    endsByLength[len] = ends.slice();
  }

  precomputedGroupedSortCache.rowCount = totalRows;
  precomputedGroupedSortCache.descriptors = normalizedDescriptors;
  precomputedGroupedSortCache.sortedOrder = sortedIndices;
  precomputedGroupedSortCache.prefixStartsByLength = startsByLength;
  precomputedGroupedSortCache.prefixEndsByLength = endsByLength;
}

function updatePrecomputedSubsetSortCache(
  rowCount,
  descriptorList,
  sortedIndices,
  sortedCount
) {
  const totalRows = Math.max(0, Number(rowCount) || 0);
  const count = Math.max(0, Number(sortedCount) || 0);
  if (!hasIndexCollection(sortedIndices) || count > sortedIndices.length) {
    precomputedSubsetSortCache.rowCount = 0;
    precomputedSubsetSortCache.descriptors = [];
    precomputedSubsetSortCache.sortedOrder = null;
    precomputedSubsetSortCache.sortedCount = 0;
    return;
  }

  precomputedSubsetSortCache.rowCount = totalRows;
  precomputedSubsetSortCache.descriptors = normalizeSortDescriptorList(descriptorList);
  precomputedSubsetSortCache.sortedOrder = sortedIndices;
  precomputedSubsetSortCache.sortedCount = count;
}

function tryReusePrecomputedSubsetSort(
  filteredIndices,
  filteredCount,
  descriptorList,
  rowCount
) {
  const totalRows = Math.max(0, Number(rowCount) || 0);
  const count = getResolvedFilteredCount(filteredIndices, filteredCount, totalRows);
  if (count <= 0) {
    return {
      sortResult: {
        changedOrder: false,
        durationMs: 0,
        sortMode: "precomputed",
        dataPath: "indices+precomputed-subset-reuse",
        descriptors: descriptorList,
        effectiveDescriptors: descriptorList,
        restoredDefault: false,
      },
      sortedIndices: new Uint32Array(0),
      rankBuildMs: 0,
    };
  }

  const cachedRowCount = Math.max(0, Number(precomputedSubsetSortCache.rowCount) || 0);
  if (cachedRowCount !== totalRows) {
    return null;
  }

  if (
    !areSortDescriptorListsEqual(
      descriptorList,
      precomputedSubsetSortCache.descriptors
    )
  ) {
    return null;
  }

  const cachedSortedOrder = precomputedSubsetSortCache.sortedOrder;
  const cachedSortedCount = Math.max(
    0,
    Number(precomputedSubsetSortCache.sortedCount) || 0
  );
  if (
    !hasIndexCollection(cachedSortedOrder) ||
    cachedSortedCount <= 0 ||
    cachedSortedCount > cachedSortedOrder.length
  ) {
    return null;
  }
  if (count > cachedSortedCount) {
    return null;
  }

  const startMs = performance.now();
  const scratch = ensurePrecomputedSortScratch(totalRows);
  const marks = scratch.marks;
  const sourceSubset = materializeFilteredIndicesIntoUint32(
    filteredIndices,
    count,
    totalRows,
    scratch.work
  );
  if (!(sourceSubset instanceof Uint32Array)) {
    return null;
  }

  const cachedEpoch = nextPrecomputedSortEpoch(scratch);
  for (let i = 0; i < cachedSortedCount; i += 1) {
    const rowId = cachedSortedOrder[i] >>> 0;
    if (rowId >= totalRows) {
      return null;
    }
    marks[rowId] = cachedEpoch;
  }

  for (let i = 0; i < sourceSubset.length; i += 1) {
    const rowId = sourceSubset[i] >>> 0;
    if (rowId >= totalRows || marks[rowId] !== cachedEpoch) {
      return null;
    }
  }

  const keepEpoch = nextPrecomputedSortEpoch(scratch);
  for (let i = 0; i < sourceSubset.length; i += 1) {
    marks[sourceSubset[i] >>> 0] = keepEpoch;
  }

  const output = scratch.output;
  let writeIndex = 0;
  for (let i = 0; i < cachedSortedCount; i += 1) {
    const rowId = cachedSortedOrder[i] >>> 0;
    if (marks[rowId] === keepEpoch) {
      output[writeIndex] = rowId;
      writeIndex += 1;
      if (writeIndex === sourceSubset.length) {
        break;
      }
    }
  }

  if (writeIndex !== sourceSubset.length) {
    return null;
  }

  return {
    sortResult: {
      changedOrder: sourceSubset.length > 1,
      durationMs: performance.now() - startMs,
      sortMode: "precomputed",
      dataPath: "indices+precomputed-subset-reuse",
      descriptors: descriptorList,
      effectiveDescriptors: descriptorList,
      restoredDefault: false,
    },
    sortedIndices: output.subarray(0, sourceSubset.length),
    rankBuildMs: 0,
  };
}

function buildSortedIndicesFromPrecomputedRanks(
  filteredIndices,
  filteredCount,
  rankByRowId,
  rowCount,
  rankMaxHint
) {
  if (!isSupportedRankArray(rankByRowId)) {
    return null;
  }

  const totalRows = Math.max(0, Number(rowCount) || 0);
  const count = getResolvedFilteredCount(filteredIndices, filteredCount, totalRows);
  if (count <= 0) {
    return new Uint32Array(0);
  }

  const scratch = ensurePrecomputedSortScratch(totalRows);
  const subset = materializeFilteredIndicesIntoUint32(
    filteredIndices,
    count,
    totalRows,
    scratch.work
  );
  if (!(subset instanceof Uint32Array)) {
    return null;
  }

  const tempBuffer = scratch.output;
  const counts = scratch.counts;
  if (!isMonotonicRowIdsForDirection(subset, false)) {
    const sortedByRowId = stableRadixSortRowIdsByUint32Key(
      subset,
      null,
      false,
      tempBuffer,
      counts,
      Math.max(0, totalRows - 1)
    );
    if (!sortedByRowId) {
      return null;
    }
  }

  let rankMaxValue = Number.isFinite(rankMaxHint)
    ? Math.max(0, Number(rankMaxHint)) >>> 0
    : 0;
  if (!Number.isFinite(rankMaxHint)) {
    for (let i = 0; i < subset.length; i += 1) {
      const rank = rankByRowId[subset[i]] >>> 0;
      if (rank > rankMaxValue) {
        rankMaxValue = rank;
      }
    }
  }
  const descending = false;
  if (
    subset.length <= PRECOMPUTED_ORDER_CHECK_MAX_GROUP_SIZE &&
    isRowIdRangeOrderedByUint32Key(
      subset,
      0,
      subset.length,
      rankByRowId,
      descending
    )
  ) {
    return subset;
  }
  const sortedByRank = stableRadixSortRowIdsByUint32Key(
    subset,
    rankByRowId,
    descending,
    tempBuffer,
    counts,
    rankMaxValue
  );
  if (!sortedByRank) {
    return null;
  }

  return subset;
}

function buildSortedIndicesViaPrecomputedRankTupleMode(
  filteredIndices,
  filteredCount,
  descriptorList,
  rowCount
) {
  const totalRows = Math.max(0, Number(rowCount) || 0);
  const count = getResolvedFilteredCount(filteredIndices, filteredCount, totalRows);
  if (count <= 0) {
    return {
      sortResult: {
        changedOrder: false,
        durationMs: 0,
        sortMode: "precomputed",
        dataPath: "indices+precomputed-ranktuple",
        descriptors: descriptorList,
        effectiveDescriptors: descriptorList,
        restoredDefault: false,
      },
      sortedIndices: new Uint32Array(0),
      rankBuildMs: 0,
    };
  }

  const rankColumns = new Array(descriptorList.length);
  const rankMaxByColumn = new Uint32Array(descriptorList.length);
  let rankBuildMs = 0;
  for (let i = 0; i < descriptorList.length; i += 1) {
    const descriptor = descriptorList[i];
    const rankBuild = buildPrecomputedRankForColumn(
      descriptor.columnKey,
      totalRows,
      descriptor.direction
    );
    if (
      !rankBuild ||
      !isSupportedRankArray(rankBuild.rankByRowId) ||
      rankBuild.rankByRowId.length !== totalRows
    ) {
      return null;
    }

    rankColumns[i] = rankBuild.rankByRowId;
    rankMaxByColumn[i] = Number(rankBuild.maxRank) >>> 0;
    rankBuildMs += Number(rankBuild.durationMs) || 0;
  }

  const scratch = ensurePrecomputedSortScratch(totalRows);
  const subset = materializeFilteredIndicesIntoUint32(
    filteredIndices,
    count,
    totalRows,
    scratch.work
  );
  if (!(subset instanceof Uint32Array)) {
    return null;
  }

  const sortStartMs = performance.now();
  const tempBuffer = scratch.output;
  const counts = scratch.counts;
  if (!isMonotonicRowIdsForDirection(subset, false)) {
    const sortedByRowId = stableRadixSortRowIdsByUint32Key(
      subset,
      null,
      false,
      tempBuffer,
      counts,
      Math.max(0, totalRows - 1)
    );
    if (!sortedByRowId) {
      return null;
    }
  }

  for (let i = descriptorList.length - 1; i >= 0; i -= 1) {
    const rankByRowId = rankColumns[i];
    if (
      subset.length <= PRECOMPUTED_ORDER_CHECK_MAX_GROUP_SIZE &&
      isRowIdRangeOrderedByUint32Key(
        subset,
        0,
        subset.length,
        rankByRowId,
        false
      )
    ) {
      continue;
    }
    const sortedPass = stableRadixSortRowIdsByUint32Key(
      subset,
      rankByRowId,
      false,
      tempBuffer,
      counts,
      rankMaxByColumn[i]
    );
    if (!sortedPass) {
      return null;
    }
  }

  const sortCoreMs = performance.now() - sortStartMs;

  return {
    sortResult: {
      changedOrder: subset.length > 1,
      durationMs: sortCoreMs,
      sortMode: "precomputed",
      dataPath: "indices+precomputed-ranktuple",
      descriptors: descriptorList,
      effectiveDescriptors: descriptorList,
      restoredDefault: false,
    },
    sortedIndices: subset,
    rankBuildMs,
  };
}

function buildSortedIndicesViaPrecomputedGroupedFullMode(
  descriptorList,
  rowCount
) {
  const totalRows = Math.max(0, Number(rowCount) || 0);
  if (totalRows <= 0) {
    return {
      sortResult: {
        changedOrder: false,
        durationMs: 0,
        sortMode: "precomputed",
        dataPath: "indices+precomputed-groups",
        descriptors: descriptorList,
        effectiveDescriptors: descriptorList,
        restoredDefault: false,
      },
      sortedIndices: new Uint32Array(0),
      rankBuildMs: 0,
    };
  }

  if (!Array.isArray(descriptorList) || descriptorList.length <= 1) {
    return null;
  }

  const rankColumns = new Array(descriptorList.length);
  const rankMaxByColumn = new Uint32Array(descriptorList.length);
  let rankBuildMs = 0;
  for (let i = 0; i < descriptorList.length; i += 1) {
    const descriptor = descriptorList[i];
    const rankBuild = buildPrecomputedRankForColumn(
      descriptor.columnKey,
      totalRows,
      descriptor.direction
    );
    if (
      !rankBuild ||
      !isSupportedRankArray(rankBuild.rankByRowId) ||
      rankBuild.rankByRowId.length !== totalRows
    ) {
      return null;
    }

    rankColumns[i] = rankBuild.rankByRowId;
    rankMaxByColumn[i] = Number(rankBuild.maxRank) >>> 0;
    rankBuildMs += Number(rankBuild.durationMs) || 0;
  }

  const cachedPrefixState = getCachedPrecomputedGroupedPrefixState(
    descriptorList,
    totalRows
  );
  if (
    cachedPrefixState &&
    cachedPrefixState.matchLength === descriptorList.length
  ) {
    return {
      sortResult: {
        changedOrder: totalRows > 1,
        durationMs: 0,
        sortMode: "precomputed",
        dataPath: "indices+precomputed-groups-cache-hit",
        descriptors: descriptorList,
        effectiveDescriptors: descriptorList,
        restoredDefault: false,
      },
      sortedIndices: cachedPrefixState.sortedOrder,
      rankBuildMs,
    };
  }

  const sortStartMs = performance.now();
  const groupedScratch = ensurePrecomputedGroupedSortScratch(totalRows);
  let output = groupedScratch.primary;
  let tempBuffer = groupedScratch.secondary;
  const counts = groupedScratch.counts;
  const prefixStartsByLength = new Array(descriptorList.length + 1);
  const prefixEndsByLength = new Array(descriptorList.length + 1);
  let dataPath = "indices+precomputed-groups";
  let activeGroupStarts = [];
  let activeGroupEnds = [];
  let startDescriptorIndex = 1;

  if (
    cachedPrefixState &&
    cachedPrefixState.matchLength >= 1 &&
    cachedPrefixState.matchLength < descriptorList.length
  ) {
    if (cachedPrefixState.sortedOrder === tempBuffer) {
      const swap = output;
      output = tempBuffer;
      tempBuffer = swap;
    } else if (cachedPrefixState.sortedOrder !== output) {
      output.set(cachedPrefixState.sortedOrder);
    }
    activeGroupStarts = Array.isArray(cachedPrefixState.groupStarts)
      ? cachedPrefixState.groupStarts.slice()
      : [];
    activeGroupEnds = Array.isArray(cachedPrefixState.groupEnds)
      ? cachedPrefixState.groupEnds.slice()
      : [];
    startDescriptorIndex = cachedPrefixState.matchLength;
    dataPath = "indices+precomputed-groups-incremental";

    const cachedStartsByLength =
      precomputedGroupedSortCache.prefixStartsByLength &&
      typeof precomputedGroupedSortCache.prefixStartsByLength === "object"
        ? precomputedGroupedSortCache.prefixStartsByLength
        : Object.create(null);
    const cachedEndsByLength =
      precomputedGroupedSortCache.prefixEndsByLength &&
      typeof precomputedGroupedSortCache.prefixEndsByLength === "object"
        ? precomputedGroupedSortCache.prefixEndsByLength
        : Object.create(null);
    for (let len = 1; len <= cachedPrefixState.matchLength; len += 1) {
      const cachedStarts = cachedStartsByLength[len];
      const cachedEnds = cachedEndsByLength[len];
      if (
        Array.isArray(cachedStarts) &&
        Array.isArray(cachedEnds) &&
        cachedStarts.length === cachedEnds.length
      ) {
        prefixStartsByLength[len] = cachedStarts.slice();
        prefixEndsByLength[len] = cachedEnds.slice();
      }
    }

    const descriptorCount = descriptorList.length;
    const lastDescriptorIndex = descriptorCount - 1;
    const cachedDescriptorList = Array.isArray(precomputedGroupedSortCache.descriptors)
      ? precomputedGroupedSortCache.descriptors
      : [];
    const canUseTailDirectionFlip =
      lastDescriptorIndex >= 1 &&
      cachedPrefixState.matchLength === lastDescriptorIndex &&
      cachedDescriptorList.length === descriptorCount &&
      cachedDescriptorList[lastDescriptorIndex] &&
      descriptorList[lastDescriptorIndex] &&
      cachedDescriptorList[lastDescriptorIndex].columnKey ===
        descriptorList[lastDescriptorIndex].columnKey &&
      normalizeSortDirection(cachedDescriptorList[lastDescriptorIndex].direction) !==
        normalizeSortDirection(descriptorList[lastDescriptorIndex].direction);
    if (canUseTailDirectionFlip) {
      const previousTailDescriptor = cachedDescriptorList[lastDescriptorIndex];
      const previousTailRankBuild = buildPrecomputedRankForColumn(
        previousTailDescriptor.columnKey,
        totalRows,
        previousTailDescriptor.direction
      );
      const currentTailRankByRowId = rankColumns[lastDescriptorIndex];
      if (
        previousTailRankBuild &&
        isSupportedRankArray(previousTailRankBuild.rankByRowId) &&
        isSupportedRankArray(currentTailRankByRowId)
      ) {
        const previousTailRankByRowId = previousTailRankBuild.rankByRowId;
        const nextGroupStarts = [];
        const nextGroupEnds = [];
        for (let g = 0; g < activeGroupStarts.length; g += 1) {
          const start = activeGroupStarts[g];
          const end = activeGroupEnds[g];
          if (end - start <= 1) {
            continue;
          }

          const runStarts = [];
          const runEnds = [];
          let runStart = start;
          let runRank = previousTailRankByRowId[output[start] >>> 0] >>> 0;
          for (let i = start + 1; i <= end; i += 1) {
            const atEnd = i === end;
            const nextRank = atEnd
              ? -1
              : previousTailRankByRowId[output[i] >>> 0] >>> 0;
            if (atEnd || nextRank !== runRank) {
              runStarts.push(runStart);
              runEnds.push(i);
              runStart = i;
              runRank = nextRank;
            }
          }

          let writeOffset = start;
          for (let r = runStarts.length - 1; r >= 0; r -= 1) {
            const runSegmentStart = runStarts[r];
            const runSegmentEnd = runEnds[r];
            tempBuffer.set(
              output.subarray(runSegmentStart, runSegmentEnd),
              writeOffset
            );
            writeOffset += runSegmentEnd - runSegmentStart;
          }
          output.set(tempBuffer.subarray(start, end), start);

          collectEqualRankSubgroups(
            output,
            start,
            end,
            currentTailRankByRowId,
            nextGroupStarts,
            nextGroupEnds
          );
        }

        activeGroupStarts = nextGroupStarts;
        activeGroupEnds = nextGroupEnds;
        prefixStartsByLength[lastDescriptorIndex + 1] = nextGroupStarts.slice();
        prefixEndsByLength[lastDescriptorIndex + 1] = nextGroupEnds.slice();
        startDescriptorIndex = descriptorCount;
        dataPath = "indices+precomputed-groups-tailflip";
      }
    }
  } else {
    const primarySortedIndices = getPrecomputedSortedIndexForColumn(
      descriptorList[0].columnKey,
      totalRows
    );
    if (!(primarySortedIndices instanceof Uint32Array)) {
      return null;
    }

    const primaryRank = rankColumns[0];
    const primaryDirectionDesc = descriptorList[0].direction === "desc";
    const ascGroupStarts = [];
    const ascGroupEnds = [];
    let groupStart = 0;
    let previousRank = primaryRank[primarySortedIndices[0] >>> 0] >>> 0;
    for (let i = 1; i <= totalRows; i += 1) {
      const atEnd = i === totalRows;
      const nextRank = atEnd
        ? -1
        : primaryRank[primarySortedIndices[i] >>> 0] >>> 0;
      if (atEnd || nextRank !== previousRank) {
        ascGroupStarts.push(groupStart);
        ascGroupEnds.push(i);
        if (ascGroupStarts.length > PRECOMPUTED_FULL_GROUP_MAX_GROUPS) {
          return null;
        }
        groupStart = i;
        previousRank = nextRank;
      }
    }

    if (!primaryDirectionDesc) {
      output.set(primarySortedIndices);
      for (let i = 0; i < ascGroupStarts.length; i += 1) {
        const start = ascGroupStarts[i];
        const end = ascGroupEnds[i];
        if (end - start > 1) {
          activeGroupStarts.push(start);
          activeGroupEnds.push(end);
        }
      }
    } else {
      let writeOffset = 0;
      for (let i = ascGroupStarts.length - 1; i >= 0; i -= 1) {
        const start = ascGroupStarts[i];
        const end = ascGroupEnds[i];
        const length = end - start;
        output.set(primarySortedIndices.subarray(start, end), writeOffset);
        if (length > 1) {
          activeGroupStarts.push(writeOffset);
          activeGroupEnds.push(writeOffset + length);
        }
        writeOffset += length;
      }
    }

    prefixStartsByLength[1] = activeGroupStarts.slice();
    prefixEndsByLength[1] = activeGroupEnds.slice();
  }

  if (startDescriptorIndex < 1) {
    startDescriptorIndex = 1;
  }

  for (let d = startDescriptorIndex; d < descriptorList.length; d += 1) {
    const nextGroupStarts = [];
    const nextGroupEnds = [];
    if (activeGroupStarts.length > 0) {
      const rankByRowId = rankColumns[d];

      for (let g = 0; g < activeGroupStarts.length; g += 1) {
        const start = activeGroupStarts[g];
        const end = activeGroupEnds[g];
        const groupLength = end - start;
        if (groupLength <= 1) {
          continue;
        }

        const shouldCheckOrder =
          dataPath === "indices+precomputed-groups-incremental" ||
          groupLength <= PRECOMPUTED_ORDER_CHECK_MAX_GROUP_SIZE;
        const alreadyOrdered = shouldCheckOrder
          ? isRowIdRangeOrderedByUint32Key(
              output,
              start,
              end,
              rankByRowId,
              false
            )
          : false;
        if (!alreadyOrdered) {
          const sorted = stableRadixSortRowIdsRangeByUint32Key(
            output,
            start,
            end,
            rankByRowId,
            false,
            tempBuffer,
            counts,
            rankMaxByColumn[d]
          );
          if (!sorted) {
            return null;
          }
        }

        collectEqualRankSubgroups(
          output,
          start,
          end,
          rankByRowId,
          nextGroupStarts,
          nextGroupEnds
        );
      }
    }

    if (nextGroupStarts.length > PRECOMPUTED_FULL_GROUP_MAX_GROUPS) {
      return null;
    }

    activeGroupStarts = nextGroupStarts;
    activeGroupEnds = nextGroupEnds;
    prefixStartsByLength[d + 1] = nextGroupStarts.slice();
    prefixEndsByLength[d + 1] = nextGroupEnds.slice();
  }

  const durationMs = performance.now() - sortStartMs;
  updatePrecomputedGroupedSortCache(
    totalRows,
    descriptorList,
    output,
    prefixStartsByLength,
    prefixEndsByLength
  );

  return {
    sortResult: {
      changedOrder: output.length > 1,
      durationMs,
      sortMode: "precomputed",
      dataPath,
      descriptors: descriptorList,
      effectiveDescriptors: descriptorList,
      restoredDefault: false,
    },
    sortedIndices: output,
    rankBuildMs,
  };
}

function buildSortedIndicesViaPrecomputedMode(
  filteredIndices,
  filteredCount,
  activeDescriptors,
  rowCount
) {
  const descriptorList = normalizeSortDescriptorList(activeDescriptors);
  if (descriptorList.length === 0) {
    return null;
  }

  if (descriptorList.length > 1) {
    const totalRows = Math.max(0, Number(rowCount) || 0);
    const count = getResolvedFilteredCount(filteredIndices, filteredCount, totalRows);
    const fullSelection = isFullSelectionForPrecomputedSort(
      filteredIndices,
      count,
      totalRows
    );
    if (!fullSelection) {
      const subsetReuseRun = tryReusePrecomputedSubsetSort(
        filteredIndices,
        count,
        descriptorList,
        totalRows
      );
      if (subsetReuseRun) {
        updatePrecomputedSubsetSortCache(
          totalRows,
          descriptorList,
          subsetReuseRun.sortedIndices,
          count
        );
        return subsetReuseRun;
      }
    }
    const canReuseGroupedPrefix = fullSelection
      ? hasReusablePrecomputedGroupedPrefix(descriptorList, totalRows)
      : false;
    if (
      fullSelection &&
      (descriptorList.length >= PRECOMPUTED_GROUP_MIN_DESCRIPTOR_COUNT ||
        canReuseGroupedPrefix)
    ) {
      const groupedRun = buildSortedIndicesViaPrecomputedGroupedFullMode(
        descriptorList,
        totalRows
      );
      if (groupedRun) {
        updatePrecomputedSubsetSortCache(
          totalRows,
          descriptorList,
          groupedRun.sortedIndices,
          totalRows
        );
        return groupedRun;
      }
    }

    const rankTupleRun = buildSortedIndicesViaPrecomputedRankTupleMode(
      filteredIndices,
      filteredCount,
      descriptorList,
      rowCount
    );
    if (rankTupleRun && hasIndexCollection(rankTupleRun.sortedIndices)) {
      updatePrecomputedSubsetSortCache(
        totalRows,
        descriptorList,
        rankTupleRun.sortedIndices,
        count
      );
    }
    return rankTupleRun;
  }

  const descriptor = descriptorList[0];
  const sortedColumnIndices = getPrecomputedSortedIndexForColumn(
    descriptor.columnKey,
    rowCount
  );
  if (!(sortedColumnIndices instanceof Uint32Array)) {
    return null;
  }

  const totalRows = Math.max(0, Number(rowCount) || 0);
  const count = getResolvedFilteredCount(filteredIndices, filteredCount, totalRows);
  const fullSelection = isFullSelectionForPrecomputedSort(
    filteredIndices,
    count,
    totalRows
  );

  let sortedIndices = null;
  let dataPath = "indices+precomputed-scan";
  let rankBuildMs = 0;
  let sortCoreMs = 0;

  if (fullSelection) {
    const coreStartMs = performance.now();
    sortedIndices =
      descriptor.direction === "desc"
        ? buildPrecomputedDescAllIndices(sortedColumnIndices, totalRows)
        : sortedColumnIndices;
    sortCoreMs = performance.now() - coreStartMs;
    dataPath = "indices+precomputed-full";
  } else {
    const useRankSort = shouldUseRankSortForPrecomputedSubset(count, totalRows);
    if (useRankSort) {
      const rankBuild = buildPrecomputedRankForColumn(
        descriptor.columnKey,
        totalRows,
        descriptor.direction
      );
      if (rankBuild) {
        rankBuildMs = Number(rankBuild.durationMs) || 0;
      }
      const coreStartMs = performance.now();
      sortedIndices = buildSortedIndicesFromPrecomputedRanks(
        filteredIndices,
        count,
        rankBuild ? rankBuild.rankByRowId : null,
        totalRows,
        rankBuild ? rankBuild.maxRank : undefined
      );
      sortCoreMs = performance.now() - coreStartMs;
      if (sortedIndices) {
        dataPath = "indices+precomputed-rank";
      }
    }

    if (!sortedIndices) {
      const coreStartMs = performance.now();
      sortedIndices = buildSortedIndicesFromPrecomputedScan(
        filteredIndices,
        count,
        sortedColumnIndices,
        descriptor.direction,
        totalRows
      );
      sortCoreMs = performance.now() - coreStartMs;
      dataPath = "indices+precomputed-scan";
    }
  }

  if (!hasIndexCollection(sortedIndices)) {
    return null;
  }

  return {
    sortResult: {
      changedOrder: sortedIndices.length > 1,
      durationMs: sortCoreMs,
      sortMode: "precomputed",
      dataPath,
      descriptors: descriptorList,
      effectiveDescriptors: descriptorList,
      restoredDefault: false,
    },
    sortedIndices,
    rankBuildMs,
  };
}

function buildSortedIndicesViaRowSort(
  baseIndices,
  sourceRows,
  sortOptions,
  sortModeOverride
) {
  const rowsToSort = new Array(baseIndices.length);
  const rowIndexByRef = new Map();

  for (let i = 0; i < baseIndices.length; i += 1) {
    const rowIndex = baseIndices[i];
    const row = sourceRows[rowIndex];
    rowsToSort[i] = row;
    rowIndexByRef.set(row, rowIndex);
  }

  const sortResult = sortController.sortRows(
    rowsToSort,
    resolveSortModeForRun(sortModeOverride),
    sortOptions
  );
  const sortedIndices = new Array(rowsToSort.length);
  for (let i = 0; i < rowsToSort.length; i += 1) {
    const mappedIndex = rowIndexByRef.get(rowsToSort[i]);
    sortedIndices[i] = typeof mappedIndex === "number" ? mappedIndex : baseIndices[i];
  }

  return {
    sortResult,
    sortedIndices,
  };
}

function buildSortedIndicesViaIndexSort(
  baseIndices,
  sourceRows,
  sortOptions,
  activeDescriptors,
  sortModeOverride
) {
  const sortedIndices = baseIndices.slice();
  const descriptorList = normalizeSortDescriptorList(activeDescriptors);
  const sourceRowCount =
    sourceRows && typeof sourceRows.length === "number" ? sourceRows.length : 0;
  const precomputedRankColumns = buildPrecomputedSortRankColumns(
    descriptorList,
    sourceRowCount
  );
  const runSortOptions = Object.assign({}, sortOptions || {});
  if (Array.isArray(precomputedRankColumns)) {
    runSortOptions.precomputedRankColumns = precomputedRankColumns;
  } else {
    runSortOptions.precomputedIndexKeys = buildPrecomputedSortKeyColumns(
      sortedIndices,
      sourceRows,
      descriptorList
    );
  }
  const sortResult = sortController.sortIndices(
    sortedIndices,
    sourceRows,
    resolveSortModeForRun(sortModeOverride),
    runSortOptions
  );

  return {
    sortResult,
    sortedIndices,
  };
}

function buildSortedIndicesForCurrentResult(filterResult, options) {
  if (!sortController) {
    return null;
  }

  const sortBuildOptions = options || {};
  const activeDescriptors = sortController.getSortDescriptors();
  if (!Array.isArray(activeDescriptors) || activeDescriptors.length === 0) {
    return null;
  }

  const selectedSortMode = getSortMode();
  const shouldPreferPrecomputedFastPath =
    sortBuildOptions.preferPrecomputedFastPath === true &&
    selectedSortMode !== "precomputed";
  const sortOptions = getSortRuntimeOptions();
  const sortTotalStartMs = performance.now();
  const loadedRowCount = getLoadedRowCount();

  let sourceRows = null;
  let sortedRun = null;

  if (selectedSortMode === "precomputed" || shouldPreferPrecomputedFastPath) {
    sortedRun = buildSortedIndicesViaPrecomputedMode(
      filterResult.filteredIndices,
      filterResult.filteredCount,
      activeDescriptors,
      loadedRowCount
    );
  }

  if (!sortedRun) {
    sourceRows = ensureObjectRowsAvailable();
    if (!Array.isArray(sourceRows) || sourceRows.length === 0) {
      return null;
    }

    const baseIndices = materializeFilteredIndexArray(
      filterResult.filteredIndices,
      sourceRows.length
    );
    const effectiveSortMode =
      selectedSortMode === "precomputed" ? "native" : selectedSortMode;
    sortedRun = sortOptions.useIndexSort
      ? buildSortedIndicesViaIndexSort(
          baseIndices,
          sourceRows,
          sortOptions,
          activeDescriptors,
          effectiveSortMode
        )
      : buildSortedIndicesViaRowSort(
          baseIndices,
          sourceRows,
          sortOptions,
          effectiveSortMode
        );
  }

  if (!sortedRun || !sortedRun.sortResult) {
    return null;
  }

  const sortTotalMs = performance.now() - sortTotalStartMs;
  const sortCoreMs = Number(sortedRun.sortResult.durationMs) || 0;
  const sortPrepMs = sortTotalMs - sortCoreMs;

  return {
    indices: sortedRun.sortedIndices,
    sortedCount: sortedRun.sortedIndices.length,
    result: sortedRun.sortResult,
    sortTotalMs,
    sortPrepMs,
    rankBuildMs: Number(sortedRun.rankBuildMs) || 0,
  };
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

function buildDictionaryKeySearchPlan(rawFilters, filterOptions) {
  const useDictionaryKeySearch =
    filterOptions && filterOptions.useDictionaryKeySearch === true;

  if (
    !useDictionaryKeySearch ||
    typeof buildDictionaryKeySearchPrefilter !== "function"
  ) {
    return null;
  }

  const numericColumnarData = getNumericColumnarDataForDictionaryKeySearch();
  if (!isValidNumericColumnarData(numericColumnarData)) {
    return null;
  }

  return buildDictionaryKeySearchPrefilter(rawFilters || {}, numericColumnarData, {
    useDictionaryKeySearch: true,
    useDictionaryIntersection:
      filterOptions && filterOptions.useDictionaryIntersection === true,
    keyToIndex: columnIndexByKey,
  });
}

function applyFiltersObjectRowPath(rawFilters, filterOptions, baseIndices) {
  const controllerOptions = Object.assign({}, filterOptions, {
    useFilterCache: false,
  });
  if (baseIndices !== undefined) {
    controllerOptions.baseIndices = baseIndices;
  }
  const rows = ensureObjectRowsAvailable();
  const filteredIndices = objectRowFilterController.apply(rawFilters, controllerOptions);

  window.fastTableFilteredRows = null;
  window.fastTableFilteredRowIndices = filteredIndices;
  window.fastTableLastFilterMode = "object-row";

  return {
    filteredCount: objectRowFilterController.getCurrentCount(),
    filteredIndices,
    rows,
  };
}

function applyFiltersObjectColumnarPath(rawFilters, filterOptions, baseIndices) {
  const controllerOptions = Object.assign({}, filterOptions, {
    useFilterCache: false,
  });
  if (baseIndices !== undefined) {
    controllerOptions.baseIndices = baseIndices;
  }
  const filteredIndices = objectColumnarFilterController.apply(
    rawFilters,
    controllerOptions
  );
  const columnarData = objectColumnarFilterController.getData();

  window.fastTableFilteredRows = null;
  window.fastTableFilteredRowIndices = filteredIndices;
  window.fastTableLastFilterMode = "object-columnar";

  return {
    filteredCount: objectColumnarFilterController.getCurrentCount(),
    filteredIndices,
    columnarData,
  };
}

function applyFiltersNumericRowPath(rawFilters, filterOptions, baseIndices) {
  const controllerOptions = Object.assign({}, filterOptions, {
    useFilterCache: false,
  });
  if (baseIndices !== undefined) {
    controllerOptions.baseIndices = baseIndices;
  }
  const filteredIndices = numericRowFilterController.apply(rawFilters, controllerOptions);
  const numericData = numericRowFilterController.getData();

  window.fastTableFilteredRows = null;
  window.fastTableFilteredRowIndices = filteredIndices;
  window.fastTableLastFilterMode = "numeric-row";

  return {
    filteredCount: numericRowFilterController.getCurrentCount(),
    filteredIndices,
    numericData,
  };
}

function applyFiltersNumericColumnarPath(rawFilters, filterOptions, baseIndices) {
  const controllerOptions = Object.assign({}, filterOptions, {
    useFilterCache: false,
  });
  if (baseIndices !== undefined) {
    controllerOptions.baseIndices = baseIndices;
  }
  const filteredIndices = numericColumnarFilterController.apply(
    rawFilters,
    controllerOptions
  );
  const columnarData = numericColumnarFilterController.getData();

  window.fastTableFilteredRows = null;
  window.fastTableFilteredRowIndices = filteredIndices;
  window.fastTableLastFilterMode = "binary-columnar";

  return {
    filteredCount: numericColumnarFilterController.getCurrentCount(),
    filteredIndices,
    columnarData,
  };
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

function buildFilterResultFromCachedEntry(cachedEntry) {
  if (!cachedEntry) {
    return null;
  }

  const filteredIndices =
    cachedEntry.filteredIndices === undefined ? null : cachedEntry.filteredIndices;
  const filteredCount = Number(cachedEntry.filteredCount) || 0;

  if (currentLayout === "columnar" && currentColumnarMode === "binary") {
    return {
      filteredCount,
      filteredIndices,
      columnarData: numericColumnarFilterController.getData(),
    };
  }

  if (currentLayout === "columnar") {
    return {
      filteredCount,
      filteredIndices,
      columnarData: objectColumnarFilterController.getData(),
    };
  }

  if (currentRepresentation === "numeric") {
    return {
      filteredCount,
      filteredIndices,
      numericData: numericRowFilterController.getData(),
    };
  }

  return {
    filteredCount,
    filteredIndices,
    rows: ensureObjectRowsAvailable(),
  };
}

function syncActiveControllerIndices(filteredIndices) {
  if (currentLayout === "columnar" && currentColumnarMode === "binary") {
    if (
      numericColumnarFilterController &&
      typeof numericColumnarFilterController.setCurrentIndices === "function"
    ) {
      numericColumnarFilterController.setCurrentIndices(filteredIndices);
    }
    return;
  }

  if (currentLayout === "columnar") {
    if (
      objectColumnarFilterController &&
      typeof objectColumnarFilterController.setCurrentIndices === "function"
    ) {
      objectColumnarFilterController.setCurrentIndices(filteredIndices);
    }
    return;
  }

  if (currentRepresentation === "numeric") {
    if (
      numericRowFilterController &&
      typeof numericRowFilterController.setCurrentIndices === "function"
    ) {
      numericRowFilterController.setCurrentIndices(filteredIndices);
    }
    return;
  }

  if (
    objectRowFilterController &&
    typeof objectRowFilterController.setCurrentIndices === "function"
  ) {
    objectRowFilterController.setCurrentIndices(filteredIndices);
  }
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

function runFiltersWithRawFilters(rawFilters, options) {
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
  const fullActiveFilters = getActiveFiltersForOrchestration(sourceRawFilters);
  const active = Object.keys(fullActiveFilters).length > 0;
  const smartStateKey = buildTopLevelSmartFilterStateKey(filterOptions);
  const smartState = getTopLevelSmartFilterStateEntry(smartStateKey);

  if (getLoadedRowCount() === 0) {
    if (!skipRender) {
      clearPreviewBody();
    }

    if (!skipStatus) {
      filterStatusEl.textContent = "No data loaded yet.";
    }

    syncClearFilterCacheButtonState();
    return null;
  }

  const filterCoreStart = performance.now();
  const useTopLevelFilterCache =
    filterOptions.useFilterCache === true && active;
  let selectedBaseCandidateCount = -1;
  let topLevelCacheKey = "";
  let topLevelCacheEvent = {
    enabled: useTopLevelFilterCache,
    hit: false,
    inserted: false,
    tier: "",
    skippedReason: "",
    lookupMs: 0,
    insertMs: 0,
    resultCount: 0,
    searchedCount: -1,
    smallSizeBytes: topLevelFilterCaches.small.totalBytes,
    smallEntryCount: topLevelFilterCaches.small.entries.size,
    smallCapacity: topLevelFilterCaches.small.capacity,
    mediumSizeBytes: topLevelFilterCaches.medium.totalBytes,
    mediumEntryCount: topLevelFilterCaches.medium.entries.size,
    mediumCapacity: topLevelFilterCaches.medium.capacity,
  };
  let filterResult = null;
  let matchedTopLevelCacheEntry = null;
  if (useTopLevelFilterCache) {
    topLevelCacheKey = buildTopLevelFilterCacheKey(
      sourceRawFilters,
      filterOptions
    );
    const cacheLookupStartMs = performance.now();
    const cachedEntry = getTopLevelFilterCacheEntryFromTiers(topLevelCacheKey);
    topLevelCacheEvent.lookupMs = performance.now() - cacheLookupStartMs;
    if (cachedEntry) {
      matchedTopLevelCacheEntry = cachedEntry;
      filterResult = buildFilterResultFromCachedEntry(cachedEntry);
      syncActiveControllerIndices(filterResult.filteredIndices);
      topLevelCacheEvent.hit = true;
      topLevelCacheEvent.resultCount = Number(cachedEntry.filteredCount) || 0;
      selectedBaseCandidateCount = Number(cachedEntry.searchedCount);
      if (
        !Number.isFinite(selectedBaseCandidateCount) ||
        selectedBaseCandidateCount < 0
      ) {
        const cachedResultCount = Number(cachedEntry.filteredCount);
        selectedBaseCandidateCount =
          Number.isFinite(cachedResultCount) && cachedResultCount >= 0
            ? Math.floor(cachedResultCount)
            : -1;
      } else {
        selectedBaseCandidateCount = Math.max(
          0,
          Math.floor(selectedBaseCandidateCount)
        );
      }
      topLevelCacheEvent.searchedCount = selectedBaseCandidateCount;
      topLevelCacheEvent.tier = cachedEntry.tier || "";
      topLevelCacheEvent.smallSizeBytes = topLevelFilterCaches.small.totalBytes;
      topLevelCacheEvent.smallEntryCount = topLevelFilterCaches.small.entries.size;
      topLevelCacheEvent.mediumSizeBytes = topLevelFilterCaches.medium.totalBytes;
      topLevelCacheEvent.mediumEntryCount = topLevelFilterCaches.medium.entries.size;
    }
  }

  let effectiveRawFilters = sourceRawFilters;
  let baseIndices = undefined;
  let dictionaryKeySearchPlan = null;
  if (filterResult === null && active) {
    let smartCandidate = null;
    let dictionarySearchRawFilters = sourceRawFilters;
    if (filterOptions && filterOptions.useSmartFiltering === true && smartState) {
      const previousActiveFilters =
        smartState.fullActiveFilters || Object.create(null);
      if (areActiveFiltersStricter(fullActiveFilters, previousActiveFilters)) {
        let smartCandidateCount = Number(smartState.filteredCount);
        if (!Number.isFinite(smartCandidateCount) || smartCandidateCount < 0) {
          smartCandidateCount = getFilteredIndicesCount(
            smartState.filteredIndices,
            getLoadedRowCount()
          );
        }
        smartCandidateCount = Math.max(0, smartCandidateCount);
        smartCandidate = {
          source: "smart",
          baseIndices: smartState.filteredIndices,
          count: smartCandidateCount,
          guaranteedActiveFilters: previousActiveFilters,
        };
        dictionarySearchRawFilters = buildRawFiltersWithoutGuaranteed(
          sourceRawFilters,
          fullActiveFilters,
          previousActiveFilters
        );
      }
    }

    dictionaryKeySearchPlan = buildDictionaryKeySearchPlan(
      dictionarySearchRawFilters,
      filterOptions
    );

    let dictionaryCandidate = null;
    if (dictionaryKeySearchPlan && dictionaryKeySearchPlan.used) {
      const guaranteedColumnKeys =
        Array.isArray(dictionaryKeySearchPlan.guaranteedColumnKeys) &&
        dictionaryKeySearchPlan.guaranteedColumnKeys.length > 0
          ? dictionaryKeySearchPlan.guaranteedColumnKeys
          : dictionaryKeySearchPlan.selectedColumnKey
            ? [dictionaryKeySearchPlan.selectedColumnKey]
            : [];
      const guaranteedFilters = Object.create(null);
      for (let i = 0; i < guaranteedColumnKeys.length; i += 1) {
        const key = guaranteedColumnKeys[i];
        if (fullActiveFilters[key] !== undefined) {
          guaranteedFilters[key] = fullActiveFilters[key];
        }
      }
      dictionaryCandidate = {
        source: "dict",
        baseIndices: dictionaryKeySearchPlan.baseIndices,
        count: Math.max(0, Number(dictionaryKeySearchPlan.candidateCount) || 0),
        guaranteedActiveFilters: guaranteedFilters,
      };
    }

    let chosenBaseCandidate = smartCandidate;
    if (
      dictionaryCandidate &&
      (!chosenBaseCandidate || dictionaryCandidate.count < chosenBaseCandidate.count)
    ) {
      chosenBaseCandidate = dictionaryCandidate;
    }

    if (chosenBaseCandidate) {
      baseIndices = chosenBaseCandidate.baseIndices;
      selectedBaseCandidateCount = chosenBaseCandidate.count;
      topLevelCacheEvent.searchedCount = selectedBaseCandidateCount;
      effectiveRawFilters = buildRawFiltersWithoutGuaranteed(
        sourceRawFilters,
        fullActiveFilters,
        chosenBaseCandidate.guaranteedActiveFilters
      );
    }
  }

  if (filterResult === null) {
    if (currentLayout === "columnar" && currentColumnarMode === "binary") {
      filterResult = applyFiltersNumericColumnarPath(
        effectiveRawFilters,
        filterOptions,
        baseIndices
      );
    } else if (currentLayout === "columnar") {
      filterResult = applyFiltersObjectColumnarPath(
        effectiveRawFilters,
        filterOptions,
        baseIndices
      );
    } else if (currentRepresentation === "numeric") {
      filterResult = applyFiltersNumericRowPath(
        effectiveRawFilters,
        filterOptions,
        baseIndices
      );
    } else {
      filterResult = applyFiltersObjectRowPath(
        effectiveRawFilters,
        filterOptions,
        baseIndices
      );
    }
  }

  const filterCoreEnd = performance.now();
  const filterCoreDurationMs = filterCoreEnd - filterCoreStart;
  let insertedTopLevelCacheEntry = null;
  if (useTopLevelFilterCache && !topLevelCacheEvent.hit) {
    const resultCount = Number(filterResult.filteredCount) || 0;
    topLevelCacheEvent.resultCount = resultCount;
    if (!(filterCoreDurationMs > TOP_LEVEL_FILTER_CACHE_MIN_INSERT_MS)) {
      topLevelCacheEvent.skippedReason = "core";
    } else {
      const tier = chooseTopLevelFilterCacheTier(resultCount);
      if (tier === "") {
        topLevelCacheEvent.skippedReason = "size";
      } else {
        topLevelCacheEvent.tier = tier;
        const cacheInsertStartMs = performance.now();
        const insertedEntry = setTopLevelFilterCacheEntryInTier(
          tier,
          topLevelCacheKey,
          filterResult.filteredIndices,
          resultCount,
          selectedBaseCandidateCount >= 0
            ? selectedBaseCandidateCount
            : getLoadedRowCount()
        );
        insertedTopLevelCacheEntry = insertedEntry;
        topLevelCacheEvent.insertMs = performance.now() - cacheInsertStartMs;
        topLevelCacheEvent.inserted = true;
        if (insertedEntry && typeof insertedEntry.tier === "string") {
          topLevelCacheEvent.tier = insertedEntry.tier;
        }
      }
    }
    topLevelCacheEvent.smallSizeBytes = topLevelFilterCaches.small.totalBytes;
    topLevelCacheEvent.smallEntryCount = topLevelFilterCaches.small.entries.size;
    topLevelCacheEvent.mediumSizeBytes = topLevelFilterCaches.medium.totalBytes;
    topLevelCacheEvent.mediumEntryCount = topLevelFilterCaches.medium.entries.size;
  }
  if (useTopLevelFilterCache && topLevelCacheEvent.hit) {
    topLevelCacheEvent.smallSizeBytes = topLevelFilterCaches.small.totalBytes;
    topLevelCacheEvent.smallEntryCount = topLevelFilterCaches.small.entries.size;
    topLevelCacheEvent.mediumSizeBytes = topLevelFilterCaches.medium.totalBytes;
    topLevelCacheEvent.mediumEntryCount = topLevelFilterCaches.medium.entries.size;
  }

  if (filterOptions && filterOptions.useSmartFiltering === true) {
    if (matchedTopLevelCacheEntry) {
      setTopLevelSmartFilterStateEntry(
        smartStateKey,
        fullActiveFilters,
        matchedTopLevelCacheEntry.filteredIndices,
        matchedTopLevelCacheEntry.filteredCount,
        true
      );
    } else if (
      insertedTopLevelCacheEntry &&
      insertedTopLevelCacheEntry.filteredIndices !== undefined
    ) {
      setTopLevelSmartFilterStateEntry(
        smartStateKey,
        fullActiveFilters,
        insertedTopLevelCacheEntry.filteredIndices,
        insertedTopLevelCacheEntry.filteredCount,
        true
      );
    } else {
      setTopLevelSmartFilterStateEntry(
        smartStateKey,
        fullActiveFilters,
        filterResult.filteredIndices,
        filterResult.filteredCount,
        false
      );
    }
  }

  let sortRun = null;
  let renderIndices = filterResult.filteredIndices;
  if (!skipRender) {
    sortRun = buildSortedIndicesForCurrentResult(filterResult, {
      preferPrecomputedFastPath,
    });
    if (sortRun && hasIndexCollection(sortRun.indices)) {
      renderIndices = sortRun.indices;
    }
  }

  let renderDurationMs = 0;
  let renderStart = 0;
  if (!skipRender) {
    renderStart = performance.now();
  }
  if (!skipRender) {
    renderFilterResultByCurrentMode(filterResult, renderIndices, keepScroll);
  }
  let renderEnd = filterCoreEnd;
  if (!skipRender) {
    renderEnd = performance.now();
  }

  if (!skipRender) {
    renderDurationMs = renderEnd - renderStart;
  }
  const totalDurationMs = skipRender
    ? filterCoreDurationMs
    : renderEnd - filterCoreStart;
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
  const output = {
    filteredCount: filterResult.filteredCount,
    filteredIndices: filterResult.filteredIndices,
    renderIndices,
    coreMs: filterCoreDurationMs,
    reverseIndexMs,
    reverseIndexSearchMs,
    reverseIndexSearchFullMs,
    reverseIndexSearchRefinedMs,
    reverseIndexMergeMs,
    reverseIndexMergeConcatMs,
    reverseIndexMergeSortMs,
    reverseIndexIntersectMs,
    renderMs: renderDurationMs,
    totalMs: totalDurationMs,
    active,
    sort: sortRun,
    topLevelFilterCacheEvent: topLevelCacheEvent,
    reverseIndex:
      dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
        ? dictionaryKeySearchPlan
        : null,
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

function applyFiltersAndRender(options) {
  const executionOptions = options || {};
  const rawFilters = executionOptions.rawFilters || readRawFilters();
  return runFiltersWithRawFilters(rawFilters, executionOptions);
}

function attachFilterListeners() {
  for (let i = 0; i < filterInputs.length; i += 1) {
    filterInputs[i].addEventListener("input", applyFiltersAndRender);
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

  applyFiltersAndRender();
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

function buildSingleFilterRawFilters(columnKey, value) {
  const rawFilters = {};

  for (let i = 0; i < columnKeys.length; i += 1) {
    rawFilters[columnKeys[i]] = "";
  }

  rawFilters[columnKey] = String(value);
  return rawFilters;
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

const fastTableEngine = createFastTableEngine({
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
    runFilterPass: applyFiltersAndRender,
    runSingleFilterPass(columnKey, value, options) {
      const rawFilters = buildSingleFilterRawFilters(columnKey, value);
      return runFiltersWithRawFilters(rawFilters, options);
    },
    runFilterPassWithRawFilters(rawFilters, options) {
      return runFiltersWithRawFilters(rawFilters || readRawFilters(), options);
    },
    getSortModes: getAvailableSortModes,
    getSortMode,
    getSortOptions: getSortRuntimeOptions,
    setSortOptions: setSortRuntimeOptions,
    buildSortRowsSnapshot: buildRowsSnapshotFromRawFilters,
    runSortSnapshotPass,
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

if (sortModeEl) {
  sortModeEl.addEventListener("change", () => {
    syncSortModeAvailability();
    runSortInteraction(() => {
      applySortAndRender();
    }, { disableButtons: false });
  });
}

if (useTypedSortComparatorEl) {
  useTypedSortComparatorEl.addEventListener("change", () => {
    runSortInteraction(() => {
      applySortAndRender();
    }, { disableButtons: false });
  });
}

if (useIndexSortEl) {
  useIndexSortEl.addEventListener("change", () => {
    runSortInteraction(() => {
      applySortAndRender();
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
      applySortAndRender();
    }, { disableButtons: false });
  });
}

enableCachingEl.addEventListener("change", () => {
  applyFiltersAndRender();
});

if (useDictionaryKeySearchEl) {
  useDictionaryKeySearchEl.addEventListener("change", () => {
    applyFiltersAndRender();
  });
}

if (useDictionaryIntersectionEl) {
  useDictionaryIntersectionEl.addEventListener("change", () => {
    applyFiltersAndRender();
  });
}

if (useSmarterPlannerEl) {
  useSmarterPlannerEl.addEventListener("change", () => {
    applyFiltersAndRender();
  });
}

useSmartFilteringEl.addEventListener("change", () => {
  applyFiltersAndRender();
});

if (useFilterCacheEl) {
  useFilterCacheEl.addEventListener("change", () => {
    applyFiltersAndRender();
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
