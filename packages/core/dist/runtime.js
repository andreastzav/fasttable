import {
  COLUMN_NAMES,
  COLUMN_KEYS,
  COLUMN_INDEX_BY_KEY,
  BASE_COLUMN_COUNT,
  NUMERIC_CACHE_OFFSET,
  generateRows,
  deriveColumnarDataFromRows,
  deriveNumericRowsFromRows,
  deriveNumericColumnarDataFromNumericRows,
} from "./generation.js";
import {
  createRowFilterController,
  createColumnarFilterController,
  createNumericRowFilterController,
  createNumericColumnarFilterController,
  buildDictionaryKeySearchPrefilter,
  precomputeDictionaryKeySearchState,
} from "./filtering.js";
import { createFilterRuntimeBridge } from "./filter-runtime-bridge.js";
import { getAvailableSortModes } from "./sorting.js";
import { createSortRuntimeBridge } from "./sort-runtime-bridge.js";
import {
  convertNumericColumnarDataToObjectRows,
} from "./io.js";
import {
  runFilterPassWithRawFiltersUsingBridge,
  runFilterPassUsingBridge,
  runSingleFilterPassUsingBridge,
  buildSortRowsSnapshotUsingFilter,
  runSortSnapshotPassUsingBridge,
  restoreRuntimeStateFromSetters,
} from "./execution-core.js";

function defaultNow() {
  if (typeof performance !== "undefined" && typeof performance.now === "function") {
    return performance.now();
  }

  return Date.now();
}

function toBoolean(value, fallback) {
  if (typeof value === "boolean") {
    return value;
  }

  return fallback;
}

function normalizeRawFilters(rawFilters) {
  const source =
    rawFilters && typeof rawFilters === "object" ? rawFilters : {};
  const keys = Object.keys(source);
  const normalized = {};

  for (let i = 0; i < keys.length; i += 1) {
    const key = keys[i];
    const value = String(source[key] ?? "");
    if (value.trim() !== "") {
      normalized[key] = value;
    }
  }

  return normalized;
}

function buildColumnTypeByKey() {
  const typeByKey = Object.create(null);
  for (let i = 0; i < COLUMN_KEYS.length; i += 1) {
    const key = COLUMN_KEYS[i];
    typeByKey[key] =
      key === "firstName" || key === "lastName" ? "string" : "number";
  }
  return typeByKey;
}

function materializeFilteredIndices(filteredIndices, fallbackCount) {
  const defaultCount = Math.max(0, Number(fallbackCount) | 0);

  if (filteredIndices === null || filteredIndices === undefined) {
    const out = new Uint32Array(defaultCount);
    for (let i = 0; i < defaultCount; i += 1) {
      out[i] = i;
    }
    return out;
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
    const out = new Uint32Array(count);
    for (let i = 0; i < count; i += 1) {
      out[i] = filteredIndices.buffer[i] >>> 0;
    }
    return out;
  }

  if (Array.isArray(filteredIndices) || ArrayBuffer.isView(filteredIndices)) {
    const out = new Uint32Array(filteredIndices.length);
    for (let i = 0; i < filteredIndices.length; i += 1) {
      out[i] = Number(filteredIndices[i]) >>> 0;
    }
    return out;
  }

  return new Uint32Array(0);
}

function normalizeRuntimeSortModes(inputSortModes) {
  const source =
    Array.isArray(inputSortModes) && inputSortModes.length > 0
      ? inputSortModes
      : getAvailableSortModes();
  const seen = Object.create(null);
  const normalized = [];

  for (let i = 0; i < source.length; i += 1) {
    const mode =
      typeof source[i] === "string" ? source[i].trim() : "";
    if (mode === "" || seen[mode] === true) {
      continue;
    }

    seen[mode] = true;
    normalized.push(mode);
  }

  if (normalized.length === 0) {
    normalized.push("native");
  }

  if (!seen.precomputed) {
    normalized.push("precomputed");
  }

  return normalized;
}

function createFastTableRuntime(options) {
  const input = options || {};
  const now = typeof input.now === "function" ? input.now : defaultNow;
  const columnTypeByKey = buildColumnTypeByKey();
  const sortModes = normalizeRuntimeSortModes(input.sortModes);

  const defaultModeOptions = {
    useColumnarData: true,
    useBinaryColumnar: true,
    useNumericData: true,
    enableCaching: true,
    useDictionaryKeySearch: true,
    useDictionaryIntersection: true,
    useSmarterPlanner: true,
    useSmartFiltering: false,
    useFilterCache: false,
  };
  const defaultSortOptions = {
    useTypedComparator: true,
    useIndexSort: true,
  };

  let objectRows = [];
  let objectColumnarData = null;
  let numericRowsData = null;
  let numericColumnarData = null;
  let lastFilterResult = null;
  let rawFilters = {};
  let modeOptions = {};
  let sortOptions = {};
  let sortMode = sortModes[0];

  const objectRowFilterController = createRowFilterController([]);
  const objectColumnarFilterController = createColumnarFilterController(null);
  const numericRowFilterController = createNumericRowFilterController([], {
    keyToIndex: COLUMN_INDEX_BY_KEY,
    baseColumnCount: BASE_COLUMN_COUNT,
    cacheOffset: NUMERIC_CACHE_OFFSET,
  });
  const numericColumnarFilterController = createNumericColumnarFilterController(
    null,
    {
      keyToIndex: COLUMN_INDEX_BY_KEY,
      baseColumnCount: BASE_COLUMN_COUNT,
      cacheOffset: NUMERIC_CACHE_OFFSET,
    }
  );
  let filterRuntimeBridge = null;
  let sortRuntimeBridge = null;

  function getSchema() {
    return {
      columnKeys: COLUMN_KEYS.slice(),
      columnNames: COLUMN_NAMES.slice(),
      baseColumnCount: BASE_COLUMN_COUNT,
      numericCacheOffset: NUMERIC_CACHE_OFFSET,
      objectCacheKeys: COLUMN_KEYS.map((key) => `${key}Cache`),
    };
  }

  function isValidObjectColumnarData(data) {
    return (
      data &&
      typeof data === "object" &&
      Number.isInteger(data.rowCount) &&
      data.rowCount >= 0 &&
      data.columns &&
      typeof data.columns === "object"
    );
  }

  function isValidNumericRowsData(data) {
    return (
      data &&
      typeof data === "object" &&
      Number.isInteger(data.rowCount) &&
      data.rowCount >= 0 &&
      Array.isArray(data.rows)
    );
  }

  function isValidNumericColumnarData(data) {
    return (
      data &&
      typeof data === "object" &&
      Number.isInteger(data.rowCount) &&
      data.rowCount >= 0 &&
      Array.isArray(data.columns)
    );
  }

  function ensureObjectRowsFromObjectColumnar(data) {
    const rowCount = Number(data.rowCount) | 0;
    const rows = new Array(rowCount);
    const columns = data.columns || {};

    for (let r = 0; r < rowCount; r += 1) {
      const row = {};
      for (let c = 0; c < COLUMN_KEYS.length; c += 1) {
        const key = COLUMN_KEYS[c];
        const values = columns[key];
        row[key] = values ? values[r] : undefined;

        const cacheKey = `${key}Cache`;
        const cacheValues = columns[cacheKey];
        row[cacheKey] =
          cacheValues && cacheValues[r] !== undefined
            ? cacheValues[r]
            : String(row[key] ?? "").toLowerCase();
      }
      rows[r] = row;
    }

    return rows;
  }

  function ensureObjectRowsFromNumericRows(data) {
    const rowCount = Number(data.rowCount) | 0;
    const rows = new Array(rowCount);
    const numericRows = data.rows;

    for (let r = 0; r < rowCount; r += 1) {
      const source = numericRows[r] || [];
      const row = {};
      for (let c = 0; c < COLUMN_KEYS.length; c += 1) {
        const key = COLUMN_KEYS[c];
        row[key] = source[c];
        row[`${key}Cache`] =
          source[c + NUMERIC_CACHE_OFFSET] !== undefined
            ? source[c + NUMERIC_CACHE_OFFSET]
            : String(source[c] ?? "").toLowerCase();
      }
      rows[r] = row;
    }

    return rows;
  }

  function ensureObjectRows() {
    if (Array.isArray(objectRows) && objectRows.length > 0) {
      return objectRows;
    }

    if (isValidNumericColumnarData(numericColumnarData)) {
      objectRows = convertNumericColumnarDataToObjectRows(
        numericColumnarData,
        getSchema(),
        true
      );
      return objectRows;
    }

    if (isValidObjectColumnarData(objectColumnarData)) {
      objectRows = ensureObjectRowsFromObjectColumnar(objectColumnarData);
      return objectRows;
    }

    if (isValidNumericRowsData(numericRowsData)) {
      objectRows = ensureObjectRowsFromNumericRows(numericRowsData);
      return objectRows;
    }

    objectRows = [];
    return objectRows;
  }

  function ensureObjectColumnarData() {
    if (isValidObjectColumnarData(objectColumnarData)) {
      return objectColumnarData;
    }

    const rows = ensureObjectRows();
    objectColumnarData = deriveColumnarDataFromRows(rows);
    return objectColumnarData;
  }

  function ensureNumericRowsData() {
    if (isValidNumericRowsData(numericRowsData)) {
      return numericRowsData;
    }

    const rows = ensureObjectRows();
    numericRowsData = deriveNumericRowsFromRows(rows);
    return numericRowsData;
  }

  function ensureNumericColumnarData() {
    if (isValidNumericColumnarData(numericColumnarData)) {
      return numericColumnarData;
    }

    const numericRows = ensureNumericRowsData();
    numericColumnarData = deriveNumericColumnarDataFromNumericRows(numericRows.rows);
    precomputeDictionaryKeySearchState(numericColumnarData);
    return numericColumnarData;
  }

  function syncControllers() {
    objectRowFilterController.setRows(ensureObjectRows());
    objectColumnarFilterController.setData(ensureObjectColumnarData());
    numericRowFilterController.setData(ensureNumericRowsData().rows);
    numericColumnarFilterController.setData(ensureNumericColumnarData());
  }

  function clearCurrentFilterState() {
    lastFilterResult = null;
    objectRowFilterController.setCurrentIndices(null);
    objectColumnarFilterController.setCurrentIndices(null);
    numericRowFilterController.setCurrentIndices(null);
    numericColumnarFilterController.setCurrentIndices(null);
  }

  function normalizeModeOptions(nextOptions) {
    const source =
      nextOptions && typeof nextOptions === "object" ? nextOptions : {};
    const normalized = {
      useColumnarData: toBoolean(
        source.useColumnarData,
        modeOptions.useColumnarData
      ),
      useBinaryColumnar: toBoolean(
        source.useBinaryColumnar,
        modeOptions.useBinaryColumnar
      ),
      useNumericData: toBoolean(source.useNumericData, modeOptions.useNumericData),
      enableCaching: toBoolean(source.enableCaching, modeOptions.enableCaching),
      useDictionaryKeySearch: toBoolean(
        source.useDictionaryKeySearch,
        modeOptions.useDictionaryKeySearch
      ),
      useDictionaryIntersection: toBoolean(
        source.useDictionaryIntersection,
        modeOptions.useDictionaryIntersection
      ),
      useSmarterPlanner: toBoolean(
        source.useSmarterPlanner,
        modeOptions.useSmarterPlanner
      ),
      useSmartFiltering: toBoolean(
        source.useSmartFiltering,
        modeOptions.useSmartFiltering
      ),
      useFilterCache: toBoolean(source.useFilterCache, modeOptions.useFilterCache),
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

  function normalizeSortOptions(nextOptions) {
    const source =
      nextOptions && typeof nextOptions === "object" ? nextOptions : {};
    return {
      useTypedComparator: toBoolean(
        source.useTypedComparator,
        sortOptions.useTypedComparator
      ),
      useIndexSort: toBoolean(source.useIndexSort, sortOptions.useIndexSort),
    };
  }

  function hasData() {
    const rows = ensureObjectRows();
    return rows.length > 0;
  }

  function getRowCount() {
    return ensureObjectRows().length;
  }

  function setDataFromRows(rows) {
    objectRows = Array.isArray(rows) ? rows.slice() : [];
    objectColumnarData = null;
    numericRowsData = null;
    numericColumnarData = null;
    if (filterRuntimeBridge) {
      filterRuntimeBridge.bumpTopLevelFilterCacheRevision();
    }
    if (sortRuntimeBridge) {
      sortRuntimeBridge.resetPrecomputedSortState(objectRows.length);
    }
    clearCurrentFilterState();

    if (objectRows.length > 0) {
      syncControllers();
    } else {
      objectRowFilterController.setRows([]);
      objectColumnarFilterController.setData(null);
      numericRowFilterController.setData([]);
      numericColumnarFilterController.setData(null);
    }

    return {
      rowCount: objectRows.length,
    };
  }

  function setDataFromNumericColumnar(data) {
    numericColumnarData = isValidNumericColumnarData(data) ? data : null;
    objectRows = [];
    objectColumnarData = null;
    numericRowsData = null;
    if (filterRuntimeBridge) {
      filterRuntimeBridge.bumpTopLevelFilterCacheRevision();
    }
    if (sortRuntimeBridge) {
      sortRuntimeBridge.resetPrecomputedSortState(
        numericColumnarData ? numericColumnarData.rowCount : 0
      );
    }
    clearCurrentFilterState();

    if (isValidNumericColumnarData(numericColumnarData)) {
      precomputeDictionaryKeySearchState(numericColumnarData);
      syncControllers();
      return {
        rowCount: numericColumnarData.rowCount,
      };
    }

    objectRowFilterController.setRows([]);
    objectColumnarFilterController.setData(null);
    numericRowFilterController.setData([]);
    numericColumnarFilterController.setData(null);
    return {
      rowCount: 0,
    };
  }

  function generate(rowCount, generationOptions) {
    const count = Math.max(0, Number(rowCount) | 0);
    const rows =
      count > 0 ? generateRows(count, generationOptions) : [];
    return setDataFromRows(rows);
  }

  function getModeOptions() {
    return { ...modeOptions };
  }

  function setModeOptions(nextOptions) {
    modeOptions = normalizeModeOptions(nextOptions);
    return getModeOptions();
  }

  function getRawFilters() {
    return { ...rawFilters };
  }

  function setRawFilters(nextRawFilters) {
    rawFilters = normalizeRawFilters(nextRawFilters);
    return getRawFilters();
  }

  function setSingleFilter(columnKey, value) {
    const next = {};
    if (
      typeof columnKey === "string" &&
      columnKey !== "" &&
      String(value ?? "").trim() !== ""
    ) {
      next[columnKey] = String(value);
    }
    rawFilters = next;
    return getRawFilters();
  }

  function clearFilters() {
    rawFilters = {};
    clearCurrentFilterState();
    return {};
  }

  function getCurrentFilterModeKey() {
    if (modeOptions.useColumnarData) {
      if (modeOptions.useBinaryColumnar) {
        return "binary-columnar";
      }
      return "object-columnar";
    }

    if (modeOptions.useNumericData) {
      return "numeric-row";
    }

    return "object-row";
  }

  function runFilterPassWithRawFilters(nextRawFilters, options) {
    return runFilterPassWithRawFiltersUsingBridge(
      {
        hasData: () => !!filterRuntimeBridge && hasData(),
        runBridgeFilterPass(rawFilters, executionOptions) {
          return filterRuntimeBridge.runFilterPassWithRawFilters(
            rawFilters,
            executionOptions
          );
        },
        setLastFilterResult(nextResult) {
          lastFilterResult = nextResult;
        },
      },
      nextRawFilters,
      options
    );
  }

  function runFilterPass(options) {
    return runFilterPassUsingBridge(
      {
        getRawFilters: () => rawFilters,
        runFilterPassWithRawFilters,
      },
      options
    );
  }

  function runSingleFilterPass(columnKey, value, options) {
    return runSingleFilterPassUsingBridge(
      {
        runFilterPassWithRawFilters,
      },
      columnKey,
      value,
      options
    );
  }

  function getSortOptions() {
    return { ...sortOptions };
  }

  function setSortOptions(nextSortOptions) {
    sortOptions = normalizeSortOptions(nextSortOptions);
    return getSortOptions();
  }

  function getSortMode() {
    return sortMode;
  }

  function setSortMode(nextSortMode) {
    const modeText =
      typeof nextSortMode === "string" ? nextSortMode.trim() : "";
    if (modeText !== "" && sortModes.includes(modeText)) {
      sortMode = modeText;
    }
    return sortMode;
  }

  function getSortModes() {
    return sortModes.slice();
  }

  function buildSortRowsSnapshot(nextRawFilters) {
    return buildSortRowsSnapshotUsingFilter(
      {
        getRawFilters: () => rawFilters,
        runFilterPassWithRawFilters,
        getRowCount,
        materializeFilteredIndices,
      },
      nextRawFilters
    );
  }

  function runSortSnapshotPass(rowsSnapshot, descriptors, sortModeOverride) {
    return runSortSnapshotPassUsingBridge(
      {
        runBridgeSortPass(
          sortRowsSnapshot,
          sortDescriptors,
          sortMode
        ) {
          if (!sortRuntimeBridge) {
            return null;
          }
          return sortRuntimeBridge.runSortSnapshotPass(
            sortRowsSnapshot,
            sortDescriptors,
            sortMode
          );
        },
      },
      rowsSnapshot,
      descriptors,
      sortModeOverride
    );
  }

  function getNumericColumnarForSave() {
    if (!hasData()) {
      return null;
    }

    return ensureNumericColumnarData();
  }

  function prewarmPrecomputedSortState() {
    if (!sortRuntimeBridge) {
      return false;
    }

    return sortRuntimeBridge.prewarmPrecomputedSortState();
  }

  function getLastFilterResult() {
    return lastFilterResult;
  }

  function restoreStateCore(statePatch) {
    return restoreRuntimeStateFromSetters(
      {
        setModeOptions,
        setRawFilters,
        setSortOptions,
        setSortMode,
        getModeOptions,
        getRawFilters,
        getSortOptions,
        getSortMode,
      },
      statePatch
    );
  }

  function hasTopLevelFilterCacheEntries() {
    return filterRuntimeBridge
      ? filterRuntimeBridge.hasTopLevelFilterCacheEntries()
      : false;
  }

  function clearTopLevelFilterCache() {
    if (filterRuntimeBridge) {
      filterRuntimeBridge.clearTopLevelFilterCache();
    }
  }

  function clearTopLevelSmartFilterState() {
    if (filterRuntimeBridge) {
      filterRuntimeBridge.clearTopLevelSmartFilterState();
    }
  }

  function clearAllFilterCaches() {
    if (filterRuntimeBridge) {
      filterRuntimeBridge.clearAllFilterCaches();
    }
  }

  function bumpTopLevelFilterCacheRevision() {
    if (filterRuntimeBridge) {
      filterRuntimeBridge.bumpTopLevelFilterCacheRevision();
    }
  }

  function getTopLevelFilterCacheSnapshot() {
    if (filterRuntimeBridge) {
      return filterRuntimeBridge.getTopLevelFilterCacheSnapshot();
    }
    return null;
  }

  filterRuntimeBridge = createFilterRuntimeBridge({
    now,
    getLoadedRowCount: () => getRowCount(),
    getCurrentFilterModeKey,
    getFilterOptions: () => ({
      enableCaching: modeOptions.enableCaching === true,
      useDictionaryKeySearch: modeOptions.useDictionaryKeySearch === true,
      useDictionaryIntersection: modeOptions.useDictionaryIntersection === true,
      useSmarterPlanner: modeOptions.useSmarterPlanner === true,
      useSmartFiltering: modeOptions.useSmartFiltering === true,
      useFilterCache: modeOptions.useFilterCache === true,
    }),
    getRawFilters: () => rawFilters,
    normalizeRawFilters,
    controllers: {
      objectRow: objectRowFilterController,
      objectColumnar: objectColumnarFilterController,
      numericRow: numericRowFilterController,
      numericColumnar: numericColumnarFilterController,
    },
    dataAccessors: {
      getObjectRows: ensureObjectRows,
      getObjectColumnarData: ensureObjectColumnarData,
      getNumericRows: () => ensureNumericRowsData().rows,
      getNumericColumnarData: ensureNumericColumnarData,
    },
    buildDictionaryKeySearchPrefilter,
    keyToIndex: COLUMN_INDEX_BY_KEY,
    isValidNumericColumnarData,
    modePathByKey: {
      "binary-columnar": "numeric-columnar",
      "object-columnar": "object-columnar",
      "numeric-row": "numeric-rows",
      "object-row": "object-rows",
    },
    syncAllControllerIndices: true,
  });

  sortRuntimeBridge = createSortRuntimeBridge({
    now,
    columnKeys: COLUMN_KEYS,
    columnIndexByKey: COLUMN_INDEX_BY_KEY,
    columnTypeByKey,
    getSortOptions,
    getSortMode,
    getRowCount,
    getSchema,
    getNumericColumnarData: ensureNumericColumnarData,
  });

  modeOptions = { ...defaultModeOptions };
  modeOptions = normalizeModeOptions(input.modeOptions || {});

  sortOptions = { ...defaultSortOptions };
  sortOptions = normalizeSortOptions(input.sortOptions || {});
  if (typeof input.sortMode === "string" && input.sortMode.trim() !== "") {
    setSortMode(input.sortMode);
  }

  if (Array.isArray(input.rows)) {
    setDataFromRows(input.rows);
  } else if (isValidNumericColumnarData(input.numericColumnarData)) {
    setDataFromNumericColumnar(input.numericColumnarData);
  } else {
    clearCurrentFilterState();
  }

  return {
    getSchema,
    hasData,
    getRowCount,
    setDataFromRows,
    setDataFromNumericColumnar,
    generate,
    getModeOptions,
    setModeOptions,
    getRawFilters,
    setRawFilters,
    setSingleFilter,
    clearFilters,
    runFilterPass,
    runSingleFilterPass,
    runFilterPassWithRawFilters,
    getLastFilterResult,
    getSortModes,
    getSortMode,
    setSortMode,
    getSortOptions,
    setSortOptions,
    buildSortRowsSnapshot,
    runSortSnapshotPass,
    prewarmPrecomputedSortState,
    getNumericColumnarForSave,
    restoreStateCore,
    hasTopLevelFilterCacheEntries,
    clearTopLevelFilterCache,
    clearTopLevelSmartFilterState,
    clearAllFilterCaches,
    bumpTopLevelFilterCacheRevision,
    getTopLevelFilterCacheSnapshot,
  };
}

export { createFastTableRuntime };
