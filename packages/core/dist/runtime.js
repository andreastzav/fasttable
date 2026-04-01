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
import { createFilteringOrchestrator } from "./filtering-orchestration.js";
import { createSortController, getAvailableSortModes } from "./sorting.js";
import {
  convertNumericColumnarDataToObjectRows,
  ensureNumericColumnarSortedIndices,
} from "./io.js";

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

function normalizeSortDescriptorList(descriptors) {
  const source = Array.isArray(descriptors) ? descriptors : [];
  const output = [];

  for (let i = 0; i < source.length; i += 1) {
    const descriptor = source[i];
    const columnKey =
      descriptor && typeof descriptor.columnKey === "string"
        ? descriptor.columnKey
        : "";
    const direction =
      descriptor && descriptor.direction === "asc" ? "asc" : "desc";

    if (columnKey !== "") {
      output.push({ columnKey, direction });
    }
  }

  return output;
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

function hasIndexCollection(indices) {
  return Array.isArray(indices) || ArrayBuffer.isView(indices);
}

function buildPrecomputedSortKeyColumns(
  indices,
  rowsByIndex,
  descriptors,
  columnTypeByKey
) {
  const descriptorList = Array.isArray(descriptors) ? descriptors : [];
  const keyColumns = new Array(descriptorList.length);

  for (let d = 0; d < descriptorList.length; d += 1) {
    const descriptor = descriptorList[d];
    const columnKey = descriptor && descriptor.columnKey;
    const valueType =
      columnTypeByKey && typeof columnKey === "string"
        ? columnTypeByKey[columnKey]
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

function isValidRowIndexCollection(indices, expectedLength, maxRowCount) {
  if (!Array.isArray(indices) && !ArrayBuffer.isView(indices)) {
    return false;
  }

  if (indices.length !== expectedLength) {
    return false;
  }

  for (let i = 0; i < indices.length; i += 1) {
    const value = Number(indices[i]);
    if (!Number.isInteger(value) || value < 0 || value >= maxRowCount) {
      return false;
    }
  }

  return true;
}

function deriveRowIndicesFromRowsByIndexColumn(rows, maxRowCount) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return [];
  }

  const out = new Array(rows.length);
  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i];
    const indexValue =
      row && row.index !== undefined && row.index !== null
        ? Number(row.index)
        : Number.NaN;
    const rowIndex = indexValue - 1;
    if (!Number.isInteger(rowIndex) || rowIndex < 0 || rowIndex >= maxRowCount) {
      return null;
    }

    out[i] = rowIndex;
  }

  return out;
}

function buildRankArrayFromSortedIndices(sortedIndices, rowCount) {
  const useCompact = rowCount <= 65536;
  const rankByRowId = useCompact ? new Uint16Array(rowCount) : new Uint32Array(rowCount);

  for (let rank = 0; rank < rowCount; rank += 1) {
    const rowIndex = Number(sortedIndices[rank]);
    if (!Number.isInteger(rowIndex) || rowIndex < 0 || rowIndex >= rowCount) {
      return null;
    }

    rankByRowId[rowIndex] = rank;
  }

  return rankByRowId;
}

function isSupportedRankArray(rankByRowId, expectedLength) {
  if (!(rankByRowId instanceof Uint32Array || rankByRowId instanceof Uint16Array)) {
    return false;
  }

  if (!Number.isFinite(expectedLength)) {
    return true;
  }

  return rankByRowId.length === expectedLength;
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
  const filteringOrchestrator = createFilteringOrchestrator({
    now,
    getLoadedRowCount: () => getRowCount(),
    getCurrentFilterModeKey,
    buildDictionaryKeySearchPlan,
    buildFilterResultFromCachedEntry,
    syncActiveControllerIndices,
    applyFilterPath,
  });

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

  function resolveSnapshotRows(source) {
    if (Array.isArray(source)) {
      return source;
    }

    if (source && typeof source === "object" && Array.isArray(source.rows)) {
      return source.rows;
    }

    return [];
  }

  function resolveSnapshotRowIndices(source, rows, totalRows) {
    const rowCount = Array.isArray(rows) ? rows.length : 0;
    const container =
      source && typeof source === "object" && !Array.isArray(source)
        ? source
        : null;
    const directCandidate = hasIndexCollection(rows.__rowIndices)
      ? rows.__rowIndices
      : container && hasIndexCollection(container.rowIndices)
        ? container.rowIndices
        : null;
    if (isValidRowIndexCollection(directCandidate, rowCount, totalRows)) {
      return directCandidate.slice();
    }

    const derived = deriveRowIndicesFromRowsByIndexColumn(rows, totalRows);
    if (isValidRowIndexCollection(derived, rowCount, totalRows)) {
      return derived;
    }

    return null;
  }

  function resolvePrecomputedRankColumnsForDescriptors(
    descriptors,
    totalRowCount
  ) {
    const descriptorList = Array.isArray(descriptors) ? descriptors : [];
    if (descriptorList.length === 0 || totalRowCount <= 0) {
      return null;
    }

    const numeric = ensureNumericColumnarSortedIndices(
      ensureNumericColumnarData(),
      getSchema()
    );
    const sortedColumns = Array.isArray(numeric.sortedIndexColumns)
      ? numeric.sortedIndexColumns
      : null;
    if (!sortedColumns || sortedColumns.length === 0) {
      return null;
    }

    const rankByKey =
      numeric.sortedRankAscByKey &&
      typeof numeric.sortedRankAscByKey === "object" &&
      !Array.isArray(numeric.sortedRankAscByKey)
        ? numeric.sortedRankAscByKey
        : Object.create(null);
    const rankByColumn = Array.isArray(numeric.sortedRankColumns)
      ? numeric.sortedRankColumns
      : new Array(sortedColumns.length);
    const descriptorRankColumns = new Array(descriptorList.length);

    for (let i = 0; i < descriptorList.length; i += 1) {
      const descriptor = descriptorList[i];
      const columnKey =
        descriptor && typeof descriptor.columnKey === "string"
          ? descriptor.columnKey
          : "";
      if (columnKey === "") {
        return null;
      }

      const columnIndex = Number(COLUMN_INDEX_BY_KEY[columnKey]);
      if (
        !Number.isInteger(columnIndex) ||
        columnIndex < 0 ||
        columnIndex >= sortedColumns.length
      ) {
        return null;
      }

      let rankByRowId = rankByKey[columnKey];
      if (!isSupportedRankArray(rankByRowId, totalRowCount)) {
        rankByRowId = rankByColumn[columnIndex];
      }

      if (!isSupportedRankArray(rankByRowId, totalRowCount)) {
        const sortedIndices = sortedColumns[columnIndex];
        if (
          !sortedIndices ||
          (!ArrayBuffer.isView(sortedIndices) && !Array.isArray(sortedIndices)) ||
          sortedIndices.length !== totalRowCount
        ) {
          return null;
        }

        rankByRowId = buildRankArrayFromSortedIndices(
          sortedIndices,
          totalRowCount
        );
        if (!isSupportedRankArray(rankByRowId, totalRowCount)) {
          return null;
        }

        rankByKey[columnKey] = rankByRowId;
        rankByColumn[columnIndex] = rankByRowId;
      }

      descriptorRankColumns[i] = rankByRowId;
    }

    numeric.sortedRankAscByKey = rankByKey;
    numeric.sortedRankColumns = rankByColumn;
    return descriptorRankColumns;
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
    filteringOrchestrator.bumpTopLevelFilterCacheRevision();
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
    filteringOrchestrator.bumpTopLevelFilterCacheRevision();
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

  function getActiveController() {
    if (modeOptions.useColumnarData) {
      if (modeOptions.useBinaryColumnar) {
        numericColumnarFilterController.setData(ensureNumericColumnarData());
        return {
          path: "numeric-columnar",
          controller: numericColumnarFilterController,
        };
      }

      objectColumnarFilterController.setData(ensureObjectColumnarData());
      return {
        path: "object-columnar",
        controller: objectColumnarFilterController,
      };
    }

    if (modeOptions.useNumericData) {
      numericRowFilterController.setData(ensureNumericRowsData().rows);
      return {
        path: "numeric-rows",
        controller: numericRowFilterController,
      };
    }

    objectRowFilterController.setRows(ensureObjectRows());
    return {
      path: "object-rows",
      controller: objectRowFilterController,
    };
  }

  function buildFilterResult(modePath, filteredIndices, filteredCount) {
    const result = {
      modePath,
      filteredCount: Math.max(0, Number(filteredCount) || 0),
      filteredIndices,
    };

    if (modePath === "numeric-columnar") {
      result.columnarData = numericColumnarFilterController.getData();
    } else if (modePath === "object-columnar") {
      result.columnarData = objectColumnarFilterController.getData();
    } else if (modePath === "numeric-rows") {
      result.numericData = numericRowFilterController.getData();
    } else {
      result.rows = ensureObjectRows();
    }

    return result;
  }

  function buildFilterResultFromCachedEntry(cachedEntry) {
    if (!cachedEntry || typeof cachedEntry !== "object") {
      return null;
    }

    const active = getActiveController();
    return buildFilterResult(
      active.path,
      cachedEntry.filteredIndices === undefined ? null : cachedEntry.filteredIndices,
      cachedEntry.filteredCount
    );
  }

  function syncActiveControllerIndices(filteredIndices) {
    objectRowFilterController.setCurrentIndices(filteredIndices);
    objectColumnarFilterController.setCurrentIndices(filteredIndices);
    numericRowFilterController.setCurrentIndices(filteredIndices);
    numericColumnarFilterController.setCurrentIndices(filteredIndices);
  }

  function applyFilterPath(effectiveRawFilters, filterOptions, baseIndices) {
    const active = getActiveController();
    const controllerOptions = {
      enableCaching: filterOptions && filterOptions.enableCaching === true,
      useSmarterPlanner: filterOptions && filterOptions.useSmarterPlanner === true,
    };
    if (baseIndices !== undefined) {
      controllerOptions.baseIndices = baseIndices;
    }
    const filteredIndices = active.controller.apply(
      effectiveRawFilters,
      controllerOptions
    );
    const filteredCount = active.controller.getCurrentCount();
    return buildFilterResult(active.path, filteredIndices, filteredCount);
  }

  function buildDictionaryKeySearchPlan(rawFilters, filterOptions) {
    const active = getActiveController();
    if (
      !filterOptions ||
      filterOptions.useDictionaryKeySearch !== true ||
      (active.path !== "numeric-columnar" && active.path !== "numeric-rows")
    ) {
      return null;
    }

    const numericData = ensureNumericColumnarData();
    return buildDictionaryKeySearchPrefilter(rawFilters, numericData, {
      useDictionaryKeySearch: true,
      useDictionaryIntersection:
        filterOptions.useDictionaryIntersection === true,
      useSmarterPlanner: filterOptions.useSmarterPlanner === true,
      keyToIndex: COLUMN_INDEX_BY_KEY,
    });
  }

  function runFilterPassWithRawFilters(nextRawFilters, options) {
    if (!hasData()) {
      lastFilterResult = null;
      return null;
    }

    const executionOptions = options || {};
    const sourceRawFilters = normalizeRawFilters(nextRawFilters);
    const filterOptions = {
      enableCaching: modeOptions.enableCaching === true,
      useDictionaryKeySearch: modeOptions.useDictionaryKeySearch === true,
      useDictionaryIntersection: modeOptions.useDictionaryIntersection === true,
      useSmarterPlanner: modeOptions.useSmarterPlanner === true,
      useSmartFiltering: modeOptions.useSmartFiltering === true,
      useFilterCache: modeOptions.useFilterCache === true,
    };
    const orchestration = filteringOrchestrator.runFilterPass(sourceRawFilters, {
      filterOptions,
    });
    const filterResult = orchestration ? orchestration.filterResult : null;
    if (!filterResult) {
      if (executionOptions.updateState !== false) {
        lastFilterResult = null;
      }
      return null;
    }
    const result = {
      modePath: filterResult.modePath,
      filteredCount: filterResult.filteredCount,
      filteredIndices: filterResult.filteredIndices,
      coreMs:
        orchestration && Number.isFinite(orchestration.coreMs)
          ? orchestration.coreMs
          : 0,
      totalMs:
        orchestration && Number.isFinite(orchestration.coreMs)
          ? orchestration.coreMs
          : 0,
      dictionaryPrefilter:
        orchestration && orchestration.dictionaryKeySearchPlan
          ? orchestration.dictionaryKeySearchPlan
          : null,
    };
    if (filterResult.columnarData !== undefined) {
      result.columnarData = filterResult.columnarData;
    }
    if (filterResult.numericData !== undefined) {
      result.numericData = filterResult.numericData;
    }
    if (filterResult.rows !== undefined) {
      result.rows = filterResult.rows;
    }

    if (executionOptions.updateState !== false) {
      lastFilterResult = result;
    }

    return result;
  }

  function runFilterPass(options) {
    return runFilterPassWithRawFilters(rawFilters, options);
  }

  function runSingleFilterPass(columnKey, value, options) {
    const singleFilters = {};
    if (
      typeof columnKey === "string" &&
      columnKey !== "" &&
      String(value ?? "").trim() !== ""
    ) {
      singleFilters[columnKey] = String(value);
    }

    return runFilterPassWithRawFilters(singleFilters, {
      ...(options || {}),
      updateState: false,
    });
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
    const raw =
      nextRawFilters && typeof nextRawFilters === "object"
        ? nextRawFilters
        : rawFilters;
    const filterResult = runFilterPassWithRawFilters(raw, { updateState: false });
    const rows = ensureObjectRows();
    const indices = materializeFilteredIndices(
      filterResult ? filterResult.filteredIndices : null,
      rows.length
    );
    const snapshotRows = new Array(indices.length);

    Object.defineProperty(snapshotRows, "__rowIndices", {
      value: indices,
      enumerable: false,
      configurable: true,
      writable: true,
    });
    Object.defineProperty(snapshotRows, "__rowsMaterialized", {
      value: false,
      enumerable: false,
      configurable: true,
      writable: true,
    });

    return {
      rows: snapshotRows,
      rowIndices: indices,
      count: snapshotRows.length,
      filterCoreMs: filterResult ? filterResult.coreMs : 0,
    };
  }

  function runSortSnapshotPass(rowsSnapshot, descriptors, sortModeOverride) {
    const sourceRows = resolveSnapshotRows(rowsSnapshot);
    const descriptorList = normalizeSortDescriptorList(descriptors);
    const controller = createSortController({
      columnKeys: COLUMN_KEYS,
      defaultColumnKey: "index",
      columnTypeByKey,
      defaultUseTypedComparator: sortOptions.useTypedComparator,
    });

    for (let i = 0; i < descriptorList.length; i += 1) {
      const descriptor = descriptorList[i];
      controller.cycle(descriptor.columnKey);
      if (descriptor.direction === "asc") {
        controller.cycle(descriptor.columnKey);
      }
    }

    const effectiveSortMode =
      typeof sortModeOverride === "string" && sortModeOverride !== ""
        ? sortModeOverride
        : sortMode;

    const allRows = ensureObjectRows();
    const snapshotRowIndices = resolveSnapshotRowIndices(
      rowsSnapshot,
      sourceRows,
      allRows.length
    );
    const snapshotCount = Array.isArray(sourceRows)
      ? sourceRows.length
      : hasIndexCollection(snapshotRowIndices)
        ? snapshotRowIndices.length
        : 0;
    const sortTotalStartMs = now();
    let rowsToSort = null;
    let result = null;
    if (sortOptions.useIndexSort) {
      const activeDescriptors = controller.getSortDescriptors();
      const canUseGlobalRowIndices =
        hasIndexCollection(snapshotRowIndices) &&
        snapshotRowIndices.length === snapshotCount &&
        allRows.length > 0;
      const indices = canUseGlobalRowIndices
        ? snapshotRowIndices.slice()
        : new Array(snapshotCount);
      if (!canUseGlobalRowIndices) {
        if (
          Array.isArray(sourceRows) &&
          sourceRows.__rowsMaterialized === false &&
          hasIndexCollection(snapshotRowIndices) &&
          snapshotRowIndices.length === snapshotCount
        ) {
          rowsToSort = new Array(snapshotCount);
          for (let i = 0; i < snapshotCount; i += 1) {
            rowsToSort[i] = allRows[snapshotRowIndices[i]];
          }
        } else {
          rowsToSort = sourceRows.slice();
        }

        for (let i = 0; i < indices.length; i += 1) {
          indices[i] = i;
        }
      }

      const runSortOptions = { ...sortOptions };
      const rowsByIndex = canUseGlobalRowIndices ? allRows : rowsToSort;
      if (canUseGlobalRowIndices) {
        const precomputedRankColumns = resolvePrecomputedRankColumnsForDescriptors(
          activeDescriptors,
          allRows.length
        );
        if (
          Array.isArray(precomputedRankColumns) &&
          precomputedRankColumns.length === activeDescriptors.length
        ) {
          runSortOptions.precomputedRankColumns = precomputedRankColumns;
        }
      } else {
        runSortOptions.precomputedIndexKeys = buildPrecomputedSortKeyColumns(
          indices,
          rowsToSort,
          activeDescriptors,
          columnTypeByKey
        );
      }

      result = controller.sortIndices(
        indices,
        rowsByIndex,
        effectiveSortMode,
        runSortOptions
      );
    } else {
      if (
        Array.isArray(sourceRows) &&
        sourceRows.__rowsMaterialized === false &&
        hasIndexCollection(snapshotRowIndices) &&
        snapshotRowIndices.length === snapshotCount
      ) {
        rowsToSort = new Array(snapshotCount);
        for (let i = 0; i < snapshotCount; i += 1) {
          rowsToSort[i] = allRows[snapshotRowIndices[i]];
        }
      } else {
        rowsToSort = sourceRows.slice();
      }

      result = controller.sortRows(rowsToSort, effectiveSortMode, sortOptions);
    }
    const sortTotalMs = now() - sortTotalStartMs;
    const sortCoreMs = Number(result.durationMs);
    const sortPrepMs = sortTotalMs - sortCoreMs;
    const sortedCount = sortOptions.useIndexSort
      ? hasIndexCollection(snapshotRowIndices)
        ? snapshotRowIndices.length
        : snapshotCount
      : rowsToSort && typeof rowsToSort.length === "number"
        ? rowsToSort.length
        : snapshotCount;

    return {
      sortMs: sortCoreMs,
      sortCoreMs,
      sortPrepMs,
      sortTotalMs,
      sortMode: result.sortMode,
      sortedCount,
      descriptors: result.effectiveDescriptors,
      dataPath: result.dataPath,
      comparatorMode: result.comparatorMode,
    };
  }

  function getNumericColumnarForSave() {
    if (!hasData()) {
      return null;
    }

    return ensureNumericColumnarData();
  }

  function getLastFilterResult() {
    return lastFilterResult;
  }

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
    getNumericColumnarForSave,
  };
}

export { createFastTableRuntime };
