import { createFilteringOrchestrator } from "./filtering-orchestration.js";

const DEFAULT_MODE_PATH_BY_KEY = Object.freeze({
  "binary-columnar": "numeric-columnar",
  "object-columnar": "object-columnar",
  "numeric-row": "numeric-row",
  "object-row": "object-row",
});

function defaultNow() {
  if (typeof performance !== "undefined" && typeof performance.now === "function") {
    return performance.now();
  }

  return Date.now();
}

function defaultNormalizeRawFilters(rawFilters) {
  return rawFilters && typeof rawFilters === "object" ? rawFilters : {};
}

function normalizeFilterOptions(sourceOptions, fallbackOptions) {
  const source =
    sourceOptions && typeof sourceOptions === "object"
      ? sourceOptions
      : fallbackOptions && typeof fallbackOptions === "object"
        ? fallbackOptions
        : {};

  return {
    enableCaching: source.enableCaching === true,
    useDictionaryKeySearch: source.useDictionaryKeySearch === true,
    useDictionaryIntersection: source.useDictionaryIntersection === true,
    useSmarterPlanner: source.useSmarterPlanner === true,
    useSmartFiltering: source.useSmartFiltering === true,
    useFilterCache: source.useFilterCache === true,
  };
}

function resolveModePath(modeKey, modePathByKey) {
  if (
    modePathByKey &&
    typeof modePathByKey === "object" &&
    typeof modePathByKey[modeKey] === "string" &&
    modePathByKey[modeKey] !== ""
  ) {
    return modePathByKey[modeKey];
  }

  if (typeof DEFAULT_MODE_PATH_BY_KEY[modeKey] === "string") {
    return DEFAULT_MODE_PATH_BY_KEY[modeKey];
  }

  return "object-row";
}

function createFilterRuntimeBridge(options) {
  const input = options || {};
  const now = typeof input.now === "function" ? input.now : defaultNow;
  const getLoadedRowCount =
    typeof input.getLoadedRowCount === "function"
      ? input.getLoadedRowCount
      : () => 0;
  const getCurrentFilterModeKey =
    typeof input.getCurrentFilterModeKey === "function"
      ? input.getCurrentFilterModeKey
      : () => "object-row";
  const getFilterOptions =
    typeof input.getFilterOptions === "function"
      ? input.getFilterOptions
      : () => ({});
  const getRawFilters =
    typeof input.getRawFilters === "function" ? input.getRawFilters : () => ({});
  const normalizeRawFilters =
    typeof input.normalizeRawFilters === "function"
      ? input.normalizeRawFilters
      : defaultNormalizeRawFilters;
  const modePathByKey =
    input.modePathByKey && typeof input.modePathByKey === "object"
      ? input.modePathByKey
      : DEFAULT_MODE_PATH_BY_KEY;

  const controllers =
    input.controllers && typeof input.controllers === "object"
      ? input.controllers
      : {};
  const dataAccessors =
    input.dataAccessors && typeof input.dataAccessors === "object"
      ? input.dataAccessors
      : {};
  const buildDictionaryKeySearchPlanAdapter =
    typeof input.buildDictionaryKeySearchPlan === "function"
      ? input.buildDictionaryKeySearchPlan
      : null;
  const buildDictionaryKeySearchPrefilter =
    typeof input.buildDictionaryKeySearchPrefilter === "function"
      ? input.buildDictionaryKeySearchPrefilter
      : null;
  const keyToIndex =
    input.keyToIndex && typeof input.keyToIndex === "object"
      ? input.keyToIndex
      : null;
  const getNumericColumnarDataForDictionary =
    typeof input.getNumericColumnarDataForDictionary === "function"
      ? input.getNumericColumnarDataForDictionary
      : typeof dataAccessors.getNumericColumnarData === "function"
        ? dataAccessors.getNumericColumnarData
        : () => null;
  const isValidNumericColumnarData =
    typeof input.isValidNumericColumnarData === "function"
      ? input.isValidNumericColumnarData
      : (data) =>
          !!(
            data &&
            typeof data === "object" &&
            Number.isInteger(data.rowCount) &&
            data.rowCount >= 0 &&
            Array.isArray(data.columns)
          );
  const syncAllControllerIndices = input.syncAllControllerIndices === true;

  function getCurrentModeKey() {
    const modeKey = String(getCurrentFilterModeKey() || "");
    if (modeKey !== "") {
      return modeKey;
    }
    return "object-row";
  }

  function getControllerBundleForMode(modeKey) {
    const resolvedModeKey = typeof modeKey === "string" ? modeKey : getCurrentModeKey();
    const modePath = resolveModePath(resolvedModeKey, modePathByKey);

    if (resolvedModeKey === "binary-columnar") {
      const controller = controllers.numericColumnar || null;
      const data =
        typeof dataAccessors.getNumericColumnarData === "function"
          ? dataAccessors.getNumericColumnarData()
          : null;
      if (controller && typeof controller.setData === "function") {
        controller.setData(data);
      }
      return {
        modeKey: resolvedModeKey,
        modePath,
        controller,
        columnarData:
          controller && typeof controller.getData === "function"
            ? controller.getData()
            : data,
      };
    }

    if (resolvedModeKey === "object-columnar") {
      const controller = controllers.objectColumnar || null;
      const data =
        typeof dataAccessors.getObjectColumnarData === "function"
          ? dataAccessors.getObjectColumnarData()
          : null;
      if (controller && typeof controller.setData === "function") {
        controller.setData(data);
      }
      return {
        modeKey: resolvedModeKey,
        modePath,
        controller,
        columnarData:
          controller && typeof controller.getData === "function"
            ? controller.getData()
            : data,
      };
    }

    if (resolvedModeKey === "numeric-row") {
      const controller = controllers.numericRow || null;
      const rows =
        typeof dataAccessors.getNumericRows === "function"
          ? dataAccessors.getNumericRows()
          : [];
      if (controller && typeof controller.setData === "function") {
        controller.setData(rows);
      } else if (controller && typeof controller.setRows === "function") {
        controller.setRows(rows);
      }
      return {
        modeKey: resolvedModeKey,
        modePath,
        controller,
        numericData:
          controller && typeof controller.getData === "function"
            ? controller.getData()
            : rows,
      };
    }

    const controller = controllers.objectRow || null;
    const rows =
      typeof dataAccessors.getObjectRows === "function"
        ? dataAccessors.getObjectRows()
        : [];
    if (controller && typeof controller.setRows === "function") {
      controller.setRows(rows);
    }
    return {
      modeKey: "object-row",
      modePath: resolveModePath("object-row", modePathByKey),
      controller,
      rows,
    };
  }

  function setControllerCurrentIndices(controller, filteredIndices) {
    if (controller && typeof controller.setCurrentIndices === "function") {
      controller.setCurrentIndices(filteredIndices);
    }
  }

  function buildFilterResult(modeBundle, filteredIndices, filteredCount) {
    const bundle = modeBundle || getControllerBundleForMode();
    const result = {
      modePath: bundle.modePath,
      filteredCount: Math.max(0, Number(filteredCount) || 0),
      filteredIndices,
    };

    if (bundle.modeKey === "binary-columnar" || bundle.modeKey === "object-columnar") {
      result.columnarData = bundle.columnarData;
    } else if (bundle.modeKey === "numeric-row") {
      result.numericData = bundle.numericData;
    } else {
      result.rows = bundle.rows;
    }

    return result;
  }

  function buildFilterResultFromCachedEntry(cachedEntry) {
    if (!cachedEntry || typeof cachedEntry !== "object") {
      return null;
    }

    const bundle = getControllerBundleForMode();
    return buildFilterResult(
      bundle,
      cachedEntry.filteredIndices === undefined ? null : cachedEntry.filteredIndices,
      cachedEntry.filteredCount
    );
  }

  function syncActiveControllerIndices(filteredIndices) {
    if (syncAllControllerIndices) {
      setControllerCurrentIndices(controllers.objectRow, filteredIndices);
      setControllerCurrentIndices(controllers.objectColumnar, filteredIndices);
      setControllerCurrentIndices(controllers.numericRow, filteredIndices);
      setControllerCurrentIndices(controllers.numericColumnar, filteredIndices);
      return;
    }

    const bundle = getControllerBundleForMode();
    setControllerCurrentIndices(bundle.controller, filteredIndices);
  }

  function applyFilterPath(effectiveRawFilters, filterOptions, baseIndices) {
    const bundle = getControllerBundleForMode();
    const controller = bundle.controller;
    if (!controller || typeof controller.apply !== "function") {
      throw new Error("Filter runtime bridge requires an active filter controller.");
    }

    const controllerOptions = {
      enableCaching: filterOptions && filterOptions.enableCaching === true,
      useSmarterPlanner: filterOptions && filterOptions.useSmarterPlanner === true,
    };
    if (baseIndices !== undefined) {
      controllerOptions.baseIndices = baseIndices;
    }

    const filteredIndices = controller.apply(effectiveRawFilters, controllerOptions);
    const filteredCount =
      controller && typeof controller.getCurrentCount === "function"
        ? controller.getCurrentCount()
        : filteredIndices && typeof filteredIndices.count === "number"
          ? filteredIndices.count
          : Array.isArray(filteredIndices) || ArrayBuffer.isView(filteredIndices)
            ? filteredIndices.length
            : 0;

    return buildFilterResult(bundle, filteredIndices, filteredCount);
  }

  function buildDictionaryKeySearchPlan(rawFilters, filterOptions) {
    if (buildDictionaryKeySearchPlanAdapter) {
      return buildDictionaryKeySearchPlanAdapter(rawFilters, filterOptions);
    }

    if (
      !filterOptions ||
      filterOptions.useDictionaryKeySearch !== true ||
      typeof buildDictionaryKeySearchPrefilter !== "function" ||
      !keyToIndex
    ) {
      return null;
    }

    const modeKey = getCurrentModeKey();
    if (modeKey !== "binary-columnar" && modeKey !== "numeric-row") {
      return null;
    }

    const numericColumnarData = getNumericColumnarDataForDictionary();
    if (!isValidNumericColumnarData(numericColumnarData)) {
      return null;
    }

    return buildDictionaryKeySearchPrefilter(rawFilters || {}, numericColumnarData, {
      useDictionaryKeySearch: true,
      useDictionaryIntersection: filterOptions.useDictionaryIntersection === true,
      useSmarterPlanner: filterOptions.useSmarterPlanner === true,
      keyToIndex,
    });
  }

  const filteringOrchestrator = createFilteringOrchestrator({
    now,
    getLoadedRowCount,
    getCurrentFilterModeKey: getCurrentModeKey,
    buildDictionaryKeySearchPlan,
    buildFilterResultFromCachedEntry,
    syncActiveControllerIndices,
    applyFilterPath,
    onCacheStateChange:
      typeof input.onCacheStateChange === "function" ? input.onCacheStateChange : null,
    topLevelFilterCacheMinInsertMs: input.topLevelFilterCacheMinInsertMs,
    topLevelFilterCacheSmallMaxResults: input.topLevelFilterCacheSmallMaxResults,
    topLevelFilterCacheMediumMaxResults: input.topLevelFilterCacheMediumMaxResults,
    topLevelFilterCacheSmallCapacity: input.topLevelFilterCacheSmallCapacity,
    topLevelFilterCacheMediumCapacity: input.topLevelFilterCacheMediumCapacity,
  });

  function hasData() {
    return Math.max(0, Number(getLoadedRowCount()) | 0) > 0;
  }

  function runFilterPassWithRawFilters(nextRawFilters, options) {
    if (!hasData()) {
      return null;
    }

    const executionOptions = options || {};
    const sourceRawFilters = normalizeRawFilters(nextRawFilters);
    const fallbackFilterOptions =
      executionOptions && typeof executionOptions.filterOptions === "object"
        ? executionOptions.filterOptions
        : getFilterOptions();
    const filterOptions = normalizeFilterOptions(
      executionOptions.filterOptions,
      fallbackFilterOptions
    );
    const orchestration = filteringOrchestrator.runFilterPass(sourceRawFilters, {
      filterOptions,
    });
    const filterResult = orchestration ? orchestration.filterResult : null;
    if (!filterResult) {
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
      active: orchestration && orchestration.active === true,
      selectedBaseCandidateCount:
        orchestration && Number.isFinite(orchestration.selectedBaseCandidateCount)
          ? Number(orchestration.selectedBaseCandidateCount)
          : -1,
      topLevelCacheEvent:
        orchestration && orchestration.topLevelCacheEvent
          ? orchestration.topLevelCacheEvent
          : null,
      fullActiveFilters:
        orchestration && orchestration.fullActiveFilters
          ? orchestration.fullActiveFilters
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

    return result;
  }

  function runFilterPass(options) {
    return runFilterPassWithRawFilters(getRawFilters(), options);
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

    return runFilterPassWithRawFilters(singleFilters, options);
  }

  return {
    hasData,
    runFilterPassWithRawFilters,
    runFilterPass,
    runSingleFilterPass,
    getCurrentFilterModeKey: getCurrentModeKey,
    buildDictionaryKeySearchPlan,
    buildFilterResultFromCachedEntry,
    syncActiveControllerIndices,
    applyFilterPath,
    hasTopLevelFilterCacheEntries:
      filteringOrchestrator.hasTopLevelFilterCacheEntries,
    clearTopLevelFilterCache: filteringOrchestrator.clearTopLevelFilterCache,
    clearTopLevelSmartFilterState: filteringOrchestrator.clearTopLevelSmartFilterState,
    clearAllFilterCaches: filteringOrchestrator.clearAllFilterCaches,
    bumpTopLevelFilterCacheRevision:
      filteringOrchestrator.bumpTopLevelFilterCacheRevision,
    getTopLevelFilterCacheSnapshot:
      filteringOrchestrator.getTopLevelFilterCacheSnapshot,
  };
}

export { createFilterRuntimeBridge };
