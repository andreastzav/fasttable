import {
  buildFilterOptionsFromModeOptions,
  createEngineRuntimeOperations,
} from "./runtime-operations.js";

function clonePlainObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }

  return { ...value };
}

function ensureRuntimeObject(runtime) {
  if (!runtime || typeof runtime !== "object") {
    throw new Error(
      "Engine runtime is required for core execution paths."
    );
  }

  return runtime;
}

function assertRuntimeMethod(runtime, methodName) {
  if (!runtime || typeof runtime[methodName] !== "function") {
    throw new Error(`Engine runtime method is unavailable: ${methodName}`);
  }
}

function createFastTableEngine(options) {
  const input = options || {};
  const adapters = input.adapters || {};
  const runtime = input.runtime && typeof input.runtime === "object" ? input.runtime : null;
  const listeners = new Set();

  const state = {
    busy: false,
    rowCount: 0,
    modeOptions: {},
    rawFilters: {},
    sortMode: "native",
    sortOptions: {},
    lastAction: null,
    lastResult: null,
    lastError: null,
  };

  function getRuntime() {
    return ensureRuntimeObject(runtime);
  }

  function validateRuntimeCapabilities() {
    const runtimeApi = getRuntime();
    const requiredRuntimeMethods = [
      "hasData",
      "getRowCount",
      "getModeOptions",
      "setModeOptions",
      "getRawFilters",
      "setRawFilters",
      "setSingleFilter",
      "clearFilters",
      "runFilterPassWithRawFilters",
      "runSingleFilterPass",
      "getSortModes",
      "getSortMode",
      "setSortMode",
      "getSortOptions",
      "setSortOptions",
      "buildSortRowsSnapshot",
      "runSortSnapshotPass",
    ];

    for (let i = 0; i < requiredRuntimeMethods.length; i += 1) {
      assertRuntimeMethod(runtimeApi, requiredRuntimeMethods[i]);
    }
  }

  function callRuntime(methodName, args) {
    const runtimeApi = getRuntime();
    assertRuntimeMethod(runtimeApi, methodName);

    return runtimeApi[methodName](...(Array.isArray(args) ? args : []));
  }

  function hasRuntimeMethod(methodName) {
    const runtimeApi = getRuntime();
    return typeof runtimeApi[methodName] === "function";
  }

  function readModeOptions() {
    return clonePlainObject(callRuntime("getModeOptions", []));
  }

  function readRawFilters() {
    return clonePlainObject(callRuntime("getRawFilters", []));
  }

  function readSortMode() {
    return String(callRuntime("getSortMode", []) || "native");
  }

  function readSortOptions() {
    return clonePlainObject(callRuntime("getSortOptions", []));
  }

  function readRowCount() {
    return Number(callRuntime("getRowCount", [])) || 0;
  }

  function readSortModes() {
    const source = callRuntime("getSortModes", []);
    return Array.isArray(source) ? source.slice() : [];
  }

  function buildSnapshot() {
    return {
      busy: state.busy,
      rowCount: state.rowCount,
      modeOptions: clonePlainObject(state.modeOptions),
      rawFilters: clonePlainObject(state.rawFilters),
      sortMode: state.sortMode,
      sortOptions: clonePlainObject(state.sortOptions),
      lastAction: state.lastAction,
      lastResult: state.lastResult,
      lastError: state.lastError,
    };
  }

  function notify() {
    const snapshot = buildSnapshot();
    listeners.forEach((listener) => {
      try {
        listener(snapshot);
      } catch (_error) {
        // Ignore listener failures so engine actions remain stable.
      }
    });
  }

  function refreshFromAdapters() {
    state.rowCount = readRowCount();
    state.modeOptions = readModeOptions();
    state.rawFilters = readRawFilters();
    state.sortMode = readSortMode();
    state.sortOptions = readSortOptions();
  }

  function onState(listener) {
    if (typeof listener !== "function") {
      return function noop() {};
    }

    listeners.add(listener);
    return function unsubscribe() {
      listeners.delete(listener);
    };
  }

  function getState() {
    refreshFromAdapters();
    return buildSnapshot();
  }

  function withActionState(actionName, callback) {
    state.busy = true;
    state.lastAction = actionName;
    state.lastError = null;
    notify();

    try {
      const result = callback();
      state.lastResult = result;
      refreshFromAdapters();
      state.busy = false;
      notify();
      return result;
    } catch (error) {
      state.lastError = String(error && error.message ? error.message : error);
      state.busy = false;
      notify();
      throw error;
    }
  }

  async function withActionStateAsync(actionName, callback) {
    state.busy = true;
    state.lastAction = actionName;
    state.lastError = null;
    notify();

    try {
      const result = await callback();
      state.lastResult = result;
      refreshFromAdapters();
      state.busy = false;
      notify();
      return result;
    } catch (error) {
      state.lastError = String(error && error.message ? error.message : error);
      state.busy = false;
      notify();
      throw error;
    }
  }

  function setModeOptions(nextOptions, switchOptions) {
    return withActionState("setModeOptions", () => {
      callRuntime("setModeOptions", [nextOptions || {}, switchOptions]);
      refreshFromAdapters();
      return buildSnapshot().modeOptions;
    });
  }

  function setRawFilters(rawFilters) {
    return withActionState("setRawFilters", () => {
      callRuntime("setRawFilters", [rawFilters || {}]);
      refreshFromAdapters();
      return buildSnapshot().rawFilters;
    });
  }

  function clearFilters() {
    return withActionState("clearFilters", () => {
      callRuntime("clearFilters", []);
      refreshFromAdapters();
      return buildSnapshot().rawFilters;
    });
  }

  function setSortOptions(nextOptions) {
    return withActionState("setSortOptions", () => {
      callRuntime("setSortOptions", [nextOptions || {}]);
      refreshFromAdapters();
      return buildSnapshot().sortOptions;
    });
  }

  function setSortMode(nextSortMode) {
    return withActionState("setSortMode", () => {
      callRuntime("setSortMode", [nextSortMode || ""]);
      refreshFromAdapters();
      return buildSnapshot().sortMode;
    });
  }

  function applyFilters(options) {
    return withActionState("applyFilters", () => {
      const sourceOptions =
        options && typeof options === "object" ? options : {};
      const sourceRawFilters =
        sourceOptions.rawFilters && typeof sourceOptions.rawFilters === "object"
          ? sourceOptions.rawFilters
          : callRuntime("getRawFilters", []);
      return callRuntime("runFilterPassWithRawFilters", [
        sourceRawFilters || {},
        sourceOptions,
      ]);
    });
  }

  function executeFilterCore(rawFilters, options) {
    return withActionState("applyFilters", () =>
      callRuntime("runFilterPassWithRawFilters", [rawFilters || {}, options])
    );
  }

  function runFilterPassWithRawFilters(rawFilters, options) {
    return executeFilterCore(rawFilters, options);
  }

  function executeSingleFilterCore(columnKey, value, options) {
    return withActionState("applySingleFilter", () =>
      callRuntime("runSingleFilterPass", [columnKey, value, options])
    );
  }

  function applySingleFilter(columnKey, value, options) {
    return executeSingleFilterCore(columnKey, value, options);
  }

  function applySort(runOptions) {
    return withActionState("applySort", () => {
      const options =
        runOptions && typeof runOptions === "object" ? runOptions : {};
      const rawFilters =
        options.rawFilters && typeof options.rawFilters === "object"
          ? options.rawFilters
          : callRuntime("getRawFilters", []);
      const rowsSnapshot = buildSortRowsSnapshot(rawFilters);
      const descriptors = Array.isArray(options.descriptors)
        ? options.descriptors
        : [];
      const sortMode =
        typeof options.sortMode === "string" && options.sortMode !== ""
          ? options.sortMode
          : getSortMode();
      return callRuntime("runSortSnapshotPass", [
        rowsSnapshot,
        descriptors,
        sortMode,
      ]);
    });
  }

  function buildSortRowsSnapshot(rawFilters) {
    return callRuntime("buildSortRowsSnapshot", [rawFilters]);
  }

  function executeSortCore(rowsSnapshot, descriptors, sortMode) {
    return withActionState("applySort", () =>
      callRuntime("runSortSnapshotPass", [rowsSnapshot, descriptors, sortMode])
    );
  }

  function runSortSnapshotPass(rowsSnapshot, descriptors, sortMode) {
    return executeSortCore(rowsSnapshot, descriptors, sortMode);
  }

  function restoreStateCore(statePatch) {
    return withActionState("restoreState", () => {
      const patch =
        statePatch && typeof statePatch === "object" ? statePatch : {};
      if (Object.prototype.hasOwnProperty.call(patch, "modeOptions")) {
        callRuntime("setModeOptions", [
          patch.modeOptions || {},
          {
            suppressFilterPass: true,
          },
        ]);
      }
      if (Object.prototype.hasOwnProperty.call(patch, "rawFilters")) {
        callRuntime("setRawFilters", [patch.rawFilters || {}]);
      }
      if (Object.prototype.hasOwnProperty.call(patch, "sortOptions")) {
        callRuntime("setSortOptions", [patch.sortOptions || {}]);
      }
      if (Object.prototype.hasOwnProperty.call(patch, "sortMode")) {
        callRuntime("setSortMode", [patch.sortMode || ""]);
      }
      refreshFromAdapters();
      return buildSnapshot();
    });
  }

  function hasData() {
    return callRuntime("hasData", []);
  }

  function getRowCount() {
    return readRowCount();
  }

  function getModeOptions() {
    return readModeOptions();
  }

  function getRawFilters() {
    return readRawFilters();
  }

  function getSortMode() {
    return readSortMode();
  }

  function getSortModes() {
    return readSortModes();
  }

  function getSortOptions() {
    return readSortOptions();
  }

  function setDataFromRows(rows) {
    return withActionState("setDataFromRows", () => {
      callRuntime("setDataFromRows", [rows]);
      refreshFromAdapters();
      return buildSnapshot().rowCount;
    });
  }

  function setDataFromNumericColumnar(numericColumnarData) {
    return withActionState("setDataFromNumericColumnar", () => {
      callRuntime("setDataFromNumericColumnar", [numericColumnarData]);
      refreshFromAdapters();
      return buildSnapshot().rowCount;
    });
  }

  function hasTopLevelFilterCacheEntries() {
    if (!hasRuntimeMethod("hasTopLevelFilterCacheEntries")) {
      return false;
    }
    return callRuntime("hasTopLevelFilterCacheEntries", []);
  }

  function clearTopLevelFilterCache() {
    return withActionState("clearTopLevelFilterCache", () => {
      if (hasRuntimeMethod("clearTopLevelFilterCache")) {
        callRuntime("clearTopLevelFilterCache", []);
      }
      refreshFromAdapters();
      return hasTopLevelFilterCacheEntries();
    });
  }

  function clearTopLevelSmartFilterState() {
    return withActionState("clearTopLevelSmartFilterState", () => {
      if (hasRuntimeMethod("clearTopLevelSmartFilterState")) {
        callRuntime("clearTopLevelSmartFilterState", []);
      }
      refreshFromAdapters();
      return true;
    });
  }

  function clearAllFilterCaches() {
    return withActionState("clearAllFilterCaches", () => {
      if (hasRuntimeMethod("clearAllFilterCaches")) {
        callRuntime("clearAllFilterCaches", []);
      }
      refreshFromAdapters();
      return true;
    });
  }

  function bumpTopLevelFilterCacheRevision() {
    if (hasRuntimeMethod("bumpTopLevelFilterCacheRevision")) {
      callRuntime("bumpTopLevelFilterCacheRevision", []);
    }
  }

  function prewarmPrecomputedSortState() {
    if (!hasRuntimeMethod("prewarmPrecomputedSortState")) {
      return false;
    }
    return callRuntime("prewarmPrecomputedSortState", []);
  }

  function getNumericColumnarForSave() {
    if (hasRuntimeMethod("getNumericColumnarForSave")) {
      return callRuntime("getNumericColumnarForSave", []);
    }
    if (typeof adapters.getNumericColumnarForSave === "function") {
      return adapters.getNumericColumnarForSave();
    }
    return null;
  }

  async function generate(generateOptions) {
    if (typeof adapters.generate !== "function") {
      throw new Error("Engine generate adapter is unavailable.");
    }

    return withActionStateAsync("generate", () =>
      adapters.generate(generateOptions || {})
    );
  }

  function createRuntimeOperations(operationOptions) {
    const inputOptions =
      operationOptions && typeof operationOptions === "object"
        ? operationOptions
        : {};
    return createEngineRuntimeOperations({
      engine: engineApi,
      getRowCount:
        typeof inputOptions.getRowCount === "function"
          ? inputOptions.getRowCount
          : getRowCount,
      getRawFilters:
        typeof inputOptions.getRawFilters === "function"
          ? inputOptions.getRawFilters
          : getRawFilters,
      getFilterOptions:
        typeof inputOptions.getFilterOptions === "function"
          ? inputOptions.getFilterOptions
          : function defaultGetFilterOptions() {
              return buildFilterOptionsFromModeOptions(getModeOptions());
            },
      getCurrentFilterModeKey:
        typeof inputOptions.getCurrentFilterModeKey === "function"
          ? inputOptions.getCurrentFilterModeKey
          : function defaultGetCurrentFilterModeKey() {
              return "";
            },
      setLastFilterMode:
        typeof inputOptions.setLastFilterMode === "function"
          ? inputOptions.setLastFilterMode
          : null,
      getSortDescriptors:
        typeof inputOptions.getSortDescriptors === "function"
          ? inputOptions.getSortDescriptors
          : function defaultGetSortDescriptors() {
              return [];
            },
      getSortMode:
        typeof inputOptions.getSortMode === "function"
          ? inputOptions.getSortMode
          : getSortMode,
      syncState:
        typeof inputOptions.syncState === "function"
          ? inputOptions.syncState
          : null,
      defaultPreferPrecomputedFastPath:
        inputOptions.defaultPreferPrecomputedFastPath !== false,
    });
  }

  function createIOBridge(ioOptions) {
    const options = ioOptions || {};
    const schemaFactory =
      typeof options.getSchema === "function"
        ? options.getSchema
        : function getSchemaFallback() {
            return null;
          };

    return {
      hasRows() {
        return hasData();
      },
      getRowCount() {
        return getRowCount();
      },
      getSchema() {
        return schemaFactory();
      },
      getNumericColumnarForSave() {
        return getNumericColumnarForSave();
      },
      applyLoadedColumnarBinaryDataset(loadedRows, loadDurationMs, loadTimingDetails) {
        if (typeof adapters.applyLoadedColumnarBinaryDataset !== "function") {
          return;
        }

        withActionState("loadObjectColumnarDataset", () => {
          adapters.applyLoadedColumnarBinaryDataset(
            loadedRows,
            loadDurationMs,
            loadTimingDetails
          );
        });
      },
      applyLoadedNumericColumnarDataset(
        numericColumnarData,
        loadDurationMs,
        loadTimingDetails
      ) {
        if (typeof adapters.applyLoadedNumericColumnarDataset !== "function") {
          return;
        }

        withActionState("loadNumericColumnarDataset", () => {
          adapters.applyLoadedNumericColumnarDataset(
            numericColumnarData,
            loadDurationMs,
            loadTimingDetails
          );
        });
      },
      setGenerationError(message) {
        if (typeof adapters.setGenerationError === "function") {
          adapters.setGenerationError(message);
        }

        state.lastError = String(message || "");
        notify();
      },
    };
  }

  function createBenchmarkApi() {
    const benchmarkRuntimeOperations = createRuntimeOperations({
      getRowCount,
      getRawFilters,
      getFilterOptions() {
        return buildFilterOptionsFromModeOptions(getModeOptions());
      },
      getSortDescriptors() {
        return [];
      },
      getSortMode,
      defaultPreferPrecomputedFastPath: true,
    });

    return {
      hasData() {
        return hasData();
      },
      getRowCount() {
        return getRowCount();
      },
      getModeOptions() {
        return getModeOptions();
      },
      setModeOptions(nextOptions, switchOptions) {
        return setModeOptions(nextOptions, switchOptions);
      },
      getRawFilters() {
        return getRawFilters();
      },
      setRawFilters(rawFilters) {
        return setRawFilters(rawFilters);
      },
      setSingleFilter(columnKey, value) {
        callRuntime("setSingleFilter", [columnKey, value]);
        refreshFromAdapters();
        notify();
      },
      clearFilters() {
        return clearFilters();
      },
      runFilterPass(options) {
        return applyFilters(options);
      },
      runSingleFilterPass(columnKey, value, options) {
        return applySingleFilter(columnKey, value, options);
      },
      runFilterPassWithRawFilters(rawFilters, options) {
        return runFilterPassWithRawFilters(rawFilters, options);
      },
      executeFilterCore(rawFilters, options) {
        return executeFilterCore(rawFilters, options);
      },
      runFilterCore(rawFilters, options) {
        return benchmarkRuntimeOperations.runFilterCore(rawFilters, options);
      },
      getSortModes() {
        return getSortModes();
      },
      getSortMode() {
        return getSortMode();
      },
      getSortOptions() {
        return getSortOptions();
      },
      setSortOptions(nextOptions) {
        return setSortOptions(nextOptions);
      },
      buildSortRowsSnapshot(rawFilters) {
        return buildSortRowsSnapshot(rawFilters);
      },
      runSortSnapshotPass(rowsSnapshot, descriptors, sortMode) {
        return runSortSnapshotPass(rowsSnapshot, descriptors, sortMode);
      },
      executeSortCore(rowsSnapshot, descriptors, sortMode) {
        return executeSortCore(rowsSnapshot, descriptors, sortMode);
      },
      runSortSnapshotCore(rowsSnapshot, descriptors, sortMode, options) {
        const runOptions =
          options && typeof options === "object" ? { ...options } : {};
        runOptions.descriptors = Array.isArray(descriptors) ? descriptors : [];
        if (typeof sortMode === "string" && sortMode !== "") {
          runOptions.sortMode = sortMode;
        }
        return benchmarkRuntimeOperations.runSortSnapshotCore(
          rowsSnapshot,
          runOptions
        );
      },
      runSortCore(filterResult, descriptors, sortMode, options) {
        const runOptions =
          options && typeof options === "object" ? { ...options } : {};
        runOptions.descriptors = Array.isArray(descriptors) ? descriptors : [];
        if (typeof sortMode === "string" && sortMode !== "") {
          runOptions.sortMode = sortMode;
        }
        return benchmarkRuntimeOperations.runSortCore(filterResult, runOptions);
      },
      runFilterSortCore(rawFilters, descriptors, sortMode, options) {
        const runOptions =
          options && typeof options === "object" ? { ...options } : {};
        runOptions.descriptors = Array.isArray(descriptors) ? descriptors : [];
        if (typeof sortMode === "string" && sortMode !== "") {
          runOptions.sortMode = sortMode;
        }
        return benchmarkRuntimeOperations.runFilterSortCore(rawFilters, runOptions);
      },
      prewarmPrecomputedSortState() {
        return prewarmPrecomputedSortState();
      },
      isTimSortAvailable() {
        const sortModes = getSortModes();
        return Array.isArray(sortModes) && sortModes.includes("timsort");
      },
      restoreStateCore(statePatch) {
        return restoreStateCore(statePatch);
      },
    };
  }

  validateRuntimeCapabilities();
  refreshFromAdapters();

  const engineApi = {
    onState,
    getState,
    hasData,
    getRowCount,
    getModeOptions,
    setModeOptions,
    getRawFilters,
    setRawFilters,
    clearFilters,
    applyFilters,
    executeFilterCore,
    runFilterPassWithRawFilters,
    applySingleFilter,
    getSortModes,
    getSortMode,
    setSortMode,
    getSortOptions,
    setSortOptions,
    applySort,
    buildSortRowsSnapshot,
    runSortSnapshotPass,
    executeSortCore,
    setDataFromRows,
    setDataFromNumericColumnar,
    hasTopLevelFilterCacheEntries,
    clearTopLevelFilterCache,
    clearTopLevelSmartFilterState,
    clearAllFilterCaches,
    bumpTopLevelFilterCacheRevision,
    prewarmPrecomputedSortState,
    getNumericColumnarForSave,
    restoreStateCore,
    createRuntimeOperations,
    generate,
    createIOBridge,
    createBenchmarkApi,
  };

  return engineApi;
}

export { createFastTableEngine };
