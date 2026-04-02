function clonePlainObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }

  return { ...value };
}

function readSortModesFallback() {
  return ["native"];
}

function createFastTableEngine(options) {
  const input = options || {};
  const adapters = input.adapters || {};
  const listeners = new Set();

  const state = {
    busy: false,
    rowCount:
      typeof adapters.getRowCount === "function"
        ? Number(adapters.getRowCount()) || 0
        : 0,
    modeOptions:
      typeof adapters.getModeOptions === "function"
        ? clonePlainObject(adapters.getModeOptions())
        : {},
    rawFilters:
      typeof adapters.getRawFilters === "function"
        ? clonePlainObject(adapters.getRawFilters())
        : {},
    sortMode:
      typeof adapters.getSortMode === "function"
        ? String(adapters.getSortMode() || "native")
        : "native",
    sortOptions:
      typeof adapters.getSortOptions === "function"
        ? clonePlainObject(adapters.getSortOptions())
        : {},
    lastAction: null,
    lastResult: null,
    lastError: null,
  };

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
    if (typeof adapters.getRowCount === "function") {
      state.rowCount = Number(adapters.getRowCount()) || 0;
    }
    if (typeof adapters.getModeOptions === "function") {
      state.modeOptions = clonePlainObject(adapters.getModeOptions());
    }
    if (typeof adapters.getRawFilters === "function") {
      state.rawFilters = clonePlainObject(adapters.getRawFilters());
    }
    if (typeof adapters.getSortMode === "function") {
      state.sortMode = String(adapters.getSortMode() || "native");
    }
    if (typeof adapters.getSortOptions === "function") {
      state.sortOptions = clonePlainObject(adapters.getSortOptions());
    }
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
    if (typeof adapters.setModeOptions !== "function") {
      return getState().modeOptions;
    }

    return withActionState("setModeOptions", () => {
      adapters.setModeOptions(nextOptions || {}, switchOptions);
      refreshFromAdapters();
      return buildSnapshot().modeOptions;
    });
  }

  function setRawFilters(rawFilters) {
    if (typeof adapters.setRawFilters !== "function") {
      return getState().rawFilters;
    }

    return withActionState("setRawFilters", () => {
      adapters.setRawFilters(rawFilters || {});
      refreshFromAdapters();
      return buildSnapshot().rawFilters;
    });
  }

  function clearFilters() {
    if (typeof adapters.clearFilters !== "function") {
      return getState().rawFilters;
    }

    return withActionState("clearFilters", () => {
      adapters.clearFilters();
      refreshFromAdapters();
      return buildSnapshot().rawFilters;
    });
  }

  function applyFilters(options) {
    if (typeof adapters.runFilterPass !== "function") {
      return null;
    }

    return withActionState("applyFilters", () => adapters.runFilterPass(options));
  }

  function applySingleFilter(columnKey, value, options) {
    if (typeof adapters.runSingleFilterPass === "function") {
      return withActionState("applySingleFilter", () =>
        adapters.runSingleFilterPass(columnKey, value, options)
      );
    }

    if (typeof adapters.setSingleFilter === "function") {
      adapters.setSingleFilter(columnKey, value);
    }

    if (typeof adapters.runFilterPassWithRawFilters === "function") {
      const rawFilters =
        typeof adapters.getRawFilters === "function"
          ? adapters.getRawFilters()
          : {};
      return withActionState("applySingleFilter", () =>
        adapters.runFilterPassWithRawFilters(rawFilters, options)
      );
    }

    return null;
  }

  function applySort(runOptions) {
    if (
      typeof adapters.buildSortRowsSnapshot !== "function" ||
      typeof adapters.runSortSnapshotPass !== "function"
    ) {
      return null;
    }

    const options = runOptions || {};
    return withActionState("applySort", () => {
      const rawFilters =
        options.rawFilters ||
        (typeof adapters.getRawFilters === "function"
          ? adapters.getRawFilters()
          : {});
      const rowsSnapshot = adapters.buildSortRowsSnapshot(rawFilters);
      const descriptors = Array.isArray(options.descriptors)
        ? options.descriptors
        : [];
      const sortMode =
        typeof options.sortMode === "string" && options.sortMode !== ""
          ? options.sortMode
          : typeof adapters.getSortMode === "function"
            ? adapters.getSortMode()
            : "native";

      return adapters.runSortSnapshotPass(rowsSnapshot, descriptors, sortMode);
    });
  }

  async function generate(generateOptions) {
    if (typeof adapters.generate !== "function") {
      throw new Error("Engine generate adapter is unavailable.");
    }

    return withActionStateAsync("generate", () =>
      adapters.generate(generateOptions || {})
    );
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
        return typeof adapters.hasData === "function"
          ? adapters.hasData()
          : state.rowCount > 0;
      },
      getRowCount() {
        if (typeof adapters.getRowCount === "function") {
          return adapters.getRowCount();
        }

        return state.rowCount;
      },
      getSchema() {
        return schemaFactory();
      },
      getNumericColumnarForSave() {
        if (typeof adapters.getNumericColumnarForSave !== "function") {
          return null;
        }

        return adapters.getNumericColumnarForSave();
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
    return {
      hasData() {
        return typeof adapters.hasData === "function"
          ? adapters.hasData()
          : state.rowCount > 0;
      },
      getRowCount() {
        if (typeof adapters.getRowCount === "function") {
          return adapters.getRowCount();
        }

        return state.rowCount;
      },
      getModeOptions() {
        if (typeof adapters.getModeOptions === "function") {
          return adapters.getModeOptions();
        }

        return clonePlainObject(state.modeOptions);
      },
      setModeOptions(nextOptions, switchOptions) {
        return setModeOptions(nextOptions, switchOptions);
      },
      getRawFilters() {
        if (typeof adapters.getRawFilters === "function") {
          return adapters.getRawFilters();
        }

        return clonePlainObject(state.rawFilters);
      },
      setRawFilters(rawFilters) {
        return setRawFilters(rawFilters);
      },
      setSingleFilter(columnKey, value) {
        if (typeof adapters.setSingleFilter === "function") {
          adapters.setSingleFilter(columnKey, value);
          refreshFromAdapters();
          notify();
        }
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
        if (typeof adapters.runFilterPassWithRawFilters !== "function") {
          return null;
        }

        return withActionState("applyFilters", () =>
          adapters.runFilterPassWithRawFilters(rawFilters, options)
        );
      },
      getSortModes() {
        if (typeof adapters.getSortModes === "function") {
          return adapters.getSortModes();
        }

        return readSortModesFallback();
      },
      getSortMode() {
        if (typeof adapters.getSortMode === "function") {
          return adapters.getSortMode();
        }

        return state.sortMode;
      },
      getSortOptions() {
        if (typeof adapters.getSortOptions === "function") {
          return adapters.getSortOptions();
        }

        return clonePlainObject(state.sortOptions);
      },
      setSortOptions(nextOptions) {
        if (typeof adapters.setSortOptions === "function") {
          adapters.setSortOptions(nextOptions);
          refreshFromAdapters();
          notify();
        }
      },
      buildSortRowsSnapshot(rawFilters) {
        if (typeof adapters.buildSortRowsSnapshot !== "function") {
          return [];
        }

        return adapters.buildSortRowsSnapshot(rawFilters);
      },
      runSortSnapshotPass(rowsSnapshot, descriptors, sortMode) {
        if (typeof adapters.runSortSnapshotPass !== "function") {
          return null;
        }

        return withActionState("applySort", () =>
          adapters.runSortSnapshotPass(rowsSnapshot, descriptors, sortMode)
        );
      },
      prewarmPrecomputedSortState() {
        if (typeof adapters.prewarmPrecomputedSortState !== "function") {
          return false;
        }
        return adapters.prewarmPrecomputedSortState();
      },
      isTimSortAvailable() {
        if (typeof adapters.isTimSortAvailable === "function") {
          return adapters.isTimSortAvailable();
        }

        return false;
      },
    };
  }

  const engineApi = {
    onState,
    getState,
    setModeOptions,
    setRawFilters,
    clearFilters,
    applyFilters,
    applySingleFilter,
    applySort,
    generate,
    createIOBridge,
    createBenchmarkApi,
  };

  return engineApi;
}

export { createFastTableEngine };
