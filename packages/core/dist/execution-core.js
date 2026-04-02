function normalizeObjectInput(value) {
  return value && typeof value === "object" ? value : {};
}

function normalizeRawFilters(rawFilters) {
  return normalizeObjectInput(rawFilters);
}

function buildSingleFilterMap(columnKey, value) {
  const out = {};
  if (
    typeof columnKey === "string" &&
    columnKey !== "" &&
    String(value ?? "").trim() !== ""
  ) {
    out[columnKey] = String(value);
  }
  return out;
}

function executeFilterCoreOnRuntime(runtime, rawFilters, options) {
  if (!runtime || typeof runtime.runFilterPassWithRawFilters !== "function") {
    throw new Error("Runtime filter core method is unavailable.");
  }
  return runtime.runFilterPassWithRawFilters(normalizeRawFilters(rawFilters), options);
}

function executeSingleFilterCoreOnRuntime(runtime, columnKey, value, options) {
  if (!runtime || typeof runtime.runSingleFilterPass !== "function") {
    throw new Error("Runtime single-filter core method is unavailable.");
  }
  return runtime.runSingleFilterPass(columnKey, value, options);
}

function applyFiltersOnRuntime(runtime, options, readRawFilters) {
  const sourceOptions = normalizeObjectInput(options);
  const rawFilters =
    sourceOptions.rawFilters && typeof sourceOptions.rawFilters === "object"
      ? sourceOptions.rawFilters
      : typeof readRawFilters === "function"
        ? readRawFilters()
        : runtime && typeof runtime.getRawFilters === "function"
          ? runtime.getRawFilters()
          : {};
  return executeFilterCoreOnRuntime(runtime, rawFilters, sourceOptions);
}

function buildSortRowsSnapshotOnRuntime(runtime, rawFilters) {
  if (!runtime || typeof runtime.buildSortRowsSnapshot !== "function") {
    throw new Error("Runtime sort snapshot builder is unavailable.");
  }
  return runtime.buildSortRowsSnapshot(rawFilters);
}

function executeSortCoreOnRuntime(runtime, rowsSnapshot, descriptors, sortMode) {
  if (!runtime || typeof runtime.runSortSnapshotPass !== "function") {
    throw new Error("Runtime sort core method is unavailable.");
  }
  return runtime.runSortSnapshotPass(rowsSnapshot, descriptors, sortMode);
}

function applySortOnRuntime(runtime, runOptions, readRawFilters) {
  const options = normalizeObjectInput(runOptions);
  const rawFilters =
    options.rawFilters && typeof options.rawFilters === "object"
      ? options.rawFilters
      : typeof readRawFilters === "function"
        ? readRawFilters()
        : runtime && typeof runtime.getRawFilters === "function"
          ? runtime.getRawFilters()
          : {};
  const rowsSnapshot = buildSortRowsSnapshotOnRuntime(runtime, rawFilters);
  const descriptors = Array.isArray(options.descriptors) ? options.descriptors : [];
  const sortMode =
    typeof options.sortMode === "string" && options.sortMode !== ""
      ? options.sortMode
      : runtime && typeof runtime.getSortMode === "function"
        ? runtime.getSortMode()
        : "native";
  return executeSortCoreOnRuntime(runtime, rowsSnapshot, descriptors, sortMode);
}

function restoreRuntimeStateOnRuntime(runtime, statePatch) {
  const patch = normalizeObjectInput(statePatch);
  if (!runtime || typeof runtime !== "object") {
    throw new Error("Runtime restore target is unavailable.");
  }

  if (Object.prototype.hasOwnProperty.call(patch, "modeOptions")) {
    if (typeof runtime.setModeOptions !== "function") {
      throw new Error("Runtime mode options setter is unavailable.");
    }
    runtime.setModeOptions(patch.modeOptions || {}, {
      suppressFilterPass: true,
    });
  }

  if (Object.prototype.hasOwnProperty.call(patch, "rawFilters")) {
    if (typeof runtime.setRawFilters !== "function") {
      throw new Error("Runtime raw filters setter is unavailable.");
    }
    runtime.setRawFilters(patch.rawFilters || {});
  }

  if (Object.prototype.hasOwnProperty.call(patch, "sortOptions")) {
    if (typeof runtime.setSortOptions !== "function") {
      throw new Error("Runtime sort options setter is unavailable.");
    }
    runtime.setSortOptions(patch.sortOptions || {});
  }

  if (Object.prototype.hasOwnProperty.call(patch, "sortMode")) {
    if (typeof runtime.setSortMode !== "function") {
      throw new Error("Runtime sort mode setter is unavailable.");
    }
    runtime.setSortMode(patch.sortMode || "");
  }

  return {
    modeOptions:
      typeof runtime.getModeOptions === "function" ? runtime.getModeOptions() : {},
    rawFilters:
      typeof runtime.getRawFilters === "function" ? runtime.getRawFilters() : {},
    sortOptions:
      typeof runtime.getSortOptions === "function" ? runtime.getSortOptions() : {},
    sortMode:
      typeof runtime.getSortMode === "function" ? runtime.getSortMode() : "native",
  };
}

function runFilterPassWithRawFiltersUsingBridge(context, nextRawFilters, options) {
  const ctx = normalizeObjectInput(context);
  if (typeof ctx.hasData !== "function" || ctx.hasData() !== true) {
    if (typeof ctx.setLastFilterResult === "function") {
      ctx.setLastFilterResult(null);
    }
    return null;
  }
  if (typeof ctx.runBridgeFilterPass !== "function") {
    throw new Error("Filter bridge runner is unavailable.");
  }

  const executionOptions = normalizeObjectInput(options);
  const result = ctx.runBridgeFilterPass(nextRawFilters, executionOptions);
  if (executionOptions.updateState !== false && typeof ctx.setLastFilterResult === "function") {
    ctx.setLastFilterResult(result || null);
  }
  return result || null;
}

function runFilterPassUsingBridge(context, options) {
  const ctx = normalizeObjectInput(context);
  if (typeof ctx.getRawFilters !== "function") {
    throw new Error("Raw filter reader is unavailable.");
  }
  if (typeof ctx.runFilterPassWithRawFilters !== "function") {
    throw new Error("Filter pass runner is unavailable.");
  }
  return ctx.runFilterPassWithRawFilters(
    ctx.getRawFilters(),
    normalizeObjectInput(options)
  );
}

function runSingleFilterPassUsingBridge(context, columnKey, value, options) {
  const ctx = normalizeObjectInput(context);
  if (typeof ctx.runFilterPassWithRawFilters !== "function") {
    throw new Error("Filter pass runner is unavailable.");
  }
  const singleFilters = buildSingleFilterMap(columnKey, value);
  const sourceOptions = normalizeObjectInput(options);
  const executionOptions = {
    ...sourceOptions,
    updateState: false,
  };
  return ctx.runFilterPassWithRawFilters(singleFilters, executionOptions);
}

function buildSortRowsSnapshotUsingFilter(context, nextRawFilters) {
  const ctx = normalizeObjectInput(context);
  if (typeof ctx.runFilterPassWithRawFilters !== "function") {
    throw new Error("Filter pass runner is unavailable for sort snapshot build.");
  }
  if (typeof ctx.getRawFilters !== "function") {
    throw new Error("Raw filter reader is unavailable for sort snapshot build.");
  }
  if (typeof ctx.getRowCount !== "function") {
    throw new Error("Row-count reader is unavailable for sort snapshot build.");
  }
  if (typeof ctx.materializeFilteredIndices !== "function") {
    throw new Error("Filtered-index materializer is unavailable for sort snapshot build.");
  }

  const raw =
    nextRawFilters && typeof nextRawFilters === "object"
      ? nextRawFilters
      : ctx.getRawFilters();
  const filterResult = ctx.runFilterPassWithRawFilters(raw, { updateState: false });
  const rowCount = ctx.getRowCount();
  const indices = ctx.materializeFilteredIndices(
    filterResult ? filterResult.filteredIndices : null,
    rowCount
  );
  return {
    snapshotType: "row-indices-v2",
    rowIndices: indices,
    count: indices.length,
    filterCoreMs: filterResult ? filterResult.coreMs : 0,
  };
}

function runSortSnapshotPassUsingBridge(context, rowsSnapshot, descriptors, sortModeOverride) {
  const ctx = normalizeObjectInput(context);
  if (typeof ctx.runBridgeSortPass !== "function") {
    return null;
  }
  return ctx.runBridgeSortPass(rowsSnapshot, descriptors, sortModeOverride);
}

function restoreRuntimeStateFromSetters(context, statePatch) {
  const ctx = normalizeObjectInput(context);
  const patch = normalizeObjectInput(statePatch);

  if (
    Object.prototype.hasOwnProperty.call(patch, "modeOptions") &&
    typeof ctx.setModeOptions === "function"
  ) {
    ctx.setModeOptions(patch.modeOptions || {}, {
      suppressFilterPass: true,
    });
  }

  if (
    Object.prototype.hasOwnProperty.call(patch, "rawFilters") &&
    typeof ctx.setRawFilters === "function"
  ) {
    ctx.setRawFilters(patch.rawFilters || {});
  }

  if (
    Object.prototype.hasOwnProperty.call(patch, "sortOptions") &&
    typeof ctx.setSortOptions === "function"
  ) {
    ctx.setSortOptions(patch.sortOptions || {});
  }

  if (
    Object.prototype.hasOwnProperty.call(patch, "sortMode") &&
    typeof ctx.setSortMode === "function"
  ) {
    ctx.setSortMode(patch.sortMode || "");
  }

  return {
    modeOptions: typeof ctx.getModeOptions === "function" ? ctx.getModeOptions() : {},
    rawFilters: typeof ctx.getRawFilters === "function" ? ctx.getRawFilters() : {},
    sortOptions: typeof ctx.getSortOptions === "function" ? ctx.getSortOptions() : {},
    sortMode: typeof ctx.getSortMode === "function" ? ctx.getSortMode() : "native",
  };
}

export {
  normalizeRawFilters,
  buildSingleFilterMap,
  executeFilterCoreOnRuntime,
  executeSingleFilterCoreOnRuntime,
  applyFiltersOnRuntime,
  buildSortRowsSnapshotOnRuntime,
  executeSortCoreOnRuntime,
  applySortOnRuntime,
  restoreRuntimeStateOnRuntime,
  runFilterPassWithRawFiltersUsingBridge,
  runFilterPassUsingBridge,
  runSingleFilterPassUsingBridge,
  buildSortRowsSnapshotUsingFilter,
  runSortSnapshotPassUsingBridge,
  restoreRuntimeStateFromSetters,
};
