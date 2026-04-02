function createSortBenchmarkRuntimeBridge(options) {
  const input = options || {};
  const runtime = input.runtime || null;
  if (!runtime || typeof runtime !== "object") {
    throw new Error(
      "Sort benchmark runtime bridge requires a runtime instance."
    );
  }

  const readRawFilters =
    typeof input.readRawFilters === "function" ? input.readRawFilters : () => ({});
  const getRowCount =
    typeof input.getRowCount === "function" ? input.getRowCount : () => 0;
  const getModeOptions =
    typeof input.getModeOptions === "function" ? input.getModeOptions : () => ({});
  const getSortOptions =
    typeof input.getSortOptions === "function" ? input.getSortOptions : () => ({});
  const getSortMode =
    typeof input.getSortMode === "function" ? input.getSortMode : () => "native";
  const getNumericColumnarData =
    typeof input.getNumericColumnarData === "function"
      ? input.getNumericColumnarData
      : () => null;
  const getRowsForRuntime =
    typeof input.getRowsForRuntime === "function" ? input.getRowsForRuntime : null;
  const isValidNumericColumnarData =
    typeof input.isValidNumericColumnarData === "function"
      ? input.isValidNumericColumnarData
      : () => false;
  const runPrecomputedSortSnapshotPass =
    typeof input.runPrecomputedSortSnapshotPass === "function"
      ? input.runPrecomputedSortSnapshotPass
      : null;

  function normalizeRawFilters(rawFilters) {
    if (rawFilters && typeof rawFilters === "object" && !Array.isArray(rawFilters)) {
      return rawFilters;
    }
    return readRawFilters();
  }

  function sync(rawFilters) {
    const runtimeRawFilters = normalizeRawFilters(rawFilters);
    const rowCount = Math.max(0, Number(getRowCount()) | 0);

    if (rowCount > 0) {
      const numericColumnarData = getNumericColumnarData();
      if (isValidNumericColumnarData(numericColumnarData)) {
        runtime.setDataFromNumericColumnar(numericColumnarData);
      } else if (getRowsForRuntime) {
        runtime.setDataFromRows(getRowsForRuntime());
      } else {
        runtime.setDataFromRows([]);
      }
    } else {
      runtime.setDataFromRows([]);
    }

    runtime.setModeOptions(getModeOptions());
    runtime.setRawFilters(runtimeRawFilters);
    runtime.setSortOptions(getSortOptions());

    const selectedSortMode = getSortMode();
    if (selectedSortMode !== "precomputed") {
      runtime.setSortMode(selectedSortMode);
    }

    return runtimeRawFilters;
  }

  function buildSortRowsSnapshot(rawFilters) {
    const runtimeRawFilters = sync(rawFilters);
    return runtime.buildSortRowsSnapshot(runtimeRawFilters);
  }

  function runSortSnapshotPass(rowsSnapshot, descriptors, sortMode) {
    const requestedMode =
      typeof sortMode === "string" && sortMode.trim() !== ""
        ? sortMode.trim()
        : undefined;
    if (requestedMode === "precomputed" && runPrecomputedSortSnapshotPass) {
      return runPrecomputedSortSnapshotPass(rowsSnapshot, descriptors);
    }

    return runtime.runSortSnapshotPass(rowsSnapshot, descriptors, requestedMode);
  }

  return {
    sync,
    buildSortRowsSnapshot,
    runSortSnapshotPass,
  };
}

export { createSortBenchmarkRuntimeBridge };
