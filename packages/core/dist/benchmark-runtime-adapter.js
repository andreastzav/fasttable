function toObject(value) {
  return value && typeof value === "object" ? value : {};
}

function callApiMethod(api, methodName, args, fallbackValue) {
  if (!api || typeof api[methodName] !== "function") {
    return fallbackValue;
  }

  return api[methodName](...(Array.isArray(args) ? args : []));
}

function createBenchmarkRuntimeAdapter(options) {
  const input = toObject(options);
  const api = input.api;
  if (!api || typeof api !== "object") {
    throw new Error(
      "Benchmark runtime adapter requires an API object."
    );
  }

  const hooks = toObject(input.hooks);
  const beforeBuildSortSnapshot =
    typeof hooks.beforeBuildSortSnapshot === "function"
      ? hooks.beforeBuildSortSnapshot
      : null;
  const beforeRunSortSnapshotPass =
    typeof hooks.beforeRunSortSnapshotPass === "function"
      ? hooks.beforeRunSortSnapshotPass
      : null;
  const beforePrewarmSortState =
    typeof hooks.beforePrewarmSortState === "function"
      ? hooks.beforePrewarmSortState
      : null;
  const restoreStateHook =
    typeof hooks.restoreState === "function" ? hooks.restoreState : null;

  function restoreStateCore(statePatch) {
    const patch = toObject(statePatch);
    if (restoreStateHook) {
      return restoreStateHook(patch);
    }

    if (typeof api.restoreStateCore === "function") {
      return api.restoreStateCore(patch);
    }

    if (Object.prototype.hasOwnProperty.call(patch, "modeOptions")) {
      callApiMethod(api, "setModeOptions", [patch.modeOptions || {}, { suppressFilterPass: true }], null);
    }
    if (Object.prototype.hasOwnProperty.call(patch, "rawFilters")) {
      callApiMethod(api, "setRawFilters", [patch.rawFilters || {}], null);
    }
    if (Object.prototype.hasOwnProperty.call(patch, "sortOptions")) {
      callApiMethod(api, "setSortOptions", [patch.sortOptions || {}], null);
    }

    return null;
  }

  return {
    hasData() {
      return callApiMethod(api, "hasData", [], false);
    },
    getRowCount() {
      return callApiMethod(api, "getRowCount", [], 0);
    },
    getModeOptions() {
      return callApiMethod(api, "getModeOptions", [], {});
    },
    setModeOptions(nextOptions, switchOptions) {
      return callApiMethod(api, "setModeOptions", [nextOptions, switchOptions], {});
    },
    getRawFilters() {
      return callApiMethod(api, "getRawFilters", [], {});
    },
    setRawFilters(rawFilters) {
      return callApiMethod(api, "setRawFilters", [rawFilters], {});
    },
    setSingleFilter(columnKey, value) {
      return callApiMethod(api, "setSingleFilter", [columnKey, value], null);
    },
    clearFilters() {
      return callApiMethod(api, "clearFilters", [], {});
    },
    runFilterPass(options) {
      return callApiMethod(api, "runFilterPass", [options], null);
    },
    runSingleFilterPass(columnKey, value, options) {
      return callApiMethod(
        api,
        "runSingleFilterPass",
        [columnKey, value, options],
        null
      );
    },
    runFilterPassWithRawFilters(rawFilters, options) {
      return callApiMethod(
        api,
        "runFilterPassWithRawFilters",
        [rawFilters, options],
        null
      );
    },
    getSortModes() {
      return callApiMethod(api, "getSortModes", [], ["native"]);
    },
    getSortMode() {
      return callApiMethod(api, "getSortMode", [], "native");
    },
    getSortOptions() {
      return callApiMethod(api, "getSortOptions", [], {});
    },
    setSortOptions(nextOptions) {
      return callApiMethod(api, "setSortOptions", [nextOptions], null);
    },
    buildSortRowsSnapshot(rawFilters) {
      if (beforeBuildSortSnapshot) {
        beforeBuildSortSnapshot(rawFilters);
      }
      return callApiMethod(api, "buildSortRowsSnapshot", [rawFilters], null);
    },
    runSortSnapshotPass(rowsSnapshot, descriptors, sortMode) {
      if (beforeRunSortSnapshotPass) {
        beforeRunSortSnapshotPass(rowsSnapshot, descriptors, sortMode);
      }
      return callApiMethod(
        api,
        "runSortSnapshotPass",
        [rowsSnapshot, descriptors, sortMode],
        null
      );
    },
    prewarmPrecomputedSortState() {
      if (beforePrewarmSortState) {
        beforePrewarmSortState();
      }
      return callApiMethod(api, "prewarmPrecomputedSortState", [], false);
    },
    isTimSortAvailable() {
      return callApiMethod(api, "isTimSortAvailable", [], false);
    },
    restoreStateCore,
  };
}

export { createBenchmarkRuntimeAdapter };
