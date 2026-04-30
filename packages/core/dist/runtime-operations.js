import { createEngineFilterSortOrchestrator } from "./filter-sort-runtime-orchestration.js";

function toObject(value) {
  return value && typeof value === "object" ? value : {};
}

function buildFilterOptionsFromModeOptions(modeOptions) {
  const source = toObject(modeOptions);
  return {
    enableCaching: source.enableCaching === true,
    useDictionaryKeySearch: source.useDictionaryKeySearch === true,
    useDictionaryIntersection: source.useDictionaryIntersection === true,
    useSmarterPlanner: source.useSmarterPlanner === true,
    useSmartFiltering: source.useSmartFiltering === true,
    useFilterCache: source.useFilterCache === true,
  };
}

function getEngineFilterOptions(engine) {
  if (!engine || typeof engine.getModeOptions !== "function") {
    return {};
  }
  return buildFilterOptionsFromModeOptions(engine.getModeOptions());
}

function resolveRawFilters(rawFilters, getRawFilters) {
  if (rawFilters && typeof rawFilters === "object") {
    return rawFilters;
  }
  if (typeof getRawFilters === "function") {
    return toObject(getRawFilters());
  }
  return {};
}

function ensureEngineSortMode(engine, requestedSortMode) {
  if (!engine || typeof engine !== "object") {
    throw new Error("Sort mode resolution requires an engine instance.");
  }

  const available =
    typeof engine.getSortModes === "function" ? engine.getSortModes() : [];
  const current =
    typeof engine.getSortMode === "function" ? engine.getSortMode() : "native";
  const requested =
    typeof requestedSortMode === "string"
      ? requestedSortMode.trim().toLowerCase()
      : "";

  if (requested === "") {
    if (typeof current === "string" && current !== "") {
      return current;
    }
    if (Array.isArray(available) && available.length > 0) {
      return String(available[0]);
    }
    return "native";
  }

  if (Array.isArray(available) && available.length > 0) {
    if (!available.includes(requested)) {
      throw new Error(
        `Invalid sort mode: ${requested}. Available: ${available.join(", ")}.`
      );
    }
  }

  if (typeof engine.setSortMode === "function") {
    engine.setSortMode(requested);
  } else if (typeof engine.restoreStateCore === "function") {
    engine.restoreStateCore({
      sortMode: requested,
    });
  } else {
    throw new Error("Engine sort mode setter is unavailable.");
  }

  return requested;
}

function createEngineRuntimeOperations(options) {
  const input = toObject(options);
  const engine = input.engine;
  if (!engine || typeof engine !== "object") {
    throw new Error("Runtime operations require an engine instance.");
  }

  const getRowCount =
    typeof input.getRowCount === "function"
      ? input.getRowCount
      : () =>
          typeof engine.getRowCount === "function" ? Number(engine.getRowCount()) || 0 : 0;
  const getRawFilters =
    typeof input.getRawFilters === "function"
      ? input.getRawFilters
      : () => (typeof engine.getRawFilters === "function" ? engine.getRawFilters() : {});
  const getFilterOptions =
    typeof input.getFilterOptions === "function"
      ? input.getFilterOptions
      : () => getEngineFilterOptions(engine);
  const getCurrentFilterModeKey =
    typeof input.getCurrentFilterModeKey === "function"
      ? input.getCurrentFilterModeKey
      : () => "";
  const setLastFilterMode =
    typeof input.setLastFilterMode === "function" ? input.setLastFilterMode : null;
  const getSortDescriptors =
    typeof input.getSortDescriptors === "function"
      ? input.getSortDescriptors
      : () => [];
  const getSortMode =
    typeof input.getSortMode === "function"
      ? input.getSortMode
      : () => (typeof engine.getSortMode === "function" ? engine.getSortMode() : "native");
  const syncState =
    typeof input.syncState === "function" ? input.syncState : null;
  const defaultPreferPrecomputedFastPath =
    input.defaultPreferPrecomputedFastPath !== false;

  const orchestrator = createEngineFilterSortOrchestrator({
    engine,
    getRowCount,
    getRawFilters,
    getFilterOptions,
    getCurrentFilterModeKey,
    setLastFilterMode,
    getSortDescriptors,
    getSortMode,
    syncState,
    defaultPreferPrecomputedFastPath,
  });

  function createOverrideOrchestrator(optionsForRun) {
    const hasDescriptorOverride = Array.isArray(optionsForRun.descriptors);
    const hasSortModeOverride =
      typeof optionsForRun.sortMode === "string" &&
      optionsForRun.sortMode.trim() !== "";
    if (!hasDescriptorOverride && !hasSortModeOverride) {
      return orchestrator;
    }

    const sortDescriptors = hasDescriptorOverride
      ? optionsForRun.descriptors
      : getSortDescriptors();
    const sortMode = hasSortModeOverride
      ? optionsForRun.sortMode
      : getSortMode();
    return createEngineFilterSortOrchestrator({
      engine,
      getRowCount,
      getRawFilters,
      getFilterOptions,
      getCurrentFilterModeKey,
      setLastFilterMode,
      getSortDescriptors() {
        return Array.isArray(sortDescriptors) ? sortDescriptors : [];
      },
      getSortMode() {
        return String(sortMode || "native");
      },
      syncState,
      defaultPreferPrecomputedFastPath,
    });
  }

  function runFilterCore(rawFilters, executionOptions) {
    const optionsForRun = toObject(executionOptions);
    const sourceRawFilters = resolveRawFilters(
      optionsForRun.rawFilters && typeof optionsForRun.rawFilters === "object"
        ? optionsForRun.rawFilters
        : rawFilters,
      getRawFilters
    );

    return createOverrideOrchestrator(optionsForRun).runFilterCore(
      sourceRawFilters,
      {
        ...optionsForRun,
        rawFilters: sourceRawFilters,
        filterOptions:
          optionsForRun.filterOptions &&
          typeof optionsForRun.filterOptions === "object"
            ? optionsForRun.filterOptions
            : getFilterOptions(),
      }
    );
  }

  function runSortSnapshotCore(snapshotPayload, executionOptions) {
    const optionsForRun = toObject(executionOptions);
    const sourceSnapshot =
      snapshotPayload && typeof snapshotPayload === "object"
        ? snapshotPayload
        : null;
    if (!sourceSnapshot) {
      return null;
    }

    const targetOrchestrator = createOverrideOrchestrator(optionsForRun);
    if (
      !targetOrchestrator ||
      !targetOrchestrator.sortOrchestrator ||
      typeof targetOrchestrator.sortOrchestrator.runForSnapshot !== "function"
    ) {
      return null;
    }

    return targetOrchestrator.sortOrchestrator.runForSnapshot(
      sourceSnapshot,
      optionsForRun
    );
  }

  function runSortCore(filterResult, executionOptions) {
    const optionsForRun = toObject(executionOptions);
    const sourceRawFilters = resolveRawFilters(
      optionsForRun.rawFilters,
      getRawFilters
    );

    return createOverrideOrchestrator(optionsForRun).runSortCore(filterResult, {
      ...optionsForRun,
      rawFilters: sourceRawFilters,
    });
  }

  function runFilterSortCore(rawFilters, executionOptions) {
    const optionsForRun = toObject(executionOptions);
    const sourceRawFilters = resolveRawFilters(
      optionsForRun.rawFilters && typeof optionsForRun.rawFilters === "object"
        ? optionsForRun.rawFilters
        : rawFilters,
      getRawFilters
    );

    const filterRun = runFilterCore(sourceRawFilters, {
      ...optionsForRun,
      rawFilters: sourceRawFilters,
    });
    if (!filterRun || filterRun.kind !== "ok") {
      return {
        kind:
          filterRun && typeof filterRun.kind === "string"
            ? filterRun.kind
            : "no-data",
        filterRun,
        sortRun: null,
      };
    }

    const sortRun = runSortCore(filterRun, {
      ...optionsForRun,
      rawFilters: sourceRawFilters,
    });

    return {
      kind:
        sortRun && typeof sortRun.kind === "string"
          ? sortRun.kind
          : "no-data",
      filterRun,
      sortRun,
    };
  }

  return {
    runFilterCore,
    runSortCore,
    runSortSnapshotCore,
    runFilterSortCore,
    ensureSortMode(requestedSortMode) {
      return ensureEngineSortMode(engine, requestedSortMode);
    },
  };
}

export {
  buildFilterOptionsFromModeOptions,
  ensureEngineSortMode,
  createEngineRuntimeOperations,
};
