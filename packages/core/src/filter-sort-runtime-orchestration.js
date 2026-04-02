import { createFilteringRuntimeOrchestrator } from "./filtering-runtime-orchestration.js";
import {
  buildSortModeAttemptOrder,
  shouldAcceptPrecomputedFastPathResult,
} from "./sort-policy.js";
import { normalizeSortDescriptorList } from "./sorting-orchestration.js";

function toObject(value) {
  return value && typeof value === "object" ? value : {};
}

function hasIndexCollection(indices) {
  return Array.isArray(indices) || ArrayBuffer.isView(indices);
}

function normalizeRuntimeSortRun(runtimeSortRun, activeDescriptors, fallbackMode) {
  if (!runtimeSortRun || !hasIndexCollection(runtimeSortRun.sortedIndices)) {
    return null;
  }

  const indices = runtimeSortRun.sortedIndices;
  const sortCoreMs = Number.isFinite(runtimeSortRun.sortCoreMs)
    ? Number(runtimeSortRun.sortCoreMs)
    : Number.isFinite(runtimeSortRun.sortMs)
      ? Number(runtimeSortRun.sortMs)
      : Number(runtimeSortRun.durationMs) || 0;
  const sortTotalMs = Number.isFinite(runtimeSortRun.sortTotalMs)
    ? Number(runtimeSortRun.sortTotalMs)
    : sortCoreMs;
  const sortPrepMs = Number.isFinite(runtimeSortRun.sortPrepMs)
    ? Number(runtimeSortRun.sortPrepMs)
    : sortTotalMs - sortCoreMs;
  const telemetryDescriptors =
    Array.isArray(runtimeSortRun.descriptors) &&
    runtimeSortRun.descriptors.length > 0
      ? normalizeSortDescriptorList(runtimeSortRun.descriptors)
      : activeDescriptors;
  const telemetrySortMode =
    typeof runtimeSortRun.sortMode === "string" && runtimeSortRun.sortMode !== ""
      ? runtimeSortRun.sortMode
      : fallbackMode;

  return {
    indices,
    sortedCount: Number.isFinite(runtimeSortRun.sortedCount)
      ? Math.max(0, Number(runtimeSortRun.sortedCount) | 0)
      : indices.length,
    result: {
      changedOrder: indices.length > 1,
      durationMs: sortCoreMs,
      sortMode: telemetrySortMode,
      dataPath:
        typeof runtimeSortRun.dataPath === "string" && runtimeSortRun.dataPath !== ""
          ? runtimeSortRun.dataPath
          : "indices",
      comparatorMode:
        typeof runtimeSortRun.comparatorMode === "string"
          ? runtimeSortRun.comparatorMode
          : "",
      descriptors: telemetryDescriptors,
      effectiveDescriptors: telemetryDescriptors,
      restoredDefault: runtimeSortRun.restoredDefault === true,
    },
    sortTotalMs,
    sortPrepMs,
    rankBuildMs: Number(runtimeSortRun.rankBuildMs) || 0,
  };
}

function createEngineSortOrchestrator(options) {
  const input = toObject(options);
  const engine = input.engine;
  if (!engine || typeof engine !== "object") {
    throw new Error("Sort orchestrator requires an engine instance.");
  }
  if (typeof engine.buildSortRowsSnapshot !== "function") {
    throw new Error("Sort orchestrator requires engine.buildSortRowsSnapshot.");
  }
  if (typeof engine.executeSortCore !== "function") {
    throw new Error("Sort orchestrator requires engine.executeSortCore.");
  }

  const getSortDescriptors =
    typeof input.getSortDescriptors === "function"
      ? input.getSortDescriptors
      : () => [];
  const getSortMode =
    typeof input.getSortMode === "function" ? input.getSortMode : () => "native";
  const getRowCount =
    typeof input.getRowCount === "function" ? input.getRowCount : () => 0;

  function runForSnapshot(rowsSnapshot, runOptions) {
    const optionsForRun = toObject(runOptions);
    const activeDescriptors = normalizeSortDescriptorList(getSortDescriptors());
    if (activeDescriptors.length === 0) {
      return null;
    }

    if (!rowsSnapshot || typeof rowsSnapshot !== "object") {
      return null;
    }

    const snapshotCount =
      Number.isFinite(rowsSnapshot.count) && Number(rowsSnapshot.count) >= 0
        ? Math.floor(Number(rowsSnapshot.count))
        : Array.isArray(rowsSnapshot.rowIndices) ||
            ArrayBuffer.isView(rowsSnapshot.rowIndices)
          ? rowsSnapshot.rowIndices.length
          : 0;
    if (snapshotCount <= 0) {
      return null;
    }

    const selectedSortMode = String(getSortMode() || "native");
    const runRuntimeSortPass = (requestedMode) => {
      const mode =
        typeof requestedMode === "string" && requestedMode !== ""
          ? requestedMode
          : selectedSortMode;
      const runtimeSortRun = engine.executeSortCore(
        rowsSnapshot,
        activeDescriptors,
        mode
      );
      return normalizeRuntimeSortRun(runtimeSortRun, activeDescriptors, mode);
    };

    const modeAttemptOrder = buildSortModeAttemptOrder(
      selectedSortMode,
      optionsForRun.preferPrecomputedFastPath === true
    );
    if (modeAttemptOrder.length === 1) {
      return runRuntimeSortPass(modeAttemptOrder[0]);
    }

    const precomputedRun = runRuntimeSortPass(modeAttemptOrder[0]);
    if (
      precomputedRun &&
      shouldAcceptPrecomputedFastPathResult(precomputedRun.result)
    ) {
      return precomputedRun;
    }

    return runRuntimeSortPass(modeAttemptOrder[1]);
  }

  function runForRawFilters(rawFilters, runOptions) {
    const loadedRowCount = Math.max(0, Number(getRowCount()) | 0);
    if (loadedRowCount <= 0) {
      return null;
    }

    const snapshotRawFilters = toObject(rawFilters);
    const rowsSnapshot = engine.buildSortRowsSnapshot(snapshotRawFilters);
    return runForSnapshot(rowsSnapshot, runOptions);
  }

  return {
    runForRawFilters,
    runForSnapshot,
    hasIndexCollection,
  };
}

function createEngineFilterSortOrchestrator(options) {
  const input = toObject(options);
  const engine = input.engine;
  if (!engine || typeof engine !== "object") {
    throw new Error("Filter/sort orchestrator requires an engine instance.");
  }
  if (typeof engine.executeFilterCore !== "function") {
    throw new Error("Filter/sort orchestrator requires engine.executeFilterCore.");
  }

  const getRawFilters =
    typeof input.getRawFilters === "function" ? input.getRawFilters : () => ({});
  const getFilterOptions =
    typeof input.getFilterOptions === "function"
      ? input.getFilterOptions
      : () => ({});
  const getCurrentFilterModeKey =
    typeof input.getCurrentFilterModeKey === "function"
      ? input.getCurrentFilterModeKey
      : () => "";
  const setLastFilterMode =
    typeof input.setLastFilterMode === "function" ? input.setLastFilterMode : null;
  const getRowCount =
    typeof input.getRowCount === "function" ? input.getRowCount : () => 0;
  const syncState =
    typeof input.syncState === "function" ? input.syncState : null;
  const defaultPreferPrecomputedFastPath =
    input.defaultPreferPrecomputedFastPath !== false;
  const sortOrchestrator =
    input.sortOrchestrator && typeof input.sortOrchestrator === "object"
      ? input.sortOrchestrator
      : createEngineSortOrchestrator({
          engine,
          getSortDescriptors: input.getSortDescriptors,
          getSortMode: input.getSortMode,
          getRowCount,
        });

  const filteringOrchestrator = createFilteringRuntimeOrchestrator({
    runFilterPassWithRawFilters(rawFilters, executionOptions) {
      if (syncState) {
        syncState(rawFilters);
      }
      return engine.executeFilterCore(rawFilters, executionOptions);
    },
    runSortForFilterResult(_filterResult, sortOptions) {
      const optionsForSort = toObject(sortOptions);
      return sortOrchestrator.runForRawFilters(
        optionsForSort.rawFilters,
        optionsForSort
      );
    },
  });

  function runFilterCore(rawFilters, executionOptions) {
    const optionsForRun = toObject(executionOptions);
    const skipRender = optionsForRun.skipRender === true;
    const skipStatus = optionsForRun.skipStatus === true;
    const keepScroll = optionsForRun.keepScroll === true;
    const preferPrecomputedFastPath =
      optionsForRun.preferPrecomputedFastPath !== false &&
      defaultPreferPrecomputedFastPath;
    const filterOptions =
      optionsForRun.filterOptions && typeof optionsForRun.filterOptions === "object"
        ? optionsForRun.filterOptions
        : getFilterOptions();
    const sourceRawFilters =
      optionsForRun.rawFilters && typeof optionsForRun.rawFilters === "object"
        ? optionsForRun.rawFilters
        : rawFilters && typeof rawFilters === "object"
          ? rawFilters
          : {};

    if (Math.max(0, Number(getRowCount()) | 0) <= 0) {
      return {
        kind: "no-data",
        skipRender,
        skipStatus,
      };
    }

    const orchestration = filteringOrchestrator.execute(sourceRawFilters, {
      skipRender,
      filterOptions,
      preferPrecomputedFastPath,
    });
    if (!orchestration) {
      return null;
    }

    const nextMode =
      orchestration.filterModePath || String(getCurrentFilterModeKey() || "");
    if (setLastFilterMode) {
      setLastFilterMode(nextMode);
    }

    return {
      kind: "ok",
      skipRender,
      skipStatus,
      keepScroll,
      filterOptions,
      orchestration,
    };
  }

  function runSortCore(filterResult, executionOptions) {
    const optionsForRun = toObject(executionOptions);
    const sourceRawFilters =
      optionsForRun.rawFilters && typeof optionsForRun.rawFilters === "object"
        ? optionsForRun.rawFilters
        : getRawFilters();

    const sourceFilterResult =
      filterResult && typeof filterResult === "object"
        ? filterResult
        : runFilterCore(sourceRawFilters, {
            skipRender: true,
            skipStatus: true,
            preferPrecomputedFastPath: false,
            filterOptions: getFilterOptions(),
            rawFilters: sourceRawFilters,
          });
    const effectiveFilterResult =
      sourceFilterResult &&
      sourceFilterResult.kind === "ok" &&
      sourceFilterResult.orchestration &&
      sourceFilterResult.orchestration.filterResult
        ? sourceFilterResult.orchestration.filterResult
        : sourceFilterResult &&
            typeof sourceFilterResult === "object" &&
            sourceFilterResult.kind === undefined
          ? sourceFilterResult
          : null;

    if (!effectiveFilterResult) {
      return {
        kind: "no-data",
      };
    }

    if (syncState) {
      syncState(sourceRawFilters);
    }

    const sortRun = sortOrchestrator.runForRawFilters(
      sourceRawFilters,
      optionsForRun
    );
    const renderIndices =
      sortRun && hasIndexCollection(sortRun.indices)
        ? sortRun.indices
        : effectiveFilterResult.filteredIndices;

    return {
      kind: "ok",
      filterResult: effectiveFilterResult,
      sortRun,
      renderIndices,
    };
  }

  return {
    runFilterCore,
    runSortCore,
    sortOrchestrator,
  };
}

export {
  hasIndexCollection,
  normalizeRuntimeSortRun,
  createEngineSortOrchestrator,
  createEngineFilterSortOrchestrator,
};
