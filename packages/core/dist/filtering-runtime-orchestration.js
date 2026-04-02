function hasIndexCollection(indices) {
  return Array.isArray(indices) || ArrayBuffer.isView(indices);
}

function toObject(value) {
  return value && typeof value === "object" ? value : {};
}

function createFilteringRuntimeOrchestrator(options) {
  const input = toObject(options);
  const runFilterPassWithRawFilters =
    typeof input.runFilterPassWithRawFilters === "function"
      ? input.runFilterPassWithRawFilters
      : null;
  const runSortForFilterResult =
    typeof input.runSortForFilterResult === "function"
      ? input.runSortForFilterResult
      : null;

  if (!runFilterPassWithRawFilters) {
    throw new Error(
      "Filtering runtime orchestrator requires runFilterPassWithRawFilters."
    );
  }

  function execute(rawFilters, executionOptions) {
    const options = toObject(executionOptions);
    const sourceRawFilters = toObject(rawFilters);
    const filterOptions = toObject(options.filterOptions);
    const skipRender = options.skipRender === true;
    const preferPrecomputedFastPath = options.preferPrecomputedFastPath !== false;

    const filterResult = runFilterPassWithRawFilters(sourceRawFilters, {
      filterOptions,
    });
    if (!filterResult) {
      return null;
    }

    const active = filterResult.active === true;
    const selectedBaseCandidateCount =
      Number.isFinite(filterResult.selectedBaseCandidateCount)
        ? Number(filterResult.selectedBaseCandidateCount)
        : -1;
    const topLevelCacheEvent = filterResult.topLevelCacheEvent || null;
    const dictionaryKeySearchPlan = filterResult.dictionaryPrefilter || null;
    const coreMs = Number(filterResult.coreMs) || 0;

    let sortRun = null;
    let renderIndices = filterResult.filteredIndices;
    if (!skipRender && runSortForFilterResult) {
      sortRun = runSortForFilterResult(filterResult, {
        preferPrecomputedFastPath,
        rawFilters: sourceRawFilters,
      });
      if (sortRun && hasIndexCollection(sortRun.indices)) {
        renderIndices = sortRun.indices;
      }
    }

    const reverseIndexMs =
      dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
        ? dictionaryKeySearchPlan.durationMs
        : 0;
    const reverseIndexSearchMs =
      dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
        ? dictionaryKeySearchPlan.searchMs || 0
        : 0;
    const reverseIndexSearchFullMs =
      dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
        ? dictionaryKeySearchPlan.searchFullMs || 0
        : 0;
    const reverseIndexSearchRefinedMs =
      dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
        ? dictionaryKeySearchPlan.searchRefinedMs || 0
        : 0;
    const reverseIndexMergeMs =
      dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
        ? dictionaryKeySearchPlan.mergeMs || 0
        : 0;
    const reverseIndexMergeConcatMs =
      dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
        ? dictionaryKeySearchPlan.mergeConcatMs || 0
        : 0;
    const reverseIndexMergeSortMs =
      dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
        ? dictionaryKeySearchPlan.mergeSortMs || 0
        : 0;
    const reverseIndexIntersectMs =
      dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
        ? dictionaryKeySearchPlan.intersectionMs || 0
        : 0;

    return {
      filterResult,
      filterModePath:
        typeof filterResult.modePath === "string" ? filterResult.modePath : "",
      filteredCount: filterResult.filteredCount,
      filteredIndices: filterResult.filteredIndices,
      renderIndices,
      coreMs,
      reverseIndexMs,
      reverseIndexSearchMs,
      reverseIndexSearchFullMs,
      reverseIndexSearchRefinedMs,
      reverseIndexMergeMs,
      reverseIndexMergeConcatMs,
      reverseIndexMergeSortMs,
      reverseIndexIntersectMs,
      active,
      sort: sortRun,
      topLevelFilterCacheEvent: topLevelCacheEvent,
      reverseIndex:
        dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
          ? dictionaryKeySearchPlan
          : null,
      selectedBaseCandidateCount,
      filterOptions,
      dictionaryKeySearchPlan,
    };
  }

  return {
    execute,
  };
}

export { createFilteringRuntimeOrchestrator };
