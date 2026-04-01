function defaultNow() {
  if (typeof performance !== "undefined" && typeof performance.now === "function") {
    return performance.now();
  }

  return Date.now();
}

function defaultNormalizeFilterValue(value) {
  return String(value).trim().toLowerCase();
}

function cloneActiveFilters(activeFilters) {
  const out = Object.create(null);
  if (!activeFilters || typeof activeFilters !== "object") {
    return out;
  }

  const keys = Object.keys(activeFilters);
  for (let i = 0; i < keys.length; i += 1) {
    const key = keys[i];
    out[key] = activeFilters[key];
  }
  return out;
}

function cloneFilteredIndicesForCache(filteredIndices) {
  if (filteredIndices === null || filteredIndices === undefined) {
    return null;
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
    const copy = new Uint32Array(count);
    if (count > 0) {
      copy.set(filteredIndices.buffer.subarray(0, count));
    }
    return {
      buffer: copy,
      count,
    };
  }

  if (Array.isArray(filteredIndices)) {
    return filteredIndices.slice();
  }

  if (ArrayBuffer.isView(filteredIndices)) {
    const copy = new filteredIndices.constructor(filteredIndices.length);
    copy.set(filteredIndices);
    return copy;
  }

  return null;
}

function getFilteredIndicesBytesForCache(filteredIndices) {
  if (
    filteredIndices &&
    ArrayBuffer.isView(filteredIndices.buffer) &&
    typeof filteredIndices.count === "number"
  ) {
    return Math.max(0, filteredIndices.count | 0) * 4;
  }

  if (Array.isArray(filteredIndices) || ArrayBuffer.isView(filteredIndices)) {
    return Math.max(0, filteredIndices.length | 0) * 4;
  }

  return 0;
}

function getActiveFiltersForOrchestration(rawFilters, normalizeFilterValue) {
  const sourceFilters =
    rawFilters && typeof rawFilters === "object" ? rawFilters : {};
  const activeFilters = Object.create(null);
  const keys = Object.keys(sourceFilters);

  for (let i = 0; i < keys.length; i += 1) {
    const key = keys[i];
    const normalized = normalizeFilterValue(sourceFilters[key]);
    if (normalized !== "") {
      activeFilters[key] = normalized;
    }
  }

  return activeFilters;
}

function areActiveFiltersStricter(nextFilters, previousFilters) {
  const prev = previousFilters || Object.create(null);
  const next = nextFilters || Object.create(null);
  const prevKeys = Object.keys(prev);
  const nextKeys = Object.keys(next);

  if (nextKeys.length < prevKeys.length) {
    return false;
  }

  let changed = false;
  for (let i = 0; i < prevKeys.length; i += 1) {
    const key = prevKeys[i];
    const prevValue = prev[key];
    const nextValue = next[key];
    if (nextValue === undefined) {
      return false;
    }
    if (!nextValue.startsWith(prevValue)) {
      return false;
    }
    if (nextValue !== prevValue) {
      changed = true;
    }
  }

  if (nextKeys.length > prevKeys.length) {
    changed = true;
  }

  return changed;
}

function buildRawFiltersWithoutGuaranteed(
  sourceRawFilters,
  fullActiveFilters,
  guaranteedActiveFilters
) {
  const source =
    sourceRawFilters && typeof sourceRawFilters === "object"
      ? sourceRawFilters
      : {};
  const fullActive =
    fullActiveFilters && typeof fullActiveFilters === "object"
      ? fullActiveFilters
      : Object.create(null);
  const guaranteed =
    guaranteedActiveFilters && typeof guaranteedActiveFilters === "object"
      ? guaranteedActiveFilters
      : Object.create(null);
  const out = {};
  const keys = Object.keys(source);

  for (let i = 0; i < keys.length; i += 1) {
    const key = keys[i];
    const activeValue = fullActive[key];
    const guaranteedValue = guaranteed[key];
    const isGuaranteed =
      activeValue !== undefined &&
      guaranteedValue !== undefined &&
      activeValue === guaranteedValue;
    if (!isGuaranteed) {
      out[key] = source[key];
    }
  }

  return out;
}

function createFilteringOrchestrator(options) {
  const input = options || {};
  const now = typeof input.now === "function" ? input.now : defaultNow;
  const normalizeFilterValue =
    typeof input.normalizeFilterValue === "function"
      ? input.normalizeFilterValue
      : defaultNormalizeFilterValue;
  const cloneFilteredIndices =
    typeof input.cloneFilteredIndicesForCache === "function"
      ? input.cloneFilteredIndicesForCache
      : cloneFilteredIndicesForCache;
  const buildDictionaryKeySearchPlan =
    typeof input.buildDictionaryKeySearchPlan === "function"
      ? input.buildDictionaryKeySearchPlan
      : () => null;
  const buildFilterResultFromCachedEntry =
    typeof input.buildFilterResultFromCachedEntry === "function"
      ? input.buildFilterResultFromCachedEntry
      : () => null;
  const syncActiveControllerIndices =
    typeof input.syncActiveControllerIndices === "function"
      ? input.syncActiveControllerIndices
      : null;
  const applyFilterPath =
    typeof input.applyFilterPath === "function" ? input.applyFilterPath : null;
  const getLoadedRowCount =
    typeof input.getLoadedRowCount === "function"
      ? input.getLoadedRowCount
      : () => 0;
  const getCurrentFilterModeKey =
    typeof input.getCurrentFilterModeKey === "function"
      ? input.getCurrentFilterModeKey
      : () => "unknown";
  const onCacheStateChange =
    typeof input.onCacheStateChange === "function" ? input.onCacheStateChange : null;

  const minInsertMs =
    Number.isFinite(Number(input.topLevelFilterCacheMinInsertMs)) &&
    Number(input.topLevelFilterCacheMinInsertMs) >= 0
      ? Number(input.topLevelFilterCacheMinInsertMs)
      : 4;
  const smallMaxResults =
    Number.isFinite(Number(input.topLevelFilterCacheSmallMaxResults)) &&
    Number(input.topLevelFilterCacheSmallMaxResults) > 0
      ? Number(input.topLevelFilterCacheSmallMaxResults)
      : 50000;
  const mediumMaxResults =
    Number.isFinite(Number(input.topLevelFilterCacheMediumMaxResults)) &&
    Number(input.topLevelFilterCacheMediumMaxResults) > 0
      ? Number(input.topLevelFilterCacheMediumMaxResults)
      : 500000;
  const smallCapacity =
    Number.isFinite(Number(input.topLevelFilterCacheSmallCapacity)) &&
    Number(input.topLevelFilterCacheSmallCapacity) > 0
      ? Number(input.topLevelFilterCacheSmallCapacity)
      : 100;
  const mediumCapacity =
    Number.isFinite(Number(input.topLevelFilterCacheMediumCapacity)) &&
    Number(input.topLevelFilterCacheMediumCapacity) > 0
      ? Number(input.topLevelFilterCacheMediumCapacity)
      : 50;

  let topLevelFilterCacheRevision = 0;
  const topLevelFilterCaches = {
    small: {
      capacity: smallCapacity,
      entries: new Map(),
      totalBytes: 0,
    },
    medium: {
      capacity: mediumCapacity,
      entries: new Map(),
      totalBytes: 0,
    },
  };
  const topLevelSmartFilterStates = new Map();

  function emitCacheStateChange() {
    if (onCacheStateChange) {
      onCacheStateChange();
    }
  }

  function hasTopLevelFilterCacheEntries() {
    return (
      topLevelFilterCaches.small.entries.size > 0 ||
      topLevelFilterCaches.medium.entries.size > 0
    );
  }

  function clearTopLevelFilterCache() {
    topLevelFilterCaches.small.entries.clear();
    topLevelFilterCaches.small.totalBytes = 0;
    topLevelFilterCaches.medium.entries.clear();
    topLevelFilterCaches.medium.totalBytes = 0;
    emitCacheStateChange();
  }

  function clearTopLevelSmartFilterState() {
    topLevelSmartFilterStates.clear();
  }

  function clearAllFilterCaches() {
    clearTopLevelFilterCache();
  }

  function bumpTopLevelFilterCacheRevision() {
    topLevelFilterCacheRevision += 1;
    clearTopLevelFilterCache();
    clearTopLevelSmartFilterState();
  }

  function buildTopLevelSmartFilterStateKey(filterOptions) {
    return [
      `mode:${getCurrentFilterModeKey()}`,
      "match:includes",
      `norm:${filterOptions && filterOptions.enableCaching === true ? 1 : 0}`,
      `dict:${filterOptions && filterOptions.useDictionaryKeySearch === true ? 1 : 0}`,
      `dictIntersect:${filterOptions && filterOptions.useDictionaryIntersection === true ? 1 : 0}`,
      `smartPlanner:${filterOptions && filterOptions.useSmarterPlanner === true ? 1 : 0}`,
    ].join("\u0001");
  }

  function getTopLevelSmartFilterStateEntry(stateKey) {
    if (!stateKey) {
      return null;
    }

    return topLevelSmartFilterStates.get(stateKey) || null;
  }

  function setTopLevelSmartFilterStateEntry(
    stateKey,
    fullActiveFilters,
    filteredIndices,
    filteredCount,
    indicesAlreadyStable
  ) {
    if (!stateKey) {
      return;
    }

    topLevelSmartFilterStates.set(stateKey, {
      fullActiveFilters: cloneActiveFilters(fullActiveFilters),
      filteredIndices:
        indicesAlreadyStable === true
          ? filteredIndices
          : cloneFilteredIndices(filteredIndices),
      filteredCount: Math.max(0, Number(filteredCount) || 0),
    });
  }

  function buildTopLevelFilterCacheKey(rawFilters, filterOptions) {
    const normalizedEntries = [];
    const sourceFilters =
      rawFilters && typeof rawFilters === "object" ? rawFilters : {};
    const keys = Object.keys(sourceFilters);
    for (let i = 0; i < keys.length; i += 1) {
      const key = keys[i];
      const normalized = normalizeFilterValue(sourceFilters[key]);
      if (normalized !== "") {
        normalizedEntries.push(`${key}=${normalized}`);
      }
    }
    normalizedEntries.sort();

    return [
      `rev:${topLevelFilterCacheRevision}`,
      `mode:${getCurrentFilterModeKey()}`,
      "match:includes",
      `norm:${filterOptions && filterOptions.enableCaching === true ? 1 : 0}`,
      `dict:${filterOptions && filterOptions.useDictionaryKeySearch === true ? 1 : 0}`,
      `dictIntersect:${filterOptions && filterOptions.useDictionaryIntersection === true ? 1 : 0}`,
      `smartPlanner:${filterOptions && filterOptions.useSmarterPlanner === true ? 1 : 0}`,
      `filters:${normalizedEntries.join("\u0002")}`,
    ].join("\u0001");
  }

  function getTopLevelFilterCacheEntry(cache, cacheKey) {
    const existing = cache.entries.get(cacheKey);
    if (!existing) {
      return null;
    }

    cache.entries.delete(cacheKey);
    cache.entries.set(cacheKey, existing);
    return existing;
  }

  function getTopLevelFilterCacheEntryFromTiers(cacheKey) {
    const fromSmall = getTopLevelFilterCacheEntry(
      topLevelFilterCaches.small,
      cacheKey
    );
    if (fromSmall) {
      return fromSmall;
    }

    return getTopLevelFilterCacheEntry(topLevelFilterCaches.medium, cacheKey);
  }

  function chooseTopLevelFilterCacheTier(resultCount) {
    if (resultCount <= smallMaxResults) {
      return "small";
    }

    if (resultCount <= mediumMaxResults) {
      return "medium";
    }

    return "";
  }

  function setTopLevelFilterCacheEntryInTier(
    tier,
    cacheKey,
    filteredIndices,
    filteredCount,
    searchedCount
  ) {
    const cache =
      tier === "small"
        ? topLevelFilterCaches.small
        : topLevelFilterCaches.medium;

    const entry = {
      filteredIndices: cloneFilteredIndices(filteredIndices),
      filteredCount: Number(filteredCount) || 0,
      searchedCount:
        Number.isFinite(Number(searchedCount)) && Number(searchedCount) >= 0
          ? Number(searchedCount)
          : -1,
      tier,
    };
    const bytes = getFilteredIndicesBytesForCache(entry.filteredIndices);

    const previous = cache.entries.get(cacheKey);
    if (previous) {
      cache.totalBytes -= previous.bytes || 0;
      cache.entries.delete(cacheKey);
    }
    entry.bytes = bytes;
    cache.entries.set(cacheKey, entry);
    cache.totalBytes += bytes;

    while (cache.entries.size > cache.capacity) {
      const oldestKey = cache.entries.keys().next().value;
      const oldestEntry = cache.entries.get(oldestKey);
      if (oldestEntry) {
        cache.totalBytes -= oldestEntry.bytes || 0;
      }
      cache.entries.delete(oldestKey);
    }

    emitCacheStateChange();
    return entry;
  }

  function buildTopLevelCacheEvent(enabled) {
    return {
      enabled: enabled === true,
      hit: false,
      inserted: false,
      tier: "",
      skippedReason: "",
      lookupMs: 0,
      insertMs: 0,
      resultCount: 0,
      searchedCount: -1,
      smallSizeBytes: topLevelFilterCaches.small.totalBytes,
      smallEntryCount: topLevelFilterCaches.small.entries.size,
      smallCapacity: topLevelFilterCaches.small.capacity,
      mediumSizeBytes: topLevelFilterCaches.medium.totalBytes,
      mediumEntryCount: topLevelFilterCaches.medium.entries.size,
      mediumCapacity: topLevelFilterCaches.medium.capacity,
    };
  }

  function syncTopLevelCacheEventSnapshot(event) {
    if (!event || typeof event !== "object") {
      return;
    }

    event.smallSizeBytes = topLevelFilterCaches.small.totalBytes;
    event.smallEntryCount = topLevelFilterCaches.small.entries.size;
    event.smallCapacity = topLevelFilterCaches.small.capacity;
    event.mediumSizeBytes = topLevelFilterCaches.medium.totalBytes;
    event.mediumEntryCount = topLevelFilterCaches.medium.entries.size;
    event.mediumCapacity = topLevelFilterCaches.medium.capacity;
  }

  function runFilterPass(rawFilters, options) {
    if (typeof applyFilterPath !== "function") {
      throw new Error("Filtering orchestrator requires an applyFilterPath callback.");
    }

    const runOptions = options || {};
    const filterOptions =
      runOptions.filterOptions && typeof runOptions.filterOptions === "object"
        ? runOptions.filterOptions
        : {};
    const sourceRawFilters =
      rawFilters && typeof rawFilters === "object" ? rawFilters : {};
    const fullActiveFilters = getActiveFiltersForOrchestration(
      sourceRawFilters,
      normalizeFilterValue
    );
    const active = Object.keys(fullActiveFilters).length > 0;
    const smartStateKey = buildTopLevelSmartFilterStateKey(filterOptions);
    const smartState = getTopLevelSmartFilterStateEntry(smartStateKey);
    const rowCount = Math.max(0, Number(getLoadedRowCount()) || 0);
    const useTopLevelFilterCache = filterOptions.useFilterCache === true && active;
    const topLevelCacheEvent = buildTopLevelCacheEvent(useTopLevelFilterCache);
    const filterCoreStartMs = now();
    let selectedBaseCandidateCount = -1;
    let topLevelCacheKey = "";
    let filterResult = null;
    let matchedTopLevelCacheEntry = null;
    let dictionaryKeySearchPlan = null;

    if (rowCount <= 0) {
      return {
        active,
        filterResult: null,
        selectedBaseCandidateCount,
        topLevelCacheEvent,
        dictionaryKeySearchPlan,
        fullActiveFilters,
        coreMs: 0,
      };
    }

    if (useTopLevelFilterCache) {
      topLevelCacheKey = buildTopLevelFilterCacheKey(sourceRawFilters, filterOptions);
      const cacheLookupStartMs = now();
      const cachedEntry = getTopLevelFilterCacheEntryFromTiers(topLevelCacheKey);
      topLevelCacheEvent.lookupMs = now() - cacheLookupStartMs;
      if (cachedEntry) {
        matchedTopLevelCacheEntry = cachedEntry;
        filterResult = buildFilterResultFromCachedEntry(cachedEntry);
        if (filterResult && syncActiveControllerIndices) {
          syncActiveControllerIndices(filterResult.filteredIndices);
        }
        topLevelCacheEvent.hit = filterResult !== null;
        topLevelCacheEvent.resultCount = Number(cachedEntry.filteredCount) || 0;
        selectedBaseCandidateCount = Number(cachedEntry.searchedCount);
        if (
          !Number.isFinite(selectedBaseCandidateCount) ||
          selectedBaseCandidateCount < 0
        ) {
          const cachedResultCount = Number(cachedEntry.filteredCount);
          selectedBaseCandidateCount =
            Number.isFinite(cachedResultCount) && cachedResultCount >= 0
              ? Math.floor(cachedResultCount)
              : -1;
        } else {
          selectedBaseCandidateCount = Math.max(
            0,
            Math.floor(selectedBaseCandidateCount)
          );
        }
        topLevelCacheEvent.searchedCount = selectedBaseCandidateCount;
        topLevelCacheEvent.tier = cachedEntry.tier || "";
      }
      syncTopLevelCacheEventSnapshot(topLevelCacheEvent);
    }

    let effectiveRawFilters = sourceRawFilters;
    let baseIndices = undefined;
    if (filterResult === null && active) {
      let smartCandidate = null;
      let dictionarySearchRawFilters = sourceRawFilters;
      if (filterOptions.useSmartFiltering === true && smartState) {
        const previousActiveFilters =
          smartState.fullActiveFilters || Object.create(null);
        if (areActiveFiltersStricter(fullActiveFilters, previousActiveFilters)) {
          let smartCandidateCount = Number(smartState.filteredCount);
          if (!Number.isFinite(smartCandidateCount) || smartCandidateCount < 0) {
            const smartIndices = smartState.filteredIndices;
            if (
              smartIndices &&
              ArrayBuffer.isView(smartIndices.buffer) &&
              typeof smartIndices.count === "number"
            ) {
              smartCandidateCount = Math.max(0, Number(smartIndices.count) || 0);
            } else if (Array.isArray(smartIndices) || ArrayBuffer.isView(smartIndices)) {
              smartCandidateCount = smartIndices.length;
            } else {
              smartCandidateCount = rowCount;
            }
          }
          smartCandidateCount = Math.max(0, smartCandidateCount);
          smartCandidate = {
            source: "smart",
            baseIndices: smartState.filteredIndices,
            count: smartCandidateCount,
            guaranteedActiveFilters: previousActiveFilters,
          };
          dictionarySearchRawFilters = buildRawFiltersWithoutGuaranteed(
            sourceRawFilters,
            fullActiveFilters,
            previousActiveFilters
          );
        }
      }

      dictionaryKeySearchPlan = buildDictionaryKeySearchPlan(
        dictionarySearchRawFilters,
        filterOptions
      );

      let dictionaryCandidate = null;
      if (dictionaryKeySearchPlan && dictionaryKeySearchPlan.used) {
        const guaranteedColumnKeys =
          Array.isArray(dictionaryKeySearchPlan.guaranteedColumnKeys) &&
          dictionaryKeySearchPlan.guaranteedColumnKeys.length > 0
            ? dictionaryKeySearchPlan.guaranteedColumnKeys
            : dictionaryKeySearchPlan.selectedColumnKey
              ? [dictionaryKeySearchPlan.selectedColumnKey]
              : [];
        const guaranteedFilters = Object.create(null);
        for (let i = 0; i < guaranteedColumnKeys.length; i += 1) {
          const key = guaranteedColumnKeys[i];
          if (fullActiveFilters[key] !== undefined) {
            guaranteedFilters[key] = fullActiveFilters[key];
          }
        }
        dictionaryCandidate = {
          source: "dict",
          baseIndices: dictionaryKeySearchPlan.baseIndices,
          count: Math.max(0, Number(dictionaryKeySearchPlan.candidateCount) || 0),
          guaranteedActiveFilters: guaranteedFilters,
        };
      }

      let chosenBaseCandidate = smartCandidate;
      if (
        dictionaryCandidate &&
        (!chosenBaseCandidate || dictionaryCandidate.count < chosenBaseCandidate.count)
      ) {
        chosenBaseCandidate = dictionaryCandidate;
      }

      if (chosenBaseCandidate) {
        baseIndices = chosenBaseCandidate.baseIndices;
        selectedBaseCandidateCount = chosenBaseCandidate.count;
        topLevelCacheEvent.searchedCount = selectedBaseCandidateCount;
        effectiveRawFilters = buildRawFiltersWithoutGuaranteed(
          sourceRawFilters,
          fullActiveFilters,
          chosenBaseCandidate.guaranteedActiveFilters
        );
      }
    }

    if (filterResult === null) {
      filterResult = applyFilterPath(effectiveRawFilters, filterOptions, baseIndices);
    }

    const filterCoreDurationMs = now() - filterCoreStartMs;
    let insertedTopLevelCacheEntry = null;
    if (useTopLevelFilterCache && !topLevelCacheEvent.hit) {
      const resultCount = Number(filterResult && filterResult.filteredCount) || 0;
      topLevelCacheEvent.resultCount = resultCount;
      if (!(filterCoreDurationMs > minInsertMs)) {
        topLevelCacheEvent.skippedReason = "core";
      } else {
        const tier = chooseTopLevelFilterCacheTier(resultCount);
        if (tier === "") {
          topLevelCacheEvent.skippedReason = "size";
        } else {
          topLevelCacheEvent.tier = tier;
          const cacheInsertStartMs = now();
          const insertedEntry = setTopLevelFilterCacheEntryInTier(
            tier,
            topLevelCacheKey,
            filterResult ? filterResult.filteredIndices : null,
            resultCount,
            selectedBaseCandidateCount >= 0 ? selectedBaseCandidateCount : rowCount
          );
          insertedTopLevelCacheEntry = insertedEntry;
          topLevelCacheEvent.insertMs = now() - cacheInsertStartMs;
          topLevelCacheEvent.inserted = true;
          if (insertedEntry && typeof insertedEntry.tier === "string") {
            topLevelCacheEvent.tier = insertedEntry.tier;
          }
        }
      }
      syncTopLevelCacheEventSnapshot(topLevelCacheEvent);
    }
    if (useTopLevelFilterCache && topLevelCacheEvent.hit) {
      syncTopLevelCacheEventSnapshot(topLevelCacheEvent);
    }

    if (filterOptions.useSmartFiltering === true && filterResult) {
      if (matchedTopLevelCacheEntry) {
        setTopLevelSmartFilterStateEntry(
          smartStateKey,
          fullActiveFilters,
          matchedTopLevelCacheEntry.filteredIndices,
          matchedTopLevelCacheEntry.filteredCount,
          true
        );
      } else if (
        insertedTopLevelCacheEntry &&
        insertedTopLevelCacheEntry.filteredIndices !== undefined
      ) {
        setTopLevelSmartFilterStateEntry(
          smartStateKey,
          fullActiveFilters,
          insertedTopLevelCacheEntry.filteredIndices,
          insertedTopLevelCacheEntry.filteredCount,
          true
        );
      } else {
        setTopLevelSmartFilterStateEntry(
          smartStateKey,
          fullActiveFilters,
          filterResult.filteredIndices,
          filterResult.filteredCount,
          false
        );
      }
    }

    return {
      active,
      filterResult,
      selectedBaseCandidateCount,
      topLevelCacheEvent,
      dictionaryKeySearchPlan:
        dictionaryKeySearchPlan && dictionaryKeySearchPlan.used
          ? dictionaryKeySearchPlan
          : null,
      fullActiveFilters,
      coreMs: filterCoreDurationMs,
    };
  }

  function getTopLevelFilterCacheSnapshot() {
    return {
      revision: topLevelFilterCacheRevision,
      small: {
        capacity: topLevelFilterCaches.small.capacity,
        totalBytes: topLevelFilterCaches.small.totalBytes,
        entryCount: topLevelFilterCaches.small.entries.size,
      },
      medium: {
        capacity: topLevelFilterCaches.medium.capacity,
        totalBytes: topLevelFilterCaches.medium.totalBytes,
        entryCount: topLevelFilterCaches.medium.entries.size,
      },
      smartStateCount: topLevelSmartFilterStates.size,
    };
  }

  return {
    runFilterPass,
    hasTopLevelFilterCacheEntries,
    clearTopLevelFilterCache,
    clearTopLevelSmartFilterState,
    clearAllFilterCaches,
    bumpTopLevelFilterCacheRevision,
    getTopLevelFilterCacheSnapshot,
  };
}

export {
  createFilteringOrchestrator,
  cloneFilteredIndicesForCache,
  getFilteredIndicesBytesForCache,
};
