  const cacheKeyByKey = Object.create(null);

  function getCacheKey(key) {
    let cacheKey = cacheKeyByKey[key];
    if (cacheKey === undefined) {
      cacheKey = `${key}Cache`;
      cacheKeyByKey[key] = cacheKey;
    }

    return cacheKey;
  }

  function normalizeFilterValue(value) {
    return String(value).trim().toLowerCase();
  }

  function getActiveFilterEntries(rawFilters) {
    const sourceFilters =
      rawFilters && typeof rawFilters === "object" ? rawFilters : {};
    const keys = Object.keys(sourceFilters);
    const entries = [];

    for (let i = 0; i < keys.length; i += 1) {
      const key = keys[i];
      const search = normalizeFilterValue(sourceFilters[key]);
      if (search !== "") {
        entries.push({
          key,
          search,
          searchLength: search.length,
          cacheKey: getCacheKey(key),
        });
      }
    }

    return entries;
  }

  function matchesText(rowValue, searchValue) {
    return rowValue.includes(searchValue);
  }

  function sortFilterEntriesBySelectivity(activeFilterEntries) {
    if (!Array.isArray(activeFilterEntries) || activeFilterEntries.length <= 1) {
      return activeFilterEntries || [];
    }

    activeFilterEntries.sort((a, b) => {
      const aLength = Number(a && a.searchLength) || 0;
      const bLength = Number(b && b.searchLength) || 0;
      return bLength - aLength;
    });

    return activeFilterEntries;
  }

  function shouldUseCache(options) {
    return options && options.enableCaching === true;
  }

  function shouldUseDictionaryKeySearch(options) {
    return options && options.useDictionaryKeySearch === true;
  }

  function shouldUseDictionaryIntersection(options) {
    return options && options.useDictionaryIntersection === true;
  }

  function shouldUseSmarterPlanner(options) {
    return options && options.useSmarterPlanner === true;
  }

  const dictionaryKeySearchStateByColumn = new Map();
  const USE_BITSET_UNION = false;
  const USE_KWAY_MERGE = false;
  const SMART_PLANNER_SAMPLE_SIZE = 2048;
  const SMART_PLANNER_MIN_CANDIDATES = 5000;

  function popcount32(value) {
    let v = value >>> 0;
    v -= (v >>> 1) & 0x55555555;
    v = (v & 0x33333333) + ((v >>> 2) & 0x33333333);
    return (((v + (v >>> 4)) & 0x0f0f0f0f) * 0x01010101) >>> 24;
  }

  function hasOwnEnumerableKeys(value) {
    if (!value || typeof value !== "object") {
      return false;
    }

    for (const key in value) {
      if (Object.prototype.hasOwnProperty.call(value, key)) {
        return true;
      }
    }

    return false;
  }

  function isLowerDictionaryPostingsMap(value) {
    return (
      value &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      hasOwnEnumerableKeys(value)
    );
  }

  function getDictionaryKeySearchState(columnIndex, lowerDictionary) {
    let state = dictionaryKeySearchStateByColumn.get(columnIndex);
    if (!state || state.lowerDictionary !== lowerDictionary) {
      state = {
        lowerDictionary,
        allKeys: Object.keys(lowerDictionary),
        lastSearchValue: "",
        lastMatchedKeys: null,
      };
      dictionaryKeySearchStateByColumn.set(columnIndex, state);
    }

    return state;
  }

  function normalizeProvidedBaseIndices(options) {
    if (!options || options.baseIndices === undefined || options.baseIndices === null) {
      return null;
    }

    const provided = options.baseIndices;
    if (
      provided &&
      ArrayBuffer.isView(provided.buffer) &&
      typeof provided.count === "number"
    ) {
      const clampedCount = Math.max(
        0,
        Math.min(provided.count | 0, provided.buffer.length)
      );
      return createIndexResult(provided.buffer, clampedCount);
    }

    if (!Array.isArray(provided) && !ArrayBuffer.isView(provided)) {
      return null;
    }

    const sourceCount = provided.length | 0;
    if (sourceCount <= 0) {
      return createIndexResult(new Uint32Array(0), 0);
    }

    const normalized = new Uint32Array(sourceCount);
    for (let i = 0; i < sourceCount; i += 1) {
      const value = Number(provided[i]);
      normalized[i] = Number.isFinite(value) && value >= 0 ? value : 0;
    }

    return createIndexResult(normalized, sourceCount);
  }

  function mergePostingListsToSortedUnique(postingLists) {
    const lists = [];
    let totalLength = 0;

    for (let i = 0; i < postingLists.length; i += 1) {
      const list = postingLists[i];
      if (!list || typeof list.length !== "number" || list.length <= 0) {
        continue;
      }

      lists.push(list);
      totalLength += list.length;
    }

    if (lists.length === 0 || totalLength === 0) {
      return {
        indices: createIndexResult(new Uint32Array(0), 0),
        concatMs: 0,
        sortMs: 0,
        firstStageLabel: "none",
      };
    }

    if (lists.length === 1) {
      const concatStartMs = performance.now();
      const source = lists[0];
      const output = new Uint32Array(source.length);
      for (let i = 0; i < source.length; i += 1) {
        output[i] = source[i];
      }
      return {
        indices: createIndexResult(output, output.length),
        concatMs: performance.now() - concatStartMs,
        sortMs: 0,
        firstStageLabel: "copy",
      };
    }

    if (USE_BITSET_UNION) {
      const unionStartMs = performance.now();
      let maxRowIndex = -1;
      for (let i = 0; i < lists.length; i += 1) {
        const list = lists[i];
        const lastValue = list[list.length - 1];
        if (lastValue > maxRowIndex) {
          maxRowIndex = lastValue;
        }
      }

      if (maxRowIndex < 0) {
        return {
          indices: createIndexResult(new Uint32Array(0), 0),
          concatMs: 0,
          sortMs: 0,
        };
      }

      const wordCount = (maxRowIndex >>> 5) + 1;
      const bitset = new Uint32Array(wordCount);

      for (let listIndex = 0; listIndex < lists.length; listIndex += 1) {
        const list = lists[listIndex];
        for (let valueIndex = 0; valueIndex < list.length; valueIndex += 1) {
          const rowIndex = list[valueIndex] >>> 0;
          const wordIndex = rowIndex >>> 5;
          const bitIndex = rowIndex & 31;
          bitset[wordIndex] |= 1 << bitIndex;
        }
      }

      let uniqueCount = 0;
      for (let wordIndex = 0; wordIndex < bitset.length; wordIndex += 1) {
        uniqueCount += popcount32(bitset[wordIndex]);
      }

      const output = new Uint32Array(uniqueCount);
      let outputCount = 0;
      for (let wordIndex = 0; wordIndex < bitset.length; wordIndex += 1) {
        let word = bitset[wordIndex] >>> 0;
        while (word !== 0) {
          const lowestBit = (word & -word) >>> 0;
          const bitIndex = 31 - Math.clz32(lowestBit);
          output[outputCount] = (wordIndex << 5) + bitIndex;
          outputCount += 1;
          word = (word & (word - 1)) >>> 0;
        }
      }

      return {
        indices: createIndexResult(output, outputCount),
        concatMs: performance.now() - unionStartMs,
        sortMs: 0,
        firstStageLabel: "union",
      };
    }

    if (!USE_KWAY_MERGE) {
      const concatStartMs = performance.now();
      const output = new Uint32Array(totalLength);
      let writeOffset = 0;
      for (let i = 0; i < lists.length; i += 1) {
        const list = lists[i];
        output.set(list, writeOffset);
        writeOffset += list.length;
      }
      const concatMs = performance.now() - concatStartMs;
      const sortStartMs = performance.now();
      output.sort();
      const sortMs = performance.now() - sortStartMs;
      return {
        indices: createIndexResult(output, output.length),
        concatMs,
        sortMs,
        firstStageLabel: "concat",
      };
    }

    const heapCapacity = lists.length;
    const heapValues = new Uint32Array(heapCapacity);
    const heapListIndices = new Uint32Array(heapCapacity);
    const heapOffsets = new Uint32Array(heapCapacity);
    let heapSize = 0;

    function swapHeapEntries(a, b) {
      const tempValue = heapValues[a];
      heapValues[a] = heapValues[b];
      heapValues[b] = tempValue;

      const tempListIndex = heapListIndices[a];
      heapListIndices[a] = heapListIndices[b];
      heapListIndices[b] = tempListIndex;

      const tempOffset = heapOffsets[a];
      heapOffsets[a] = heapOffsets[b];
      heapOffsets[b] = tempOffset;
    }

    function heapPush(listIndex, offset, value) {
      let cursor = heapSize;
      heapSize += 1;
      heapValues[cursor] = value;
      heapListIndices[cursor] = listIndex;
      heapOffsets[cursor] = offset;

      while (cursor > 0) {
        const parent = (cursor - 1) >> 1;
        if (heapValues[parent] <= heapValues[cursor]) {
          break;
        }

        swapHeapEntries(parent, cursor);
        cursor = parent;
      }
    }

    function heapFixDown(startIndex) {
      let cursor = startIndex;

      while (true) {
        const left = cursor * 2 + 1;
        const right = left + 1;
        let smallest = cursor;

        if (left < heapSize && heapValues[left] < heapValues[smallest]) {
          smallest = left;
        }

        if (right < heapSize && heapValues[right] < heapValues[smallest]) {
          smallest = right;
        }

        if (smallest === cursor) {
          return;
        }

        swapHeapEntries(cursor, smallest);
        cursor = smallest;
      }
    }

    for (let listIndex = 0; listIndex < lists.length; listIndex += 1) {
      heapPush(listIndex, 0, lists[listIndex][0]);
    }

    const output = new Uint32Array(totalLength);
    let outputCount = 0;
    let previousValue = 0;
    let hasPreviousValue = false;

    while (heapSize > 0) {
      const currentValue = heapValues[0];
      if (!hasPreviousValue || currentValue !== previousValue) {
        output[outputCount] = currentValue;
        outputCount += 1;
        previousValue = currentValue;
        hasPreviousValue = true;
      }

      const sourceListIndex = heapListIndices[0];
      const nextOffset = heapOffsets[0] + 1;
      const sourceList = lists[sourceListIndex];

      if (nextOffset < sourceList.length) {
        heapOffsets[0] = nextOffset;
        heapValues[0] = sourceList[nextOffset];
        heapFixDown(0);
      } else {
        heapSize -= 1;
        if (heapSize > 0) {
          heapValues[0] = heapValues[heapSize];
          heapListIndices[0] = heapListIndices[heapSize];
          heapOffsets[0] = heapOffsets[heapSize];
          heapFixDown(0);
        }
      }
    }

    return {
      indices: createIndexResult(output, outputCount),
      concatMs: 0,
      sortMs: 0,
      firstStageLabel: "heap",
    };
  }

  function intersectSortedIndexResults(left, right) {
    if (
      !left ||
      !right ||
      !left.buffer ||
      !right.buffer ||
      typeof left.count !== "number" ||
      typeof right.count !== "number"
    ) {
      return createIndexResult(new Uint32Array(0), 0);
    }

    const leftCount = Math.max(0, Math.min(left.count | 0, left.buffer.length));
    const rightCount = Math.max(0, Math.min(right.count | 0, right.buffer.length));
    if (leftCount === 0 || rightCount === 0) {
      return createIndexResult(new Uint32Array(0), 0);
    }

    const output = new Uint32Array(Math.min(leftCount, rightCount));
    let leftCursor = 0;
    let rightCursor = 0;
    let outputCount = 0;

    while (leftCursor < leftCount && rightCursor < rightCount) {
      const leftValue = left.buffer[leftCursor];
      const rightValue = right.buffer[rightCursor];

      if (leftValue === rightValue) {
        output[outputCount] = leftValue;
        outputCount += 1;
        leftCursor += 1;
        rightCursor += 1;
      } else if (leftValue < rightValue) {
        leftCursor += 1;
      } else {
        rightCursor += 1;
      }
    }

    return createIndexResult(output, outputCount);
  }

  function buildDictionaryKeySearchPrefilter(rawFilters, numericData, options) {
    const sourceFilters =
      rawFilters && typeof rawFilters === "object" ? rawFilters : {};
    const activeFilterEntries = sortFilterEntriesBySelectivity(
      getActiveFilterEntries(sourceFilters)
    );
    const activeFilterCount = activeFilterEntries.length;
    const defaultResult = {
      used: false,
      durationMs: 0,
      searchMs: 0,
      searchFullMs: 0,
      searchRefinedMs: 0,
      searchFullKeyCount: 0,
      searchRefinedKeyCount: 0,
      searchFullColumnCount: 0,
      searchRefinedColumnCount: 0,
      mergeMs: 0,
      mergeConcatMs: 0,
      mergeSortMs: 0,
      mergeFirstStageLabel: "concat",
      mergeSearchMs: 0,
      intersectionMs: 0,
      baseIndices: null,
      selectedColumnKey: "",
      guaranteedColumnKeys: [],
      handledColumnCount: 0,
      handledColumns: [],
      candidateKeyCount: 0,
      remainingRawFilters: sourceFilters,
      remainingActiveFilterCount: activeFilterCount,
      candidateCount: 0,
    };

    if (!shouldUseDictionaryKeySearch(options) || activeFilterCount === 0) {
      return defaultResult;
    }

    if (!numericData || !Array.isArray(numericData.lowerDictionaries)) {
      return defaultResult;
    }

    const keyToIndex = options && options.keyToIndex;
    if (!keyToIndex || typeof keyToIndex !== "object") {
      return defaultResult;
    }

    const startedAtMs = performance.now();
    const lowerDictionaries = numericData.lowerDictionaries;
    const useDictionaryIntersection = shouldUseDictionaryIntersection(options);
    const perColumnCandidates = [];
    const handledColumns = [];
    let totalSearchMs = 0;
    let totalSearchFullMs = 0;
    let totalSearchRefinedMs = 0;
    let totalSearchFullKeyCount = 0;
    let totalSearchRefinedKeyCount = 0;
    let totalSearchFullColumnCount = 0;
    let totalSearchRefinedColumnCount = 0;
    let totalMergeMs = 0;
    let totalMergeConcatMs = 0;
    let totalMergeSortMs = 0;
    let totalCandidateKeyCount = 0;
    const mergeFirstStageCounts = Object.create(null);

    for (let i = 0; i < activeFilterEntries.length; i += 1) {
      const filterEntry = activeFilterEntries[i];
      const filterKey = filterEntry.key;
      const searchValue = filterEntry.search;
      const columnIndex = keyToIndex[filterKey];

      if (!Number.isInteger(columnIndex) || columnIndex < 0) {
        continue;
      }

      const lowerDictionary = lowerDictionaries[columnIndex];
      if (!isLowerDictionaryPostingsMap(lowerDictionary)) {
        continue;
      }

      const columnSearchState = getDictionaryKeySearchState(
        columnIndex,
        lowerDictionary
      );
      const canUseRefinedSearch =
        columnSearchState.lastSearchValue !== "" &&
        Array.isArray(columnSearchState.lastMatchedKeys) &&
        searchValue.length > columnSearchState.lastSearchValue.length &&
        searchValue.startsWith(columnSearchState.lastSearchValue);
      const dictionaryKeys = canUseRefinedSearch
        ? columnSearchState.lastMatchedKeys
        : columnSearchState.allKeys;
      if (!Array.isArray(dictionaryKeys)) {
        continue;
      }

      const searchStartMs = performance.now();
      const matchedPostingLists = [];
      const matchedDictionaryKeys = [];
      let matchedKeyCount = 0;
      for (let keyIndex = 0; keyIndex < dictionaryKeys.length; keyIndex += 1) {
        const dictionaryKey = dictionaryKeys[keyIndex];
        if (!dictionaryKey.includes(searchValue)) {
          continue;
        }

        const postings = lowerDictionary[dictionaryKey];
        if (!postings || typeof postings.length !== "number" || postings.length === 0) {
          continue;
        }

        matchedPostingLists.push(postings);
        matchedDictionaryKeys.push(dictionaryKey);
        matchedKeyCount += 1;
      }
      const searchDurationMs = performance.now() - searchStartMs;
      totalSearchMs += searchDurationMs;
      if (canUseRefinedSearch) {
        totalSearchRefinedMs += searchDurationMs;
        totalSearchRefinedKeyCount += dictionaryKeys.length;
        totalSearchRefinedColumnCount += 1;
      } else {
        totalSearchFullMs += searchDurationMs;
        totalSearchFullKeyCount += dictionaryKeys.length;
        totalSearchFullColumnCount += 1;
      }
      columnSearchState.lastSearchValue = searchValue;
      columnSearchState.lastMatchedKeys = matchedDictionaryKeys;

      const mergeStartMs = performance.now();
      const mergeResult = mergePostingListsToSortedUnique(matchedPostingLists);
      const mergedCandidates = mergeResult.indices;
      totalMergeMs += performance.now() - mergeStartMs;
      totalMergeConcatMs += mergeResult.concatMs || 0;
      totalMergeSortMs += mergeResult.sortMs || 0;
      const firstStageLabel =
        typeof mergeResult.firstStageLabel === "string" &&
        mergeResult.firstStageLabel !== ""
          ? mergeResult.firstStageLabel
          : "concat";
      mergeFirstStageCounts[firstStageLabel] =
        (mergeFirstStageCounts[firstStageLabel] || 0) + 1;
      perColumnCandidates.push({
        filterKey,
        indices: mergedCandidates,
        matchedKeyCount,
      });
      totalCandidateKeyCount += matchedKeyCount;
      handledColumns.push(filterKey);

      if (mergedCandidates.count === 0) {
        break;
      }
    }

    if (handledColumns.length === 0) {
      return defaultResult;
    }

    let selectedColumnKey = perColumnCandidates[0].filterKey;
    let selectedCandidateKeyCount = perColumnCandidates[0].matchedKeyCount;
    let baseIndices = perColumnCandidates[0].indices;
    let guaranteedColumnKeys = [selectedColumnKey];
    let candidateKeyCount = selectedCandidateKeyCount;
    let intersectionMs = 0;
    if (useDictionaryIntersection && perColumnCandidates.length > 1) {
      const intersectionCandidates = perColumnCandidates
        .slice()
        .sort((a, b) => a.indices.count - b.indices.count);
      selectedColumnKey = intersectionCandidates[0].filterKey;
      const intersectionStartMs = performance.now();
      let intersectedIndices = intersectionCandidates[0].indices;
      for (let i = 1; i < intersectionCandidates.length; i += 1) {
        intersectedIndices = intersectSortedIndexResults(
          intersectedIndices,
          intersectionCandidates[i].indices
        );
        if (intersectedIndices.count === 0) {
          break;
        }
      }
      intersectionMs = performance.now() - intersectionStartMs;
      baseIndices = intersectedIndices;
      guaranteedColumnKeys = handledColumns.slice();
      candidateKeyCount = totalCandidateKeyCount;
    } else {
      for (let i = 1; i < perColumnCandidates.length; i += 1) {
        const candidate = perColumnCandidates[i];
        if (candidate.indices.count < baseIndices.count) {
          baseIndices = candidate.indices;
          selectedColumnKey = candidate.filterKey;
          selectedCandidateKeyCount = candidate.matchedKeyCount;
        }
      }
      guaranteedColumnKeys = [selectedColumnKey];
      candidateKeyCount = selectedCandidateKeyCount;
    }

    const remainingRawFilters = {};
    const guaranteedLookup = Object.create(null);
    for (let i = 0; i < guaranteedColumnKeys.length; i += 1) {
      guaranteedLookup[guaranteedColumnKeys[i]] = true;
    }
    const sourceFilterKeys = Object.keys(sourceFilters);
    for (let i = 0; i < sourceFilterKeys.length; i += 1) {
      const key = sourceFilterKeys[i];
      if (guaranteedLookup[key] !== true) {
        remainingRawFilters[key] = sourceFilters[key];
      }
    }

    const finishedAtMs = performance.now();
    const mergeFirstStageLabels = Object.keys(mergeFirstStageCounts);
    const mergeFirstStageLabel =
      mergeFirstStageLabels.length === 1
        ? mergeFirstStageLabels[0]
        : mergeFirstStageLabels.length > 1
          ? "mixed"
          : "concat";
    return {
      used: true,
      durationMs: finishedAtMs - startedAtMs,
      searchMs: totalSearchMs,
      searchFullMs: totalSearchFullMs,
      searchRefinedMs: totalSearchRefinedMs,
      searchFullKeyCount: totalSearchFullKeyCount,
      searchRefinedKeyCount: totalSearchRefinedKeyCount,
      searchFullColumnCount: totalSearchFullColumnCount,
      searchRefinedColumnCount: totalSearchRefinedColumnCount,
      mergeMs: totalMergeMs,
      mergeConcatMs: totalMergeConcatMs,
      mergeSortMs: totalMergeSortMs,
      mergeFirstStageLabel,
      mergeSearchMs: totalSearchMs + totalMergeMs,
      intersectionMs,
      baseIndices,
      selectedColumnKey,
      guaranteedColumnKeys,
      handledColumnCount: handledColumns.length,
      handledColumns,
      candidateKeyCount,
      remainingRawFilters,
      remainingActiveFilterCount:
        Math.max(0, activeFilterCount - guaranteedColumnKeys.length),
      candidateCount: baseIndices ? baseIndices.count : 0,
    };
  }

  function precomputeDictionaryKeySearchState(numericData) {
    if (!numericData || !Array.isArray(numericData.lowerDictionaries)) {
      return 0;
    }

    const lowerDictionaries = numericData.lowerDictionaries;
    let warmedColumnCount = 0;
    for (let columnIndex = 0; columnIndex < lowerDictionaries.length; columnIndex += 1) {
      const lowerDictionary = lowerDictionaries[columnIndex];
      if (!isLowerDictionaryPostingsMap(lowerDictionary)) {
        continue;
      }

      getDictionaryKeySearchState(columnIndex, lowerDictionary);
      warmedColumnCount += 1;
    }

    return warmedColumnCount;
  }

  function hasObjectRowCache(rows) {
    if (!Array.isArray(rows) || rows.length === 0) {
      return false;
    }

    return rows[0].indexCache !== undefined;
  }

  function hasObjectColumnarCache(columnarData) {
    if (!columnarData || !columnarData.columns) {
      return false;
    }

    return columnarData.columns.indexCache !== undefined;
  }

  function buildRowDescriptors(activeFilterEntries, useCache) {
    if (!Array.isArray(activeFilterEntries) || activeFilterEntries.length === 0) {
      return [];
    }

    const descriptors = new Array(activeFilterEntries.length);
    for (let j = 0; j < activeFilterEntries.length; j += 1) {
      const entry = activeFilterEntries[j];
      if (useCache) {
        descriptors[j] = {
          key: entry.key,
          cacheKey: entry.cacheKey,
          search: entry.search,
          searchLength: entry.searchLength,
        };
      } else {
        descriptors[j] = {
          key: entry.key,
          search: entry.search,
          searchLength: entry.searchLength,
        };
      }
    }

    return descriptors;
  }

  function createIndexScratch(capacity) {
    const initialCapacity = Math.max(1, capacity | 0);
    return {
      first: new Uint32Array(initialCapacity),
      second: new Uint32Array(initialCapacity),
    };
  }

  function nextScratchCapacity(minCapacity) {
    let size = 1;
    while (size < minCapacity) {
      size <<= 1;
    }
    return size;
  }

  function ensureIndexScratchCapacity(scratch, minCapacity) {
    if (minCapacity <= 0) {
      return;
    }

    if (scratch.first.length >= minCapacity) {
      return;
    }

    const nextCapacity = nextScratchCapacity(minCapacity);
    scratch.first = new Uint32Array(nextCapacity);
    scratch.second = new Uint32Array(nextCapacity);
  }

  function getBaseIndicesCount(baseIndices) {
    if (baseIndices === null) {
      return 0;
    }

    if (typeof baseIndices.count === "number") {
      return baseIndices.count;
    }

    if (Array.isArray(baseIndices) || ArrayBuffer.isView(baseIndices)) {
      return baseIndices.length;
    }

    return 0;
  }

  function getBaseIndicesBuffer(baseIndices) {
    if (
      baseIndices &&
      ArrayBuffer.isView(baseIndices.buffer) &&
      typeof baseIndices.count === "number"
    ) {
      return baseIndices.buffer;
    }

    if (ArrayBuffer.isView(baseIndices)) {
      return baseIndices;
    }

    return null;
  }

  function getBaseIndexAt(baseIndices, idx) {
    if (baseIndices === null) {
      return idx;
    }

    if (
      baseIndices &&
      ArrayBuffer.isView(baseIndices.buffer) &&
      typeof baseIndices.count === "number"
    ) {
      return baseIndices.buffer[idx];
    }

    return baseIndices[idx];
  }

  function selectOutputBuffer(scratch, inputBuffer, requiredCapacity) {
    ensureIndexScratchCapacity(scratch, requiredCapacity);
    if (inputBuffer === scratch.first) {
      return scratch.second;
    }

    return scratch.first;
  }

  function createIndexResult(buffer, count) {
    return {
      buffer,
      count,
    };
  }

  function createPlannerSampleIndices(baseIndices, totalRowCount) {
    const candidateCount =
      baseIndices === null ? totalRowCount : getBaseIndicesCount(baseIndices);
    if (candidateCount <= 0 || candidateCount < SMART_PLANNER_MIN_CANDIDATES) {
      return null;
    }

    const sampleCount = Math.max(
      1,
      Math.min(candidateCount, SMART_PLANNER_SAMPLE_SIZE)
    );
    const sampleBuffer = new Uint32Array(sampleCount);

    if (sampleCount === candidateCount) {
      for (let i = 0; i < sampleCount; i += 1) {
        sampleBuffer[i] = baseIndices === null ? i : getBaseIndexAt(baseIndices, i);
      }
      return createIndexResult(sampleBuffer, sampleCount);
    }

    const step = candidateCount / sampleCount;
    for (let i = 0; i < sampleCount; i += 1) {
      const sourceOffset = Math.min(
        candidateCount - 1,
        Math.floor((i + 0.5) * step)
      );
      sampleBuffer[i] =
        baseIndices === null
          ? sourceOffset
          : getBaseIndexAt(baseIndices, sourceOffset);
    }

    return createIndexResult(sampleBuffer, sampleCount);
  }

  function orderDescriptorsBySampleHits(
    descriptors,
    sampleIndices,
    countSampleMatches
  ) {
    if (
      !Array.isArray(descriptors) ||
      descriptors.length <= 1 ||
      !sampleIndices ||
      !sampleIndices.buffer ||
      sampleIndices.count <= 0 ||
      typeof countSampleMatches !== "function"
    ) {
      return descriptors;
    }

    const scored = new Array(descriptors.length);
    for (let i = 0; i < descriptors.length; i += 1) {
      const descriptor = descriptors[i];
      scored[i] = {
        descriptor,
        hitCount: countSampleMatches(descriptor, sampleIndices) | 0,
        searchLength:
          descriptor && typeof descriptor.search === "string"
            ? descriptor.search.length
            : 0,
        originalIndex: i,
      };
    }

    scored.sort((a, b) => {
      if (a.hitCount !== b.hitCount) {
        return a.hitCount - b.hitCount;
      }

      if (a.searchLength !== b.searchLength) {
        return b.searchLength - a.searchLength;
      }

      return a.originalIndex - b.originalIndex;
    });

    const orderedDescriptors = new Array(scored.length);
    for (let i = 0; i < scored.length; i += 1) {
      orderedDescriptors[i] = scored[i].descriptor;
    }

    return orderedDescriptors;
  }

  // Shared scratch buffers limit peak memory: one pair for object modes,
  // one pair for numeric modes.
  const sharedObjectScratch = createIndexScratch(1);
  const sharedNumericScratch = createIndexScratch(1);

  function filterRowIndicesCached(
    rows,
    baseIndices,
    descriptors,
    matchText,
    scratch
  ) {
    const inputBuffer = getBaseIndicesBuffer(baseIndices);
    const inputCount =
      baseIndices === null ? rows.length : getBaseIndicesCount(baseIndices);
    const outputBuffer = selectOutputBuffer(scratch, inputBuffer, inputCount);
    let outputCount = 0;

    if (baseIndices === null) {
      for (let rowIndex = 0; rowIndex < rows.length; rowIndex += 1) {
        const row = rows[rowIndex];
        let include = true;

        for (let j = 0; j < descriptors.length; j += 1) {
          const descriptor = descriptors[j];
          const rowValue = row[descriptor.cacheKey];

          if (!matchText(rowValue, descriptor.search)) {
            include = false;
            break;
          }
        }

        if (include) {
          outputBuffer[outputCount] = rowIndex;
          outputCount += 1;
        }
      }

      return createIndexResult(outputBuffer, outputCount);
    }

    for (let i = 0; i < inputCount; i += 1) {
      const rowIndex = getBaseIndexAt(baseIndices, i);
      const row = rows[rowIndex];
      let include = true;

      for (let j = 0; j < descriptors.length; j += 1) {
        const descriptor = descriptors[j];
        const rowValue = row[descriptor.cacheKey];

        if (!matchText(rowValue, descriptor.search)) {
          include = false;
          break;
        }
      }

      if (include) {
        outputBuffer[outputCount] = rowIndex;
        outputCount += 1;
      }
    }

    return createIndexResult(outputBuffer, outputCount);
  }

  function filterRowIndicesNoCache(
    rows,
    baseIndices,
    descriptors,
    matchText,
    scratch
  ) {
    const inputBuffer = getBaseIndicesBuffer(baseIndices);
    const inputCount =
      baseIndices === null ? rows.length : getBaseIndicesCount(baseIndices);
    const outputBuffer = selectOutputBuffer(scratch, inputBuffer, inputCount);
    let outputCount = 0;

    if (baseIndices === null) {
      for (let rowIndex = 0; rowIndex < rows.length; rowIndex += 1) {
        const row = rows[rowIndex];
        let include = true;

        for (let j = 0; j < descriptors.length; j += 1) {
          const descriptor = descriptors[j];
          const rowValue = String(row[descriptor.key]).toLowerCase();

          if (!matchText(rowValue, descriptor.search)) {
            include = false;
            break;
          }
        }

        if (include) {
          outputBuffer[outputCount] = rowIndex;
          outputCount += 1;
        }
      }

      return createIndexResult(outputBuffer, outputCount);
    }

    for (let i = 0; i < inputCount; i += 1) {
      const rowIndex = getBaseIndexAt(baseIndices, i);
      const row = rows[rowIndex];
      let include = true;

      for (let j = 0; j < descriptors.length; j += 1) {
        const descriptor = descriptors[j];
        const rowValue = String(row[descriptor.key]).toLowerCase();

        if (!matchText(rowValue, descriptor.search)) {
          include = false;
          break;
        }
      }

      if (include) {
        outputBuffer[outputCount] = rowIndex;
        outputCount += 1;
      }
    }

    return createIndexResult(outputBuffer, outputCount);
  }

  function filterRowIndices(
    rows,
    baseIndices,
    activeFilterEntries,
    useCache,
    scratch,
    options
  ) {
    let descriptors = buildRowDescriptors(activeFilterEntries, useCache);
    if (descriptors.length === 0) {
      return null;
    }

    if (shouldUseSmarterPlanner(options) && descriptors.length > 1) {
      const sampleIndices = createPlannerSampleIndices(baseIndices, rows.length);
      descriptors = orderDescriptorsBySampleHits(
        descriptors,
        sampleIndices,
        (descriptor, sample) => {
          let hitCount = 0;
          for (let i = 0; i < sample.count; i += 1) {
            const rowIndex = sample.buffer[i];
            const row = rows[rowIndex];
            const rowValue = useCache
              ? row[descriptor.cacheKey]
              : String(row[descriptor.key]).toLowerCase();
            if (matchesText(rowValue, descriptor.search)) {
              hitCount += 1;
            }
          }

          return hitCount;
        }
      );
    }

    if (useCache) {
      return filterRowIndicesCached(
        rows,
        baseIndices,
        descriptors,
        matchesText,
        scratch
      );
    }

    return filterRowIndicesNoCache(
      rows,
      baseIndices,
      descriptors,
      matchesText,
      scratch
    );
  }

  function filterOneColumnCached(
    values,
    searchValue,
    baseIndices,
    matchText,
    outputBuffer
  ) {
    let outputCount = 0;
    const inputCount = getBaseIndicesCount(baseIndices);

    if (baseIndices === null) {
      for (let rowIndex = 0; rowIndex < values.length; rowIndex += 1) {
        if (matchText(values[rowIndex], searchValue)) {
          outputBuffer[outputCount] = rowIndex;
          outputCount += 1;
        }
      }

      return outputCount;
    }

    for (let i = 0; i < inputCount; i += 1) {
      const rowIndex = getBaseIndexAt(baseIndices, i);
      if (matchText(values[rowIndex], searchValue)) {
        outputBuffer[outputCount] = rowIndex;
        outputCount += 1;
      }
    }

    return outputCount;
  }

  function filterOneColumnNoCache(
    values,
    searchValue,
    baseIndices,
    matchText,
    outputBuffer
  ) {
    let outputCount = 0;
    const inputCount = getBaseIndicesCount(baseIndices);

    if (baseIndices === null) {
      for (let rowIndex = 0; rowIndex < values.length; rowIndex += 1) {
        const rowValue = String(values[rowIndex]).toLowerCase();

        if (matchText(rowValue, searchValue)) {
          outputBuffer[outputCount] = rowIndex;
          outputCount += 1;
        }
      }

      return outputCount;
    }

    for (let i = 0; i < inputCount; i += 1) {
      const rowIndex = getBaseIndexAt(baseIndices, i);
      const rowValue = String(values[rowIndex]).toLowerCase();

      if (matchText(rowValue, searchValue)) {
        outputBuffer[outputCount] = rowIndex;
        outputCount += 1;
      }
    }

    return outputCount;
  }

  function filterColumnarData(
    columnarData,
    baseIndices,
    activeFilterEntries,
    useCache,
    scratch,
    options
  ) {
    if (!Array.isArray(activeFilterEntries) || activeFilterEntries.length === 0) {
      return null;
    }

    const columns = columnarData.columns;
    let descriptors = new Array(activeFilterEntries.length);
    for (let j = 0; j < activeFilterEntries.length; j += 1) {
      const entry = activeFilterEntries[j];
      descriptors[j] = {
        key: entry.key,
        values: useCache ? columns[entry.cacheKey] : columns[entry.key],
        search: entry.search,
        searchLength: entry.searchLength,
      };
    }

    if (shouldUseSmarterPlanner(options) && descriptors.length > 1) {
      const sampleIndices = createPlannerSampleIndices(
        baseIndices,
        columnarData.rowCount
      );
      descriptors = orderDescriptorsBySampleHits(
        descriptors,
        sampleIndices,
        (descriptor, sample) => {
          let hitCount = 0;
          const values = descriptor.values;
          for (let i = 0; i < sample.count; i += 1) {
            const rowIndex = sample.buffer[i];
            const rowValue = useCache
              ? values[rowIndex]
              : String(values[rowIndex]).toLowerCase();
            if (matchesText(rowValue, descriptor.search)) {
              hitCount += 1;
            }
          }

          return hitCount;
        }
      );
    }

    const filterOneColumn = useCache ? filterOneColumnCached : filterOneColumnNoCache;
    let candidates = baseIndices;

    for (let j = 0; j < descriptors.length; j += 1) {
      const descriptor = descriptors[j];
      const values = descriptor.values;
      const searchValue = descriptor.search;

      const inputBuffer = getBaseIndicesBuffer(candidates);
      const inputCount =
        candidates === null ? values.length : getBaseIndicesCount(candidates);
      const outputBuffer = selectOutputBuffer(
        scratch,
        inputBuffer,
        inputCount
      );
      const outputCount = filterOneColumn(
        values,
        searchValue,
        candidates,
        matchesText,
        outputBuffer
      );

      candidates = createIndexResult(outputBuffer, outputCount);

      if (outputCount === 0) {
        return candidates;
      }
    }

    return candidates;
  }

  function createRowFilterController(initialRows) {
    let allRows = Array.isArray(initialRows) ? initialRows : [];
    let cacheAvailable = hasObjectRowCache(allRows);
    let currentIndices = null;

    return {
      setRows(nextRows) {
        allRows = Array.isArray(nextRows) ? nextRows : [];
        cacheAvailable = hasObjectRowCache(allRows);
        currentIndices = null;
      },
      apply(rawFilters, options) {
        const providedBaseIndices = normalizeProvidedBaseIndices(options);
        const hasProvidedBaseIndices = providedBaseIndices !== null;
        const activeFilterEntries = sortFilterEntriesBySelectivity(
          getActiveFilterEntries(rawFilters)
        );
        if (activeFilterEntries.length === 0) {
          currentIndices = hasProvidedBaseIndices ? providedBaseIndices : null;
          return currentIndices;
        }

        const useCache = shouldUseCache(options) && cacheAvailable;
        currentIndices = filterRowIndices(
          allRows,
          providedBaseIndices,
          activeFilterEntries,
          useCache,
          sharedObjectScratch,
          options
        );
        return currentIndices;
      },
      getAllRows() {
        return allRows;
      },
      getCurrentIndices() {
        return currentIndices;
      },
      setCurrentIndices(nextIndices) {
        currentIndices = normalizeProvidedBaseIndices({
          baseIndices: nextIndices,
        });
      },
      getCurrentRows() {
        if (currentIndices === null) {
          return allRows;
        }

        const out = new Array(currentIndices.count);
        for (let i = 0; i < currentIndices.count; i += 1) {
          out[i] = allRows[currentIndices.buffer[i]];
        }
        return out;
      },
      getCurrentCount() {
        if (currentIndices === null) {
          return allRows.length;
        }

        return currentIndices.count;
      },
    };
  }

  function createColumnarFilterController(initialData) {
    let allData =
      initialData && typeof initialData.rowCount === "number"
        ? initialData
        : { rowCount: 0, columns: {} };
    let cacheAvailable = hasObjectColumnarCache(allData);
    let currentIndices = null;

    return {
      setData(nextData) {
        allData =
          nextData && typeof nextData.rowCount === "number"
            ? nextData
            : { rowCount: 0, columns: {} };
        cacheAvailable = hasObjectColumnarCache(allData);
        currentIndices = null;
      },
      apply(rawFilters, options) {
        const providedBaseIndices = normalizeProvidedBaseIndices(options);
        const hasProvidedBaseIndices = providedBaseIndices !== null;
        const activeFilterEntries = sortFilterEntriesBySelectivity(
          getActiveFilterEntries(rawFilters)
        );
        if (activeFilterEntries.length === 0) {
          currentIndices = hasProvidedBaseIndices ? providedBaseIndices : null;
          return currentIndices;
        }

        const useCache = shouldUseCache(options) && cacheAvailable;
        currentIndices = filterColumnarData(
          allData,
          providedBaseIndices,
          activeFilterEntries,
          useCache,
          sharedObjectScratch,
          options
        );
        return currentIndices;
      },
      getCurrentIndices() {
        return currentIndices;
      },
      setCurrentIndices(nextIndices) {
        currentIndices = normalizeProvidedBaseIndices({
          baseIndices: nextIndices,
        });
      },
      getData() {
        return allData;
      },
      getCurrentCount() {
        if (currentIndices === null) {
          return allData.rowCount;
        }

        return currentIndices.count;
      },
    };
  }

  function createEmptyNumericData(baseColumnCount, cacheOffset) {
    return {
      rowCount: 0,
      columnCount: baseColumnCount,
      baseColumnCount,
      cacheOffset,
      hasCacheColumns: false,
      columns: [],
      columnKinds: [],
      dictionaries: [],
      lowerDictionaries: [],
      lowerDictionaryValues: [],
      cacheColumns: null,
    };
  }

  function normalizeNumericData(data, baseColumnCount, cacheOffset) {
    if (
      data &&
      typeof data.rowCount === "number" &&
      Array.isArray(data.columns)
    ) {
      return data;
    }

    return createEmptyNumericData(baseColumnCount, cacheOffset);
  }

  function hasNumericDataCache(numericData) {
    return (
      numericData &&
      Array.isArray(numericData.cacheColumns) &&
      numericData.cacheColumns.length > 0
    );
  }

  function hasNumericRowsCache(rows, cacheOffset) {
    if (!Array.isArray(rows) || rows.length === 0) {
      return false;
    }

    const firstRow = rows[0];
    if (!firstRow || typeof firstRow.length !== "number") {
      return false;
    }

    return firstRow.length > cacheOffset;
  }

  function getNumericFilterEntries(activeFilterEntries, keyToIndex) {
    if (!Array.isArray(activeFilterEntries) || activeFilterEntries.length === 0) {
      return [];
    }

    const entries = [];
    for (let i = 0; i < activeFilterEntries.length; i += 1) {
      const entry = activeFilterEntries[i];
      const columnIndex = keyToIndex[entry.key];
      if (columnIndex === undefined) {
        continue;
      }

      entries.push({
        key: entry.key,
        search: entry.search,
        searchLength: entry.searchLength,
        cacheKey: entry.cacheKey,
        columnIndex,
      });
    }

    return entries;
  }

  function buildNumericRowDescriptors(
    numericFilterEntries,
    useCache,
    baseColumnCount,
    cacheOffset
  ) {
    if (!Array.isArray(numericFilterEntries) || numericFilterEntries.length === 0) {
      return [];
    }

    const descriptors = [];

    for (let j = 0; j < numericFilterEntries.length; j += 1) {
      const entry = numericFilterEntries[j];
      const columnIndex = entry.columnIndex;
      if (columnIndex < 0 || columnIndex >= baseColumnCount) {
        continue;
      }

      if (useCache) {
        descriptors.push({
          key: entry.key,
          valueIndex: columnIndex + cacheOffset,
          search: entry.search,
          searchLength: entry.searchLength,
        });
      } else {
        descriptors.push({
          key: entry.key,
          valueIndex: columnIndex,
          search: entry.search,
          searchLength: entry.searchLength,
        });
      }
    }

    return descriptors;
  }

  function filterNumericRowIndicesCached(
    rows,
    baseIndices,
    descriptors,
    matchText,
    scratch
  ) {
    const rowCount = rows.length;
    const inputBuffer = getBaseIndicesBuffer(baseIndices);
    const inputCount =
      baseIndices === null ? rowCount : getBaseIndicesCount(baseIndices);
    const outputBuffer = selectOutputBuffer(scratch, inputBuffer, inputCount);
    let outputCount = 0;

    if (baseIndices === null) {
      for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
        const row = rows[rowIndex];
        let include = true;

        for (let j = 0; j < descriptors.length; j += 1) {
          const descriptor = descriptors[j];
          const rowValue = row[descriptor.valueIndex];

          if (!matchText(rowValue, descriptor.search)) {
            include = false;
            break;
          }
        }

        if (include) {
          outputBuffer[outputCount] = rowIndex;
          outputCount += 1;
        }
      }

      return createIndexResult(outputBuffer, outputCount);
    }

    for (let i = 0; i < inputCount; i += 1) {
      const rowIndex = getBaseIndexAt(baseIndices, i);
      const row = rows[rowIndex];
      let include = true;

      for (let j = 0; j < descriptors.length; j += 1) {
        const descriptor = descriptors[j];
        const rowValue = row[descriptor.valueIndex];

        if (!matchText(rowValue, descriptor.search)) {
          include = false;
          break;
        }
      }

      if (include) {
        outputBuffer[outputCount] = rowIndex;
        outputCount += 1;
      }
    }

    return createIndexResult(outputBuffer, outputCount);
  }

  function filterNumericRowIndicesNoCache(
    rows,
    baseIndices,
    descriptors,
    matchText,
    scratch
  ) {
    const rowCount = rows.length;
    const inputBuffer = getBaseIndicesBuffer(baseIndices);
    const inputCount =
      baseIndices === null ? rowCount : getBaseIndicesCount(baseIndices);
    const outputBuffer = selectOutputBuffer(scratch, inputBuffer, inputCount);
    let outputCount = 0;

    if (baseIndices === null) {
      for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
        const row = rows[rowIndex];
        let include = true;

        for (let j = 0; j < descriptors.length; j += 1) {
          const descriptor = descriptors[j];
          const rowValue = String(row[descriptor.valueIndex]).toLowerCase();

          if (!matchText(rowValue, descriptor.search)) {
            include = false;
            break;
          }
        }

        if (include) {
          outputBuffer[outputCount] = rowIndex;
          outputCount += 1;
        }
      }

      return createIndexResult(outputBuffer, outputCount);
    }

    for (let i = 0; i < inputCount; i += 1) {
      const rowIndex = getBaseIndexAt(baseIndices, i);
      const row = rows[rowIndex];
      let include = true;

      for (let j = 0; j < descriptors.length; j += 1) {
        const descriptor = descriptors[j];
        const rowValue = String(row[descriptor.valueIndex]).toLowerCase();

        if (!matchText(rowValue, descriptor.search)) {
          include = false;
          break;
        }
      }

      if (include) {
        outputBuffer[outputCount] = rowIndex;
        outputCount += 1;
      }
    }

    return createIndexResult(outputBuffer, outputCount);
  }

  function filterNumericRowIndices(
    rows,
    baseIndices,
    activeFilterEntries,
    keyToIndex,
    useCache,
    baseColumnCount,
    cacheOffset,
    scratch,
    options
  ) {
    const numericFilterEntries = getNumericFilterEntries(
      activeFilterEntries,
      keyToIndex
    );
    let descriptors = buildNumericRowDescriptors(
      numericFilterEntries,
      useCache,
      baseColumnCount,
      cacheOffset
    );
    if (descriptors.length === 0) {
      return null;
    }

    if (shouldUseSmarterPlanner(options) && descriptors.length > 1) {
      const sampleIndices = createPlannerSampleIndices(baseIndices, rows.length);
      descriptors = orderDescriptorsBySampleHits(
        descriptors,
        sampleIndices,
        (descriptor, sample) => {
          let hitCount = 0;
          for (let i = 0; i < sample.count; i += 1) {
            const rowIndex = sample.buffer[i];
            const row = rows[rowIndex];
            const rowValue = useCache
              ? row[descriptor.valueIndex]
              : String(row[descriptor.valueIndex]).toLowerCase();
            if (matchesText(rowValue, descriptor.search)) {
              hitCount += 1;
            }
          }

          return hitCount;
        }
      );
    }

    if (useCache) {
      return filterNumericRowIndicesCached(
        rows,
        baseIndices,
        descriptors,
        matchesText,
        scratch
      );
    }

    return filterNumericRowIndicesNoCache(
      rows,
      baseIndices,
      descriptors,
      matchesText,
      scratch
    );
  }

  function buildNumericColumnDescriptors(
    numericData,
    numericFilterEntries,
    useCache,
    baseColumnCount
  ) {
    if (!Array.isArray(numericFilterEntries) || numericFilterEntries.length === 0) {
      return [];
    }

    const descriptors = [];

    for (let j = 0; j < numericFilterEntries.length; j += 1) {
      const entry = numericFilterEntries[j];
      const columnIndex = entry.columnIndex;
      if (columnIndex < 0 || columnIndex >= baseColumnCount) {
        continue;
      }

      if (useCache) {
        descriptors.push({
          key: entry.key,
          mode: "cache",
          values: numericData.cacheColumns[columnIndex],
          search: entry.search,
          searchLength: entry.searchLength,
        });
      } else {
        const kind = numericData.columnKinds[columnIndex];
        if (kind === "stringId") {
          const dictionary =
            Array.isArray(numericData.dictionaries) &&
            Array.isArray(numericData.dictionaries[columnIndex])
              ? numericData.dictionaries[columnIndex]
              : [];
          let lowerValues =
            Array.isArray(numericData.lowerDictionaryValues) &&
            Array.isArray(numericData.lowerDictionaryValues[columnIndex])
              ? numericData.lowerDictionaryValues[columnIndex]
              : null;
          if (lowerValues === null) {
            lowerValues = new Array(dictionary.length);
            for (let i = 0; i < dictionary.length; i += 1) {
              lowerValues[i] = String(dictionary[i]).toLowerCase();
            }
          }

          descriptors.push({
            key: entry.key,
            mode: "stringId",
            ids: numericData.columns[columnIndex],
            dictionary,
            lowerValues,
            search: entry.search,
            searchLength: entry.searchLength,
          });
          continue;
        }

        descriptors.push({
          key: entry.key,
          mode: "number",
          values: numericData.columns[columnIndex],
          search: entry.search,
          searchLength: entry.searchLength,
        });
      }
    }

    return descriptors;
  }

  function filterOneNumericColumnCached(
    values,
    searchValue,
    baseIndices,
    matchText,
    outputBuffer
  ) {
    let outputCount = 0;
    const inputCount = getBaseIndicesCount(baseIndices);

    if (baseIndices === null) {
      for (let rowIndex = 0; rowIndex < values.length; rowIndex += 1) {
        if (matchText(values[rowIndex], searchValue)) {
          outputBuffer[outputCount] = rowIndex;
          outputCount += 1;
        }
      }

      return outputCount;
    }

    for (let i = 0; i < inputCount; i += 1) {
      const rowIndex = getBaseIndexAt(baseIndices, i);
      if (matchText(values[rowIndex], searchValue)) {
        outputBuffer[outputCount] = rowIndex;
        outputCount += 1;
      }
    }

    return outputCount;
  }

  function filterOneNumericColumnNoCache(
    values,
    searchValue,
    baseIndices,
    matchText,
    outputBuffer
  ) {
    let outputCount = 0;
    const inputCount = getBaseIndicesCount(baseIndices);

    if (baseIndices === null) {
      for (let rowIndex = 0; rowIndex < values.length; rowIndex += 1) {
        const rowValue = String(values[rowIndex]).toLowerCase();

        if (matchText(rowValue, searchValue)) {
          outputBuffer[outputCount] = rowIndex;
          outputCount += 1;
        }
      }

      return outputCount;
    }

    for (let i = 0; i < inputCount; i += 1) {
      const rowIndex = getBaseIndexAt(baseIndices, i);
      const rowValue = String(values[rowIndex]).toLowerCase();

      if (matchText(rowValue, searchValue)) {
        outputBuffer[outputCount] = rowIndex;
        outputCount += 1;
      }
    }

    return outputCount;
  }

  function filterOneNumericStringIdColumn(
    ids,
    dictionary,
    lowerValues,
    searchValue,
    baseIndices,
    matchText,
    outputBuffer
  ) {
    let outputCount = 0;
    const inputCount = getBaseIndicesCount(baseIndices);

    if (baseIndices === null) {
      for (let rowIndex = 0; rowIndex < ids.length; rowIndex += 1) {
        const id = ids[rowIndex];
        const rowValue =
          lowerValues[id] !== undefined
            ? lowerValues[id]
            : String(dictionary[id] === undefined ? "" : dictionary[id]).toLowerCase();
        if (matchText(rowValue, searchValue)) {
          outputBuffer[outputCount] = rowIndex;
          outputCount += 1;
        }
      }

      return outputCount;
    }

    for (let i = 0; i < inputCount; i += 1) {
      const rowIndex = getBaseIndexAt(baseIndices, i);
      const id = ids[rowIndex];
      const rowValue =
        lowerValues[id] !== undefined
          ? lowerValues[id]
          : String(dictionary[id] === undefined ? "" : dictionary[id]).toLowerCase();
      if (matchText(rowValue, searchValue)) {
        outputBuffer[outputCount] = rowIndex;
        outputCount += 1;
      }
    }

    return outputCount;
  }

  function filterNumericColumnarData(
    numericData,
    baseIndices,
    activeFilterEntries,
    keyToIndex,
    useCache,
    baseColumnCount,
    scratch,
    options
  ) {
    const numericFilterEntries = getNumericFilterEntries(
      activeFilterEntries,
      keyToIndex
    );
    let descriptors = buildNumericColumnDescriptors(
      numericData,
      numericFilterEntries,
      useCache,
      baseColumnCount
    );
    if (descriptors.length === 0) {
      return null;
    }

    if (shouldUseSmarterPlanner(options) && descriptors.length > 1) {
      const sampleIndices = createPlannerSampleIndices(
        baseIndices,
        numericData.rowCount
      );
      descriptors = orderDescriptorsBySampleHits(
        descriptors,
        sampleIndices,
        (descriptor, sample) => {
          let hitCount = 0;
          for (let i = 0; i < sample.count; i += 1) {
            const rowIndex = sample.buffer[i];
            let rowValue = "";
            if (descriptor.mode === "cache") {
              rowValue = descriptor.values[rowIndex];
            } else if (descriptor.mode === "stringId") {
              const id = descriptor.ids[rowIndex];
              rowValue =
                descriptor.lowerValues[id] !== undefined
                  ? descriptor.lowerValues[id]
                  : String(
                      descriptor.dictionary[id] === undefined
                        ? ""
                        : descriptor.dictionary[id]
                    ).toLowerCase();
            } else {
              rowValue = String(descriptor.values[rowIndex]).toLowerCase();
            }

            if (matchesText(rowValue, descriptor.search)) {
              hitCount += 1;
            }
          }

          return hitCount;
        }
      );
    }

    let candidates = baseIndices;

    for (let j = 0; j < descriptors.length; j += 1) {
      const descriptor = descriptors[j];
      const inputBuffer = getBaseIndicesBuffer(candidates);
      const inputCount =
        candidates === null
          ? descriptor.mode === "cache" || descriptor.mode === "number"
            ? descriptor.values.length
            : descriptor.ids.length
          : getBaseIndicesCount(candidates);
      const outputBuffer = selectOutputBuffer(
        scratch,
        inputBuffer,
        inputCount
      );
      let outputCount = 0;
      if (descriptor.mode === "cache") {
        outputCount = filterOneNumericColumnCached(
          descriptor.values,
          descriptor.search,
          candidates,
          matchesText,
          outputBuffer
        );
      } else if (descriptor.mode === "stringId") {
        outputCount = filterOneNumericStringIdColumn(
          descriptor.ids,
          descriptor.dictionary,
          descriptor.lowerValues,
          descriptor.search,
          candidates,
          matchesText,
          outputBuffer
        );
      } else {
        outputCount = filterOneNumericColumnNoCache(
          descriptor.values,
          descriptor.search,
          candidates,
          matchesText,
          outputBuffer
        );
      }

      candidates = createIndexResult(outputBuffer, outputCount);

      if (outputCount === 0) {
        return candidates;
      }
    }

    return candidates;
  }

  function createNumericRowFilterController(initialRows, options) {
    const keyToIndex =
      (options && options.keyToIndex) || Object.create(null);
    const configuredBaseColumnCount = options && options.baseColumnCount;
    const configuredCacheOffset = options && options.cacheOffset;
    const baseColumnCount = Number.isFinite(configuredBaseColumnCount)
      ? configuredBaseColumnCount
      : 0;
    const cacheOffset = Number.isFinite(configuredCacheOffset)
      ? configuredCacheOffset
      : baseColumnCount;
    let allRows = Array.isArray(initialRows) ? initialRows : [];
    let cacheAvailable = hasNumericRowsCache(allRows, cacheOffset);
    let currentIndices = null;

    function resetRowsState(nextRows) {
      allRows = Array.isArray(nextRows) ? nextRows : [];
      cacheAvailable = hasNumericRowsCache(allRows, cacheOffset);
      currentIndices = null;
    }

    return {
      setData(nextData) {
        resetRowsState(nextData);
      },
      apply(rawFilters, options) {
        const providedBaseIndices = normalizeProvidedBaseIndices(options);
        const hasProvidedBaseIndices = providedBaseIndices !== null;
        const activeFilterEntries = sortFilterEntriesBySelectivity(
          getActiveFilterEntries(rawFilters)
        );
        if (activeFilterEntries.length === 0) {
          currentIndices = hasProvidedBaseIndices ? providedBaseIndices : null;
          return currentIndices;
        }

        const useCache = shouldUseCache(options) && cacheAvailable;
        currentIndices = filterNumericRowIndices(
          allRows,
          providedBaseIndices,
          activeFilterEntries,
          keyToIndex,
          useCache,
          baseColumnCount,
          cacheOffset,
          sharedNumericScratch,
          options
        );
        return currentIndices;
      },
      setRows(nextRows) {
        resetRowsState(nextRows);
      },
      getCurrentIndices() {
        return currentIndices;
      },
      setCurrentIndices(nextIndices) {
        currentIndices = normalizeProvidedBaseIndices({
          baseIndices: nextIndices,
        });
      },
      getData() {
        return allRows;
      },
      getCurrentCount() {
        if (currentIndices === null) {
          return allRows.length;
        }

        return currentIndices.count;
      },
    };
  }

  function createNumericColumnarFilterController(initialData, options) {
    const keyToIndex =
      (options && options.keyToIndex) || Object.create(null);
    const baseColumnCount = options && options.baseColumnCount;
    const cacheOffset = options && options.cacheOffset;
    let allData = normalizeNumericData(initialData, baseColumnCount, cacheOffset);
    let cacheAvailable = hasNumericDataCache(allData);
    let currentIndices = null;

    return {
      setData(nextData) {
        allData = normalizeNumericData(nextData, baseColumnCount, cacheOffset);
        cacheAvailable = hasNumericDataCache(allData);
        currentIndices = null;
      },
      apply(rawFilters, options) {
        const providedBaseIndices = normalizeProvidedBaseIndices(options);
        const hasProvidedBaseIndices = providedBaseIndices !== null;
        const activeFilterEntries = sortFilterEntriesBySelectivity(
          getActiveFilterEntries(rawFilters)
        );
        if (activeFilterEntries.length === 0) {
          currentIndices = hasProvidedBaseIndices ? providedBaseIndices : null;
          return currentIndices;
        }

        const useCache = shouldUseCache(options) && cacheAvailable;
        currentIndices = filterNumericColumnarData(
          allData,
          providedBaseIndices,
          activeFilterEntries,
          keyToIndex,
          useCache,
          baseColumnCount,
          sharedNumericScratch,
          options
        );
        return currentIndices;
      },
      getCurrentIndices() {
        return currentIndices;
      },
      setCurrentIndices(nextIndices) {
        currentIndices = normalizeProvidedBaseIndices({
          baseIndices: nextIndices,
        });
      },
      getData() {
        return allData;
      },
      getCurrentCount() {
        if (currentIndices === null) {
          return allData.rowCount;
        }

        return currentIndices.count;
      },
    };
  }

const fastTableFilteringApi = {
  createRowFilterController,
  createColumnarFilterController,
  createNumericRowFilterController,
  createNumericColumnarFilterController,
  buildDictionaryKeySearchPrefilter,
  precomputeDictionaryKeySearchState,
};

export {
  createRowFilterController,
  createColumnarFilterController,
  createNumericRowFilterController,
  createNumericColumnarFilterController,
  buildDictionaryKeySearchPrefilter,
  precomputeDictionaryKeySearchState,
  fastTableFilteringApi,
};
