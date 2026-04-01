  const SORT_STATE_NONE = "none";
  const SORT_STATE_DESC = "desc";
  const SORT_STATE_ASC = "asc";
  const DEFAULT_SORT_MODE = "native";
  const SYMBOL_BY_STATE = {
    none: "\u25B6",
    desc: "\u25BC",
    asc: "\u25B2",
  };
  const VALUE_TYPE_NUMBER = "number";
  const VALUE_TYPE_STRING = "string";
  const SORT_METHOD_AVAILABILITY_FLAG = "__fastTableSortMethodAvailable";
  const sortMethodRegistry = Object.create(null);

  function normalizeSortMode(mode) {
    if (typeof mode === "string" && mode.trim() !== "") {
      const normalized = mode.trim();
      if (sortMethodRegistry[normalized]) {
        return normalized;
      }
    }

    return DEFAULT_SORT_MODE;
  }

  function compareValues(a, b) {
    if (a === b) {
      return 0;
    }

    if (a === undefined || a === null) {
      return 1;
    }

    if (b === undefined || b === null) {
      return -1;
    }

    if (typeof a === "number" && typeof b === "number") {
      if (Number.isNaN(a) && Number.isNaN(b)) {
        return 0;
      }

      if (Number.isNaN(a)) {
        return 1;
      }

      if (Number.isNaN(b)) {
        return -1;
      }

      return a - b;
    }

    const aText = String(a);
    const bText = String(b);

    if (aText < bText) {
      return -1;
    }

    if (aText > bText) {
      return 1;
    }

    return 0;
  }

  function compareNumberValues(a, b) {
    if (a === b) {
      return 0;
    }

    if (a === undefined || a === null) {
      return 1;
    }

    if (b === undefined || b === null) {
      return -1;
    }

    if (Number.isNaN(a) && Number.isNaN(b)) {
      return 0;
    }

    if (Number.isNaN(a)) {
      return 1;
    }

    if (Number.isNaN(b)) {
      return -1;
    }

    return a - b;
  }

  function compareStringValues(a, b) {
    if (a === b) {
      return 0;
    }

    if (a === undefined || a === null) {
      return 1;
    }

    if (b === undefined || b === null) {
      return -1;
    }

    if (a < b) {
      return -1;
    }

    if (a > b) {
      return 1;
    }

    return 0;
  }

  function normalizeValueType(valueType) {
    if (valueType === VALUE_TYPE_NUMBER || valueType === VALUE_TYPE_STRING) {
      return valueType;
    }

    return null;
  }

  function createDescriptorComparator(
    descriptor,
    useTypedComparator,
    columnTypeByKey
  ) {
    const columnKey = descriptor.columnKey;
    const directionMultiplier =
      descriptor.direction === SORT_STATE_DESC ? -1 : 1;

    if (!useTypedComparator || !columnTypeByKey) {
      return function (rowA, rowB) {
        return (
          compareValues(rowA[columnKey], rowB[columnKey]) * directionMultiplier
        );
      };
    }

    const valueType = normalizeValueType(columnTypeByKey[columnKey]);
    if (valueType === VALUE_TYPE_NUMBER) {
      return function (rowA, rowB) {
        return (
          compareNumberValues(rowA[columnKey], rowB[columnKey]) *
          directionMultiplier
        );
      };
    }

    if (valueType === VALUE_TYPE_STRING) {
      return function (rowA, rowB) {
        return (
          compareStringValues(rowA[columnKey], rowB[columnKey]) *
          directionMultiplier
        );
      };
    }

    return function (rowA, rowB) {
      return compareValues(rowA[columnKey], rowB[columnKey]) * directionMultiplier;
    };
  }

  function createIndexDescriptorComparator(
    descriptor,
    useTypedComparator,
    columnTypeByKey,
    rowsByIndex
  ) {
    const columnKey = descriptor.columnKey;
    const directionMultiplier =
      descriptor.direction === SORT_STATE_DESC ? -1 : 1;

    if (!useTypedComparator || !columnTypeByKey) {
      return function (indexA, indexB) {
        const rowA = rowsByIndex[indexA];
        const rowB = rowsByIndex[indexB];
        const valueA = rowA ? rowA[columnKey] : undefined;
        const valueB = rowB ? rowB[columnKey] : undefined;
        return compareValues(valueA, valueB) * directionMultiplier;
      };
    }

    const valueType = normalizeValueType(columnTypeByKey[columnKey]);
    if (valueType === VALUE_TYPE_NUMBER) {
      return function (indexA, indexB) {
        const rowA = rowsByIndex[indexA];
        const rowB = rowsByIndex[indexB];
        const valueA = rowA ? rowA[columnKey] : undefined;
        const valueB = rowB ? rowB[columnKey] : undefined;
        return compareNumberValues(valueA, valueB) * directionMultiplier;
      };
    }

    if (valueType === VALUE_TYPE_STRING) {
      return function (indexA, indexB) {
        const rowA = rowsByIndex[indexA];
        const rowB = rowsByIndex[indexB];
        const valueA = rowA ? rowA[columnKey] : undefined;
        const valueB = rowB ? rowB[columnKey] : undefined;
        return compareStringValues(valueA, valueB) * directionMultiplier;
      };
    }

    return function (indexA, indexB) {
      const rowA = rowsByIndex[indexA];
      const rowB = rowsByIndex[indexB];
      const valueA = rowA ? rowA[columnKey] : undefined;
      const valueB = rowB ? rowB[columnKey] : undefined;
      return compareValues(valueA, valueB) * directionMultiplier;
    };
  }

  function resolveComparatorOptions(comparatorOptions, fallbackOptions) {
    const options = comparatorOptions || {};
    const fallback = fallbackOptions || {};
    const useTypedComparator =
      typeof options.useTypedComparator === "boolean"
        ? options.useTypedComparator
        : typeof fallback.useTypedComparator === "boolean"
          ? fallback.useTypedComparator
          : false;
    const columnTypeByKey =
      options.columnTypeByKey || fallback.columnTypeByKey || null;

    return {
      useTypedComparator,
      columnTypeByKey,
    };
  }

  function removeFromOrderedKeys(orderedKeys, key) {
    const nextKeys = [];

    for (let i = 0; i < orderedKeys.length; i += 1) {
      if (orderedKeys[i] !== key) {
        nextKeys.push(orderedKeys[i]);
      }
    }

    return nextKeys;
  }

  function createCompositeComparator(descriptors, comparatorOptions) {
    const resolvedOptions = resolveComparatorOptions(comparatorOptions);
    const descriptorComparators = new Array(descriptors.length);

    for (let i = 0; i < descriptors.length; i += 1) {
      descriptorComparators[i] = createDescriptorComparator(
        descriptors[i],
        resolvedOptions.useTypedComparator,
        resolvedOptions.columnTypeByKey
      );
    }

    return function (rowA, rowB) {
      for (let i = 0; i < descriptorComparators.length; i += 1) {
        const compareResult = descriptorComparators[i](rowA, rowB);

        if (compareResult !== 0) {
          return compareResult;
        }
      }

      return 0;
    };
  }

  function createCompositeIndexComparator(
    descriptors,
    comparatorOptions,
    rowsByIndex
  ) {
    const resolvedOptions = resolveComparatorOptions(comparatorOptions);
    const descriptorComparators = new Array(descriptors.length);

    for (let i = 0; i < descriptors.length; i += 1) {
      descriptorComparators[i] = createIndexDescriptorComparator(
        descriptors[i],
        resolvedOptions.useTypedComparator,
        resolvedOptions.columnTypeByKey,
        rowsByIndex
      );
    }

    return function (indexA, indexB) {
      for (let i = 0; i < descriptorComparators.length; i += 1) {
        const compareResult = descriptorComparators[i](indexA, indexB);

        if (compareResult !== 0) {
          return compareResult;
        }
      }

      return 0;
    };
  }

  function hasValidPrecomputedIndexKeys(
    precomputedIndexKeys,
    descriptorCount,
    rowCount
  ) {
    if (!Array.isArray(precomputedIndexKeys)) {
      return false;
    }

    if (precomputedIndexKeys.length < descriptorCount) {
      return false;
    }

    for (let i = 0; i < descriptorCount; i += 1) {
      const columnKeys = precomputedIndexKeys[i];
      if (!columnKeys) {
        return false;
      }

      const length = columnKeys.length;
      if (!Number.isFinite(length) || length < rowCount) {
        return false;
      }
    }

    return true;
  }

  function hasValidPrecomputedRankColumns(
    precomputedRankColumns,
    descriptorCount,
    rowCount
  ) {
    if (!Array.isArray(precomputedRankColumns)) {
      return false;
    }

    if (precomputedRankColumns.length < descriptorCount) {
      return false;
    }

    for (let i = 0; i < descriptorCount; i += 1) {
      const rankByRowId = precomputedRankColumns[i];
      if (!rankByRowId || !ArrayBuffer.isView(rankByRowId)) {
        return false;
      }

      const length = rankByRowId.length;
      if (!Number.isFinite(length) || length < rowCount) {
        return false;
      }
    }

    return true;
  }

  function hasSortableCollection(value) {
    if (!value) {
      return false;
    }

    if (!Array.isArray(value) && !ArrayBuffer.isView(value)) {
      return false;
    }

    return value.length > 1;
  }

  function createCompositePrecomputedRankComparator(
    descriptors,
    precomputedRankColumns
  ) {
    const descriptorComparators = new Array(descriptors.length);

    for (let i = 0; i < descriptors.length; i += 1) {
      const descriptor = descriptors[i];
      const rankByRowId = precomputedRankColumns[i];
      const directionMultiplier =
        descriptor.direction === SORT_STATE_DESC ? -1 : 1;

      descriptorComparators[i] = function (indexA, indexB) {
        const rankA = rankByRowId[indexA] >>> 0;
        const rankB = rankByRowId[indexB] >>> 0;
        if (rankA === rankB) {
          return 0;
        }

        return (rankA < rankB ? -1 : 1) * directionMultiplier;
      };
    }

    return function (indexA, indexB) {
      for (let i = 0; i < descriptorComparators.length; i += 1) {
        const compareResult = descriptorComparators[i](indexA, indexB);

        if (compareResult !== 0) {
          return compareResult;
        }
      }

      return 0;
    };
  }

  function createCompositePrecomputedIndexComparator(
    descriptors,
    comparatorOptions,
    precomputedIndexKeys
  ) {
    const resolvedOptions = resolveComparatorOptions(comparatorOptions);
    const descriptorComparators = new Array(descriptors.length);

    for (let i = 0; i < descriptors.length; i += 1) {
      const descriptor = descriptors[i];
      const values = precomputedIndexKeys[i];
      const directionMultiplier =
        descriptor.direction === SORT_STATE_DESC ? -1 : 1;
      const valueType = normalizeValueType(
        resolvedOptions.columnTypeByKey &&
          resolvedOptions.columnTypeByKey[descriptor.columnKey]
      );

      if (resolvedOptions.useTypedComparator && valueType === VALUE_TYPE_NUMBER) {
        descriptorComparators[i] = function (positionA, positionB) {
          return (
            compareNumberValues(values[positionA], values[positionB]) *
            directionMultiplier
          );
        };
        continue;
      }

      if (resolvedOptions.useTypedComparator && valueType === VALUE_TYPE_STRING) {
        descriptorComparators[i] = function (positionA, positionB) {
          return (
            compareStringValues(values[positionA], values[positionB]) *
            directionMultiplier
          );
        };
        continue;
      }

      descriptorComparators[i] = function (positionA, positionB) {
        return (
          compareValues(values[positionA], values[positionB]) *
          directionMultiplier
        );
      };
    }

    return function (positionA, positionB) {
      for (let i = 0; i < descriptorComparators.length; i += 1) {
        const compareResult = descriptorComparators[i](positionA, positionB);

        if (compareResult !== 0) {
          return compareResult;
        }
      }

      return 0;
    };
  }

  function sortRowsNative(rows, compareFn) {
    if (!Array.isArray(rows) || rows.length <= 1 || typeof compareFn !== "function") {
      return;
    }

    rows.sort(compareFn);
  }

  function createUnavailableSortMethod(label) {
    const method = function sortRowsUnavailable(rows, compareFn) {
      if (!Array.isArray(rows) || rows.length <= 1 || typeof compareFn !== "function") {
        return;
      }

      throw new Error(`${label} mode requested but API is unavailable.`);
    };
    Object.defineProperty(method, SORT_METHOD_AVAILABILITY_FLAG, {
      value: false,
      configurable: true,
    });
    return method;
  }

  function markSortMethodAsAvailable(method) {
    if (typeof method !== "function") {
      return method;
    }

    Object.defineProperty(method, SORT_METHOD_AVAILABILITY_FLAG, {
      value: true,
      configurable: true,
    });
    return method;
  }

  function isSortModeAvailable(mode) {
    if (typeof mode !== "string" || mode.trim() === "") {
      return false;
    }

    const normalized = mode.trim();
    const method = sortMethodRegistry[normalized];
    if (typeof method !== "function") {
      return false;
    }

    return method[SORT_METHOD_AVAILABILITY_FLAG] !== false;
  }

  function getAvailableSortModes() {
    const modeNames = Object.keys(sortMethodRegistry);
    const availableModes = [];
    for (let i = 0; i < modeNames.length; i += 1) {
      const mode = modeNames[i];
      if (isSortModeAvailable(mode)) {
        availableModes.push(mode);
      }
    }

    if (availableModes.length === 0 && typeof sortMethodRegistry.native === "function") {
      return [DEFAULT_SORT_MODE];
    }

    return availableModes;
  }

  sortMethodRegistry.native = markSortMethodAsAvailable(sortRowsNative);
  sortMethodRegistry.timsort = createUnavailableSortMethod("TimSort");
  sortMethodRegistry.timsort0060 = createUnavailableSortMethod("TimSort 0060");
  sortMethodRegistry.timsort0018 = createUnavailableSortMethod("TimSort 0018");
  sortMethodRegistry.quadsort = createUnavailableSortMethod("QuadSort");
  sortMethodRegistry.fluxsort = createUnavailableSortMethod("FluxSort");

  function registerSortMethod(mode, method) {
    if (typeof mode !== "string" || mode.trim() === "") {
      return false;
    }

    if (typeof method !== "function") {
      return false;
    }

    const normalized = mode.trim();
    sortMethodRegistry[normalized] = markSortMethodAsAvailable(method);
    return true;
  }

  function getSortMethod(mode) {
    const normalizedMode = normalizeSortMode(mode);
    return sortMethodRegistry[normalizedMode] || sortMethodRegistry.native;
  }

  function cloneDescriptors(descriptors) {
    const out = new Array(descriptors.length);

    for (let i = 0; i < descriptors.length; i += 1) {
      out[i] = {
        columnKey: descriptors[i].columnKey,
        direction: descriptors[i].direction,
      };
    }

    return out;
  }

  function getSortSymbol(state) {
    return SYMBOL_BY_STATE[state] || SYMBOL_BY_STATE.none;
  }

  function createSortController(options) {
    const config = options || {};
    const configuredColumnKeys = Array.isArray(config.columnKeys)
      ? config.columnKeys
      : [];
    const allowedKeySet = Object.create(null);
    for (let i = 0; i < configuredColumnKeys.length; i += 1) {
      allowedKeySet[configuredColumnKeys[i]] = true;
    }
    const defaultColumnKey =
      typeof config.defaultColumnKey === "string"
        ? config.defaultColumnKey
        : "index";
    const defaultComparatorOptions = resolveComparatorOptions(
      {
        useTypedComparator: config.defaultUseTypedComparator,
        columnTypeByKey: config.columnTypeByKey,
      },
      null
    );
    const stateByKey = Object.create(null);
    let orderedKeys = [];

    function getState(columnKey) {
      const next = stateByKey[columnKey];
      if (next !== SORT_STATE_DESC && next !== SORT_STATE_ASC) {
        return SORT_STATE_NONE;
      }

      return next;
    }

    function setState(columnKey, nextState) {
      if (nextState === SORT_STATE_NONE) {
        delete stateByKey[columnKey];
        orderedKeys = removeFromOrderedKeys(orderedKeys, columnKey);
        return;
      }

      const hadState = getState(columnKey) !== SORT_STATE_NONE;
      stateByKey[columnKey] = nextState;
      if (!hadState) {
        orderedKeys = removeFromOrderedKeys(orderedKeys, columnKey);
        orderedKeys.push(columnKey);
      }
    }

    function getDescriptors() {
      const descriptors = [];

      for (let i = 0; i < orderedKeys.length; i += 1) {
        const columnKey = orderedKeys[i];
        const state = getState(columnKey);
        if (state === SORT_STATE_NONE) {
          continue;
        }

        descriptors.push({
          columnKey,
          direction: state,
        });
      }

      return descriptors;
    }

    function isAllowedKey(columnKey) {
      if (configuredColumnKeys.length === 0) {
        return true;
      }

      return allowedKeySet[columnKey] === true;
    }

    function getSortPlan(mode, sortOptions) {
      const sortMode = normalizeSortMode(mode);
      const descriptors = getDescriptors();
      const comparatorOptions = resolveComparatorOptions(
        sortOptions,
        defaultComparatorOptions
      );
      let effectiveDescriptors = cloneDescriptors(descriptors);
      let restoredDefault = false;

      if (effectiveDescriptors.length === 0) {
        effectiveDescriptors = [
          {
            columnKey: defaultColumnKey,
            direction: SORT_STATE_ASC,
          },
        ];
        restoredDefault = true;
      }

      return {
        sortMode,
        descriptors,
        effectiveDescriptors,
        restoredDefault,
        comparatorOptions,
      };
    }

    return {
      cycle(columnKey) {
        if (!isAllowedKey(columnKey)) {
          return this.getSortDescriptors();
        }

        const currentState = getState(columnKey);
        let nextState = SORT_STATE_NONE;
        if (currentState === SORT_STATE_NONE) {
          nextState = SORT_STATE_DESC;
        } else if (currentState === SORT_STATE_DESC) {
          nextState = SORT_STATE_ASC;
        } else {
          nextState = SORT_STATE_NONE;
        }

        setState(columnKey, nextState);
        return this.getSortDescriptors();
      },
      reset() {
        orderedKeys = [];
        for (const key in stateByKey) {
          if (Object.prototype.hasOwnProperty.call(stateByKey, key)) {
            delete stateByKey[key];
          }
        }
      },
      getStateForKey(columnKey) {
        return getState(columnKey);
      },
      getSortDescriptors() {
        return getDescriptors();
      },
      sortRows(rows, mode, sortOptions) {
        const startMs = performance.now();
        const plan = getSortPlan(mode, sortOptions);
        const compareFn = createCompositeComparator(
          plan.effectiveDescriptors,
          plan.comparatorOptions
        );
        const sortMethod = getSortMethod(plan.sortMode);
        sortMethod(rows, compareFn);

        return {
          changedOrder: hasSortableCollection(rows),
          durationMs: performance.now() - startMs,
          sortMode: plan.sortMode,
          comparatorMode: plan.comparatorOptions.useTypedComparator
            ? "typed"
            : "generic",
          dataPath: "rows",
          descriptors: cloneDescriptors(plan.descriptors),
          effectiveDescriptors: cloneDescriptors(plan.effectiveDescriptors),
          restoredDefault: plan.restoredDefault,
        };
      },
      sortIndices(indices, rowsByIndex, mode, sortOptions) {
        const startMs = performance.now();
        const plan = getSortPlan(mode, sortOptions);
        const sortMethod = getSortMethod(plan.sortMode);
        const rowCount =
          rowsByIndex && typeof rowsByIndex.length === "number"
            ? rowsByIndex.length
            : 0;
        const precomputedRankColumns =
          sortOptions && Array.isArray(sortOptions.precomputedRankColumns)
            ? sortOptions.precomputedRankColumns
            : null;
        const canUsePrecomputedRanks = hasValidPrecomputedRankColumns(
          precomputedRankColumns,
          plan.effectiveDescriptors.length,
          rowCount
        );
        const precomputedIndexKeys =
          sortOptions && Array.isArray(sortOptions.precomputedIndexKeys)
            ? sortOptions.precomputedIndexKeys
            : null;
        const canUsePrecomputedKeys = hasValidPrecomputedIndexKeys(
          precomputedIndexKeys,
          plan.effectiveDescriptors.length,
          indices.length
        );

        if (canUsePrecomputedRanks) {
          const compareFn = createCompositePrecomputedRankComparator(
            plan.effectiveDescriptors,
            precomputedRankColumns
          );
          sortMethod(indices, compareFn);
        } else if (canUsePrecomputedKeys) {
          const order = new Array(indices.length);
          for (let i = 0; i < order.length; i += 1) {
            order[i] = i;
          }

          const compareFn = createCompositePrecomputedIndexComparator(
            plan.effectiveDescriptors,
            plan.comparatorOptions,
            precomputedIndexKeys
          );
          sortMethod(order, compareFn);

          const sortedIndices = new Array(indices.length);
          for (let i = 0; i < order.length; i += 1) {
            sortedIndices[i] = indices[order[i]];
          }

          for (let i = 0; i < sortedIndices.length; i += 1) {
            indices[i] = sortedIndices[i];
          }
        } else {
          const compareFn = createCompositeIndexComparator(
            plan.effectiveDescriptors,
            plan.comparatorOptions,
            rowsByIndex
          );
          sortMethod(indices, compareFn);
        }

        return {
          changedOrder: hasSortableCollection(indices),
          durationMs: performance.now() - startMs,
          sortMode: plan.sortMode,
          comparatorMode: canUsePrecomputedRanks
            ? "rank"
            : plan.comparatorOptions.useTypedComparator
              ? "typed"
              : "generic",
          dataPath: canUsePrecomputedRanks
            ? "indices+ranks"
            : canUsePrecomputedKeys
              ? "indices+keys"
              : "indices",
          descriptors: cloneDescriptors(plan.descriptors),
          effectiveDescriptors: cloneDescriptors(plan.effectiveDescriptors),
          restoredDefault: plan.restoredDefault,
        };
      },
    };
  }

  const fastTableSortingApi = {
  createSortController,
  registerSortMethod,
  getAvailableSortModes,
  isSortModeAvailable,
  getSortSymbol,
  SORT_STATE_NONE,
  SORT_STATE_DESC,
  SORT_STATE_ASC,
  DEFAULT_SORT_MODE,
};

export {
  createSortController,
  registerSortMethod,
  getAvailableSortModes,
  isSortModeAvailable,
  getSortSymbol,
  SORT_STATE_NONE,
  SORT_STATE_DESC,
  SORT_STATE_ASC,
  DEFAULT_SORT_MODE,
  fastTableSortingApi,
};
