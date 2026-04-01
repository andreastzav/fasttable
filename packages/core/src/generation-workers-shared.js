function defaultNow() {
  if (typeof performance !== "undefined" && typeof performance.now === "function") {
    return performance.now();
  }
  return Date.now();
}
function normalizeEnvironmentError(value) {
  if (!value) {
    return null;
  }
  if (value instanceof Error) {
    return value;
  }
  return new Error(String(value));
}
function createGenerationWorkersApi(options) {
  const input = options || {};
  const generationApi = input.generationApi;
  const createWorkerFactory =
    typeof input.createGenerationWorker === "function"
      ? input.createGenerationWorker
      : null;
  const validateGenerationEnvironment =
    typeof input.validateGenerationEnvironment === "function"
      ? input.validateGenerationEnvironment
      : null;
  const validateSortingEnvironment =
    typeof input.validateSortingEnvironment === "function"
      ? input.validateSortingEnvironment
      : null;
  const now = typeof input.now === "function" ? input.now : defaultNow;
  if (!generationApi) {
    throw new Error("generationApi is required.");
  }
  if (!createWorkerFactory) {
    throw new Error("createGenerationWorker factory is required.");
  }

  const columnKeys = Array.isArray(generationApi.COLUMN_KEYS)
    ? generationApi.COLUMN_KEYS.slice()
    : [];
  const baseColumnCount = Number.isFinite(generationApi.BASE_COLUMN_COUNT)
    ? generationApi.BASE_COLUMN_COUNT
    : columnKeys.length;
  const numericCacheOffset = Number.isFinite(generationApi.NUMERIC_CACHE_OFFSET)
    ? generationApi.NUMERIC_CACHE_OFFSET
    : baseColumnCount;
  const cacheKeys = columnKeys.map((key) => `${key}Cache`);
  const WORKER_STALL_TIMEOUT_MS = 120000;
  const DICTIONARY_CARDINALITY_RATIO_LIMIT = 0.2;
  const DICTIONARY_MAX_UNIQUE_KEYS = 65535;
  function createGenerationWorker() {
    return createWorkerFactory();
  }

  function toPositiveInt(value, fallback) {
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return fallback;
    }

    return parsed;
  }

  function buildChunkQueue(totalRows, selectedChunkSize) {
    const queue = [];
    let remaining = totalRows;
    let nextStartIndex = 1;
    let nextChunkId = 0;

    while (remaining > 0) {
      const chunkRowCount = Math.min(remaining, selectedChunkSize);
      queue.push({
        chunkId: nextChunkId,
        rowCount: chunkRowCount,
        startIndex: nextStartIndex,
      });
      remaining -= chunkRowCount;
      nextStartIndex += chunkRowCount;
      nextChunkId += 1;
    }

    return queue;
  }

  function createEmptyWorkerMetrics() {
    return {
      rowGenerationMs: 0,
      rowCacheGenerationMs: 0,
      numericTransformMs: 0,
      numericCacheGenerationMs: 0,
      columnarDerivationMs: 0,
      columnarCacheGenerationMs: 0,
      totalMs: 0,
    };
  }

  function createDefaultObjectColumn(columnKey, totalRows) {
    if (columnKey === "firstName" || columnKey === "lastName") {
      return new Array(totalRows);
    }

    if (columnKey === "age") {
      return new Uint8Array(totalRows);
    }

    return new Int32Array(totalRows);
  }

  function createObjectColumnStore(totalRows, chunkColumns) {
    const columns = {};
    const incomingKeys = Object.keys(chunkColumns || {});
    for (let i = 0; i < incomingKeys.length; i += 1) {
      const key = incomingKeys[i];
      const value = chunkColumns[key];
      if (ArrayBuffer.isView(value) && typeof value.constructor === "function") {
        columns[key] = new value.constructor(totalRows);
      } else {
        columns[key] = new Array(totalRows);
      }
    }

    for (let i = 0; i < columnKeys.length; i += 1) {
      const key = columnKeys[i];
      if (columns[key] === undefined) {
        columns[key] = createDefaultObjectColumn(key, totalRows);
      }
    }

    for (let i = 0; i < cacheKeys.length; i += 1) {
      const key = cacheKeys[i];
      if (columns[key] === undefined) {
        columns[key] = new Array(totalRows);
      }
    }

    return {
      rowCount: totalRows,
      columns,
    };
  }

  function copyArrayLike(dest, src, startOffset, length) {
    if (length <= 0) {
      return;
    }

    if (
      ArrayBuffer.isView(dest) &&
      ArrayBuffer.isView(src) &&
      typeof dest.set === "function"
    ) {
      if (src.length < length) {
        throw new Error("Chunk column length is smaller than expected.");
      }
      dest.set(src.subarray(0, length), startOffset);
      return;
    }

    if (src.length < length) {
      throw new Error("Chunk column length is smaller than expected.");
    }

    for (let i = 0; i < length; i += 1) {
      dest[startOffset + i] = src[i];
    }
  }

  function getArrayLikeLength(value) {
    if (!value) {
      return -1;
    }

    if (ArrayBuffer.isView(value) || Array.isArray(value)) {
      return value.length;
    }

    return -1;
  }

  function validateObjectChunkColumns(chunkColumns, expectedLength, expectedKeys) {
    const keys = Object.keys(chunkColumns || {});
    if (Array.isArray(expectedKeys) && expectedKeys.length > 0) {
      for (let i = 0; i < expectedKeys.length; i += 1) {
        if (!Object.prototype.hasOwnProperty.call(chunkColumns, expectedKeys[i])) {
          throw new Error(`Object chunk is missing column ${expectedKeys[i]}.`);
        }
      }
    }

    for (let i = 0; i < keys.length; i += 1) {
      const key = keys[i];
      const source = chunkColumns[key];
      const sourceLength = getArrayLikeLength(source);
      if (sourceLength !== expectedLength) {
        throw new Error(
          `Object chunk column length mismatch for ${key}: expected ${expectedLength}, got ${sourceLength}.`
        );
      }
    }
  }

  function validateNumericChunkColumns(
    chunkNumeric,
    expectedLength,
    expectedColumnCount,
    expectedHasCache
  ) {
    const numericColumns = chunkNumeric.columns || [];
    if (
      Number.isFinite(expectedColumnCount) &&
      numericColumns.length !== expectedColumnCount
    ) {
      throw new Error(
        `Numeric chunk column count mismatch: expected ${expectedColumnCount}, got ${numericColumns.length}.`
      );
    }

    for (let colIndex = 0; colIndex < numericColumns.length; colIndex += 1) {
      const source = numericColumns[colIndex];
      const sourceLength = getArrayLikeLength(source);
      if (sourceLength !== expectedLength) {
        throw new Error(
          `Numeric chunk column length mismatch at column ${colIndex}: expected ${expectedLength}, got ${sourceLength}.`
        );
      }
    }

    if (
      typeof expectedHasCache === "boolean" &&
      chunkNumeric.hasCacheColumns !== expectedHasCache
    ) {
      throw new Error(
        `Numeric chunk cache presence mismatch: expected hasCacheColumns=${expectedHasCache}, got ${chunkNumeric.hasCacheColumns}.`
      );
    }

    if (chunkNumeric.hasCacheColumns === true) {
      if (!Array.isArray(chunkNumeric.cacheColumns)) {
        throw new Error("Numeric chunk cacheColumns is missing.");
      }
      if (chunkNumeric.cacheColumns.length !== numericColumns.length) {
        throw new Error(
          `Numeric chunk cache column count mismatch: expected ${numericColumns.length}, got ${chunkNumeric.cacheColumns.length}.`
        );
      }

      for (let colIndex = 0; colIndex < chunkNumeric.cacheColumns.length; colIndex += 1) {
        const source = chunkNumeric.cacheColumns[colIndex];
        const sourceLength = getArrayLikeLength(source);
        if (sourceLength !== expectedLength) {
          throw new Error(
            `Numeric chunk cache column length mismatch at column ${colIndex}: expected ${expectedLength}, got ${sourceLength}.`
          );
        }
      }
    }
  }

  function mergeObjectColumnChunk(store, chunkColumns, startOffset, length) {
    const keys = Object.keys(store.columns);
    for (let i = 0; i < keys.length; i += 1) {
      const key = keys[i];
      const source = chunkColumns[key];
      if (source === undefined || source === null) {
        continue;
      }

      copyArrayLike(store.columns[key], source, startOffset, length);
    }
  }

  function normalizeDictionaryValue(value) {
    return String(value).toLowerCase();
  }

  function ensurePostingBucket(postings, lowerValue) {
    let bucket = postings[lowerValue];
    if (bucket === undefined) {
      bucket = [];
      postings[lowerValue] = bucket;
    }

    return bucket;
  }

  function createNumericColumnarAccumulator(totalRows, chunkNumeric) {
    const columnCount =
      chunkNumeric && Array.isArray(chunkNumeric.columns)
        ? chunkNumeric.columns.length
        : baseColumnCount;
    const columns = new Array(columnCount);
    const columnKinds = Array.isArray(chunkNumeric.columnKinds)
      ? chunkNumeric.columnKinds.slice()
      : new Array(columnCount).fill("int");
    const dictionaries = new Array(columnCount).fill(null);
    const lowerDictionaries = new Array(columnCount).fill(null);
    const lowerDictionaryValues = new Array(columnCount).fill(null);
    const stringMaps = new Array(columnCount).fill(null);

    for (let colIndex = 0; colIndex < columnCount; colIndex += 1) {
      const sourceColumn = chunkNumeric.columns[colIndex];
      if (columnKinds[colIndex] === "stringId") {
        columns[colIndex] = new Uint32Array(totalRows);
        dictionaries[colIndex] = [];
        lowerDictionaries[colIndex] = Object.create(null);
        lowerDictionaryValues[colIndex] = [];
        stringMaps[colIndex] = new Map();
      } else {
        if (
          ArrayBuffer.isView(sourceColumn) &&
          typeof sourceColumn.constructor === "function"
        ) {
          columns[colIndex] = new sourceColumn.constructor(totalRows);
        } else if (Array.isArray(sourceColumn)) {
          columns[colIndex] = new Array(totalRows);
        } else {
          columns[colIndex] = new Int32Array(totalRows);
        }

        dictionaries[colIndex] = [];
        lowerDictionaries[colIndex] = Object.create(null);
        lowerDictionaryValues[colIndex] = [];
      }
    }

    const hasCacheColumns =
      chunkNumeric &&
      chunkNumeric.hasCacheColumns === true &&
      Array.isArray(chunkNumeric.cacheColumns);
    const cacheColumns = hasCacheColumns ? new Array(columnCount) : null;
    if (cacheColumns) {
      for (let colIndex = 0; colIndex < columnCount; colIndex += 1) {
        cacheColumns[colIndex] = new Array(totalRows);
      }
    }

    return {
      rowCount: totalRows,
      columnCount,
      baseColumnCount: columnCount,
      cacheOffset: numericCacheOffset,
      hasCacheColumns,
      columns,
      columnKinds,
      dictionaries,
      lowerDictionaries,
      lowerDictionaryValues,
      cacheColumns,
      stringMaps,
    };
  }

  function mergeNumericColumnarChunk(store, chunkNumeric, startOffset, length) {
    for (let colIndex = 0; colIndex < store.columnCount; colIndex += 1) {
      const kind = store.columnKinds[colIndex];
      const sourceColumn = chunkNumeric.columns[colIndex];

      if (kind === "stringId") {
        const sourceIds = sourceColumn;
        const sourceDictionary =
          (chunkNumeric.dictionaries && chunkNumeric.dictionaries[colIndex]) || [];
        const sourceLowerValues =
          (chunkNumeric.lowerDictionaryValues &&
            chunkNumeric.lowerDictionaryValues[colIndex]) ||
          null;
        const destIds = store.columns[colIndex];
        const stringMap = store.stringMaps[colIndex];
        const destDictionary = store.dictionaries[colIndex];
        const destLowerPostings = store.lowerDictionaries[colIndex];
        const destLowerValues = store.lowerDictionaryValues[colIndex];
        const sourceDictionaryLength = sourceDictionary.length;
        const remap = new Uint32Array(sourceDictionaryLength);

        for (let sourceId = 0; sourceId < sourceDictionaryLength; sourceId += 1) {
          const rawValue =
            sourceDictionary[sourceId] === undefined
              ? ""
              : sourceDictionary[sourceId];
          const normalizedKey = normalizeDictionaryValue(rawValue);
          let valueId = stringMap.get(normalizedKey);
          if (valueId === undefined) {
            valueId = destDictionary.length;
            stringMap.set(normalizedKey, valueId);
            destDictionary.push(rawValue);
            const lowerValue =
              sourceLowerValues && sourceLowerValues[sourceId] !== undefined
                ? sourceLowerValues[sourceId]
                : normalizeDictionaryValue(rawValue);
            destLowerValues.push(lowerValue);
            ensurePostingBucket(destLowerPostings, lowerValue);
          }

          remap[sourceId] = valueId;
        }

        for (let i = 0; i < length; i += 1) {
          const sourceId = sourceIds[i];
          let valueId = 0;
          if (sourceId < sourceDictionaryLength) {
            valueId = remap[sourceId];
          } else {
            const rawValue =
              sourceDictionary[sourceId] === undefined
                ? ""
                : sourceDictionary[sourceId];
            const normalizedKey = normalizeDictionaryValue(rawValue);
            valueId = stringMap.get(normalizedKey);
            if (valueId === undefined) {
              valueId = destDictionary.length;
              stringMap.set(normalizedKey, valueId);
              destDictionary.push(rawValue);
              const lowerValue =
                sourceLowerValues && sourceLowerValues[sourceId] !== undefined
                  ? sourceLowerValues[sourceId]
                  : normalizeDictionaryValue(rawValue);
              destLowerValues.push(lowerValue);
              ensurePostingBucket(destLowerPostings, lowerValue);
            }
          }

          destIds[startOffset + i] = valueId;

          const normalized =
            destLowerValues[valueId] !== undefined
              ? destLowerValues[valueId]
              : normalizeDictionaryValue(
                  destDictionary[valueId] === undefined ? "" : destDictionary[valueId]
                );
          ensurePostingBucket(destLowerPostings, normalized).push(startOffset + i);
        }
      } else {
        copyArrayLike(store.columns[colIndex], sourceColumn, startOffset, length);
      }

      if (store.hasCacheColumns && Array.isArray(chunkNumeric.cacheColumns)) {
        const sourceCache = chunkNumeric.cacheColumns[colIndex];
        const destCache = store.cacheColumns[colIndex];
        if (!sourceCache || !destCache) {
          continue;
        }

        for (let i = 0; i < length; i += 1) {
          destCache[startOffset + i] = sourceCache[i];
        }
      }
    }
  }

  function finalizeNumericColumnarAccumulator(store) {
    const rowCount = store.rowCount;
    const columns = new Array(store.columnCount);
    const columnKinds = store.columnKinds.slice();
    const dictionaries = new Array(store.columnCount);
    const lowerDictionaries = new Array(store.columnCount);
    const lowerDictionaryValues = new Array(store.columnCount);

    for (let colIndex = 0; colIndex < store.columnCount; colIndex += 1) {
      const kind = store.columnKinds[colIndex];
      const sourceColumn = store.columns[colIndex];
      const dictionary = store.dictionaries[colIndex] || [];
      const lowerPostings = store.lowerDictionaries[colIndex] || Object.create(null);
      const lowerValues = store.lowerDictionaryValues[colIndex] || [];

      if (kind !== "stringId") {
        columns[colIndex] = sourceColumn;
        dictionaries[colIndex] = [];
        lowerDictionaries[colIndex] = Object.create(null);
        lowerDictionaryValues[colIndex] = [];
        continue;
      }

      const uniqueCount = dictionary.length;
      const canUseCompactDictionary =
        uniqueCount > 0 &&
        uniqueCount <= DICTIONARY_MAX_UNIQUE_KEYS &&
        uniqueCount / Math.max(1, rowCount) <= DICTIONARY_CARDINALITY_RATIO_LIMIT;

      if (canUseCompactDictionary) {
        const compactIds = new Uint16Array(rowCount);
        compactIds.set(sourceColumn);
        columns[colIndex] = compactIds;
        dictionaries[colIndex] = dictionary;
        lowerDictionaries[colIndex] = lowerPostings;
        lowerDictionaryValues[colIndex] = lowerValues;
        continue;
      }

      let allNumbers = true;
      let allIntegers = true;
      for (let i = 0; i < dictionary.length; i += 1) {
        const value = dictionary[i];
        if (typeof value !== "number" || !Number.isFinite(value)) {
          allNumbers = false;
          break;
        }
        if (!Number.isInteger(value)) {
          allIntegers = false;
        }
      }

      if (allNumbers) {
        if (allIntegers) {
          const decoded = new Int32Array(rowCount);
          for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
            const id = sourceColumn[rowIndex];
            decoded[rowIndex] =
              dictionary[id] === undefined ? 0 : dictionary[id];
          }
          columns[colIndex] = decoded;
          columnKinds[colIndex] = "int";
        } else {
          const decoded = new Float64Array(rowCount);
          for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
            const id = sourceColumn[rowIndex];
            decoded[rowIndex] =
              dictionary[id] === undefined ? 0 : dictionary[id];
          }
          columns[colIndex] = decoded;
          columnKinds[colIndex] = "float";
        }

        dictionaries[colIndex] = [];
        lowerDictionaries[colIndex] = Object.create(null);
        lowerDictionaryValues[colIndex] = [];
        continue;
      }

      columns[colIndex] = sourceColumn;
      dictionaries[colIndex] = dictionary;
      lowerDictionaries[colIndex] = lowerPostings;
      lowerDictionaryValues[colIndex] = lowerValues;
    }

    return {
      rowCount,
      columnCount: store.columnCount,
      baseColumnCount: store.baseColumnCount,
      cacheOffset: store.cacheOffset,
      hasCacheColumns: store.hasCacheColumns,
      columns,
      columnKinds,
      dictionaries,
      lowerDictionaries,
      lowerDictionaryValues,
      cacheColumns: store.cacheColumns,
    };
  }

  function addChunkMetrics(total, chunk) {
    total.rowGenerationMs += chunk.rowGenerationMs || 0;
    total.rowCacheGenerationMs += chunk.rowCacheGenerationMs || 0;
    total.numericTransformMs += chunk.numericTransformMs || 0;
    total.numericCacheGenerationMs += chunk.numericCacheGenerationMs || 0;
    total.columnarDerivationMs += chunk.columnarDerivationMs || 0;
    total.columnarCacheGenerationMs += chunk.columnarCacheGenerationMs || 0;
    total.totalMs += chunk.totalMs || 0;
  }

  function terminateWorkers(workerSlots) {
    for (let i = 0; i < workerSlots.length; i += 1) {
      const slot = workerSlots[i];
      if (slot && slot.worker) {
        slot.worker.terminate();
      }
    }
  }

  function emitProgress(onProgress, data) {
    if (typeof onProgress === "function") {
      onProgress(data);
    }
  }

  function createEmptyDerivedData() {
    return {
      objectColumnarData: {
        rowCount: 0,
        columns: {},
      },
      numericColumnarData: {
        rowCount: 0,
        columnCount: baseColumnCount,
        baseColumnCount,
        cacheOffset: numericCacheOffset,
        hasCacheColumns: false,
        columns: [],
        columnKinds: [],
        dictionaries: [],
        lowerDictionaries: [],
        lowerDictionaryValues: [],
        cacheColumns: null,
      },
    };
  }

  function generateRowsWithWorkers(options) {
    const generationOptions = options || {};
    const totalRows = toPositiveInt(generationOptions.rowCount, 0);
    const requestedWorkerCount = toPositiveInt(generationOptions.workerCount, 1);
    const selectedChunkSize = toPositiveInt(generationOptions.chunkSize, 10000);
    const onProgress = generationOptions.onProgress;

    const generationEnvironmentError = normalizeEnvironmentError(
      validateGenerationEnvironment ? validateGenerationEnvironment() : null
    );
    if (generationEnvironmentError) {
      return Promise.reject(generationEnvironmentError);
    }

    if (totalRows <= 0) {
      emitProgress(onProgress, {
        completedRows: 0,
        totalRows: 0,
        completedChunks: 0,
        totalChunks: 0,
        percent: 100,
        lastChunkMs: 0,
        avgChunkMs: 0,
      });
      return Promise.resolve({
        rows: [],
        derivedData: createEmptyDerivedData(),
        workerMetrics: createEmptyWorkerMetrics(),
        wallMs: 0,
        avgChunkMs: 0,
        perWorkerWallMs: [],
      });
    }

    const activeWorkerCount = Math.min(requestedWorkerCount, totalRows);
    const chunkQueue = buildChunkQueue(totalRows, selectedChunkSize);
    const totalChunkCount = chunkQueue.length;
    let nextChunkIndex = 0;
    const workerSlots = new Array(activeWorkerCount);
    for (let i = 0; i < activeWorkerCount; i += 1) {
      workerSlots[i] = {
        workerIndex: i,
        worker: null,
        startedAtMs: null,
        completedAtMs: null,
      };
    }

    const workerMetrics = createEmptyWorkerMetrics();
    let objectColumnarStore = null;
    let numericColumnarStore = null;
    const wallStartMs = now();
    const jobId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    let completedRows = 0;
    let completedChunks = 0;
    let chunkTimeSum = 0;
    let finishedWorkers = 0;
    let settled = false;
    const seenChunkIds = new Set();
    let expectedObjectKeys = null;
    let lastProgressAtMs = wallStartMs;
    let stallTimerId = null;

    function createProgressPayload(lastChunkMs) {
      return {
        completedRows,
        totalRows,
        completedChunks,
        totalChunks: totalChunkCount,
        percent: totalRows === 0 ? 100 : (completedRows / totalRows) * 100,
        lastChunkMs,
        avgChunkMs:
          completedChunks === 0 ? 0 : chunkTimeSum / completedChunks,
      };
    }

    return new Promise((resolve, reject) => {
      function resolveOnce() {
        if (settled) {
          return;
        }

        if (finishedWorkers < workerSlots.length || completedChunks < totalChunkCount) {
          return;
        }

        settled = true;
        if (stallTimerId !== null) {
          clearInterval(stallTimerId);
          stallTimerId = null;
        }
        const wallEndMs = now();
        terminateWorkers(workerSlots);
        resolve({
          rows: [],
          derivedData: {
            objectColumnarData: objectColumnarStore || {
              rowCount: totalRows,
              columns: {},
            },
            numericColumnarData: numericColumnarStore
              ? finalizeNumericColumnarAccumulator(numericColumnarStore)
              : {
                  rowCount: totalRows,
                  columnCount: baseColumnCount,
                  baseColumnCount,
                  cacheOffset: numericCacheOffset,
                  hasCacheColumns: false,
                  columns: [],
                  columnKinds: [],
                  dictionaries: [],
                  lowerDictionaries: [],
                  lowerDictionaryValues: [],
                  cacheColumns: null,
                },
          },
          workerMetrics,
          wallMs: wallEndMs - wallStartMs,
          avgChunkMs: completedChunks === 0 ? 0 : chunkTimeSum / completedChunks,
          totalChunks: totalChunkCount,
          completedChunks,
          perWorkerWallMs: workerSlots.map((slot) => {
            if (
              slot.startedAtMs === null ||
              slot.completedAtMs === null ||
              slot.completedAtMs < slot.startedAtMs
            ) {
              return 0;
            }

            return slot.completedAtMs - slot.startedAtMs;
          }),
        });
      }

      function rejectOnce(error) {
        if (settled) {
          return;
        }

        settled = true;
        if (stallTimerId !== null) {
          clearInterval(stallTimerId);
          stallTimerId = null;
        }
        terminateWorkers(workerSlots);
        reject(error);
      }

      function dispatchNextChunk(slot) {
        if (settled) {
          return;
        }

        if (nextChunkIndex >= chunkQueue.length) {
          if (slot.completedAtMs === null) {
            slot.completedAtMs = now();
          }
          finishedWorkers += 1;
          resolveOnce();
          return;
        }

        const nextChunk = chunkQueue[nextChunkIndex];
        nextChunkIndex += 1;
        if (slot.startedAtMs === null) {
          slot.startedAtMs = now();
        }

        slot.worker.postMessage({
          type: "generateChunk",
          jobId,
          chunkId: `${slot.workerIndex}-${nextChunk.chunkId}`,
          rowCount: nextChunk.rowCount,
          startIndex: nextChunk.startIndex,
          totalRowCount: totalRows,
        });
      }

      for (let i = 0; i < workerSlots.length; i += 1) {
        const slot = workerSlots[i];
        let worker = null;
        try {
          worker = createGenerationWorker();
        } catch (error) {
          rejectOnce(
            new Error(
              `Failed to start worker: ${String(
                error && error.message ? error.message : error
              )}`
            )
          );
          return;
        }
        slot.worker = worker;

        worker.onmessage = function (event) {
          if (settled) {
            return;
          }

          try {
            const message = event && event.data ? event.data : {};
            if (message.type === "workerInitError") {
              rejectOnce(
                new Error(
                  `Worker initialization failed: ${String(
                    message.error || "unknown worker init error"
                  )}`
                )
              );
              return;
            }
            if (message.jobId !== jobId) {
              return;
            }

            if (message.type === "chunkError") {
              rejectOnce(new Error(message.error || "Worker chunk failed."));
              return;
            }

            if (message.type !== "chunkResult") {
              return;
            }

            const chunkId = String(message.chunkId || "");
            if (seenChunkIds.has(chunkId)) {
              rejectOnce(new Error(`Duplicate chunk result received: ${chunkId}`));
              return;
            }
            seenChunkIds.add(chunkId);

            const startIndex = toPositiveInt(message.startIndex, 1);
            const chunkRowCount = toPositiveInt(message.rowCount, 0);
            const chunkStartOffset = startIndex - 1;
            const objectChunk =
              message.objectColumnar &&
              message.objectColumnar.columns &&
              typeof message.objectColumnar.columns === "object"
                ? message.objectColumnar.columns
                : null;
            const numericChunk =
              message.numericColumnar &&
              Array.isArray(message.numericColumnar.columns)
                ? message.numericColumnar
                : null;

            if (chunkRowCount <= 0) {
              rejectOnce(new Error(`Chunk row count is invalid: ${chunkId}`));
              return;
            }

            if (chunkStartOffset + chunkRowCount > totalRows) {
              rejectOnce(new Error(`Chunk range out of bounds: ${chunkId}`));
              return;
            }

            if (!objectChunk || !numericChunk) {
              rejectOnce(new Error(`Chunk derived data missing: ${chunkId}`));
              return;
            }

            if (objectColumnarStore === null) {
              objectColumnarStore = createObjectColumnStore(totalRows, objectChunk);
              expectedObjectKeys = Object.keys(objectColumnarStore.columns);
            }
            if (numericColumnarStore === null) {
              numericColumnarStore = createNumericColumnarAccumulator(
                totalRows,
                numericChunk
              );
            }

            validateObjectChunkColumns(
              objectChunk,
              chunkRowCount,
              expectedObjectKeys
            );
            validateNumericChunkColumns(
              numericChunk,
              chunkRowCount,
              numericColumnarStore.columnCount,
              numericColumnarStore.hasCacheColumns
            );

            mergeObjectColumnChunk(
              objectColumnarStore,
              objectChunk,
              chunkStartOffset,
              chunkRowCount
            );
            mergeNumericColumnarChunk(
              numericColumnarStore,
              numericChunk,
              chunkStartOffset,
              chunkRowCount
            );

            const chunkTimings = message.timings || {};
            addChunkMetrics(workerMetrics, chunkTimings);

            completedRows += chunkRowCount;
            completedChunks += 1;
            const lastChunkMs = Number(chunkTimings.totalMs) || 0;
            chunkTimeSum += lastChunkMs;
            lastProgressAtMs = now();

            emitProgress(onProgress, createProgressPayload(lastChunkMs));
            dispatchNextChunk(slot);
          } catch (error) {
            rejectOnce(
              new Error(
                `Worker merge failed: ${String(
                  error && error.message ? error.message : error
                )}`
              )
            );
          }
        };

        worker.onerror = function () {
          rejectOnce(new Error("Worker runtime error."));
        };

        worker.onmessageerror = function () {
          rejectOnce(new Error("Worker message deserialization error."));
        };
      }

      stallTimerId = setInterval(() => {
        if (settled || completedChunks >= totalChunkCount) {
          return;
        }

        const stalledForMs = now() - lastProgressAtMs;
        if (stalledForMs >= WORKER_STALL_TIMEOUT_MS) {
          rejectOnce(
            new Error(
              `Worker generation stalled for ${Math.round(
                stalledForMs
              )} ms without progress.`
            )
          );
        }
      }, 1000);

      emitProgress(onProgress, createProgressPayload(0));
      for (let i = 0; i < workerSlots.length; i += 1) {
        dispatchNextChunk(workerSlots[i]);
      }
    });
  }

  function buildSortedIndexByKey(sortedIndexColumns) {
    const byKey = Object.create(null);
    const totalColumns = Math.min(columnKeys.length, sortedIndexColumns.length);
    for (let i = 0; i < totalColumns; i += 1) {
      const sortedIndices = sortedIndexColumns[i];
      if (sortedIndices instanceof Uint32Array) {
        byKey[columnKeys[i]] = sortedIndices;
      }
    }
    return byKey;
  }

  function buildSortedIndicesWithWorkers(options) {
    const runOptions = options || {};
    const numericColumnarData = runOptions.numericColumnarData;
    const onProgress = runOptions.onProgress;

    if (
      !numericColumnarData ||
      !Array.isArray(numericColumnarData.columns) ||
      !Array.isArray(numericColumnarData.columnKinds)
    ) {
      return Promise.reject(new Error("Invalid numeric columnar data."));
    }

    const sortingEnvironmentError = normalizeEnvironmentError(
      validateSortingEnvironment ? validateSortingEnvironment() : null
    );
    if (sortingEnvironmentError) {
      return Promise.reject(sortingEnvironmentError);
    }

    const rowCount = Math.max(0, Number(numericColumnarData.rowCount) || 0);
    const columnCount = Math.min(baseColumnCount, numericColumnarData.columns.length);
    const dictionaries = Array.isArray(numericColumnarData.dictionaries)
      ? numericColumnarData.dictionaries
      : [];
    const existingSortedColumns = Array.isArray(numericColumnarData.sortedIndexColumns)
      ? numericColumnarData.sortedIndexColumns
      : [];
    const sortedIndexColumns = new Array(columnCount);
    const tasks = [];

    for (let colIndex = 0; colIndex < columnCount; colIndex += 1) {
      const dictionary = dictionaries[colIndex];
      const hasDictionary = Array.isArray(dictionary) && dictionary.length > 0;
      if (hasDictionary) {
        sortedIndexColumns[colIndex] = null;
        continue;
      }

      const existing = existingSortedColumns[colIndex];
      if (existing instanceof Uint32Array && existing.length === rowCount) {
        sortedIndexColumns[colIndex] = existing;
        continue;
      }

      const values = numericColumnarData.columns[colIndex];
      if (
        !values ||
        (!ArrayBuffer.isView(values) && !Array.isArray(values)) ||
        values.length !== rowCount
      ) {
        throw new Error(`Invalid sortable column data at index ${colIndex}.`);
      }

      tasks.push({
        columnIndex: colIndex,
        columnKind: numericColumnarData.columnKinds[colIndex] || "int",
        values,
      });
      sortedIndexColumns[colIndex] = null;
    }

    const totalColumns = tasks.length;
    const startedAtMs = now();

    if (totalColumns === 0) {
      const sortedIndexByKey = buildSortedIndexByKey(sortedIndexColumns);
      return Promise.resolve({
        sortedIndexColumns,
        sortedIndexByKey,
        totalColumns: 0,
        completedColumns: 0,
        durationMs: 0,
      });
    }

    const requestedWorkerCount = toPositiveInt(runOptions.workerCount, 1);
    const activeWorkerCount = Math.min(requestedWorkerCount, totalColumns);
    const workerSlots = new Array(activeWorkerCount);
    for (let i = 0; i < activeWorkerCount; i += 1) {
      workerSlots[i] = {
        workerIndex: i,
        worker: null,
      };
    }

    const seenTasks = new Set();
    let nextTaskIndex = 0;
    let completedColumns = 0;
    let finishedWorkers = 0;
    let settled = false;
    let totalWorkerSortMs = 0;
    const jobId = `sort-${Date.now()}-${Math.random().toString(16).slice(2)}`;

    function emitSortProgress(lastColumnIndex, lastDurationMs) {
      if (typeof onProgress !== "function") {
        return;
      }

      onProgress({
        completedColumns,
        totalColumns,
        lastColumnIndex,
        lastDurationMs,
      });
    }

    return new Promise((resolve, reject) => {
      function resolveOnce() {
        if (settled) {
          return;
        }

        if (finishedWorkers < workerSlots.length || completedColumns < totalColumns) {
          return;
        }

        settled = true;
        terminateWorkers(workerSlots);
        const sortedIndexByKey = buildSortedIndexByKey(sortedIndexColumns);
        resolve({
          sortedIndexColumns,
          sortedIndexByKey,
          totalColumns,
          completedColumns,
          durationMs: now() - startedAtMs,
          workerSortMs: totalWorkerSortMs,
        });
      }

      function rejectOnce(error) {
        if (settled) {
          return;
        }

        settled = true;
        terminateWorkers(workerSlots);
        reject(error);
      }

      function dispatchNextTask(slot) {
        if (settled) {
          return;
        }

        if (nextTaskIndex >= tasks.length) {
          finishedWorkers += 1;
          resolveOnce();
          return;
        }

        const task = tasks[nextTaskIndex];
        nextTaskIndex += 1;
        const taskId = `${slot.workerIndex}-${task.columnIndex}-${nextTaskIndex}`;

        slot.worker.postMessage({
          type: "sortColumnIndices",
          jobId,
          taskId,
          columnIndex: task.columnIndex,
          columnKind: task.columnKind,
          values: task.values,
        });
      }

      for (let i = 0; i < workerSlots.length; i += 1) {
        const slot = workerSlots[i];
        let worker = null;
        try {
          worker = createGenerationWorker();
        } catch (error) {
          rejectOnce(
            new Error(
              `Failed to start sort worker: ${String(
                error && error.message ? error.message : error
              )}`
            )
          );
          return;
        }
        slot.worker = worker;

        worker.onmessage = function (event) {
          if (settled) {
            return;
          }

          try {
            const message = event && event.data ? event.data : {};
            if (message.type === "workerInitError") {
              rejectOnce(
                new Error(
                  `Sort worker initialization failed: ${String(
                    message.error || "unknown worker init error"
                  )}`
                )
              );
              return;
            }
            if (message.jobId !== jobId) {
              return;
            }

            if (message.type === "sortColumnError") {
              rejectOnce(new Error(message.error || "Worker sort task failed."));
              return;
            }

            if (message.type !== "sortColumnResult") {
              return;
            }

            const taskId = String(message.taskId || "");
            if (seenTasks.has(taskId)) {
              rejectOnce(new Error(`Duplicate sort task result received: ${taskId}`));
              return;
            }
            seenTasks.add(taskId);

            const colIndex = Number(message.columnIndex);
            if (!Number.isInteger(colIndex) || colIndex < 0 || colIndex >= columnCount) {
              rejectOnce(new Error("Worker sort returned invalid column index."));
              return;
            }

            const sortedIndices = message.sortedIndices;
            if (!(sortedIndices instanceof Uint32Array) || sortedIndices.length !== rowCount) {
              rejectOnce(new Error("Worker sort returned invalid sorted index payload."));
              return;
            }

            sortedIndexColumns[colIndex] = sortedIndices;
            completedColumns += 1;
            totalWorkerSortMs += Number(message.durationMs) || 0;
            emitSortProgress(colIndex, Number(message.durationMs) || 0);
            dispatchNextTask(slot);
          } catch (error) {
            rejectOnce(
              new Error(
                `Worker sort merge failed: ${String(
                  error && error.message ? error.message : error
                )}`
              )
            );
          }
        };

        worker.onerror = function () {
          rejectOnce(new Error("Sort worker runtime error."));
        };

        worker.onmessageerror = function () {
          rejectOnce(new Error("Sort worker message deserialization error."));
        };
      }

      emitSortProgress(-1, 0);
      for (let i = 0; i < workerSlots.length; i += 1) {
        dispatchNextTask(workerSlots[i]);
      }
    });
  }
  return {
    generateRowsWithWorkers,
    buildSortedIndicesWithWorkers,
  };
}
export { createGenerationWorkersApi };
