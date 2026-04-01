function defaultNow() {
  if (typeof performance !== "undefined" && typeof performance.now === "function") {
    return performance.now();
  }

  return Date.now();
}

function compareSortValuesAsc(aValue, bValue, columnKind) {
  if (columnKind === "int" || columnKind === "float") {
    const aNumber = Number(aValue);
    const bNumber = Number(bValue);
    const aFinite = Number.isFinite(aNumber);
    const bFinite = Number.isFinite(bNumber);
    if (!aFinite && !bFinite) {
      return 0;
    }
    if (!aFinite) {
      return 1;
    }
    if (!bFinite) {
      return -1;
    }
    if (aNumber < bNumber) {
      return -1;
    }
    if (aNumber > bNumber) {
      return 1;
    }
    return 0;
  }

  const aText =
    aValue === undefined || aValue === null ? "" : String(aValue);
  const bText =
    bValue === undefined || bValue === null ? "" : String(bValue);
  if (aText < bText) {
    return -1;
  }
  if (aText > bText) {
    return 1;
  }
  return 0;
}

function buildSortedIndexColumn(values, columnKind) {
  const sourceValues =
    values && typeof values.length === "number" ? values : [];
  const rowCount = sourceValues.length | 0;
  const indices = new Uint32Array(rowCount);
  for (let i = 0; i < rowCount; i += 1) {
    indices[i] = i;
  }

  indices.sort((a, b) => {
    const compared = compareSortValuesAsc(
      sourceValues[a],
      sourceValues[b],
      columnKind
    );
    if (compared !== 0) {
      return compared;
    }
    return a - b;
  });

  return indices;
}

function appendTypedArrayBuffer(transferables, seenBuffers, value) {
  if (!ArrayBuffer.isView(value)) {
    return;
  }

  const buffer = value.buffer;
  if (!(buffer instanceof ArrayBuffer)) {
    return;
  }

  if (seenBuffers.has(buffer)) {
    return;
  }

  seenBuffers.add(buffer);
  transferables.push(buffer);
}

function collectTransferablesForChunk(objectColumnarData, numericColumnarData) {
  const transferables = [];
  const seenBuffers = new Set();
  const objectColumns = objectColumnarData && objectColumnarData.columns;
  const numericColumns = numericColumnarData && numericColumnarData.columns;

  if (objectColumns && typeof objectColumns === "object") {
    const objectKeys = Object.keys(objectColumns);
    for (let i = 0; i < objectKeys.length; i += 1) {
      appendTypedArrayBuffer(
        transferables,
        seenBuffers,
        objectColumns[objectKeys[i]]
      );
    }
  }

  if (Array.isArray(numericColumns)) {
    for (let i = 0; i < numericColumns.length; i += 1) {
      appendTypedArrayBuffer(transferables, seenBuffers, numericColumns[i]);
    }
  }

  return transferables;
}

function createGenerationWorkerMessageHandler(options) {
  const input = options || {};
  const generationApi = input.generationApi;
  const postMessage = input.postMessage;
  const now = typeof input.now === "function" ? input.now : defaultNow;

  if (!generationApi) {
    throw new Error("generationApi is required.");
  }

  if (typeof postMessage !== "function") {
    throw new Error("postMessage callback is required.");
  }

  const generateRowsWithoutCache = generationApi.generateRowsWithoutCache;
  const deriveObjectAndNumericColumnarFromRows =
    generationApi.deriveObjectAndNumericColumnarFromRows;

  if (
    typeof generateRowsWithoutCache !== "function" ||
    typeof deriveObjectAndNumericColumnarFromRows !== "function"
  ) {
    throw new Error("generationApi is missing required worker methods.");
  }

  return function handleWorkerMessage(event) {
    const message = event && event.data ? event.data : {};
    if (message.type === "sortColumnIndices") {
      try {
        const startMs = now();
        const columnIndex = Number(message.columnIndex) | 0;
        const columnKind = String(message.columnKind || "int");
        const sortedIndices = buildSortedIndexColumn(
          message.values,
          columnKind
        );
        const endMs = now();
        postMessage(
          {
            type: "sortColumnResult",
            jobId: message.jobId,
            taskId: message.taskId,
            columnIndex,
            sortedIndices,
            durationMs: endMs - startMs,
          },
          [sortedIndices.buffer]
        );
      } catch (error) {
        postMessage({
          type: "sortColumnError",
          jobId: message.jobId,
          taskId: message.taskId,
          columnIndex: Number(message.columnIndex) | 0,
          error: String(error && error.message ? error.message : error),
        });
      }
      return;
    }

    if (message.type !== "generateChunk") {
      return;
    }

    try {
      const startMs = now();
      const rowCount = Number(message.rowCount) | 0;
      const startIndex = Number(message.startIndex) | 0;
      const totalRowCount = Number(message.totalRowCount) | 0;

      const rowGenerationStartMs = now();
      const rows = generateRowsWithoutCache(rowCount, {
        startIndex,
        totalRowCount,
      });
      const rowGenerationEndMs = now();

      const columnarMetrics = { cacheGenerationMs: 0 };
      const columnarStartMs = now();
      const fusedDerived = deriveObjectAndNumericColumnarFromRows(rows, {
        metrics: columnarMetrics,
      });
      const columnarEndMs = now();
      const objectColumnarData = fusedDerived.objectColumnarData;
      const numericColumnarData = fusedDerived.numericColumnarData;

      const totalEndMs = now();
      const transferables = collectTransferablesForChunk(
        objectColumnarData,
        numericColumnarData
      );

      postMessage(
        {
          type: "chunkResult",
          jobId: message.jobId,
          chunkId: message.chunkId,
          rowCount,
          startIndex,
          objectColumnar: objectColumnarData,
          numericColumnar: numericColumnarData,
          timings: {
            rowGenerationMs: rowGenerationEndMs - rowGenerationStartMs,
            rowCacheGenerationMs: 0,
            numericTransformMs: 0,
            numericCacheGenerationMs: 0,
            columnarDerivationMs: columnarEndMs - columnarStartMs,
            columnarCacheGenerationMs: columnarMetrics.cacheGenerationMs,
            totalMs: totalEndMs - startMs,
          },
        },
        transferables
      );
    } catch (error) {
      postMessage({
        type: "chunkError",
        jobId: message.jobId,
        chunkId: message.chunkId,
        error: String(error && error.message ? error.message : error),
      });
    }
  };
}

function attachGenerationWorkerProtocol(workerScope, options) {
  if (!workerScope || typeof workerScope.postMessage !== "function") {
    throw new Error("workerScope with postMessage is required.");
  }

  const handler = createGenerationWorkerMessageHandler({
    generationApi: options && options.generationApi,
    postMessage(payload, transferables) {
      if (Array.isArray(transferables) && transferables.length > 0) {
        workerScope.postMessage(payload, transferables);
        return;
      }

      workerScope.postMessage(payload);
    },
    now: options && options.now,
  });

  workerScope.onmessage = function onWorkerScopeMessage(event) {
    handler(event);
  };

  return handler;
}

export {
  buildSortedIndexColumn,
  collectTransferablesForChunk,
  createGenerationWorkerMessageHandler,
  attachGenerationWorkerProtocol,
};
