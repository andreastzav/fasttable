(function () {
  const saveObjectBtnEl = document.getElementById("saveObjectBtn");
  const loadPregeneratedBtnEl = document.getElementById("loadPregeneratedBtn");
  const loadPregeneratedInputEl = document.getElementById("loadPregeneratedInput");
  const loadPregeneratedPresetEl = document.getElementById("loadPregeneratedPreset");
  const COLUMNAR_BINARY_FORMAT = "fasttable-columnar-binary-v2";
  const TABLE_PRESETS_FOLDER = "tables_presets";

  if (!saveObjectBtnEl || !loadPregeneratedBtnEl || !loadPregeneratedInputEl) {
    return;
  }

  function getBridge() {
    return window.fastTableIOBridge;
  }

  function setAllActionButtonsDisabled(disabled) {
    if (typeof window.fastTableSetActionButtonsDisabled === "function") {
      window.fastTableSetActionButtonsDisabled(disabled);
      return;
    }

    saveObjectBtnEl.disabled = disabled;
    loadPregeneratedBtnEl.disabled = disabled;
    if (loadPregeneratedPresetEl) {
      loadPregeneratedPresetEl.disabled = disabled;
    }
  }

  function alignOffset(offset, alignment) {
    const remainder = offset % alignment;
    if (remainder === 0) {
      return offset;
    }

    return offset + (alignment - remainder);
  }

  function createEmptyLowerDictionaryPostings() {
    return Object.create(null);
  }

  function normalizeDictionaryValue(value) {
    return String(value).toLowerCase();
  }

  function formatByteSize(bytes) {
    const numeric = Number(bytes);
    if (!Number.isFinite(numeric) || numeric <= 0) {
      return "0 B";
    }

    if (numeric >= 1024 * 1024) {
      return `${(numeric / (1024 * 1024)).toFixed(1)} MB`;
    }

    if (numeric >= 1024) {
      return `${(numeric / 1024).toFixed(1)} KB`;
    }

    return `${Math.round(numeric)} B`;
  }

  function buildLowerDictionaryValues(dictionary, lowerDictionary) {
    const dict = Array.isArray(dictionary) ? dictionary : [];
    const lowerValues = new Array(dict.length);

    if (Array.isArray(lowerDictionary)) {
      for (let i = 0; i < dict.length; i += 1) {
        const lowerValue = lowerDictionary[i];
        lowerValues[i] =
          lowerValue !== undefined
            ? String(lowerValue)
            : normalizeDictionaryValue(dict[i]);
      }
      return lowerValues;
    }

    for (let i = 0; i < dict.length; i += 1) {
      lowerValues[i] = normalizeDictionaryValue(dict[i]);
    }
    return lowerValues;
  }

  function buildLowerDictionaryPostingsFromIds(ids, lowerValues) {
    const postings = createEmptyLowerDictionaryPostings();
    const rowCount = ids.length;

    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      const id = ids[rowIndex];
      const lowerValue =
        lowerValues[id] !== undefined ? lowerValues[id] : "";
      let bucket = postings[lowerValue];
      if (bucket === undefined) {
        bucket = [];
        postings[lowerValue] = bucket;
      }
      bucket.push(rowIndex);
    }

    return postings;
  }

  function resolveLowerDictionaryPostings(
    dictionary,
    lowerDictionary,
    ids
  ) {
    if (
      lowerDictionary &&
      typeof lowerDictionary === "object" &&
      !Array.isArray(lowerDictionary)
    ) {
      return lowerDictionary;
    }

    const lowerValues = buildLowerDictionaryValues(dictionary, lowerDictionary);
    return buildLowerDictionaryPostingsFromIds(ids, lowerValues);
  }

  function getPostingListLength(postings) {
    if (Array.isArray(postings) || ArrayBuffer.isView(postings)) {
      return postings.length | 0;
    }

    return 0;
  }

  function createLowerDictionaryPostingBinary(lowerDictionary) {
    const postingsByKey =
      lowerDictionary &&
      typeof lowerDictionary === "object" &&
      !Array.isArray(lowerDictionary)
        ? lowerDictionary
        : createEmptyLowerDictionaryPostings();
    const keys = Object.keys(postingsByKey);
    const indexByKey = createEmptyLowerDictionaryPostings();
    const entries = new Array(keys.length);
    let totalCount = 0;

    for (let i = 0; i < keys.length; i += 1) {
      const key = keys[i];
      const postingList = postingsByKey[key];
      const length = Math.max(0, getPostingListLength(postingList));
      entries[i] = {
        key,
        postingList,
        start: totalCount,
        length,
      };
      indexByKey[key] = {
        start: totalCount,
        length,
      };
      totalCount += length;
    }

    const buffer = new Uint32Array(totalCount);
    for (let i = 0; i < entries.length; i += 1) {
      const entry = entries[i];
      const postingList = entry.postingList;
      if (
        !postingList ||
        (!Array.isArray(postingList) && !ArrayBuffer.isView(postingList)) ||
        entry.length <= 0
      ) {
        continue;
      }

      for (let j = 0; j < entry.length; j += 1) {
        const value = Number(postingList[j]);
        buffer[entry.start + j] =
          Number.isFinite(value) && value >= 0 ? value >>> 0 : 0;
      }
    }

    return {
      buffer,
      indexByKey,
    };
  }

  function decodeLowerDictionaryPostingBinary(lowerDictionary, binaryBuffer) {
    if (
      !lowerDictionary ||
      typeof lowerDictionary !== "object" ||
      Array.isArray(lowerDictionary)
    ) {
      throw new Error("Missing lowerDictionary metadata for stringId column.");
    }

    const postingsMeta = lowerDictionary.postings;
    if (!postingsMeta || typeof postingsMeta !== "object") {
      throw new Error("Missing lowerDictionary postings block metadata.");
    }

    if (postingsMeta.storageKind !== "uint32") {
      throw new Error("Unsupported lowerDictionary postings storage kind.");
    }

    const byteOffset = Number(postingsMeta.byteOffset);
    const byteLength = Number(postingsMeta.byteLength);
    if (
      !Number.isInteger(byteOffset) ||
      byteOffset < 0 ||
      !Number.isInteger(byteLength) ||
      byteLength < 0 ||
      byteLength % 4 !== 0
    ) {
      throw new Error("Invalid lowerDictionary postings byte range metadata.");
    }

    if (byteOffset + byteLength > binaryBuffer.byteLength) {
      throw new Error("lowerDictionary postings range is out of binary bounds.");
    }

    const postingsByKey = createEmptyLowerDictionaryPostings();
    const indexByKey = lowerDictionary.indexByKey;
    if (
      !indexByKey ||
      typeof indexByKey !== "object" ||
      Array.isArray(indexByKey)
    ) {
      return postingsByKey;
    }

    const postingsBuffer = new Uint32Array(binaryBuffer, byteOffset, byteLength / 4);
    const keys = Object.keys(indexByKey);
    for (let i = 0; i < keys.length; i += 1) {
      const key = keys[i];
      const range = indexByKey[key];
      const start = Number(range && range.start);
      const length = Number(range && range.length);
      if (
        !Number.isInteger(start) ||
        start < 0 ||
        !Number.isInteger(length) ||
        length < 0 ||
        start + length > postingsBuffer.length
      ) {
        throw new Error("Invalid lowerDictionary posting index range.");
      }

      postingsByKey[key] = postingsBuffer.subarray(start, start + length);
    }

    return postingsByKey;
  }

  function getStorageKindFromNumericColumn(numericColumnarData, columnIndex) {
    const kind = numericColumnarData.columnKinds[columnIndex];
    if (kind === "stringId") {
      const values = numericColumnarData.columns[columnIndex];
      if (values instanceof Uint16Array) {
        return "uint16";
      }
      return "uint32";
    }

    if (kind === "float") {
      return "float64";
    }

    if (kind === "string") {
      throw new Error("Cannot export non-dictionary string columns to binary format.");
    }

    return "int32";
  }

  function getBytesPerElementForStorageKind(storageKind) {
    if (storageKind === "float64") {
      return 8;
    }

    if (storageKind === "uint16") {
      return 2;
    }

    return 4;
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

  function buildSortedIndexColumn(values, columnKind, rowCount) {
    const count = Math.max(0, rowCount | 0);
    const indices = new Uint32Array(count);
    const sourceValues =
      values && typeof values.length === "number" ? values : new Array(count);
    for (let i = 0; i < count; i += 1) {
      indices[i] = i;
    }

    indices.sort((a, b) => {
      const aValue = sourceValues[a];
      const bValue = sourceValues[b];
      const compared = compareSortValuesAsc(aValue, bValue, columnKind);
      if (compared !== 0) {
        return compared;
      }
      return a - b;
    });

    return indices;
  }

  function getOrBuildSortedIndexColumnForExport(
    numericColumnarData,
    columnIndex,
    rowCount,
    columnKey
  ) {
    const existingColumns = Array.isArray(numericColumnarData.sortedIndexColumns)
      ? numericColumnarData.sortedIndexColumns
      : null;
    const existing = existingColumns ? existingColumns[columnIndex] : null;
    if (existing instanceof Uint32Array && existing.length === rowCount) {
      return existing;
    }

    const columnKind =
      Array.isArray(numericColumnarData.columnKinds) &&
      numericColumnarData.columnKinds[columnIndex]
        ? numericColumnarData.columnKinds[columnIndex]
        : "int";
    const columns = Array.isArray(numericColumnarData.columns)
      ? numericColumnarData.columns
      : [];
    const values = columns[columnIndex];
    const sortedIndices = buildSortedIndexColumn(values, columnKind, rowCount);

    if (existingColumns) {
      existingColumns[columnIndex] = sortedIndices;
    } else {
      const nextColumns = new Array(columns.length);
      nextColumns[columnIndex] = sortedIndices;
      numericColumnarData.sortedIndexColumns = nextColumns;
    }

    if (
      numericColumnarData.sortedIndexByKey &&
      typeof numericColumnarData.sortedIndexByKey === "object"
    ) {
      if (columnKey) {
        numericColumnarData.sortedIndexByKey[columnKey] = sortedIndices;
      }
    }

    return sortedIndices;
  }

  function createColumnarBinaryExport(numericColumnarData, schema) {
    const rowCount = numericColumnarData.rowCount;
    const columns = numericColumnarData.columns;
    const dictionaries = numericColumnarData.dictionaries || [];
    const lowerDictionaries = numericColumnarData.lowerDictionaries || [];
    const columnMetadata = new Array(schema.baseColumnCount);
    const parts = [];
    let currentOffset = 0;

    for (let colIndex = 0; colIndex < schema.baseColumnCount; colIndex += 1) {
      const typedColumn = columns[colIndex];
      if (!ArrayBuffer.isView(typedColumn)) {
        throw new Error("Cannot export non-binary column in numeric dataset.");
      }
      const storageKind = getStorageKindFromNumericColumn(
        numericColumnarData,
        colIndex
      );
      const alignment =
        storageKind === "float64" ? 8 : storageKind === "uint16" ? 2 : 4;
      const alignedOffset = alignOffset(currentOffset, alignment);

      if (alignedOffset > currentOffset) {
        parts.push(new Uint8Array(alignedOffset - currentOffset));
        currentOffset = alignedOffset;
      }

      const byteView = new Uint8Array(
        typedColumn.buffer,
        typedColumn.byteOffset,
        typedColumn.byteLength
      );
      parts.push(byteView);

      const metadata = {
        index: colIndex,
        key: schema.columnKeys[colIndex],
        storageKind,
        byteOffset: currentOffset,
        byteLength: typedColumn.byteLength,
      };

      const dictionary = Array.isArray(dictionaries[colIndex])
        ? dictionaries[colIndex]
        : [];
      metadata.dictionary = dictionary;
      const hasDictionary = dictionary.length > 0;

      currentOffset += typedColumn.byteLength;

      if (hasDictionary) {
        metadata.sortedIndices = {
          storageKind: "uint32",
          byteOffset: 0,
          byteLength: 0,
        };
        const lowerDictionaryPostings = resolveLowerDictionaryPostings(
          dictionary,
          lowerDictionaries[colIndex],
          typedColumn
        );
        const lowerDictionaryBinary = createLowerDictionaryPostingBinary(
          lowerDictionaryPostings
        );
        const postingsAlignedOffset = alignOffset(currentOffset, 4);

        if (postingsAlignedOffset > currentOffset) {
          parts.push(new Uint8Array(postingsAlignedOffset - currentOffset));
          currentOffset = postingsAlignedOffset;
        }

        const postingsByteView = new Uint8Array(
          lowerDictionaryBinary.buffer.buffer,
          lowerDictionaryBinary.buffer.byteOffset,
          lowerDictionaryBinary.buffer.byteLength
        );
        parts.push(postingsByteView);
        metadata.lowerDictionary = {
          postings: {
            storageKind: "uint32",
            byteOffset: currentOffset,
            byteLength: lowerDictionaryBinary.buffer.byteLength,
          },
          indexByKey: lowerDictionaryBinary.indexByKey,
        };
        currentOffset += lowerDictionaryBinary.buffer.byteLength;
      } else {
        metadata.lowerDictionary = {
          postings: {
            storageKind: "uint32",
            byteOffset: 0,
            byteLength: 0,
          },
          indexByKey: createEmptyLowerDictionaryPostings(),
        };
        const sortedIndices = getOrBuildSortedIndexColumnForExport(
          numericColumnarData,
          colIndex,
          rowCount,
          schema.columnKeys[colIndex]
        );
        const sortedAlignedOffset = alignOffset(currentOffset, 4);
        if (sortedAlignedOffset > currentOffset) {
          parts.push(new Uint8Array(sortedAlignedOffset - currentOffset));
          currentOffset = sortedAlignedOffset;
        }

        const sortedIndicesByteView = new Uint8Array(
          sortedIndices.buffer,
          sortedIndices.byteOffset,
          sortedIndices.byteLength
        );
        parts.push(sortedIndicesByteView);
        metadata.sortedIndices = {
          storageKind: "uint32",
          byteOffset: currentOffset,
          byteLength: sortedIndices.byteLength,
        };
        currentOffset += sortedIndices.byteLength;
      }

      columnMetadata[colIndex] = metadata;
    }

    const metadata = {
      format: COLUMNAR_BINARY_FORMAT,
      rowCount,
      baseColumnCount: schema.baseColumnCount,
      columnKeys: schema.columnKeys,
      columnNames: schema.columnNames,
      columns: columnMetadata,
    };

    const binaryBlob = new Blob(parts, { type: "application/octet-stream" });
    return { metadata, binaryBlob };
  }

  function stringifyMetadataWithCompactLowerDictionaryPostings(metadata) {
    const rawArrays = [];

    function markPostings(value, inLowerDictionary) {
      if (Array.isArray(value)) {
        if (inLowerDictionary) {
          const marker = `__FT_RAW_ARRAY_${rawArrays.length}__`;
          rawArrays.push(`[${value.join(",")}]`);
          return marker;
        }

        const outArray = new Array(value.length);
        for (let i = 0; i < value.length; i += 1) {
          outArray[i] = markPostings(value[i], false);
        }
        return outArray;
      }

      if (!value || typeof value !== "object") {
        return value;
      }

      const out = {};
      const keys = Object.keys(value);
      for (let i = 0; i < keys.length; i += 1) {
        const key = keys[i];
        out[key] = markPostings(value[key], inLowerDictionary || key === "lowerDictionary");
      }
      return out;
    }

    const markedMetadata = markPostings(metadata, false);
    return JSON.stringify(markedMetadata, null, "\t").replace(
      /"__FT_RAW_ARRAY_(\d+)__"/g,
      function (_, indexText) {
        const index = Number(indexText);
        return rawArrays[index] || "[]";
      }
    );
  }

  function triggerBlobDownload(blob, fileName) {
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = fileName;
    link.click();
    URL.revokeObjectURL(url);
  }

  function saveCurrentRowsToBinaryFiles() {
    const bridge = getBridge();
    if (!bridge) {
      return;
    }

    if (!bridge.hasRows()) {
      bridge.setGenerationError("No generated rows to save.");
      return;
    }

    const schema = bridge.getSchema();
    const rowCount = bridge.getRowCount();
    const numericColumnarData = bridge.getNumericColumnarForSave();
    if (!numericColumnarData) {
      bridge.setGenerationError("No generated rows to save.");
      return;
    }

    const payload = createColumnarBinaryExport(numericColumnarData, schema);
    const metadataText = stringifyMetadataWithCompactLowerDictionaryPostings(
      payload.metadata
    );
    const metadataBlob = new Blob([metadataText], {
      type: "application/json",
    });

    triggerBlobDownload(metadataBlob, `fasttable-columnar-${rowCount}.json`);
    triggerBlobDownload(payload.binaryBlob, `fasttable-columnar-${rowCount}.bin`);
  }

  function readFileAsText(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result || ""));
      reader.onerror = () => reject(new Error("Failed to read text file."));
      reader.readAsText(file);
    });
  }

  function readFileAsArrayBuffer(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result);
      reader.onerror = () => reject(new Error("Failed to read binary file."));
      reader.readAsArrayBuffer(file);
    });
  }

  async function readUrlAsText(url) {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`Failed to fetch metadata file (${response.status}).`);
    }

    return response.text();
  }

  async function readUrlAsArrayBuffer(url, onProgress) {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`Failed to fetch binary file (${response.status}).`);
    }

    const progressCallback =
      typeof onProgress === "function" ? onProgress : null;
    const headerLength = Number.parseInt(
      response.headers.get("content-length") || "",
      10
    );
    const totalBytes =
      Number.isFinite(headerLength) && headerLength > 0 ? headerLength : 0;

    if (!response.body || typeof response.body.getReader !== "function") {
      const fallbackBuffer = await response.arrayBuffer();
      if (progressCallback) {
        const fallbackTotal =
          totalBytes > 0 ? totalBytes : fallbackBuffer.byteLength;
        progressCallback(fallbackBuffer.byteLength, fallbackTotal);
      }
      return fallbackBuffer;
    }

    const reader = response.body.getReader();
    const chunks = [];
    let receivedBytes = 0;

    while (true) {
      const next = await reader.read();
      if (!next || next.done) {
        break;
      }

      const chunk = next.value;
      if (chunk && chunk.byteLength > 0) {
        chunks.push(chunk);
        receivedBytes += chunk.byteLength;
        if (progressCallback) {
          progressCallback(receivedBytes, totalBytes);
        }
      }
    }

    const output = new Uint8Array(receivedBytes);
    let writeOffset = 0;
    for (let i = 0; i < chunks.length; i += 1) {
      const chunk = chunks[i];
      output.set(chunk, writeOffset);
      writeOffset += chunk.byteLength;
    }

    if (progressCallback) {
      progressCallback(receivedBytes, totalBytes > 0 ? totalBytes : receivedBytes);
    }

    return output.buffer;
  }

  function parseColumnarBinaryMetadataText(metadataText, schema) {
    const metadata = JSON.parse(metadataText);

    if (!metadata || metadata.format !== COLUMNAR_BINARY_FORMAT) {
      throw new Error("Invalid columnar binary metadata format.");
    }

    if (
      !Array.isArray(metadata.columns) ||
      metadata.columns.length !== schema.baseColumnCount
    ) {
      throw new Error("Invalid columnar metadata columns.");
    }

    if (
      !Array.isArray(metadata.columnKeys) ||
      metadata.columnKeys.length !== schema.baseColumnCount
    ) {
      throw new Error("Invalid columnar metadata column keys.");
    }

    for (let colIndex = 0; colIndex < schema.baseColumnCount; colIndex += 1) {
      if (metadata.columnKeys[colIndex] !== schema.columnKeys[colIndex]) {
        throw new Error("Columnar metadata schema mismatch.");
      }
    }

    if (!Number.isInteger(metadata.rowCount) || metadata.rowCount < 0) {
      throw new Error("Invalid columnar metadata row count.");
    }

    return metadata;
  }

  function applyPregeneratedPayload(
    bridge,
    schema,
    metadataText,
    binaryBuffer,
    loadStart,
    loadTimings
  ) {
    const jsonParseStartMs = performance.now();
    const metadata = parseColumnarBinaryMetadataText(metadataText, schema);
    loadTimings.jsonParseMs = performance.now() - jsonParseStartMs;

    const decodeStartMs = performance.now();
    const numericColumnarData = decodeColumnarBinaryData(
      metadata,
      binaryBuffer,
      schema
    );
    loadTimings.decodeMs = performance.now() - decodeStartMs;
    loadTimings.totalMs = performance.now() - loadStart;
    const loadDurationMs = loadTimings.totalMs;

    if (typeof bridge.applyLoadedNumericColumnarDataset === "function") {
      bridge.applyLoadedNumericColumnarDataset(
        numericColumnarData,
        loadDurationMs,
        loadTimings
      );
      return;
    }

    const loadedRows = convertNumericColumnarDataToObjectRows(
      numericColumnarData,
      schema,
      true
    );
    bridge.applyLoadedColumnarBinaryDataset(
      loadedRows,
      loadDurationMs,
      loadTimings
    );
  }

  function decodeColumnarBinaryData(metadata, binaryBuffer, schema) {
    const rowCount = metadata.rowCount;
    const columns = new Array(schema.baseColumnCount);
    const columnKinds = new Array(schema.baseColumnCount);
    const dictionaries = new Array(schema.baseColumnCount).fill(null);
    const lowerDictionaries = new Array(schema.baseColumnCount).fill(null);
    const lowerDictionaryValues = new Array(schema.baseColumnCount).fill(null);
    const cacheColumns = new Array(schema.baseColumnCount);
    const sortedIndexColumns = new Array(schema.baseColumnCount).fill(null);

    for (let colIndex = 0; colIndex < schema.baseColumnCount; colIndex += 1) {
      const colMeta = metadata.columns[colIndex];
      const storageKind = colMeta.storageKind;
      const byteOffset = colMeta.byteOffset;
      const byteLength = colMeta.byteLength;
      const bytesPerElement = getBytesPerElementForStorageKind(storageKind);
      const expectedByteLength = rowCount * bytesPerElement;

      if (byteLength !== expectedByteLength) {
        throw new Error("Invalid column byte length in metadata.");
      }

      if (byteOffset < 0 || byteOffset + byteLength > binaryBuffer.byteLength) {
        throw new Error("Column byte range is out of binary file bounds.");
      }

      if (storageKind === "float64") {
        columns[colIndex] = new Float64Array(binaryBuffer, byteOffset, rowCount);
        columnKinds[colIndex] = "float";
      } else if (storageKind === "uint16") {
        columns[colIndex] = new Uint16Array(binaryBuffer, byteOffset, rowCount);
        columnKinds[colIndex] = "stringId";
        dictionaries[colIndex] = Array.isArray(colMeta.dictionary)
          ? colMeta.dictionary
          : [];
        lowerDictionaryValues[colIndex] = buildLowerDictionaryValues(
          dictionaries[colIndex],
          colMeta.lowerDictionary
        );
      } else if (storageKind === "uint32") {
        columns[colIndex] = new Uint32Array(binaryBuffer, byteOffset, rowCount);
        columnKinds[colIndex] = "stringId";
        dictionaries[colIndex] = Array.isArray(colMeta.dictionary)
          ? colMeta.dictionary
          : [];
        lowerDictionaryValues[colIndex] = buildLowerDictionaryValues(
          dictionaries[colIndex],
          colMeta.lowerDictionary
        );
      } else if (storageKind === "int32") {
        columns[colIndex] = new Int32Array(binaryBuffer, byteOffset, rowCount);
        columnKinds[colIndex] = "int";
      } else {
        throw new Error("Unsupported storage kind in metadata.");
      }

      const values = columns[colIndex];
      const kind = columnKinds[colIndex];
      const cacheCol = new Array(rowCount);
      const sortedIndicesMeta = colMeta.sortedIndices;
      const hasDictionary =
        Array.isArray(dictionaries[colIndex]) &&
        dictionaries[colIndex].length > 0;

      if (hasDictionary) {
        const ids = values;
        const dict = dictionaries[colIndex] || [];
        const lowerValues = lowerDictionaryValues[colIndex] || [];
        const postings = decodeLowerDictionaryPostingBinary(
          colMeta.lowerDictionary,
          binaryBuffer
        );

        for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
          const id = ids[rowIndex];
          const lowerValue =
            lowerValues[id] !== undefined
              ? lowerValues[id]
              : normalizeDictionaryValue(dict[id] === undefined ? "" : dict[id]);
          cacheCol[rowIndex] = lowerValue;
        }

        lowerDictionaries[colIndex] = postings;
        sortedIndexColumns[colIndex] = null;
      } else {
        if (
          !sortedIndicesMeta ||
          typeof sortedIndicesMeta !== "object" ||
          sortedIndicesMeta.storageKind !== "uint32"
        ) {
          throw new Error("Missing sorted indices metadata for non-dictionary column.");
        }
        const sortedByteOffset = Number(sortedIndicesMeta.byteOffset);
        const sortedByteLength = Number(sortedIndicesMeta.byteLength);
        const expectedSortedByteLength = rowCount * 4;
        if (
          !Number.isInteger(sortedByteOffset) ||
          sortedByteOffset < 0 ||
          !Number.isInteger(sortedByteLength) ||
          sortedByteLength !== expectedSortedByteLength
        ) {
          throw new Error("Invalid sorted indices byte range metadata.");
        }
        if (
          sortedByteOffset + sortedByteLength > binaryBuffer.byteLength
        ) {
          throw new Error("Sorted indices range is out of binary bounds.");
        }
        sortedIndexColumns[colIndex] = new Uint32Array(
          binaryBuffer,
          sortedByteOffset,
          rowCount
        );

        for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
          cacheCol[rowIndex] = String(values[rowIndex]).toLowerCase();
        }

        dictionaries[colIndex] = [];
        lowerDictionaries[colIndex] = createEmptyLowerDictionaryPostings();
        lowerDictionaryValues[colIndex] = [];
      }

      cacheColumns[colIndex] = cacheCol;
    }

    const sortedIndexByKey = Object.create(null);
    for (let i = 0; i < schema.baseColumnCount; i += 1) {
      if (sortedIndexColumns[i] instanceof Uint32Array) {
        sortedIndexByKey[schema.columnKeys[i]] = sortedIndexColumns[i];
      }
    }

    return {
      rowCount,
      columnCount: schema.baseColumnCount,
      baseColumnCount: schema.baseColumnCount,
      cacheOffset: schema.numericCacheOffset,
      hasCacheColumns: true,
      columns,
      columnKinds,
      dictionaries,
      lowerDictionaries,
      lowerDictionaryValues,
      cacheColumns,
      sortedIndexColumns,
      sortedIndexByKey,
    };
  }

  function convertNumericColumnarDataToObjectRows(
    numericColumnarData,
    schema,
    includeCache
  ) {
    const rowCount =
      numericColumnarData && typeof numericColumnarData.rowCount === "number"
        ? numericColumnarData.rowCount
        : 0;
    const rows = new Array(rowCount);
    const columnKinds = numericColumnarData.columnKinds || [];
    const columns = numericColumnarData.columns || [];
    const dictionaries = numericColumnarData.dictionaries || [];
    const cacheColumns = numericColumnarData.cacheColumns;
    const hasCacheColumns =
      Array.isArray(cacheColumns) && cacheColumns.length >= schema.baseColumnCount;

    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      const row = {};

      for (let colIndex = 0; colIndex < schema.baseColumnCount; colIndex += 1) {
        const kind = columnKinds[colIndex];
        const key = schema.columnKeys[colIndex];
        let value = null;

        if (kind === "stringId") {
          const ids = columns[colIndex];
          const dict = dictionaries[colIndex] || [];
          value = dict[ids[rowIndex]];
        } else {
          value = columns[colIndex][rowIndex];
        }

        row[key] = value;

        if (includeCache) {
          const cacheValue =
            hasCacheColumns && cacheColumns[colIndex] !== undefined
              ? cacheColumns[colIndex][rowIndex]
              : undefined;
          row[schema.objectCacheKeys[colIndex]] =
            cacheValue !== undefined
              ? cacheValue
              : String(value).toLowerCase();
        }
      }

      rows[rowIndex] = row;
    }

    return rows;
  }

  async function handlePregeneratedFileLoad(files) {
    const bridge = getBridge();
    if (!bridge) {
      return;
    }

    const fileList = Array.isArray(files) ? files : [];
    if (fileList.length === 0) {
      return;
    }

    const loadStart = performance.now();
    const loadTimings = {
      jsonReadMs: 0,
      jsonParseMs: 0,
      binReadMs: 0,
      decodeMs: 0,
      downloadMs: NaN,
      totalMs: 0,
    };

    try {
      const metadataFiles = fileList.filter((file) =>
        String(file.name || "").toLowerCase().endsWith(".json")
      );
      const binaryFiles = fileList.filter((file) =>
        String(file.name || "").toLowerCase().endsWith(".bin")
      );
      const unsupportedFiles = fileList.filter((file) => {
        const name = String(file.name || "").toLowerCase();
        return !name.endsWith(".json") && !name.endsWith(".bin");
      });

      if (
        metadataFiles.length !== 1 ||
        binaryFiles.length !== 1 ||
        unsupportedFiles.length > 0
      ) {
        throw new Error("Select exactly one .json metadata file and one .bin file.");
      }

      const schema = bridge.getSchema();
      const jsonReadStartMs = performance.now();
      const metadataText = await readFileAsText(metadataFiles[0]);
      loadTimings.jsonReadMs = performance.now() - jsonReadStartMs;

      const binReadStartMs = performance.now();
      const binaryBuffer = await readFileAsArrayBuffer(binaryFiles[0]);
      loadTimings.binReadMs = performance.now() - binReadStartMs;

      applyPregeneratedPayload(
        bridge,
        schema,
        metadataText,
        binaryBuffer,
        loadStart,
        loadTimings
      );
    } catch (error) {
      bridge.setGenerationError(
        `Failed to load pregenerated file: ${String(
          error && error.message ? error.message : error
        )}`
      );
    } finally {
      loadPregeneratedInputEl.value = "";
    }
  }

  async function handlePregeneratedPresetLoad(presetRowCountText) {
    const bridge = getBridge();
    if (!bridge) {
      return;
    }

    const presetRowCount = Number.parseInt(String(presetRowCountText || ""), 10);
    if (!Number.isInteger(presetRowCount) || presetRowCount <= 0) {
      bridge.setGenerationError("Invalid table preset selection.");
      return;
    }

    const baseFileName = `fasttable-columnar-${presetRowCount}`;
    const metadataUrl = `${TABLE_PRESETS_FOLDER}/${baseFileName}.json`;
    const binaryUrl = `${TABLE_PRESETS_FOLDER}/${baseFileName}.bin`;
    const loadStart = performance.now();
    const loadTimings = {
      jsonReadMs: 0,
      jsonParseMs: 0,
      binReadMs: 0,
      decodeMs: 0,
      downloadMs: 0,
      totalMs: 0,
    };

    try {
      const schema = bridge.getSchema();
      const setPresetLoadingStatus =
        typeof bridge.setGenerationError === "function"
          ? bridge.setGenerationError.bind(bridge)
          : null;
      if (setPresetLoadingStatus) {
        setPresetLoadingStatus(
          `Loading table preset ${presetRowCount}: downloading metadata...`
        );
      }

      const jsonReadStartMs = performance.now();
      const metadataText = await readUrlAsText(metadataUrl);
      loadTimings.jsonReadMs = performance.now() - jsonReadStartMs;

      const binReadStartMs = performance.now();
      let lastProgressUpdateMs = 0;
      const binaryBuffer = await readUrlAsArrayBuffer(
        binaryUrl,
        (receivedBytes, totalBytes) => {
          if (!setPresetLoadingStatus) {
            return;
          }

          const nowMs = performance.now();
          const isComplete =
            totalBytes > 0 ? receivedBytes >= totalBytes : false;
          if (!isComplete && nowMs - lastProgressUpdateMs < 100) {
            return;
          }
          lastProgressUpdateMs = nowMs;

          if (totalBytes > 0) {
            const percent = Math.min(
              100,
              Math.max(0, (receivedBytes / totalBytes) * 100)
            );
            setPresetLoadingStatus(
              `Loading table preset ${presetRowCount}: downloading BIN ${percent.toFixed(
                1
              )}% (${formatByteSize(receivedBytes)} / ${formatByteSize(
                totalBytes
              )})...`
            );
          } else {
            setPresetLoadingStatus(
              `Loading table preset ${presetRowCount}: downloading BIN (${formatByteSize(
                receivedBytes
              )})...`
            );
          }
        }
      );
      loadTimings.binReadMs = performance.now() - binReadStartMs;
      loadTimings.downloadMs = loadTimings.jsonReadMs + loadTimings.binReadMs;
      if (setPresetLoadingStatus) {
        setPresetLoadingStatus(`Loading table preset ${presetRowCount}: decoding...`);
      }

      applyPregeneratedPayload(
        bridge,
        schema,
        metadataText,
        binaryBuffer,
        loadStart,
        loadTimings
      );
    } catch (error) {
      bridge.setGenerationError(
        `Failed to load table preset ${presetRowCount}: ${String(
          error && error.message ? error.message : error
        )}`
      );
    }
  }

  saveObjectBtnEl.addEventListener("click", () => {
    setAllActionButtonsDisabled(true);
    try {
      saveCurrentRowsToBinaryFiles();
    } finally {
      setAllActionButtonsDisabled(false);
    }
  });

  loadPregeneratedBtnEl.addEventListener("click", () => {
    loadPregeneratedInputEl.click();
  });

  loadPregeneratedInputEl.addEventListener("change", async (event) => {
    const files = event.target.files ? Array.from(event.target.files) : [];
    if (files.length === 0) {
      return;
    }

    setAllActionButtonsDisabled(true);
    try {
      await handlePregeneratedFileLoad(files);
    } finally {
      setAllActionButtonsDisabled(false);
    }
  });

  if (loadPregeneratedPresetEl) {
    loadPregeneratedPresetEl.addEventListener("change", async (event) => {
      const selectedPreset = String(event.target.value || "");
      if (selectedPreset === "") {
        return;
      }

      setAllActionButtonsDisabled(true);
      try {
        await handlePregeneratedPresetLoad(selectedPreset);
      } finally {
        setAllActionButtonsDisabled(false);
      }
    });
  }
})();
