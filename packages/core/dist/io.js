const COLUMNAR_BINARY_FORMAT = "fasttable-columnar-binary-v2";

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

function resolveLowerDictionaryPostings(dictionary, lowerDictionary, ids) {
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

function decodeLowerDictionaryPostingBinary(lowerDictionary, binarySource) {
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

  if (byteOffset + byteLength > binarySource.byteLength) {
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

  const postingsBuffer = createTypedArrayFromBinarySource(
    Uint32Array,
    binarySource,
    byteOffset,
    byteLength / 4,
    "lowerDictionary postings range is out of binary bounds."
  );
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

function isNumericSortColumnKey(columnKey, dictionary) {
  if (columnKey === "firstName" || columnKey === "lastName") {
    return false;
  }

  if (Array.isArray(dictionary) && dictionary.length > 0) {
    const probeCount = Math.min(dictionary.length, 16);
    for (let i = 0; i < probeCount; i += 1) {
      if (typeof dictionary[i] !== "number") {
        return false;
      }
    }
  }

  return true;
}

function compareDictionaryKeyValuesAsc(aKey, bKey, columnKey) {
  if (isNumericSortColumnKey(columnKey)) {
    return compareSortValuesAsc(Number(aKey), Number(bKey), "float");
  }

  return compareSortValuesAsc(aKey, bKey, "string");
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

function buildSortedIndexColumnFromLowerDictionary(
  lowerDictionary,
  columnKey,
  rowCount
) {
  if (
    !lowerDictionary ||
    typeof lowerDictionary !== "object" ||
    Array.isArray(lowerDictionary)
  ) {
    return null;
  }

  const keys = Object.keys(lowerDictionary);
  const count = Math.max(0, rowCount | 0);
  if (keys.length === 0) {
    return count === 0 ? new Uint32Array(0) : null;
  }

  const sortedKeys = keys
    .slice()
    .sort((a, b) => compareDictionaryKeyValuesAsc(a, b, columnKey));
  const output = new Uint32Array(count);
  let writeIndex = 0;

  for (let i = 0; i < sortedKeys.length; i += 1) {
    const postings = lowerDictionary[sortedKeys[i]];
    if (!Array.isArray(postings) && !ArrayBuffer.isView(postings)) {
      continue;
    }

    for (let j = 0; j < postings.length; j += 1) {
      if (writeIndex >= count) {
        return null;
      }

      const rowIndex = Number(postings[j]);
      if (!Number.isInteger(rowIndex) || rowIndex < 0 || rowIndex >= count) {
        return null;
      }

      output[writeIndex] = rowIndex >>> 0;
      writeIndex += 1;
    }
  }

  if (writeIndex !== count) {
    return null;
  }

  return output;
}

function buildSortedIndexColumnFromDictionaryIds(ids, dictionary, columnKey, rowCount) {
  if (!ids || typeof ids.length !== "number") {
    return new Uint32Array(0);
  }

  const count = Math.max(0, rowCount | 0);
  const values = new Array(count);
  const useNumericValues = isNumericSortColumnKey(columnKey, dictionary);

  for (let i = 0; i < count; i += 1) {
    const id = Number(ids[i]);
    const dictValue = Array.isArray(dictionary) ? dictionary[id] : undefined;
    if (useNumericValues) {
      values[i] = Number(dictValue);
    } else {
      values[i] =
        dictValue === undefined || dictValue === null ? "" : String(dictValue);
    }
  }

  return buildSortedIndexColumn(
    values,
    useNumericValues ? "float" : "string",
    count
  );
}

function nowMs() {
  if (typeof performance !== "undefined" && typeof performance.now === "function") {
    return performance.now();
  }

  return Date.now();
}

function isSupportedRankArray(rankByRowId, expectedLength) {
  if (!(rankByRowId instanceof Uint16Array || rankByRowId instanceof Uint32Array)) {
    return false;
  }

  if (!Number.isFinite(expectedLength)) {
    return true;
  }

  return rankByRowId.length === Math.max(0, Number(expectedLength) | 0);
}

function buildRankArrayForColumnFromSortedIndices(
  numericColumnarData,
  columnIndex,
  sortedIndices,
  rowCount
) {
  if (
    !numericColumnarData ||
    typeof numericColumnarData !== "object" ||
    !(sortedIndices instanceof Uint32Array) ||
    sortedIndices.length !== rowCount
  ) {
    return null;
  }

  const columnKinds = Array.isArray(numericColumnarData.columnKinds)
    ? numericColumnarData.columnKinds
    : [];
  const columns = Array.isArray(numericColumnarData.columns)
    ? numericColumnarData.columns
    : [];
  const columnKind = columnKinds[columnIndex];
  const values = columns[columnIndex];
  const rank32 = new Uint32Array(rowCount);
  if (rowCount <= 0) {
    return {
      rankByRowId: rank32,
      maxRank: 0,
    };
  }

  const firstRowIndex = sortedIndices[0];
  let previousToken = values ? values[firstRowIndex] : undefined;
  let currentRank = 0;
  rank32[firstRowIndex] = currentRank;

  for (let i = 1; i < rowCount; i += 1) {
    const rowIndex = sortedIndices[i];
    const nextToken = values ? values[rowIndex] : undefined;
    let isSame = false;

    if (columnKind === "float") {
      const previousNumber = Number(previousToken);
      const nextNumber = Number(nextToken);
      isSame =
        (Number.isNaN(previousNumber) && Number.isNaN(nextNumber)) ||
        previousNumber === nextNumber;
    } else {
      isSame = nextToken === previousToken;
    }

    if (!isSame) {
      currentRank += 1;
      previousToken = nextToken;
    }

    rank32[rowIndex] = currentRank;
  }

  if (currentRank <= 0xffff) {
    const rank16 = new Uint16Array(rowCount);
    rank16.set(rank32);
    return {
      rankByRowId: rank16,
      maxRank: currentRank >>> 0,
    };
  }

  return {
    rankByRowId: rank32,
    maxRank: currentRank >>> 0,
  };
}

function buildDescendingRankArrayFromAscending(rankByRowId, maxRank, rowCount) {
  if (!isSupportedRankArray(rankByRowId, rowCount)) {
    return null;
  }

  const maxRankValue = Number(maxRank) >>> 0;
  if (maxRankValue <= 0xffff) {
    const out16 = new Uint16Array(rowCount);
    for (let i = 0; i < rowCount; i += 1) {
      out16[i] = (maxRankValue - (rankByRowId[i] >>> 0)) >>> 0;
    }
    return out16;
  }

  const out32 = new Uint32Array(rowCount);
  for (let i = 0; i < rowCount; i += 1) {
    out32[i] = (maxRankValue - (rankByRowId[i] >>> 0)) >>> 0;
  }
  return out32;
}

function ensureNumericColumnarSortedIndices(numericColumnarData, schema) {
  if (!numericColumnarData || typeof numericColumnarData !== "object") {
    return numericColumnarData;
  }

  const columns = Array.isArray(numericColumnarData.columns)
    ? numericColumnarData.columns
    : [];
  const schemaColumnKeys = Array.isArray(schema && schema.columnKeys)
    ? schema.columnKeys
    : [];
  const schemaBaseCount =
    schema && Number.isInteger(schema.baseColumnCount)
      ? Math.max(0, schema.baseColumnCount | 0)
      : schemaColumnKeys.length;
  const baseCount = Math.min(
    columns.length,
    schemaBaseCount > 0 ? schemaBaseCount : columns.length
  );
  const rowCount = Math.max(0, Number(numericColumnarData.rowCount) | 0);
  const dictionaries = Array.isArray(numericColumnarData.dictionaries)
    ? numericColumnarData.dictionaries
    : [];
  const lowerDictionaries = Array.isArray(numericColumnarData.lowerDictionaries)
    ? numericColumnarData.lowerDictionaries
    : [];
  const columnKinds = Array.isArray(numericColumnarData.columnKinds)
    ? numericColumnarData.columnKinds
    : [];
  const existingSortedColumns = Array.isArray(numericColumnarData.sortedIndexColumns)
    ? numericColumnarData.sortedIndexColumns
    : null;
  const sortedColumns =
    existingSortedColumns && existingSortedColumns.length >= baseCount
      ? existingSortedColumns
      : new Array(baseCount);
  const sortedByKey = createEmptyLowerDictionaryPostings();

  for (let colIndex = 0; colIndex < baseCount; colIndex += 1) {
    const columnKey =
      typeof schemaColumnKeys[colIndex] === "string" && schemaColumnKeys[colIndex] !== ""
        ? schemaColumnKeys[colIndex]
        : `column${colIndex + 1}`;
    const existing = sortedColumns[colIndex];
    if (existing instanceof Uint32Array && existing.length === rowCount) {
      sortedByKey[columnKey] = existing;
      continue;
    }

    const dictionary = Array.isArray(dictionaries[colIndex])
      ? dictionaries[colIndex]
      : [];
    const hasDictionary = dictionary.length > 0;
    let nextSorted = null;

    if (hasDictionary) {
      nextSorted = buildSortedIndexColumnFromLowerDictionary(
        lowerDictionaries[colIndex],
        columnKey,
        rowCount
      );
      if (!(nextSorted instanceof Uint32Array) || nextSorted.length !== rowCount) {
        nextSorted = buildSortedIndexColumnFromDictionaryIds(
          columns[colIndex],
          dictionary,
          columnKey,
          rowCount
        );
      }
    } else {
      nextSorted = buildSortedIndexColumn(
        columns[colIndex],
        columnKinds[colIndex],
        rowCount
      );
    }

    if (nextSorted instanceof Uint32Array && nextSorted.length === rowCount) {
      sortedColumns[colIndex] = nextSorted;
      sortedByKey[columnKey] = nextSorted;
    } else {
      sortedColumns[colIndex] = null;
    }
  }

  numericColumnarData.sortedIndexColumns = sortedColumns;
  numericColumnarData.sortedIndexByKey = sortedByKey;
  return numericColumnarData;
}

function ensureNumericColumnarSortedRanks(numericColumnarData, schema) {
  if (!numericColumnarData || typeof numericColumnarData !== "object") {
    return {
      numericColumnarData,
      durationMs: 0,
      computedColumns: 0,
    };
  }

  const rowCount = Math.max(0, Number(numericColumnarData.rowCount) | 0);
  const sortedColumnarData = ensureNumericColumnarSortedIndices(
    numericColumnarData,
    schema
  );
  const sortedColumns = Array.isArray(sortedColumnarData.sortedIndexColumns)
    ? sortedColumnarData.sortedIndexColumns
    : [];
  const schemaColumnKeys = Array.isArray(schema && schema.columnKeys)
    ? schema.columnKeys
    : [];
  const schemaBaseCount =
    schema && Number.isInteger(schema.baseColumnCount)
      ? Math.max(0, schema.baseColumnCount | 0)
      : schemaColumnKeys.length;
  const baseCount = Math.min(
    sortedColumns.length,
    schemaBaseCount > 0 ? schemaBaseCount : schemaColumnKeys.length
  );
  const existingAscRankColumns = Array.isArray(sortedColumnarData.sortedRankAscColumns)
    ? sortedColumnarData.sortedRankAscColumns
    : Array.isArray(sortedColumnarData.sortedRankColumns)
      ? sortedColumnarData.sortedRankColumns
      : [];
  const existingAscRankMaxColumns = Array.isArray(
    sortedColumnarData.sortedRankAscMaxColumns
  )
    ? sortedColumnarData.sortedRankAscMaxColumns
    : Array.isArray(sortedColumnarData.sortedRankMaxColumns)
      ? sortedColumnarData.sortedRankMaxColumns
      : [];
  const existingDescRankColumns = Array.isArray(
    sortedColumnarData.sortedRankDescColumns
  )
    ? sortedColumnarData.sortedRankDescColumns
    : [];
  const existingDescRankMaxColumns = Array.isArray(
    sortedColumnarData.sortedRankDescMaxColumns
  )
    ? sortedColumnarData.sortedRankDescMaxColumns
    : [];
  const ascRankColumns = new Array(baseCount).fill(null);
  const ascRankMaxColumns = new Array(baseCount).fill(0);
  const ascRankByKey = createEmptyLowerDictionaryPostings();
  const ascRankMaxByKey = createEmptyLowerDictionaryPostings();
  const descRankColumns = new Array(baseCount).fill(null);
  const descRankMaxColumns = new Array(baseCount).fill(0);
  const descRankByKey = createEmptyLowerDictionaryPostings();
  const descRankMaxByKey = createEmptyLowerDictionaryPostings();
  const startMs = nowMs();
  let computedColumns = 0;

  for (let colIndex = 0; colIndex < baseCount; colIndex += 1) {
    const sortedIndices = sortedColumns[colIndex];
    if (!(sortedIndices instanceof Uint32Array) || sortedIndices.length !== rowCount) {
      continue;
    }

    const existingAscRank = existingAscRankColumns[colIndex];
    const existingAscMax = Number(existingAscRankMaxColumns[colIndex]);
    let ascRankByRowId = null;
    let ascMaxRank = 0;
    if (isSupportedRankArray(existingAscRank, rowCount)) {
      ascRankByRowId = existingAscRank;
      ascMaxRank = Number.isFinite(existingAscMax)
        ? Math.max(0, existingAscMax) >>> 0
        : rowCount > 0
          ? existingAscRank[sortedIndices[rowCount - 1]] >>> 0
          : 0;
    } else {
      const builtRank = buildRankArrayForColumnFromSortedIndices(
        sortedColumnarData,
        colIndex,
        sortedIndices,
        rowCount
      );
      if (!builtRank || !isSupportedRankArray(builtRank.rankByRowId, rowCount)) {
        continue;
      }

      ascRankByRowId = builtRank.rankByRowId;
      ascMaxRank = Number(builtRank.maxRank) >>> 0;
      computedColumns += 1;
    }

    ascRankColumns[colIndex] = ascRankByRowId;
    ascRankMaxColumns[colIndex] = ascMaxRank;

    const existingDescRank = existingDescRankColumns[colIndex];
    const existingDescMax = Number(existingDescRankMaxColumns[colIndex]);
    let descRankByRowId = null;
    if (isSupportedRankArray(existingDescRank, rowCount)) {
      descRankByRowId = existingDescRank;
      descRankMaxColumns[colIndex] = Number.isFinite(existingDescMax)
        ? Math.max(0, existingDescMax) >>> 0
        : ascMaxRank;
    } else {
      const builtDescRank = buildDescendingRankArrayFromAscending(
        ascRankByRowId,
        ascMaxRank,
        rowCount
      );
      if (!isSupportedRankArray(builtDescRank, rowCount)) {
        continue;
      }
      descRankByRowId = builtDescRank;
      descRankMaxColumns[colIndex] = ascMaxRank;
      computedColumns += 1;
    }
    descRankColumns[colIndex] = descRankByRowId;

    const columnKey =
      typeof schemaColumnKeys[colIndex] === "string" &&
      schemaColumnKeys[colIndex] !== ""
        ? schemaColumnKeys[colIndex]
        : `column${colIndex + 1}`;
    ascRankByKey[columnKey] = ascRankByRowId;
    ascRankMaxByKey[columnKey] = ascMaxRank;
    descRankByKey[columnKey] = descRankByRowId;
    descRankMaxByKey[columnKey] = descRankMaxColumns[colIndex];
  }

  sortedColumnarData.sortedRankAscColumns = ascRankColumns;
  sortedColumnarData.sortedRankAscByKey = ascRankByKey;
  sortedColumnarData.sortedRankAscMaxColumns = ascRankMaxColumns;
  sortedColumnarData.sortedRankAscMaxByKey = ascRankMaxByKey;
  sortedColumnarData.sortedRankDescColumns = descRankColumns;
  sortedColumnarData.sortedRankDescByKey = descRankByKey;
  sortedColumnarData.sortedRankDescMaxColumns = descRankMaxColumns;
  sortedColumnarData.sortedRankDescMaxByKey = descRankMaxByKey;
  // Back-compat aliases: default rank = ascending.
  sortedColumnarData.sortedRankColumns = ascRankColumns;
  sortedColumnarData.sortedRankByKey = ascRankByKey;
  sortedColumnarData.sortedRankMaxColumns = ascRankMaxColumns;
  sortedColumnarData.sortedRankMaxByKey = ascRankMaxByKey;

  return {
    numericColumnarData: sortedColumnarData,
    durationMs: nowMs() - startMs,
    computedColumns,
  };
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

function ensureArrayBuffer(value) {
  if (value instanceof ArrayBuffer) {
    return value;
  }

  if (ArrayBuffer.isView(value)) {
    return value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength);
  }

  throw new Error("Expected ArrayBuffer-compatible binary payload.");
}

function normalizeBinarySource(value) {
  if (value instanceof ArrayBuffer) {
    return {
      buffer: value,
      byteOffset: 0,
      byteLength: value.byteLength,
      bytesView: new Uint8Array(value),
    };
  }

  if (ArrayBuffer.isView(value)) {
    return {
      buffer: value.buffer,
      byteOffset: value.byteOffset,
      byteLength: value.byteLength,
      bytesView: new Uint8Array(value.buffer, value.byteOffset, value.byteLength),
    };
  }

  throw new Error("Expected ArrayBuffer-compatible binary payload.");
}

function createTypedArrayFromBinarySource(
  typedArrayCtor,
  binarySource,
  byteOffset,
  elementCount,
  outOfBoundsMessage
) {
  const bytesPerElement = typedArrayCtor.BYTES_PER_ELEMENT;
  const byteLength = elementCount * bytesPerElement;
  if (
    !Number.isInteger(byteOffset) ||
    byteOffset < 0 ||
    !Number.isInteger(elementCount) ||
    elementCount < 0 ||
    byteOffset + byteLength > binarySource.byteLength
  ) {
    throw new Error(outOfBoundsMessage);
  }

  const absoluteByteOffset = binarySource.byteOffset + byteOffset;
  if (absoluteByteOffset % bytesPerElement === 0) {
    try {
      return new typedArrayCtor(
        binarySource.buffer,
        absoluteByteOffset,
        elementCount
      );
    } catch (_error) {
      // Fall back to a copied aligned segment below.
    }
  }

  const sourceSegment = binarySource.bytesView.subarray(
    byteOffset,
    byteOffset + byteLength
  );
  const copiedSegment = new Uint8Array(byteLength);
  copiedSegment.set(sourceSegment);
  return new typedArrayCtor(copiedSegment.buffer);
}

function createColumnarBinaryExportPayload(numericColumnarData, schema) {
  const normalizedNumericColumnarData = ensureNumericColumnarSortedIndices(
    numericColumnarData,
    schema
  );
  const rowCount = normalizedNumericColumnarData.rowCount;
  const columns = normalizedNumericColumnarData.columns;
  const dictionaries = normalizedNumericColumnarData.dictionaries || [];
  const lowerDictionaries = normalizedNumericColumnarData.lowerDictionaries || [];
  const columnMetadata = new Array(schema.baseColumnCount);
  const parts = [];
  let currentOffset = 0;

  for (let colIndex = 0; colIndex < schema.baseColumnCount; colIndex += 1) {
    const typedColumn = columns[colIndex];
    if (!ArrayBuffer.isView(typedColumn)) {
      throw new Error("Cannot export non-binary column in numeric dataset.");
    }
    const storageKind = getStorageKindFromNumericColumn(
      normalizedNumericColumnarData,
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
    }

    const sortedIndices = getOrBuildSortedIndexColumnForExport(
      normalizedNumericColumnarData,
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

  const binaryBytes = new Uint8Array(currentOffset);
  let writeOffset = 0;
  for (let i = 0; i < parts.length; i += 1) {
    const part = parts[i];
    binaryBytes.set(part, writeOffset);
    writeOffset += part.byteLength;
  }

  return {
    metadata,
    binaryBytes,
  };
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

function createColumnarBinaryExportFiles(numericColumnarData, schema) {
  const payload = createColumnarBinaryExportPayload(numericColumnarData, schema);
  return {
    metadata: payload.metadata,
    metadataText: stringifyMetadataWithCompactLowerDictionaryPostings(
      payload.metadata
    ),
    binaryBytes: payload.binaryBytes,
  };
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

function decodeColumnarBinaryData(metadata, binaryBuffer, schema) {
  const resolvedBinarySource = normalizeBinarySource(binaryBuffer);
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

    if (byteOffset < 0 || byteOffset + byteLength > resolvedBinarySource.byteLength) {
      throw new Error("Column byte range is out of binary file bounds.");
    }

    if (storageKind === "float64") {
      columns[colIndex] = createTypedArrayFromBinarySource(
        Float64Array,
        resolvedBinarySource,
        byteOffset,
        rowCount,
        "Column byte range is out of binary file bounds."
      );
      columnKinds[colIndex] = "float";
    } else if (storageKind === "uint16") {
      columns[colIndex] = createTypedArrayFromBinarySource(
        Uint16Array,
        resolvedBinarySource,
        byteOffset,
        rowCount,
        "Column byte range is out of binary file bounds."
      );
      columnKinds[colIndex] = "stringId";
      dictionaries[colIndex] = Array.isArray(colMeta.dictionary)
        ? colMeta.dictionary
        : [];
      lowerDictionaryValues[colIndex] = buildLowerDictionaryValues(
        dictionaries[colIndex],
        colMeta.lowerDictionary
      );
    } else if (storageKind === "uint32") {
      columns[colIndex] = createTypedArrayFromBinarySource(
        Uint32Array,
        resolvedBinarySource,
        byteOffset,
        rowCount,
        "Column byte range is out of binary file bounds."
      );
      columnKinds[colIndex] = "stringId";
      dictionaries[colIndex] = Array.isArray(colMeta.dictionary)
        ? colMeta.dictionary
        : [];
      lowerDictionaryValues[colIndex] = buildLowerDictionaryValues(
        dictionaries[colIndex],
        colMeta.lowerDictionary
      );
    } else if (storageKind === "int32") {
      columns[colIndex] = createTypedArrayFromBinarySource(
        Int32Array,
        resolvedBinarySource,
        byteOffset,
        rowCount,
        "Column byte range is out of binary file bounds."
      );
      columnKinds[colIndex] = "int";
    } else {
      throw new Error("Unsupported storage kind in metadata.");
    }

    const values = columns[colIndex];
    const cacheCol = new Array(rowCount);
    const sortedIndicesMeta = colMeta.sortedIndices;
    const hasDictionary =
      Array.isArray(dictionaries[colIndex]) &&
      dictionaries[colIndex].length > 0;
    if (
      !sortedIndicesMeta ||
      typeof sortedIndicesMeta !== "object" ||
      sortedIndicesMeta.storageKind !== "uint32"
    ) {
      throw new Error("Missing sorted indices metadata.");
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
    if (sortedByteOffset + sortedByteLength > resolvedBinarySource.byteLength) {
      throw new Error("Sorted indices range is out of binary bounds.");
    }
    sortedIndexColumns[colIndex] = createTypedArrayFromBinarySource(
      Uint32Array,
      resolvedBinarySource,
      sortedByteOffset,
      rowCount,
      "Sorted indices range is out of binary bounds."
    );

    if (hasDictionary) {
      const ids = values;
      const dict = dictionaries[colIndex] || [];
      const lowerValues = lowerDictionaryValues[colIndex] || [];
      const postings = decodeLowerDictionaryPostingBinary(
        colMeta.lowerDictionary,
        resolvedBinarySource
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
    } else {
      for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
        cacheCol[rowIndex] = String(values[rowIndex]).toLowerCase();
      }

      dictionaries[colIndex] = [];
      lowerDictionaries[colIndex] = createEmptyLowerDictionaryPostings();
      lowerDictionaryValues[colIndex] = [];
    }

    cacheColumns[colIndex] = cacheCol;
  }

  const sortedIndexByKey = createEmptyLowerDictionaryPostings();
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

function decodeColumnarBinaryPayload(metadataText, binaryBuffer, schema) {
  const metadata = parseColumnarBinaryMetadataText(metadataText, schema);
  const numericColumnarData = decodeColumnarBinaryData(
    metadata,
    binaryBuffer,
    schema
  );

  return {
    metadata,
    numericColumnarData,
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
          cacheValue !== undefined ? cacheValue : String(value).toLowerCase();
      }
    }

    rows[rowIndex] = row;
  }

  return rows;
}

export {
  COLUMNAR_BINARY_FORMAT,
  alignOffset,
  createColumnarBinaryExportPayload,
  createColumnarBinaryExportFiles,
  stringifyMetadataWithCompactLowerDictionaryPostings,
  parseColumnarBinaryMetadataText,
  decodeColumnarBinaryData,
  decodeColumnarBinaryPayload,
  convertNumericColumnarDataToObjectRows,
  ensureNumericColumnarSortedIndices,
  ensureNumericColumnarSortedRanks,
  ensureArrayBuffer,
};
