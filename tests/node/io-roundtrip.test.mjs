import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { promises as fs } from "node:fs";
import { before, test } from "node:test";
import {
  ensureCoreDistBuilt,
  createDeterministicRows,
  cloneRows,
  assertArrayLikeEqual,
} from "./helpers.mjs";

let srcGeneration;
let srcIo;
let distIo;
let srcIoNode;
let distIoNode;

function buildSchema(generationApi) {
  return {
    columnKeys: generationApi.COLUMN_KEYS.slice(),
    columnNames: generationApi.COLUMN_NAMES.slice(),
    baseColumnCount: generationApi.BASE_COLUMN_COUNT,
    numericCacheOffset: generationApi.NUMERIC_CACHE_OFFSET,
    objectCacheKeys: generationApi.COLUMN_KEYS.map((key) => `${key}Cache`),
  };
}

function assertNumericColumnarEqual(left, right) {
  assert.equal(left.rowCount, right.rowCount);
  assert.equal(left.columnCount, right.columnCount);
  assert.deepEqual(left.columnKinds, right.columnKinds);
  assert.equal(left.columns.length, right.columns.length);

  for (let i = 0; i < left.columns.length; i += 1) {
    assertArrayLikeEqual(left.columns[i], right.columns[i], `column mismatch ${i}`);
  }

  if (Array.isArray(left.cacheColumns) && Array.isArray(right.cacheColumns)) {
    assert.equal(left.cacheColumns.length, right.cacheColumns.length);
    for (let i = 0; i < left.cacheColumns.length; i += 1) {
      assert.deepEqual(left.cacheColumns[i], right.cacheColumns[i]);
    }
  } else {
    assert.equal(Boolean(left.cacheColumns), Boolean(right.cacheColumns));
  }

  assert.deepEqual(left.dictionaries, right.dictionaries);
  assert.deepEqual(left.lowerDictionaryValues, right.lowerDictionaryValues);
}

before(async () => {
  ensureCoreDistBuilt();

  srcGeneration = await import("../../packages/core/src/generation.js");
  srcIo = await import("../../packages/core/src/io.js");
  distIo = await import("../../packages/core/dist/io.js");
  srcIoNode = await import("../../packages/core/src/io-node.js");
  distIoNode = await import("../../packages/core/dist/io-node.js");
});

test("io in-memory roundtrip parity (src export -> src/dist decode)", () => {
  const rows = createDeterministicRows(1024);
  const numericRows = srcGeneration.deriveNumericRowsFromRows(cloneRows(rows));
  const numericColumnar =
    srcGeneration.deriveNumericColumnarDataFromNumericRows(numericRows.rows);
  const schema = buildSchema(srcGeneration);

  const exported = srcIo.createColumnarBinaryExportFiles(numericColumnar, schema);

  const parsedSrc = srcIo.parseColumnarBinaryMetadataText(
    exported.metadataText,
    schema
  );
  const decodedSrc = srcIo.decodeColumnarBinaryData(
    parsedSrc,
    exported.binaryBytes.buffer.slice(
      exported.binaryBytes.byteOffset,
      exported.binaryBytes.byteOffset + exported.binaryBytes.byteLength
    ),
    schema
  );
  assertNumericColumnarEqual(numericColumnar, decodedSrc);

  const parsedDist = distIo.parseColumnarBinaryMetadataText(
    exported.metadataText,
    schema
  );
  const decodedDist = distIo.decodeColumnarBinaryData(
    parsedDist,
    exported.binaryBytes.buffer.slice(
      exported.binaryBytes.byteOffset,
      exported.binaryBytes.byteOffset + exported.binaryBytes.byteLength
    ),
    schema
  );
  assertNumericColumnarEqual(numericColumnar, decodedDist);
});

test("io decode supports aligned offset binary views without full copy", () => {
  const rows = createDeterministicRows(512);
  const numericRows = srcGeneration.deriveNumericRowsFromRows(cloneRows(rows));
  const numericColumnar =
    srcGeneration.deriveNumericColumnarDataFromNumericRows(numericRows.rows);
  const schema = buildSchema(srcGeneration);

  const exported = srcIo.createColumnarBinaryExportFiles(numericColumnar, schema);
  const parsedDist = distIo.parseColumnarBinaryMetadataText(
    exported.metadataText,
    schema
  );

  const startOffset = 16;
  const padded = new Uint8Array(exported.binaryBytes.byteLength + startOffset + 8);
  padded.set(exported.binaryBytes, startOffset);
  const offsetView = new Uint8Array(
    padded.buffer,
    startOffset,
    exported.binaryBytes.byteLength
  );

  const decoded = distIo.decodeColumnarBinaryData(parsedDist, offsetView, schema);
  assertNumericColumnarEqual(numericColumnar, decoded);

  // Aligned offset views should stay zero-copy with column views backed by source bytes.
  assert.equal(decoded.columns[0].buffer, padded.buffer);
  const firstSortedColumn = decoded.sortedIndexColumns.find(
    (column) => column instanceof Uint32Array
  );
  assert.ok(firstSortedColumn instanceof Uint32Array);
  assert.equal(firstSortedColumn.buffer, padded.buffer);
});

test("io decode handles misaligned offset binary views", () => {
  const rows = createDeterministicRows(257);
  const numericRows = srcGeneration.deriveNumericRowsFromRows(cloneRows(rows));
  const numericColumnar =
    srcGeneration.deriveNumericColumnarDataFromNumericRows(numericRows.rows);
  const schema = buildSchema(srcGeneration);

  const exported = srcIo.createColumnarBinaryExportFiles(numericColumnar, schema);
  const parsedDist = distIo.parseColumnarBinaryMetadataText(
    exported.metadataText,
    schema
  );

  const startOffset = 1;
  const padded = new Uint8Array(exported.binaryBytes.byteLength + startOffset + 8);
  padded.set(exported.binaryBytes, startOffset);
  const offsetView = new Uint8Array(
    padded.buffer,
    startOffset,
    exported.binaryBytes.byteLength
  );

  const decoded = distIo.decodeColumnarBinaryData(parsedDist, offsetView, schema);
  assertNumericColumnarEqual(numericColumnar, decoded);
});

test("io-node file roundtrip parity (src save -> dist load)", async () => {
  const rows = createDeterministicRows(768);
  const numericRows = srcGeneration.deriveNumericRowsFromRows(cloneRows(rows));
  const numericColumnar =
    srcGeneration.deriveNumericColumnarDataFromNumericRows(numericRows.rows);
  const schema = buildSchema(srcGeneration);

  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "fasttable-io-"));
  try {
    const saved = await srcIoNode.saveColumnarBinaryFiles({
      numericColumnarData: numericColumnar,
      schema,
      dirPath: tempDir,
      baseFileName: "roundtrip",
    });

    const loaded = await distIoNode.loadColumnarBinaryFiles({
      schema,
      metadataPath: saved.metadataPath,
      binaryPath: saved.binaryPath,
    });

    assert.ok(ArrayBuffer.isView(loaded.binaryBuffer));
    assert.equal(loaded.binaryBuffer.byteLength, saved.binaryBytes.byteLength);
    assertNumericColumnarEqual(numericColumnar, loaded.numericColumnarData);
  } finally {
    await fs.rm(tempDir, { recursive: true, force: true });
  }
});
