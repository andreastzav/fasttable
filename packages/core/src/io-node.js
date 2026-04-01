import { promises as fs } from "node:fs";
import path from "node:path";
import {
  createColumnarBinaryExportFiles,
  parseColumnarBinaryMetadataText,
  decodeColumnarBinaryData,
} from "./io.js";

function toUint8ViewFromBuffer(buffer) {
  if (!buffer || !ArrayBuffer.isView(buffer)) {
    throw new Error("Expected Buffer-compatible binary payload.");
  }

  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);
}

async function saveColumnarBinaryFiles(options) {
  const input = options || {};
  const numericColumnarData = input.numericColumnarData;
  const schema = input.schema;
  const dirPath = typeof input.dirPath === "string" && input.dirPath !== ""
    ? input.dirPath
    : ".";

  if (!numericColumnarData || !schema) {
    throw new Error("numericColumnarData and schema are required.");
  }

  const rowCount = Number.isFinite(input.rowCount)
    ? Math.max(0, Number(input.rowCount) | 0)
    : Math.max(0, Number(numericColumnarData.rowCount) | 0);
  const baseFileName =
    typeof input.baseFileName === "string" && input.baseFileName !== ""
      ? input.baseFileName
      : `fasttable-columnar-${rowCount}`;

  const files = createColumnarBinaryExportFiles(numericColumnarData, schema);
  const metadataPath = path.join(dirPath, `${baseFileName}.json`);
  const binaryPath = path.join(dirPath, `${baseFileName}.bin`);

  await fs.writeFile(metadataPath, files.metadataText, "utf8");
  await fs.writeFile(binaryPath, Buffer.from(files.binaryBytes));

  return {
    metadataPath,
    binaryPath,
    metadata: files.metadata,
    metadataText: files.metadataText,
    binaryBytes: files.binaryBytes,
  };
}

async function loadColumnarBinaryFiles(options) {
  const input = options || {};
  const schema = input.schema;
  const metadataPath = input.metadataPath;
  const binaryPath = input.binaryPath;

  if (!schema || typeof metadataPath !== "string" || typeof binaryPath !== "string") {
    throw new Error("schema, metadataPath, and binaryPath are required.");
  }

  const metadataText = await fs.readFile(metadataPath, "utf8");
  const binaryNodeBuffer = await fs.readFile(binaryPath);
  const binaryBuffer = toUint8ViewFromBuffer(binaryNodeBuffer);
  const metadata = parseColumnarBinaryMetadataText(metadataText, schema);
  const numericColumnarData = decodeColumnarBinaryData(
    metadata,
    binaryBuffer,
    schema
  );

  return {
    metadata,
    metadataText,
    binaryBuffer,
    numericColumnarData,
  };
}

async function loadColumnarBinaryPreset(options) {
  const input = options || {};
  const schema = input.schema;
  const presetDir = typeof input.presetDir === "string" && input.presetDir !== ""
    ? input.presetDir
    : ".";
  const rowCount = Number(input.rowCount);
  const baseFileName =
    typeof input.baseFileName === "string" && input.baseFileName !== ""
      ? input.baseFileName
      : Number.isFinite(rowCount) && rowCount > 0
        ? `fasttable-columnar-${Math.floor(rowCount)}`
        : "";

  if (!schema || baseFileName === "") {
    throw new Error("schema and rowCount/baseFileName are required.");
  }

  return loadColumnarBinaryFiles({
    schema,
    metadataPath: path.join(presetDir, `${baseFileName}.json`),
    binaryPath: path.join(presetDir, `${baseFileName}.bin`),
  });
}

export {
  saveColumnarBinaryFiles,
  loadColumnarBinaryFiles,
  loadColumnarBinaryPreset,
};
