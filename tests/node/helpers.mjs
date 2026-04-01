import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..", "..");

let distBuilt = false;

function ensureCoreDistBuilt() {
  if (distBuilt) {
    return;
  }

  const distIndexPath = path.join(
    repoRoot,
    "packages",
    "core",
    "dist",
    "index.js"
  );
  if (!fs.existsSync(distIndexPath)) {
    throw new Error(
      "Missing packages/core/dist. Run `node build-core.mjs` before tests."
    );
  }

  distBuilt = true;
}

function createDeterministicRows(rowCount) {
  const total = Math.max(0, Number(rowCount) | 0);
  const firstNames = [
    "Alex",
    "Andreas",
    "Maria",
    "Nikos",
    "Elena",
    "Sofia",
    "John",
    "Chris",
  ];
  const lastNames = [
    "Anderson",
    "Papadopoulos",
    "Smith",
    "Johnson",
    "Olsen",
    "Miller",
    "Brown",
    "Wilson",
  ];
  const rows = new Array(total);

  for (let i = 0; i < total; i += 1) {
    const index = i + 1;
    const row = {
      index,
      firstName: firstNames[i % firstNames.length],
      lastName: lastNames[(i * 7) % lastNames.length],
      age: 18 + (i % 82),
      column5: (index * 3) % 1000,
      column6: (index * 5 + 7) % 2500,
      column7: (index * 2) % 8000,
      column8: (index * 2 - 1) % 12000,
      column9: (index * 11 + 13) % 40000,
      column10: (index * 17 + 19) % 55000,
      column11: (index * 23 + 29) % 65000,
      column12: (index * 31 + 37) % 75000,
      column13: (index * 41 + 43) % 90000,
      column14: (index * 47 + 53) % 120000,
      column15: (index * 59 + 61) % 160000,
    };

    const keys = Object.keys(row);
    for (let k = 0; k < keys.length; k += 1) {
      const key = keys[k];
      row[`${key}Cache`] = String(row[key]).toLowerCase();
    }

    rows[i] = row;
  }

  return rows;
}

function cloneRows(rows) {
  const source = Array.isArray(rows) ? rows : [];
  const out = new Array(source.length);
  for (let i = 0; i < source.length; i += 1) {
    out[i] = { ...source[i] };
  }
  return out;
}

function indexResultToArray(result, fallbackCount = 0) {
  if (result === null || result === undefined) {
    const out = new Array(fallbackCount);
    for (let i = 0; i < fallbackCount; i += 1) {
      out[i] = i;
    }
    return out;
  }

  if (
    result &&
    ArrayBuffer.isView(result.buffer) &&
    typeof result.count === "number"
  ) {
    const count = Math.max(0, Math.min(result.count | 0, result.buffer.length));
    const out = new Array(count);
    for (let i = 0; i < count; i += 1) {
      out[i] = result.buffer[i];
    }
    return out;
  }

  if (Array.isArray(result) || ArrayBuffer.isView(result)) {
    return Array.from(result);
  }

  return [];
}

function arrayLikeToArray(value) {
  if (Array.isArray(value)) {
    return value.slice();
  }

  if (ArrayBuffer.isView(value)) {
    return Array.from(value);
  }

  return [];
}

function assertArrayLikeEqual(actual, expected, message) {
  const left = arrayLikeToArray(actual);
  const right = arrayLikeToArray(expected);
  assert.deepEqual(left, right, message);
}

function buildColumnTypeByKey(columnKeys) {
  const keys = Array.isArray(columnKeys) ? columnKeys : [];
  const out = Object.create(null);
  for (let i = 0; i < keys.length; i += 1) {
    const key = keys[i];
    out[key] = key === "firstName" || key === "lastName" ? "string" : "number";
  }
  return out;
}

function applySortDescriptors(controller, descriptors) {
  const descriptorList = Array.isArray(descriptors) ? descriptors : [];
  controller.reset();

  for (let i = 0; i < descriptorList.length; i += 1) {
    const descriptor = descriptorList[i];
    if (!descriptor || typeof descriptor.columnKey !== "string") {
      continue;
    }

    controller.cycle(descriptor.columnKey);
    if (descriptor.direction === "asc") {
      controller.cycle(descriptor.columnKey);
    }
  }
}

function buildPrecomputedSortKeyColumns(
  indices,
  rowsByIndex,
  descriptors,
  columnTypeByKey
) {
  const descriptorList = Array.isArray(descriptors) ? descriptors : [];
  const keyColumns = new Array(descriptorList.length);

  for (let d = 0; d < descriptorList.length; d += 1) {
    const descriptor = descriptorList[d];
    const columnKey = descriptor && descriptor.columnKey;
    const valueType =
      columnTypeByKey && typeof columnKey === "string"
        ? columnTypeByKey[columnKey]
        : "string";
    const useNumericValues = valueType === "number";
    const values = useNumericValues
      ? new Float64Array(indices.length)
      : new Array(indices.length);

    for (let i = 0; i < indices.length; i += 1) {
      const rowIndex = indices[i];
      const row = rowsByIndex[rowIndex];
      const rawValue = row && columnKey ? row[columnKey] : undefined;
      if (useNumericValues) {
        if (rawValue === undefined || rawValue === null) {
          values[i] = Number.NaN;
        } else {
          const numericValue = Number(rawValue);
          values[i] = Number.isFinite(numericValue) ? numericValue : Number.NaN;
        }
      } else {
        values[i] = rawValue;
      }
    }

    keyColumns[d] = values;
  }

  return keyColumns;
}

export {
  ensureCoreDistBuilt,
  createDeterministicRows,
  cloneRows,
  indexResultToArray,
  assertArrayLikeEqual,
  buildColumnTypeByKey,
  applySortDescriptors,
  buildPrecomputedSortKeyColumns,
};
