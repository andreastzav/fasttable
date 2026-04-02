function defaultNow() {
  if (typeof performance !== "undefined" && typeof performance.now === "function") {
    return performance.now();
  }

  return Date.now();
}

function hasIndexCollection(indices) {
  return Array.isArray(indices) || ArrayBuffer.isView(indices);
}

function normalizeSortDescriptorList(descriptors) {
  const source = Array.isArray(descriptors) ? descriptors : [];
  const output = [];

  for (let i = 0; i < source.length; i += 1) {
    const descriptor = source[i];
    const columnKey =
      descriptor && typeof descriptor.columnKey === "string"
        ? descriptor.columnKey
        : "";
    if (columnKey === "") {
      continue;
    }

    output.push({
      columnKey,
      direction: descriptor.direction === "asc" ? "asc" : "desc",
    });
  }

  return output;
}

function resolveSnapshotIndicesCandidate(rowsSnapshot) {
  if (
    rowsSnapshot &&
    typeof rowsSnapshot === "object" &&
    hasIndexCollection(rowsSnapshot.rowIndices)
  ) {
    return rowsSnapshot.rowIndices;
  }

  if (
    rowsSnapshot &&
    typeof rowsSnapshot === "object" &&
    hasIndexCollection(rowsSnapshot.indices)
  ) {
    return rowsSnapshot.indices;
  }

  if (hasIndexCollection(rowsSnapshot)) {
    return rowsSnapshot;
  }

  return null;
}

function createSortBenchmarkOrchestrator(options) {
  const input = options || {};
  const now = typeof input.now === "function" ? input.now : defaultNow;
  const materializeIndices =
    typeof input.materializeIndices === "function"
      ? input.materializeIndices
      : (indices) => indices;
  const runFallbackSort =
    typeof input.runFallbackSort === "function" ? input.runFallbackSort : null;
  const runPrecomputedSort =
    typeof input.runPrecomputedSort === "function"
      ? input.runPrecomputedSort
      : null;

  function runPrecomputedSortSnapshotPass(rowsSnapshot, descriptors, rowCount) {
    if (!runFallbackSort || !runPrecomputedSort) {
      throw new Error(
        "Sort benchmark orchestrator requires runFallbackSort and runPrecomputedSort callbacks."
      );
    }

    const descriptorList = normalizeSortDescriptorList(descriptors);
    const fallback = () => runFallbackSort(rowsSnapshot, descriptorList);

    if (descriptorList.length === 0) {
      return fallback();
    }

    const totalRows = Math.max(0, Number(rowCount) | 0);
    const snapshotIndicesCandidate = resolveSnapshotIndicesCandidate(rowsSnapshot);
    if (!hasIndexCollection(snapshotIndicesCandidate)) {
      return fallback();
    }

    const snapshotIndices = materializeIndices(snapshotIndicesCandidate, totalRows);
    if (!hasIndexCollection(snapshotIndices)) {
      return fallback();
    }

    const snapshotCount = snapshotIndices.length | 0;
    const totalStartMs = now();
    const precomputedRun = runPrecomputedSort({
      rowsSnapshot,
      descriptorList,
      snapshotIndices,
      snapshotCount,
      rowCount: totalRows,
    });
    if (!precomputedRun) {
      return fallback();
    }

    // Callback can either return already-normalized benchmark result
    // or an app-style { sortResult, sortedIndices } payload.
    if (
      Number.isFinite(precomputedRun.sortCoreMs) ||
      Number.isFinite(precomputedRun.sortMs)
    ) {
      const sortCoreMs = Number.isFinite(precomputedRun.sortCoreMs)
        ? Number(precomputedRun.sortCoreMs)
        : Number(precomputedRun.sortMs) || 0;
      const sortTotalMs = Number.isFinite(precomputedRun.sortTotalMs)
        ? Number(precomputedRun.sortTotalMs)
        : now() - totalStartMs;
      const sortPrepMs = Number.isFinite(precomputedRun.sortPrepMs)
        ? Number(precomputedRun.sortPrepMs)
        : sortTotalMs - sortCoreMs;

      return {
        ...precomputedRun,
        sortMs: sortCoreMs,
        sortCoreMs,
        sortPrepMs,
        sortTotalMs,
      };
    }

    const sortResult = precomputedRun.sortResult;
    const sortedIndices = precomputedRun.sortedIndices;
    if (
      !sortResult ||
      typeof sortResult !== "object" ||
      !hasIndexCollection(sortedIndices)
    ) {
      return fallback();
    }

    const sortTotalMs = now() - totalStartMs;
    const sortCoreMs = Number(sortResult.durationMs) || 0;
    return {
      sortMs: sortCoreMs,
      sortCoreMs,
      sortPrepMs: sortTotalMs - sortCoreMs,
      sortTotalMs,
      sortMode: sortResult.sortMode || "precomputed",
      sortedCount: sortedIndices.length,
      descriptors:
        sortResult.effectiveDescriptors ||
        sortResult.descriptors ||
        descriptorList,
      dataPath: sortResult.dataPath,
      comparatorMode: sortResult.comparatorMode || "precomputed",
    };
  }

  return {
    runPrecomputedSortSnapshotPass,
    normalizeSortDescriptorList,
    hasIndexCollection,
  };
}

export {
  hasIndexCollection,
  normalizeSortDescriptorList,
  createSortBenchmarkOrchestrator,
};
