function normalizeSortMode(sortMode, fallbackMode = "native") {
  const fallback =
    typeof fallbackMode === "string" && fallbackMode.trim() !== ""
      ? fallbackMode.trim()
      : "native";
  const normalized =
    typeof sortMode === "string" ? sortMode.trim() : "";
  return normalized !== "" ? normalized : fallback;
}

function isPrecomputedSortMode(sortMode) {
  return (
    typeof sortMode === "string" &&
    sortMode.toLowerCase().startsWith("precomputed")
  );
}

function shouldPreferPrecomputedFastPath(sortMode, preferPrecomputedFastPath) {
  return (
    preferPrecomputedFastPath === true &&
    !isPrecomputedSortMode(normalizeSortMode(sortMode))
  );
}

function buildSortModeAttemptOrder(sortMode, preferPrecomputedFastPath) {
  const selectedMode = normalizeSortMode(sortMode);
  if (isPrecomputedSortMode(selectedMode)) {
    return [selectedMode];
  }
  if (!shouldPreferPrecomputedFastPath(selectedMode, preferPrecomputedFastPath)) {
    return [selectedMode];
  }
  return ["precomputed", selectedMode];
}

function shouldAcceptPrecomputedFastPathResult(runtimeSortRun) {
  if (!runtimeSortRun || typeof runtimeSortRun !== "object") {
    return false;
  }
  return isPrecomputedSortMode(runtimeSortRun.sortMode);
}

export {
  normalizeSortMode,
  isPrecomputedSortMode,
  shouldPreferPrecomputedFastPath,
  buildSortModeAttemptOrder,
  shouldAcceptPrecomputedFastPathResult,
};
