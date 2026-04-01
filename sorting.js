import {
  fastTableSortingApi,
  registerSortMethod,
} from "@fasttable/core/sorting";

function getGlobalSortMethod(globalName) {
  if (
    typeof globalThis !== "undefined" &&
    typeof globalThis[globalName] === "function"
  ) {
    return globalThis[globalName];
  }

  return null;
}

function registerBrowserSortMethods() {
  const timSort = getGlobalSortMethod("FastTimSort");
  if (timSort) {
    registerSortMethod("timsort", timSort);
  }

  const timSort0060 = getGlobalSortMethod("FastTimSort0060");
  if (timSort0060) {
    registerSortMethod("timsort0060", timSort0060);
  }

  const timSort0018 = getGlobalSortMethod("FastTimSort0018");
  if (timSort0018) {
    registerSortMethod("timsort0018", timSort0018);
  }

  const quadSort = getGlobalSortMethod("FastQuadSort");
  if (quadSort) {
    registerSortMethod("quadsort", quadSort);
  }

  const fluxSort = getGlobalSortMethod("FastFluxSort");
  if (fluxSort) {
    registerSortMethod("fluxsort", fluxSort);
  }
}

registerBrowserSortMethods();

if (typeof window !== "undefined") {
  window.fastTableSorting = fastTableSortingApi;
}

export { fastTableSortingApi };
