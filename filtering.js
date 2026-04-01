import { fastTableFilteringApi } from "@fasttable/core/filtering";

if (typeof window !== "undefined") {
  window.fastTableFiltering = fastTableFilteringApi;
}

export { fastTableFilteringApi };
