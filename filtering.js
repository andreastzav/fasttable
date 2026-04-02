import { fastTableFilteringApi } from "@fasttable/core";

if (typeof window !== "undefined") {
  window.fastTableFiltering = fastTableFilteringApi;
}

export { fastTableFilteringApi };
