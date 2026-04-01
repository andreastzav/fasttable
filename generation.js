import { fastTableGenerationApi } from "@fasttable/core/generation";

if (typeof window !== "undefined") {
  window.fastTableGeneration = fastTableGenerationApi;
}

if (typeof self !== "undefined") {
  self.fastTableGeneration = fastTableGenerationApi;
}

export { fastTableGenerationApi };
