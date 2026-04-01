import {
  fastTableGenerationWorkersBrowserApi,
  attachGenerationWorkersBrowserApi,
} from "@fasttable/core/generation-workers-browser";

if (typeof window !== "undefined") {
  attachGenerationWorkersBrowserApi(window);
}

export { fastTableGenerationWorkersBrowserApi };
