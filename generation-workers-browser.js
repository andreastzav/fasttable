import {
  fastTableGenerationWorkersBrowserApi,
  attachGenerationWorkersBrowserApi,
} from "@fasttable/core";

if (typeof window !== "undefined") {
  attachGenerationWorkersBrowserApi(window);
}

export { fastTableGenerationWorkersBrowserApi };
