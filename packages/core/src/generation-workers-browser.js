import { fastTableGenerationApi as generationApi } from "./generation.js";
import { createGenerationWorkersApi } from "./generation-workers-shared.js";

function createBrowserGenerationWorker() {
  return new Worker(new URL("./generation-worker-browser-entry.js", import.meta.url), {
    type: "module",
  });
}

function validateBrowserGenerationEnvironment() {
  if (typeof Worker === "undefined") {
    return new Error("Web Worker is not supported in this browser.");
  }

  if (typeof location !== "undefined" && location.protocol === "file:") {
    return new Error(
      "Worker generation is blocked on file:// in this browser. Use http://localhost or fallback to non-worker generation."
    );
  }

  return null;
}

function validateBrowserSortingEnvironment() {
  if (typeof Worker === "undefined") {
    return new Error("Web Worker is not supported in this browser.");
  }

  if (typeof location !== "undefined" && location.protocol === "file:") {
    return new Error(
      "Worker sorting is blocked on file:// in this browser. Use http://localhost."
    );
  }

  return null;
}

const browserWorkersApi = createGenerationWorkersApi({
  generationApi,
  createGenerationWorker: createBrowserGenerationWorker,
  validateGenerationEnvironment: validateBrowserGenerationEnvironment,
  validateSortingEnvironment: validateBrowserSortingEnvironment,
});

const { generateRowsWithWorkers, buildSortedIndicesWithWorkers } = browserWorkersApi;

const fastTableGenerationWorkersBrowserApi = {
  generateRowsWithWorkers,
  buildSortedIndicesWithWorkers,
};

function attachGenerationWorkersBrowserApi(targetWindow) {
  if (!targetWindow || typeof targetWindow !== "object") {
    return false;
  }

  targetWindow.fastTableGenerationWorkers = fastTableGenerationWorkersBrowserApi;
  return true;
}

export {
  generateRowsWithWorkers,
  buildSortedIndicesWithWorkers,
  fastTableGenerationWorkersBrowserApi,
  attachGenerationWorkersBrowserApi,
};
