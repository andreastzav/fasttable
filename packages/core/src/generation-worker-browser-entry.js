import { fastTableGenerationApi } from "./generation.js";
import { attachGenerationWorkerProtocol } from "./generation-worker-protocol.js";

const isWorkerContext =
  typeof WorkerGlobalScope !== "undefined" &&
  typeof self !== "undefined" &&
  self instanceof WorkerGlobalScope;

if (isWorkerContext) {
  try {
    attachGenerationWorkerProtocol(self, {
      generationApi: fastTableGenerationApi,
      now() {
        return performance.now();
      },
    });
  } catch (error) {
    self.postMessage({
      type: "workerInitError",
      error: String(error && error.message ? error.message : error),
    });
  }
}
