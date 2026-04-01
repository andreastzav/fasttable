import { parentPort } from "node:worker_threads";
import { fastTableGenerationApi } from "./generation.js";
import { createGenerationWorkerMessageHandler } from "./generation-worker-protocol.js";

if (parentPort) {
  try {
    const handler = createGenerationWorkerMessageHandler({
      generationApi: fastTableGenerationApi,
      postMessage(payload, transferables) {
        if (Array.isArray(transferables) && transferables.length > 0) {
          parentPort.postMessage(payload, transferables);
          return;
        }

        parentPort.postMessage(payload);
      },
      now() {
        return performance.now();
      },
    });

    parentPort.on("message", (message) => {
      handler({ data: message });
    });
  } catch (error) {
    parentPort.postMessage({
      type: "workerInitError",
      error: String(error && error.message ? error.message : error),
    });
  }
}
