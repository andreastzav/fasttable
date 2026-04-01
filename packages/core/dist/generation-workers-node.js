import { Worker } from "node:worker_threads";
import { fastTableGenerationApi as generationApi } from "./generation.js";
import { createGenerationWorkersApi } from "./generation-workers-shared.js";

function createNodeWorkerBridge(nodeWorker) {
  let onmessage = null;
  let onerror = null;
  let onmessageerror = null;

  nodeWorker.on("message", (data) => {
    if (typeof onmessage === "function") {
      onmessage({ data });
    }
  });

  nodeWorker.on("error", (error) => {
    if (typeof onerror === "function") {
      onerror(error);
    }
  });

  nodeWorker.on("messageerror", (error) => {
    if (typeof onmessageerror === "function") {
      onmessageerror(error);
    }
  });

  const bridge = {
    postMessage(payload, transferables) {
      if (Array.isArray(transferables) && transferables.length > 0) {
        nodeWorker.postMessage(payload, transferables);
        return;
      }

      nodeWorker.postMessage(payload);
    },
    terminate() {
      return nodeWorker.terminate();
    },
  };

  Object.defineProperty(bridge, "onmessage", {
    enumerable: true,
    configurable: true,
    get() {
      return onmessage;
    },
    set(handler) {
      onmessage = typeof handler === "function" ? handler : null;
    },
  });

  Object.defineProperty(bridge, "onerror", {
    enumerable: true,
    configurable: true,
    get() {
      return onerror;
    },
    set(handler) {
      onerror = typeof handler === "function" ? handler : null;
    },
  });

  Object.defineProperty(bridge, "onmessageerror", {
    enumerable: true,
    configurable: true,
    get() {
      return onmessageerror;
    },
    set(handler) {
      onmessageerror = typeof handler === "function" ? handler : null;
    },
  });

  return bridge;
}

function createNodeGenerationWorker() {
  const nodeWorker = new Worker(new URL("./generation-worker-node-entry.js", import.meta.url), {
    type: "module",
  });
  return createNodeWorkerBridge(nodeWorker);
}

function validateNodeWorkerEnvironment() {
  if (typeof Worker === "undefined") {
    return new Error("worker_threads Worker is unavailable in this Node runtime.");
  }

  return null;
}

const nodeWorkersApi = createGenerationWorkersApi({
  generationApi,
  createGenerationWorker: createNodeGenerationWorker,
  validateGenerationEnvironment: validateNodeWorkerEnvironment,
  validateSortingEnvironment: validateNodeWorkerEnvironment,
});

const { generateRowsWithWorkers, buildSortedIndicesWithWorkers } = nodeWorkersApi;

const fastTableGenerationWorkersNodeApi = {
  generateRowsWithWorkers,
  buildSortedIndicesWithWorkers,
};

export {
  generateRowsWithWorkers,
  buildSortedIndicesWithWorkers,
  fastTableGenerationWorkersNodeApi,
};
