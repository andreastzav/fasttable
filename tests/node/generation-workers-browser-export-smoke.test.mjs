import assert from "node:assert/strict";
import { before, test } from "node:test";
import { ensureCoreDistBuilt } from "./helpers.mjs";

let browserWorkers;
let coreRoot;

before(async () => {
  ensureCoreDistBuilt();
  coreRoot = await import("../../packages/core/dist/index.js");
  browserWorkers = await import(
    "../../packages/core/dist/generation-workers-browser.js"
  );
});

test("browser worker adapter exports are available in dist", async () => {
  assert.equal(typeof browserWorkers.generateRowsWithWorkers, "function");
  assert.equal(typeof browserWorkers.buildSortedIndicesWithWorkers, "function");
  assert.equal(
    typeof browserWorkers.attachGenerationWorkersBrowserApi,
    "function"
  );
  assert.equal(
    typeof browserWorkers.fastTableGenerationWorkersBrowserApi,
    "object"
  );
});

test("browser worker adapter attach helper is safe in node", () => {
  const attached = browserWorkers.attachGenerationWorkersBrowserApi(null);
  assert.equal(attached, false);
});

test("root dist entrypoint re-exports browser-safe adapters only", () => {
  assert.equal(
    Object.prototype.hasOwnProperty.call(
      coreRoot,
      "fastTableGenerationWorkersBrowserApi"
    ),
    true
  );
  assert.equal(
    Object.prototype.hasOwnProperty.call(
      coreRoot,
      "fastTableGenerationWorkersNodeApi"
    ),
    false
  );
  assert.equal(
    Object.prototype.hasOwnProperty.call(coreRoot, "createColumnarBinaryExportBlobs"),
    true
  );
  assert.equal(
    Object.prototype.hasOwnProperty.call(coreRoot, "saveColumnarBinaryFiles"),
    false
  );
});
