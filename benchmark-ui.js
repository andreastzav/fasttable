import { createBenchmarkRuntimeAdapter } from "@fasttable/core";
import {
  createBenchmarkDelayTick,
  resolveBenchmarkTickPolicy,
} from "@fasttable/core/benchmark";

function getBenchmarkApiFromWindow() {
  if (typeof window === "undefined") {
    return null;
  }

  return window.fastTableBenchmarkApi || null;
}

function setAllActionButtonsDisabled(disabled, primaryBtnEl, currentBtnEl) {
  if (typeof window !== "undefined" && typeof window.fastTableSetActionButtonsDisabled === "function") {
    window.fastTableSetActionButtonsDisabled(disabled);
    return;
  }

  if (primaryBtnEl) {
    primaryBtnEl.disabled = disabled;
  }

  if (currentBtnEl) {
    currentBtnEl.disabled = disabled;
  }
}

function readGlobalBenchmarkTickPolicy() {
  if (typeof window === "undefined") {
    return "";
  }

  return typeof window.fastTableBenchmarkTickPolicy === "string"
    ? window.fastTableBenchmarkTickPolicy
    : "";
}

function createUiBenchmarkDelayTick(input) {
  const requestedPolicy =
    input && typeof input.tickPolicy === "function"
      ? input.tickPolicy()
      : input
        ? input.tickPolicy
        : "";
  const fallbackPolicy = "macro";
  const resolvedPolicy = resolveBenchmarkTickPolicy(
    requestedPolicy || readGlobalBenchmarkTickPolicy(),
    fallbackPolicy
  );
  return createBenchmarkDelayTick(resolvedPolicy);
}

function renderBenchmarkLines(statusEl, lines) {
  if (!statusEl) {
    return;
  }

  const source = Array.isArray(lines) ? lines : [];
  statusEl.innerHTML = source.join("<br>");
}

function formatPrefixedBenchmarkLines(lines, linePrefix) {
  const source = Array.isArray(lines) ? lines : [];
  const prefix =
    typeof linePrefix === "string" && linePrefix.trim() !== ""
      ? `[${linePrefix.trim()}] `
      : "";
  if (prefix === "") {
    return source;
  }

  const out = new Array(source.length);
  for (let i = 0; i < source.length; i += 1) {
    out[i] = `${prefix}${String(source[i])}`;
  }
  return out;
}

function formatBenchmarkError(error) {
  return String(error && error.message ? error.message : error);
}

function bindBenchmarkUi(options) {
  const input = options || {};
  const primaryBtnEl = input.primaryBtnEl || null;
  const currentBtnEl = input.currentBtnEl || null;
  const statusEl = input.statusEl || null;
  const runBenchmark = input.runBenchmark;
  const linePrefix =
    typeof input.linePrefix === "string" ? input.linePrefix : "";
  const emptyMessage =
    typeof input.emptyMessage === "string" && input.emptyMessage.trim() !== ""
      ? input.emptyMessage
      : "Benchmark API not available.";

  if (!primaryBtnEl || !statusEl || typeof runBenchmark !== "function") {
    return false;
  }

  async function run(currentOnly) {
    const sourceApi =
      typeof input.getBenchmarkApi === "function"
        ? input.getBenchmarkApi()
        : getBenchmarkApiFromWindow();
    const api =
      sourceApi && typeof sourceApi === "object"
        ? createBenchmarkRuntimeAdapter({ api: sourceApi })
        : sourceApi;
    const delayTick = createUiBenchmarkDelayTick(input);

    setAllActionButtonsDisabled(true, primaryBtnEl, currentBtnEl);
    try {
      // Yield once so busy state/cursor can repaint before heavy benchmark setup.
      await delayTick();
      const result = await runBenchmark({
        api,
        currentOnly: currentOnly === true,
        delayTick,
        now: () => performance.now(),
        onUpdate(lines) {
          renderBenchmarkLines(
            statusEl,
            formatPrefixedBenchmarkLines(lines, linePrefix)
          );
        },
      });

      if (!result || !Array.isArray(result.lines) || result.lines.length === 0) {
        statusEl.textContent = emptyMessage;
      }
    } catch (error) {
      statusEl.textContent = formatBenchmarkError(error);
    } finally {
      setAllActionButtonsDisabled(false, primaryBtnEl, currentBtnEl);
    }
  }

  primaryBtnEl.addEventListener("click", () => {
    run(false);
  });

  if (currentBtnEl) {
    currentBtnEl.addEventListener("click", () => {
      run(true);
    });
  }

  return true;
}

export { bindBenchmarkUi };
