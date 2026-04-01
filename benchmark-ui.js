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

function delayBenchmarkTick() {
  return new Promise((resolve) => {
    setTimeout(resolve, 0);
  });
}

function renderBenchmarkLines(statusEl, lines) {
  if (!statusEl) {
    return;
  }

  const source = Array.isArray(lines) ? lines : [];
  statusEl.innerHTML = source.join("<br>");
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
  const emptyMessage =
    typeof input.emptyMessage === "string" && input.emptyMessage.trim() !== ""
      ? input.emptyMessage
      : "Benchmark API not available.";

  if (!primaryBtnEl || !statusEl || typeof runBenchmark !== "function") {
    return false;
  }

  async function run(currentOnly) {
    const api =
      typeof input.getBenchmarkApi === "function"
        ? input.getBenchmarkApi()
        : getBenchmarkApiFromWindow();

    setAllActionButtonsDisabled(true, primaryBtnEl, currentBtnEl);
    try {
      const result = await runBenchmark({
        api,
        currentOnly: currentOnly === true,
        delayTick: delayBenchmarkTick,
        now: () => performance.now(),
        onUpdate(lines) {
          renderBenchmarkLines(statusEl, lines);
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
