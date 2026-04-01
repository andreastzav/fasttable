import {
  createColumnarBinaryExportFiles,
  parseColumnarBinaryMetadataText,
  decodeColumnarBinaryData,
  convertNumericColumnarDataToObjectRows,
} from "@fasttable/core/io";
import {
  formatByteSize,
  createBinaryBlob,
  triggerBlobDownload,
  readFileAsText,
  readFileAsArrayBuffer,
  readUrlAsText,
  readUrlAsArrayBuffer,
} from "@fasttable/core/io-browser";

(function () {
  const saveObjectBtnEl = document.getElementById("saveObjectBtn");
  const loadPregeneratedBtnEl = document.getElementById("loadPregeneratedBtn");
  const loadPregeneratedInputEl = document.getElementById("loadPregeneratedInput");
  const loadPregeneratedPresetEl = document.getElementById("loadPregeneratedPreset");
  const DEFAULT_TABLE_PRESETS_BASE_URL =
    "https://pub-c9f856de3fa0426290595486c4ea4d73.r2.dev/tables_presets";

  function normalizePresetBaseUrl(value) {
    const text = String(value || "").trim();
    if (text === "") {
      return "";
    }

    return text.replace(/\/+$/, "");
  }

  function resolvePresetBaseUrl() {
    if (typeof window !== "undefined") {
      const windowValue = normalizePresetBaseUrl(
        window.FASTTABLE_PRESETS_BASE_URL
      );
      if (windowValue !== "") {
        return windowValue;
      }
    }

    if (typeof document !== "undefined" && typeof document.querySelector === "function") {
      const metaEl = document.querySelector(
        'meta[name="fasttable-presets-base-url"]'
      );
      if (metaEl && typeof metaEl.content === "string") {
        const metaValue = normalizePresetBaseUrl(metaEl.content);
        if (metaValue !== "") {
          return metaValue;
        }
      }
    }

    return DEFAULT_TABLE_PRESETS_BASE_URL;
  }

  const TABLE_PRESETS_BASE_URL = resolvePresetBaseUrl();

  if (!saveObjectBtnEl || !loadPregeneratedBtnEl || !loadPregeneratedInputEl) {
    return;
  }

  function getBridge() {
    return window.fastTableIOBridge;
  }

  function setAllActionButtonsDisabled(disabled) {
    if (typeof window.fastTableSetActionButtonsDisabled === "function") {
      window.fastTableSetActionButtonsDisabled(disabled);
      return;
    }

    saveObjectBtnEl.disabled = disabled;
    loadPregeneratedBtnEl.disabled = disabled;
    if (loadPregeneratedPresetEl) {
      loadPregeneratedPresetEl.disabled = disabled;
    }
  }

  function saveCurrentRowsToBinaryFiles() {
    const bridge = getBridge();
    if (!bridge) {
      return;
    }

    if (!bridge.hasRows()) {
      bridge.setGenerationError("No generated rows to save.");
      return;
    }

    const schema = bridge.getSchema();
    const rowCount = bridge.getRowCount();
    const numericColumnarData = bridge.getNumericColumnarForSave();
    if (!numericColumnarData) {
      bridge.setGenerationError("No generated rows to save.");
      return;
    }

    const files = createColumnarBinaryExportFiles(numericColumnarData, schema);
    const metadataBlob = new Blob([files.metadataText], {
      type: "application/json",
    });
    const binaryBlob = createBinaryBlob(files.binaryBytes);

    triggerBlobDownload(metadataBlob, `fasttable-columnar-${rowCount}.json`);
    triggerBlobDownload(binaryBlob, `fasttable-columnar-${rowCount}.bin`);
  }

  function applyPregeneratedPayload(
    bridge,
    schema,
    metadataText,
    binaryBuffer,
    loadStart,
    loadTimings
  ) {
    const jsonParseStartMs = performance.now();
    const metadata = parseColumnarBinaryMetadataText(metadataText, schema);
    loadTimings.jsonParseMs = performance.now() - jsonParseStartMs;

    const decodeStartMs = performance.now();
    const numericColumnarData = decodeColumnarBinaryData(
      metadata,
      binaryBuffer,
      schema
    );
    loadTimings.decodeMs = performance.now() - decodeStartMs;
    loadTimings.totalMs = performance.now() - loadStart;
    const loadDurationMs = loadTimings.totalMs;

    if (typeof bridge.applyLoadedNumericColumnarDataset === "function") {
      bridge.applyLoadedNumericColumnarDataset(
        numericColumnarData,
        loadDurationMs,
        loadTimings
      );
      return;
    }

    const loadedRows = convertNumericColumnarDataToObjectRows(
      numericColumnarData,
      schema,
      true
    );
    bridge.applyLoadedColumnarBinaryDataset(
      loadedRows,
      loadDurationMs,
      loadTimings
    );
  }

  async function handlePregeneratedFileLoad(files) {
    const bridge = getBridge();
    if (!bridge) {
      return;
    }

    const fileList = Array.isArray(files) ? files : [];
    if (fileList.length === 0) {
      return;
    }

    const loadStart = performance.now();
    const loadTimings = {
      jsonReadMs: 0,
      jsonParseMs: 0,
      binReadMs: 0,
      decodeMs: 0,
      downloadMs: Number.NaN,
      totalMs: 0,
    };

    try {
      const metadataFiles = fileList.filter((file) =>
        String(file.name || "").toLowerCase().endsWith(".json")
      );
      const binaryFiles = fileList.filter((file) =>
        String(file.name || "").toLowerCase().endsWith(".bin")
      );
      const unsupportedFiles = fileList.filter((file) => {
        const name = String(file.name || "").toLowerCase();
        return !name.endsWith(".json") && !name.endsWith(".bin");
      });

      if (
        metadataFiles.length !== 1 ||
        binaryFiles.length !== 1 ||
        unsupportedFiles.length > 0
      ) {
        throw new Error("Select exactly one .json metadata file and one .bin file.");
      }

      const schema = bridge.getSchema();
      const jsonReadStartMs = performance.now();
      const metadataText = await readFileAsText(metadataFiles[0]);
      loadTimings.jsonReadMs = performance.now() - jsonReadStartMs;

      const binReadStartMs = performance.now();
      const binaryBuffer = await readFileAsArrayBuffer(binaryFiles[0]);
      loadTimings.binReadMs = performance.now() - binReadStartMs;

      applyPregeneratedPayload(
        bridge,
        schema,
        metadataText,
        binaryBuffer,
        loadStart,
        loadTimings
      );
    } catch (error) {
      bridge.setGenerationError(
        `Failed to load pregenerated file: ${String(
          error && error.message ? error.message : error
        )}`
      );
    } finally {
      loadPregeneratedInputEl.value = "";
    }
  }

  async function handlePregeneratedPresetLoad(presetRowCountText) {
    const bridge = getBridge();
    if (!bridge) {
      return;
    }

    const presetRowCount = Number.parseInt(String(presetRowCountText || ""), 10);
    if (!Number.isInteger(presetRowCount) || presetRowCount <= 0) {
      bridge.setGenerationError("Invalid table preset selection.");
      return;
    }

    const baseFileName = `fasttable-columnar-${presetRowCount}`;
    const metadataUrl = `${TABLE_PRESETS_BASE_URL}/${baseFileName}.json`;
    const binaryUrl = `${TABLE_PRESETS_BASE_URL}/${baseFileName}.bin`;
    const loadStart = performance.now();
    const loadTimings = {
      jsonReadMs: 0,
      jsonParseMs: 0,
      binReadMs: 0,
      decodeMs: 0,
      downloadMs: 0,
      totalMs: 0,
    };

    try {
      const schema = bridge.getSchema();
      const setPresetLoadingStatus =
        typeof bridge.setGenerationError === "function"
          ? bridge.setGenerationError.bind(bridge)
          : null;
      if (setPresetLoadingStatus) {
        setPresetLoadingStatus(
          `Loading table preset ${presetRowCount}: downloading metadata...`
        );
      }

      const jsonReadStartMs = performance.now();
      const metadataText = await readUrlAsText(metadataUrl);
      loadTimings.jsonReadMs = performance.now() - jsonReadStartMs;

      const binReadStartMs = performance.now();
      let lastProgressUpdateMs = 0;
      const binaryBuffer = await readUrlAsArrayBuffer(
        binaryUrl,
        (receivedBytes, totalBytes) => {
          if (!setPresetLoadingStatus) {
            return;
          }

          const nowMs = performance.now();
          const isComplete =
            totalBytes > 0 ? receivedBytes >= totalBytes : false;
          if (!isComplete && nowMs - lastProgressUpdateMs < 100) {
            return;
          }
          lastProgressUpdateMs = nowMs;

          if (totalBytes > 0) {
            const percent = Math.min(
              100,
              Math.max(0, (receivedBytes / totalBytes) * 100)
            );
            setPresetLoadingStatus(
              `Loading table preset ${presetRowCount}: downloading BIN ${percent.toFixed(
                1
              )}% (${formatByteSize(receivedBytes)} / ${formatByteSize(
                totalBytes
              )})...`
            );
          } else {
            setPresetLoadingStatus(
              `Loading table preset ${presetRowCount}: downloading BIN (${formatByteSize(
                receivedBytes
              )})...`
            );
          }
        }
      );
      loadTimings.binReadMs = performance.now() - binReadStartMs;
      loadTimings.downloadMs = loadTimings.jsonReadMs + loadTimings.binReadMs;
      if (setPresetLoadingStatus) {
        setPresetLoadingStatus(`Loading table preset ${presetRowCount}: decoding...`);
      }

      applyPregeneratedPayload(
        bridge,
        schema,
        metadataText,
        binaryBuffer,
        loadStart,
        loadTimings
      );
    } catch (error) {
      bridge.setGenerationError(
        `Failed to load table preset ${presetRowCount}: ${String(
          error && error.message ? error.message : error
        )}`
      );
    }
  }

  saveObjectBtnEl.addEventListener("click", () => {
    setAllActionButtonsDisabled(true);
    try {
      saveCurrentRowsToBinaryFiles();
    } finally {
      setAllActionButtonsDisabled(false);
    }
  });

  loadPregeneratedBtnEl.addEventListener("click", () => {
    loadPregeneratedInputEl.click();
  });

  loadPregeneratedInputEl.addEventListener("change", async (event) => {
    const files = event.target.files ? Array.from(event.target.files) : [];
    if (files.length === 0) {
      return;
    }

    setAllActionButtonsDisabled(true);
    try {
      await handlePregeneratedFileLoad(files);
    } finally {
      setAllActionButtonsDisabled(false);
    }
  });

  if (loadPregeneratedPresetEl) {
    loadPregeneratedPresetEl.addEventListener("change", async (event) => {
      const selectedPreset = String(event.target.value || "");
      if (selectedPreset === "") {
        return;
      }

      setAllActionButtonsDisabled(true);
      try {
        await handlePregeneratedPresetLoad(selectedPreset);
      } finally {
        setAllActionButtonsDisabled(false);
      }
    });
  }
})();
