import { createColumnarBinaryExportFiles } from "./io.js";

function formatByteSize(bytes) {
  const numeric = Number(bytes);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "0 B";
  }

  if (numeric >= 1024 * 1024) {
    return `${(numeric / (1024 * 1024)).toFixed(1)} MB`;
  }

  if (numeric >= 1024) {
    return `${(numeric / 1024).toFixed(1)} KB`;
  }

  return `${Math.round(numeric)} B`;
}

function createBinaryBlob(binaryBytes) {
  if (typeof Blob !== "function") {
    throw new Error("Blob API is unavailable in this runtime.");
  }

  return new Blob([binaryBytes], { type: "application/octet-stream" });
}

function createMetadataBlob(metadataText) {
  if (typeof Blob !== "function") {
    throw new Error("Blob API is unavailable in this runtime.");
  }

  return new Blob([metadataText], { type: "application/json" });
}

function createColumnarBinaryExportBlobs(numericColumnarData, schema) {
  const files = createColumnarBinaryExportFiles(numericColumnarData, schema);

  return {
    metadata: files.metadata,
    metadataText: files.metadataText,
    binaryBytes: files.binaryBytes,
    metadataBlob: createMetadataBlob(files.metadataText),
    binaryBlob: createBinaryBlob(files.binaryBytes),
  };
}

function triggerBlobDownload(blob, fileName) {
  if (typeof URL === "undefined" || typeof URL.createObjectURL !== "function") {
    throw new Error("URL API is unavailable in this runtime.");
  }

  if (typeof document === "undefined" || typeof document.createElement !== "function") {
    throw new Error("DOM API is unavailable in this runtime.");
  }

  const url = URL.createObjectURL(blob);
  try {
    const link = document.createElement("a");
    link.href = url;
    link.download = fileName;
    link.click();
  } finally {
    URL.revokeObjectURL(url);
  }
}

function readFileAsText(file) {
  return new Promise((resolve, reject) => {
    if (typeof FileReader !== "function") {
      reject(new Error("FileReader API is unavailable in this runtime."));
      return;
    }

    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("Failed to read text file."));
    reader.readAsText(file);
  });
}

function readFileAsArrayBuffer(file) {
  return new Promise((resolve, reject) => {
    if (typeof FileReader !== "function") {
      reject(new Error("FileReader API is unavailable in this runtime."));
      return;
    }

    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = () => reject(new Error("Failed to read binary file."));
    reader.readAsArrayBuffer(file);
  });
}

function resolveFetch(fetchImpl) {
  if (typeof fetchImpl === "function") {
    return fetchImpl;
  }

  if (typeof fetch === "function") {
    return fetch.bind(globalThis);
  }

  throw new Error("Fetch API is unavailable in this runtime.");
}

async function readUrlAsText(url, fetchImpl) {
  const effectiveFetch = resolveFetch(fetchImpl);
  const response = await effectiveFetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Failed to fetch metadata file (${response.status}).`);
  }

  return response.text();
}

async function readUrlAsArrayBuffer(url, onProgress, fetchImpl) {
  const effectiveFetch = resolveFetch(fetchImpl);
  const response = await effectiveFetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Failed to fetch binary file (${response.status}).`);
  }

  const progressCallback =
    typeof onProgress === "function" ? onProgress : null;
  const headerLength = Number.parseInt(
    response.headers.get("content-length") || "",
    10
  );
  const totalBytes =
    Number.isFinite(headerLength) && headerLength > 0 ? headerLength : 0;

  if (!response.body || typeof response.body.getReader !== "function") {
    const fallbackBuffer = await response.arrayBuffer();
    if (progressCallback) {
      const fallbackTotal =
        totalBytes > 0 ? totalBytes : fallbackBuffer.byteLength;
      progressCallback(fallbackBuffer.byteLength, fallbackTotal);
    }
    return fallbackBuffer;
  }

  const reader = response.body.getReader();
  let receivedBytes = 0;
  let output =
    totalBytes > 0 ? new Uint8Array(totalBytes) : null;
  const chunks = output ? null : [];

  while (true) {
    const next = await reader.read();
    if (!next || next.done) {
      break;
    }

    const chunk = next.value;
    if (chunk && chunk.byteLength > 0) {
      if (output) {
        const requiredLength = receivedBytes + chunk.byteLength;
        if (requiredLength > output.length) {
          const grownLength = Math.max(
            requiredLength,
            output.length * 2,
            64 * 1024
          );
          const grownOutput = new Uint8Array(grownLength);
          grownOutput.set(output.subarray(0, receivedBytes), 0);
          output = grownOutput;
        }
        output.set(chunk, receivedBytes);
      } else {
        chunks.push(chunk);
      }
      receivedBytes += chunk.byteLength;
      if (progressCallback) {
        progressCallback(receivedBytes, totalBytes);
      }
    }
  }

  if (!output) {
    output = new Uint8Array(receivedBytes);
    let writeOffset = 0;
    for (let i = 0; i < chunks.length; i += 1) {
      const chunk = chunks[i];
      output.set(chunk, writeOffset);
      writeOffset += chunk.byteLength;
    }
  }

  if (progressCallback) {
    progressCallback(receivedBytes, totalBytes > 0 ? totalBytes : receivedBytes);
  }

  if (receivedBytes === output.length) {
    return output.buffer;
  }

  return output.buffer.slice(0, receivedBytes);
}

export {
  formatByteSize,
  createBinaryBlob,
  createMetadataBlob,
  createColumnarBinaryExportBlobs,
  triggerBlobDownload,
  readFileAsText,
  readFileAsArrayBuffer,
  readUrlAsText,
  readUrlAsArrayBuffer,
};
