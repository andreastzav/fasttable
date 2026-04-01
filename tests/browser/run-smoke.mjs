import http from "node:http";
import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..", "..");

function contentTypeForPath(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === ".html") return "text/html; charset=utf-8";
  if (ext === ".js" || ext === ".mjs") return "text/javascript; charset=utf-8";
  if (ext === ".css") return "text/css; charset=utf-8";
  if (ext === ".json") return "application/json; charset=utf-8";
  if (ext === ".bin") return "application/octet-stream";
  if (ext === ".txt") return "text/plain; charset=utf-8";
  return "application/octet-stream";
}

function sanitizeUrlPath(urlPath) {
  const decoded = decodeURIComponent(urlPath || "/");
  const normalized = path.normalize(decoded).replace(/^(\.\.[/\\])+/, "");
  return normalized.startsWith(path.sep)
    ? normalized.slice(1)
    : normalized;
}

function createStaticServer(rootDir) {
  return http.createServer(async (req, res) => {
    try {
      const requestUrl = new URL(req.url || "/", "http://127.0.0.1");
      let relativePath = sanitizeUrlPath(requestUrl.pathname);
      if (relativePath === "") {
        relativePath = "index.html";
      }

      let resolvedPath = path.resolve(rootDir, relativePath);
      if (!resolvedPath.startsWith(path.resolve(rootDir))) {
        res.writeHead(403, { "content-type": "text/plain; charset=utf-8" });
        res.end("Forbidden");
        return;
      }

      let stats = null;
      try {
        stats = await fs.stat(resolvedPath);
      } catch (_error) {
        stats = null;
      }

      if (stats && stats.isDirectory()) {
        resolvedPath = path.join(resolvedPath, "index.html");
        try {
          stats = await fs.stat(resolvedPath);
        } catch (_error) {
          stats = null;
        }
      }

      if (!stats || !stats.isFile()) {
        res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
        res.end("Not found");
        return;
      }

      const body = await fs.readFile(resolvedPath);
      res.writeHead(200, {
        "content-type": contentTypeForPath(resolvedPath),
        "cache-control": "no-store",
      });
      res.end(body);
    } catch (error) {
      res.writeHead(500, { "content-type": "text/plain; charset=utf-8" });
      res.end(
        `Server error: ${String(error && error.message ? error.message : error)}`
      );
    }
  });
}

async function startServer(server, port) {
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", resolve);
  });
}

async function main() {
  let chromium = null;
  try {
    ({ chromium } = await import("playwright"));
  } catch (error) {
    throw new Error(
      `Playwright is not installed. ${String(
        error && error.message ? error.message : error
      )}`
    );
  }

  const port = Number.parseInt(process.env.FASTTABLE_SMOKE_PORT || "", 10) || 4173;
  const server = createStaticServer(repoRoot);
  await startServer(server, port);

  let browser = null;
  try {
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();
    const smokeUrl = `http://127.0.0.1:${port}/tests/browser/smoke.html`;
    await page.goto(smokeUrl, { waitUntil: "load", timeout: 120000 });

    await page.waitForFunction(
      () =>
        typeof window.fastTableBrowserSmokeResult === "object" &&
        window.fastTableBrowserSmokeResult !== null &&
        typeof window.fastTableBrowserSmokeResult.ok === "boolean",
      { timeout: 120000 }
    );

    const result = await page.evaluate(() => window.fastTableBrowserSmokeResult);
    if (!result || result.ok !== true) {
      throw new Error(
        result && result.error
          ? String(result.error)
          : "Browser smoke returned unknown failure."
      );
    }

    console.log("Browser smoke passed.");
  } finally {
    if (browser) {
      await browser.close();
    }
    await new Promise((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  }
}

main().catch((error) => {
  console.error(
    `Browser smoke failed: ${String(error && error.message ? error.message : error)}`
  );
  process.exitCode = 1;
});
