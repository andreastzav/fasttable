import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const coreDir = path.join(__dirname, "packages", "core");
const srcDir = path.join(coreDir, "src");
const distDir = path.join(coreDir, "dist");

async function copyDirectoryRecursive(sourceDir, targetDir) {
  await fs.mkdir(targetDir, { recursive: true });
  const entries = await fs.readdir(sourceDir, { withFileTypes: true });
  let copiedFileCount = 0;

  for (const entry of entries) {
    const sourcePath = path.join(sourceDir, entry.name);
    const targetPath = path.join(targetDir, entry.name);

    if (entry.isDirectory()) {
      copiedFileCount += await copyDirectoryRecursive(sourcePath, targetPath);
      continue;
    }

    if (entry.isFile()) {
      await fs.copyFile(sourcePath, targetPath);
      copiedFileCount += 1;
    }
  }

  return copiedFileCount;
}

async function main() {
  await fs.rm(distDir, { recursive: true, force: true });
  const copiedFileCount = await copyDirectoryRecursive(srcDir, distDir);

  const relDistDir = path.relative(__dirname, distDir) || ".";
  console.log(
    `Core build complete: copied ${copiedFileCount} file(s) to ${relDistDir}.`
  );
}

main().catch((error) => {
  console.error(
    `Core build failed: ${String(error && error.message ? error.message : error)}`
  );
  process.exitCode = 1;
});
