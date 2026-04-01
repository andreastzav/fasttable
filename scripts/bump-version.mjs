#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");
const versionJsonRelPath = "version.json";
const corePackageRelPath = path.join("packages", "core", "package.json");
const shouldStage = process.argv.includes("--stage");

function bumpPatchVersion(version) {
  const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(String(version || "").trim());
  if (!match) {
    throw new Error(
      `Invalid version "${version}". Expected strict semver x.y.z`
    );
  }

  const major = Number.parseInt(match[1], 10);
  const minor = Number.parseInt(match[2], 10);
  const patch = Number.parseInt(match[3], 10);
  return `${major}.${minor}.${patch + 1}`;
}

async function readJsonFile(absolutePath) {
  const raw = await fs.readFile(absolutePath, "utf8");
  return JSON.parse(raw);
}

async function writeJsonFile(absolutePath, payload) {
  const serialized = `${JSON.stringify(payload, null, 2)}\n`;
  await fs.writeFile(absolutePath, serialized, "utf8");
}

function stageFiles() {
  execFileSync("git", ["add", versionJsonRelPath, corePackageRelPath], {
    cwd: repoRoot,
    stdio: "inherit",
  });
}

async function main() {
  const versionJsonPath = path.join(repoRoot, versionJsonRelPath);
  const corePackagePath = path.join(repoRoot, corePackageRelPath);

  const versionPayload = await readJsonFile(versionJsonPath);
  const currentVersion =
    versionPayload && typeof versionPayload.version === "string"
      ? versionPayload.version
      : "";
  const nextVersion = bumpPatchVersion(currentVersion);
  versionPayload.version = nextVersion;

  const corePackagePayload = await readJsonFile(corePackagePath);
  corePackagePayload.version = nextVersion;

  await writeJsonFile(versionJsonPath, versionPayload);
  await writeJsonFile(corePackagePath, corePackagePayload);

  if (shouldStage) {
    stageFiles();
  }

  process.stdout.write(`Version bumped: ${currentVersion} -> ${nextVersion}\n`);
}

main().catch((error) => {
  const message = String(error && error.message ? error.message : error);
  process.stderr.write(`Failed to bump version: ${message}\n`);
  process.exitCode = 1;
});
