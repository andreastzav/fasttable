# Releasing @fasttable/core

This guide describes the release flow for `packages/core`.

## 1. Prepare changes

- Update code in `packages/core/src`
- Run build:

```bash
node build-core.mjs
```

- Run tests:

```bash
node tests/node/parity.test.mjs
node tests/node/io-roundtrip.test.mjs
node tests/node/benchmark-smoke.test.mjs
```

Optional browser smoke:

```bash
node tests/browser/run-smoke.mjs
```

## 2. Update metadata

- Bump `packages/core/package.json` version
- Add an entry in `packages/core/CHANGELOG.md`
- Ensure `docs/API.md` reflects any API changes

## 3. Validate package contents

From `packages/core`:

```bash
npm pack --dry-run
```

Confirm expected files are included:

- `dist/*`
- `package.json`
- `README.md`
- `CHANGELOG.md`

## 4. Commit and tag

- Commit release changes
- Create a git tag for the package version (for example `core-v0.1.1`)

## 5. Publish (when ready)

From `packages/core`:

```bash
npm publish --access public
```

If this repository uses another registry, publish using the configured registry workflow.
