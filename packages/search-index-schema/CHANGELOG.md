# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.0] - 2026-06-18

### Added

- `numPartitions` property on the Atlas Vector Search index schemas (including the auto-embedding
  preview variants). Allowed values are `1`, `2`, and `4`, defaulting to `1`.

## [2.0.0] - 2026-06-11

### Added

- `getSchema(indexType, enabledFlags)` selector and a `variants.json` manifest that maps
  feature-flag combinations to schema variants, so consumers can request a schema by enabled flags
  instead of importing variant files by path. The selector imports the bundled schemas statically
  (no runtime filesystem access), so it works in browser/bundler builds.

### Changed

- **BREAKING:** the package now declares an `exports` map that exposes only the package entry point.
  Deep imports of schema files by path (e.g. `@mongodb-js/search-index-schema/schema/...` or
  `.../output/...`) are no longer supported — use `getSchema(indexType, enabledFlags)` instead.

## [1.0.0] - 2025-12-17

### Added

- Initial setup and release of @mongodb-js/search-index-schema package
