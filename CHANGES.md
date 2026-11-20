# Changelog

## 1.0.2 (2026-11-21)

- **Error handling:** add proper handling for errors raised when the import
  record's identifiers belong to a previously deleted record

## 1.0.1 (2026-11-20)

- **Bug fix:** importer was trying to sanitize files on the local file system
  for any metadata-only import. We've now cleared up the confusion so that this
  only runs for true local-files imports.

**Restarting version numbering after a long period of neglect**

## 0.4.0 (2025-10-05)

- Added service methods and a CLI to updated created dates for legacy CORE
  records and communities based on their original creation dates.
- Updated testing infrastructure.
- Updated dependencies and submodule structure.

## 0.3.0-alpha1

- Initial release of the streamlined importer API.
