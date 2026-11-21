# Changelog

## 1.1.0 (2026-11-21)

- **Error reporting**
  - Normalize validation errors from all origins to a consistent format: lists
    of {field, messages} with dot-separated field paths for readability.
  - Catch ValidationErrorWithMessageAsList raised by PID failures on publication
    _after_ schema validation passes. (This was causing 502 errors that
    short-circuited the proper result reporting.)
- **Safer record cleanup**
  - Avoid deleting records or files that weren't created during the current
    import session. (Pre-existing drafts, but especially pre-existing published
    records.)
  - Document limitations to rollback of earlier successfully imported records
    after a subsequent failure with the `all_or_none` flag.
- **Better partial-success handling**
  - Correctly return HTTP 207 response after a partial success (with
    `all_or_none=False`);
- **Pre-existing reviews**
  - Properly cancel pre-existing reviews before trying to delete them.
  - Centralize review cancellation logic in a new ReviewsHelper
    (`services/reviews.py`)
- **Other refactoring**
  - Encapsulate DOI/source-id deleted-record paths;
- **Other bug fixes**
  - Fix key error caused by empty DOI field.
- **New/updated tests**
  - Mixed publish-validation REST cases (all_or_none true/false)
  - Empty-DOI / metadata error typing fixes
  - Soft-deleted re-import service test (new)
  - Records-helper coverage additions
  - conftest / uv.lock tweaks
  - tests/fixtures pointer: a18ce09 → f027b59 (source of the 3 ruff fails)

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
