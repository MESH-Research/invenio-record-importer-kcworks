# Record State Tree for Invenio Record Importer

This document provides a comprehensive analysis of all the different record states that the `invenio-record-importer-kcworks` package is designed to handle. The analysis is based on the `RecordLoader` class and its helper classes (`RecordsHelper`, `FilesHelper`, `CommunitiesHelper`).

## Overview

The record importer handles a complex set of state combinations when importing records into InvenioRDM. This includes managing existing records, file uploads, community integration, and various error conditions that require specific workarounds.

## Record State Tree

### **Root: Import Request**
```
Record Import Request
├── DOI Status
│   ├── No DOI (new record)
│   ├── DOI exists (check for duplicates)
│   │   ├── Single record with matching DOI
│   │   ├── Multiple records with same DOI (corruption case)
│   │   │   ├── Delete extra records
│   │   │   └── Keep one (prefer published over draft)
│   │   └── DOI exists but corrupted (different DOI in draft)
│   │       └── Delete corrupted draft
│   └── DOI registered but no published record (stranded PID)
│       └── Mark PID as "N" and delete
```

### **Record Existence States**
```
Existing Record Check
├── No existing record
│   └── Create new draft
├── Existing record found
│   ├── Metadata comparison
│   │   ├── Metadata identical
│   │   │   ├── Existing draft (unchanged)
│   │   │   └── Existing published (unchanged)
│   │   └── Metadata different
│   │       ├── no_updates flag set
│   │       │   └── Skip update (NoUpdates error)
│   │       └── Update allowed
│   │           ├── Existing draft exists
│   │           │   └── Update existing draft
│   │           └── Only published version exists
│   │               ├── Create new draft
│   │               └── Update new draft
│   └── File handling for existing records
│       ├── Existing record has files
│       │   └── Copy existing files to avoid validation errors
│       └── Rights metadata cleanup
│           └── Strip to IDs only
```

### **File States**
```
File Handling States
├── No files to upload
│   └── Set to metadata-only mode
├── Files to upload
│   ├── New record (no existing files)
│   │   └── Upload files directly
│   └── Existing record with files
│       ├── File comparison
│       │   ├── Files identical
│       │   │   ├── Skip upload
│       │   │   ├── Check published record files
│       │   │   └── Check draft record files
│       │   │       └── Sync if missing
│       │   └── Files different
│       │       ├── Unlock published files (if published)
│       │       ├── Upload new files
│       │       └── Lock published files (if published)
│       └── File upload process
│           ├── Ensure draft exists
│           ├── Initialize file upload
│           ├── Handle file size validation
│           ├── Set file content
│           └── Commit file upload
│           └── Handle failures
│               ├── Retry initialization
│               ├── Delete empty files
│               └── Skip remaining files on failure
```

### **Draft vs Published States**
```
Record Publication States
├── Draft record
│   ├── New draft (no published version)
│   ├── Draft of published record
│   │   ├── Draft exists
│   │   └── Draft doesn't exist (create via edit)
│   └── Unpublished draft
└── Published record
    ├── Published only (no draft)
    ├── Published with draft
    └── Published with new version draft
```

### **Community Integration States**
```
Community Review States
├── Record not in community
│   ├── Unpublished record
│   │   ├── Create community-submission request
│   │   ├── Submit request
│   │   └── Accept request (publishes record)
│   └── Published record
│       ├── Create community-inclusion request
│       ├── Submit request
│       └── Accept request
├── Record already in community
│   ├── Already published to community
│   │   ├── No changes needed
│   │   └── New files uploaded
│   │       ├── Create new draft
│   │       └── Publish new version
│   └── Already included in community
│       └── Skip inclusion
└── Community access restrictions
    ├── Public record → restricted community
    │   └── Set community to public
    └── Restricted record → public community
        └── Handle access restrictions
```

### **Error States and Workarounds**
```
Error Handling States
├── Validation errors
│   ├── Strict validation enabled
│   │   └── Raise DraftValidationError
│   └── Strict validation disabled
│       └── Add to errors list, continue
├── File upload errors
│   ├── File not found
│   ├── File size mismatch
│   ├── Upload initialization failed
│   └── File content upload failed
│       └── Rollback created records
├── Community errors
│   ├── Missing parent metadata (StaleDataError)
│   ├── Invalid access restrictions
│   └── Multiple active collections
├── Record deletion errors
│   ├── Draft deletion failed (PID unregistered)
│   ├── Published record deletion failed
│   └── Review request exists (must delete first)
└── Recovery mechanisms
    ├── Retry on StaleDataError
    ├── Delete corrupted drafts
    ├── Clean up stranded PIDs
    └── Rollback on all_or_none flag
```

### **Special State Combinations**
```
Complex State Scenarios
├── Published record with new files
│   ├── Create new draft
│   ├── Upload files to draft
│   └── Publish new version
├── Draft with different metadata and files
│   ├── Update metadata
│   ├── Handle file changes
│   └── Maintain draft state
├── Record with timestamp override
│   ├── Override created timestamp
│   └── Update stats-community-events index
├── Record with ownership changes
│   ├── Find/create users
│   ├── Assign ownership
│   └── Add to collection membership
└── Record with stats events
    ├── Create fictional usage events
    ├── Handle views/downloads counts
    └── Set publication date
```

## Key Problematic States Requiring Workarounds

The following states represent particularly complex scenarios that require specific workarounds in the code:

### 1. **Duplicate DOI Records**
- **Problem**: Multiple records with the same DOI (indicates corruption)
- **Workaround**: Delete extra records, keep one (preferring published over draft)
- **Code Location**: `RecordsHelper.create_invenio_record()` lines 557-597

### 2. **Corrupted Drafts**
- **Problem**: Drafts with different DOIs than expected
- **Workaround**: Delete corrupted drafts
- **Code Location**: `RecordsHelper.create_invenio_record()` lines 541-555

### 3. **Stranded PIDs**
- **Problem**: Registered DOIs without published records
- **Workaround**: Mark PID as "N" and delete
- **Code Location**: `RecordsHelper.create_invenio_record()` lines 514-526

### 4. **File Manager Sync Issues**
- **Problem**: Files in metadata don't match file manager state
- **Workaround**: Sync file manager with metadata, copy existing files to avoid validation errors
- **Code Location**: `FilesHelper.handle_record_files()` lines 367-407

### 5. **Published File Locks**
- **Problem**: Need to unlock/lock files for updates to published records
- **Workaround**: Unlock files before upload, lock after upload
- **Code Location**: `FilesHelper._unlock_files()` and `FilesHelper._lock_files()`

### 6. **Community Access Conflicts**
- **Problem**: Public records can't join restricted communities
- **Workaround**: Set community to public before inclusion
- **Code Location**: `CommunitiesHelper.publish_record_to_community()` lines 655-667

### 7. **StaleDataError**
- **Problem**: Concurrent modifications during community operations
- **Workaround**: Retry operation (handled in main load loop)
- **Code Location**: `RecordLoader.load_all()` lines 1260-1270

### 8. **Review Request Conflicts**
- **Problem**: Can't delete drafts with existing review requests
- **Workaround**: Delete review requests before deleting drafts
- **Code Location**: `RecordsHelper.delete_invenio_record()` lines 854-890

### 9. **File Upload Failures**
- **Problem**: Empty files, size mismatches, initialization failures
- **Workaround**: Retry initialization, delete empty files, skip remaining files on failure
- **Code Location**: `FilesHelper._upload_draft_files()` and `FilesHelper._retry_file_initialization()`

### 10. **Metadata Validation Edge Cases**
- **Problem**: Rights metadata format, missing fields
- **Workaround**: Strip rights metadata to IDs only, handle validation errors gracefully
- **Code Location**: `RecordsHelper.create_invenio_record()` lines 675-679

## Record Status Values

The system uses the following status values to track the state of record operations:

- `new_record`: Brand new record created
- `updated_draft`: Existing draft updated with new metadata
- `updated_published`: Published record updated (new draft created and published)
- `unchanged_existing_draft`: Existing draft with no changes needed
- `unchanged_existing_published`: Existing published record with no changes needed

## Community Review Status Values

- `already_published`: Record already published to community
- `already_included`: Record already included in community
- `accepted`: Review request accepted (record published/included)

## File Upload Status Values

- `uploaded`: File successfully uploaded
- `already_uploaded`: File already exists and is identical
- `failed`: File upload failed
- `skipped`: File upload skipped due to prior failure

## Configuration Flags

The system uses several flags to control behavior:

- `no_updates`: Skip updating existing records
- `strict_validation`: Raise errors on validation issues vs. continuing
- `all_or_none`: Rollback all changes if any record fails
- `stop_on_error`: Stop processing on first error
- `retry_failed`: Retry previously failed records
- `use_sourceids`: Use source system IDs instead of file indices

## Error Recovery Strategies

1. **Rollback Strategy**: When `all_or_none` is true, delete all successfully created records if any fail
2. **Retry Strategy**: Retry operations that fail due to `StaleDataError`
3. **Cleanup Strategy**: Delete corrupted records, stranded PIDs, and empty files
4. **Skip Strategy**: Skip problematic records and continue with others
5. **Fallback Strategy**: Change community visibility to resolve access conflicts

## Conclusion

The record importer handles a complex state space with many edge cases and error conditions. The extensive workarounds and error handling suggest that the underlying InvenioRDM system has various state inconsistencies and race conditions that the importer must work around. Understanding these states is crucial for debugging import issues and extending the importer's functionality.
