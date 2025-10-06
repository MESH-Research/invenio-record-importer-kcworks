# Implementation Plan: Update Created Dates for Migrated Records and Communities

## Overview

This document outlines the implementation plan for updating the "created" dates for records and communities that were migrated from a previous repository system. The migration resulted in records and communities having "created" dates that reflect when they were imported into the new system, rather than their original creation dates.

## Problem Statement

### Records Issue
- Records migrated from the legacy CORE repository have a "created" date that reflects the migration date, not the original creation date
- The original creation date is stored in `custom_fields.hclegacy:record_creation_date`
- We need to update the record's `created` field to match the original creation date when available

### Communities Issue
- Communities (group collections) also have "created" dates that reflect when they were created in the new system
- Communities don't have their own original creation date field
- We need to infer the community's original creation date from the oldest record that belongs to that community
- Communities are identified by `custom_fields.kcr:commons_group_id`
- Records link to communities via `custom_fields.hclegacy:groups_for_deposit[].group_identifier`

## Solution Architecture

### Component Organization

1. **Celery Tasks** (`tasks.py`)
   - Background task for updating record created dates
   - Background task for updating community created dates
   - Allows long-running operations without blocking

2. **RecordsHelper Methods** (`services/records.py`)
   - Core logic for finding and updating record created dates
   - Query methods for finding records needing updates
   - Update methods using unit of work pattern

3. **CommunitiesHelper Methods** (`services/communities.py`)
   - Core logic for finding and updating community created dates
   - Methods to find oldest record for each community
   - Update methods using unit of work pattern

4. **CLI Command** (`cli.py`)
   - User-facing command to trigger updates
   - Options for filtering, batch size, dry-run, background execution
   - Progress reporting and statistics

### Execution Order

**Critical**: Records must be updated before communities to ensure accurate data.

1. Update all record created dates first
2. Then update community created dates based on the updated record dates

## Detailed Implementation

### 1. Celery Tasks (`tasks.py`)

#### Task 1: `update_record_created_dates_task`

```python
@shared_task(ignore_result=False, bind=True)
def update_record_created_dates_task(
    self,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    batch_size: int = 100,
    dry_run: bool = False,
    verbose: bool = False
) -> dict:
    """
    Celery task to update record created dates in background.

    Args:
        start_date: ISO format date string (YYYY-MM-DD) - only process records
                   created after this date
        end_date: ISO format date string (YYYY-MM-DD) - only process records
                 created before this date
        batch_size: Number of records to process in each batch
        dry_run: If True, show what would be updated without making changes
        verbose: If True, log detailed progress information

    Returns:
        dict: Statistics about the operation
            {
                'total_found': int,
                'updated': int,
                'skipped': int,
                'errors': list[dict]
            }
    """
```

**Implementation Details:**
- Uses `bind=True` to access task instance for progress updates
- Calls `RecordsHelper().update_record_created_dates()`
- Updates task state periodically: `self.update_state(state='PROGRESS', meta={...})`
- Returns comprehensive statistics

#### Task 2: `update_community_created_dates_task`

```python
@shared_task(ignore_result=False, bind=True)
def update_community_created_dates_task(
    self,
    batch_size: int = 100,
    dry_run: bool = False,
    verbose: bool = False
) -> dict:
    """
    Celery task to update community created dates in background.

    Args:
        batch_size: Number of communities to process in each batch
        dry_run: If True, show what would be updated without making changes
        verbose: If True, log detailed progress information

    Returns:
        dict: Statistics about the operation
            {
                'total_found': int,
                'updated': int,
                'skipped': int,
                'no_records': int,
                'errors': list[dict]
            }
    """
```

**Implementation Details:**
- Uses `bind=True` to access task instance for progress updates
- Calls `CommunitiesHelper().update_community_created_dates()`
- Updates task state periodically
- Returns comprehensive statistics

### 2. RecordsHelper Methods (`services/records.py`)

#### Method 1: `find_records_needing_created_date_update`

```python
def find_records_needing_created_date_update(
    self,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> list[dict]:
    """
    Find all records that have hclegacy:record_creation_date and need updating.

    Queries the search index for records with the legacy creation date field,
    then compares with the current created date to determine which need updating.

    Args:
        start_date: ISO format date string - only include records created after this
        end_date: ISO format date string - only include records created before this

    Returns:
        list[dict]: List of records needing update, each dict contains:
            {
                'id': str,  # record UUID
                'current_created': str,  # current created timestamp
                'new_created': str,  # hclegacy:record_creation_date value
                'pid': str  # record PID for logging
            }
    """
```

**Implementation Details:**
- Use OpenSearch `Search` API with `current_search_client`
- Query: `search.filter("exists", field="custom_fields.hclegacy:record_creation_date")`
- Apply date range filters if provided
- Use `search.scan()` to iterate through all results efficiently
- Compare dates and only return records where dates differ
- Handle index prefix from config: `app.config.get("SEARCH_INDEX_PREFIX", "")`

**OpenSearch Query Structure:**
```python
from opensearchpy.helpers.search import Search
from invenio_search.proxies import current_search_client

prefix = app.config.get("SEARCH_INDEX_PREFIX", "")
index = f"{prefix}rdmrecords" if prefix else "rdmrecords"

search = Search(
    using=current_search_client,
    index=index
)
search = search.filter("exists", field="custom_fields.hclegacy:record_creation_date")

if start_date:
    search = search.filter("range", created={"gte": start_date})
if end_date:
    search = search.filter("range", created={"lte": end_date})

# Iterate through all results
for hit in search.scan():
    # Process each hit
```

#### Method 2: `update_single_record_created_date`

```python
@unit_of_work()
def update_single_record_created_date(
    self,
    record_id: str,
    new_created_date: str,
    uow: Optional[UnitOfWork] = None
) -> bool:
    """
    Update the created date for a single record.

    Uses the Invenio unit of work pattern to ensure the database change
    is committed and the search index is automatically updated.

    Args:
        record_id: UUID of the record to update
        new_created_date: New created timestamp in ISO format with timezone
        uow: Unit of work instance (injected by decorator)

    Returns:
        bool: True if updated, False if skipped (dates already match)

    Raises:
        ValueError: If timestamp format is invalid
    """
```

**Implementation Details:**
- Validate timestamp using existing `RecordsHelper._validate_timestamp()` method
- Read record using `records_service.read(system_identity, id_=record_id)._record`
- Check if update is needed: `if record.model.created != new_created_date`
- Update: `record.model.created = new_created_date`
- Register with UoW: `uow.register(RecordCommitOp(record))`
- The `@unit_of_work()` decorator ensures:
  - Database transaction is committed
  - Search index is updated automatically
  - Changes are atomic

**Date Validation:**
- Reuse existing `_validate_timestamp()` method
- Accepts formats:
  - `YYYY-MM-DDTHH:mm:ssZ` (ISO 8601 with Z)
  - `YYYY-MM-DDTHH:mm:ss.SSSSSS+00:00` (with microseconds and timezone)
- Must be UTC timezone

#### Method 3: `update_record_created_dates`

```python
def update_record_created_dates(
    self,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    batch_size: int = 100,
    dry_run: bool = False,
    verbose: bool = False
) -> dict:
    """
    Update created dates for all records with hclegacy:record_creation_date.

    This is the main orchestration method that:
    1. Finds all records needing updates
    2. Processes them in batches
    3. Updates each record's created date
    4. Performs health checks between batches
    5. Tracks statistics and errors

    Args:
        start_date: ISO format date - only process records created after this
        end_date: ISO format date - only process records created before this
        batch_size: Number of records to process before committing
        dry_run: If True, log what would be done without making changes
        verbose: If True, log detailed progress

    Returns:
        dict: Statistics about the operation
            {
                'total_found': int,
                'updated': int,
                'skipped': int,
                'errors': list[dict],
                'stopped_early': bool (optional - True if stopped due to health check),
                'stopped_at_record': int (optional - number of records processed before stopping)
            }
    """
```

**Implementation Details:**
- Call `find_records_needing_created_date_update()` to get list
- Process in batches to avoid memory issues
- For each batch:
  - Process all records in the batch
  - After batch completion, perform OpenSearch health check
  - If unhealthy: pause 30 seconds, retry health check
  - If still unhealthy: stop processing and return stats
  - Add 1 second delay between batches
- For each record:
  - Log if verbose
  - Skip if dry_run (but count)
  - Call `update_single_record_created_date()`
  - Catch and log errors, continue processing
- Track statistics:
  - `total_found`: Total records with hclegacy:record_creation_date
  - `updated`: Successfully updated
  - `skipped`: Already had correct date
  - `errors`: List of `{'record_id': str, 'error': str}` dicts
  - `stopped_early`: True if stopped due to health check failure
  - `stopped_at_record`: Number of records processed before stopping
- Log progress every batch

**Error Handling:**
```python
try:
    updated = self.update_single_record_created_date(record_id, new_date)
    if updated:
        stats['updated'] += 1
    else:
        stats['skipped'] += 1
except Exception as e:
    app.logger.error(f"Error updating record {record_id}: {str(e)}")
    stats['errors'].append({
        'record_id': record_id,
        'error': str(e)
    })
```

### 3. CommunitiesHelper Methods (`services/communities.py`)

#### Method 1: `find_communities_needing_created_date_update`

```python
def find_communities_needing_created_date_update(self) -> list[dict]:
    """
    Find all communities with kcr:commons_group_id that may need updating.

    Queries the search index for all communities that represent migrated
    group collections (identified by having a commons_group_id).

    Returns:
        list[dict]: List of communities to check, each dict contains:
            {
                'id': str,  # community UUID
                'slug': str,  # community slug for logging
                'group_id': str,  # kcr:commons_group_id value
                'current_created': str  # current created timestamp
            }
    """
```

**Implementation Details:**
- Query communities index for those with `custom_fields.kcr:commons_group_id`
- Use OpenSearch `Search` API
- Return all matching communities with their metadata

**OpenSearch Query:**
```python
from opensearchpy.helpers.search import Search
from invenio_search.proxies import current_search_client

prefix = app.config.get("SEARCH_INDEX_PREFIX", "")
index = f"{prefix}communities" if prefix else "communities"

search = Search(
    using=current_search_client,
    index=index
)
search = search.filter("exists", field="custom_fields.kcr:commons_group_id")

for hit in search.scan():
    # Process each community
```

#### Method 2: `find_oldest_record_for_community`

```python
def find_oldest_record_for_community(self, group_id: str) -> Optional[arrow.Arrow]:
    """
    Find the oldest record creation date for records in a community.

    Searches for all records that have this group_id in their
    hclegacy:groups_for_deposit array, filters for those with
    hclegacy:record_creation_date, and returns the oldest date.

    Args:
        group_id: The kcr:commons_group_id to search for

    Returns:
        arrow.Arrow: The oldest record creation date (floored to start of day),
                    or None if no records found
    """
```

**Implementation Details:**
- Query records index for matching group_id
- Array field query: `custom_fields.hclegacy:groups_for_deposit.group_identifier`
- Filter for records with `hclegacy:record_creation_date`
- Sort by `hclegacy:record_creation_date` ascending
- Take first result (oldest)
- Return date floored to start of day using arrow: `arrow.get(date).floor('day')`

**OpenSearch Query:**
```python
prefix = app.config.get("SEARCH_INDEX_PREFIX", "")
index = f"{prefix}rdmrecords" if prefix else "rdmrecords"

search = Search(
    using=current_search_client,
    index=index
)

# Query for records with this group_id in the array field
search = search.filter(
    "term",
    **{"custom_fields.hclegacy:groups_for_deposit.group_identifier": group_id}
)

# Must also have the creation date field
search = search.filter(
    "exists",
    field="custom_fields.hclegacy:record_creation_date"
)

# Sort by creation date ascending to get oldest first
search = search.sort(
    {"custom_fields.hclegacy:record_creation_date": {"order": "asc"}}
)

# Only need the first result
search = search[:1]

results = search.execute()
if results.hits.total.value > 0:
    oldest_hit = results.hits[0]
    creation_date = oldest_hit.custom_fields.get("hclegacy:record_creation_date")
    # Return start of day
    return arrow.get(creation_date).floor('day')
return None
```

#### Method 3: `update_single_community_created_date`

```python
@unit_of_work()
def update_single_community_created_date(
    self,
    community_id: str,
    new_created_date: arrow.Arrow,
    uow: Optional[UnitOfWork] = None
) -> bool:
    """
    Update the created date for a single community.

    Uses the Invenio unit of work pattern to ensure the database change
    is committed and the search index is automatically updated.

    Args:
        community_id: UUID of the community to update
        new_created_date: New created date as arrow.Arrow object
        uow: Unit of work instance (injected by decorator)

    Returns:
        bool: True if updated, False if skipped (date already earlier)
    """
```

**Implementation Details:**
- Read community: `current_communities.service.read(system_identity, id_=community_id)._record`
- Convert arrow date to datetime: `new_created_date.datetime`
- Check if update is needed: only update if new date is earlier than current
- Update: `community.model.created = new_created_date.datetime`
- Register with UoW: `uow.register(RecordCommitOp(community))`
- The `@unit_of_work()` decorator ensures atomic updates

**Update Logic:**
```python
current_created = arrow.get(community.model.created)
if new_created_date < current_created:
    community.model.created = new_created_date.datetime
    uow.register(RecordCommitOp(community))
    return True
return False
```

#### Method 4: `update_community_created_dates`

```python
def update_community_created_dates(
    self,
    batch_size: int = 100,
    dry_run: bool = False,
    verbose: bool = False
) -> dict:
    """
    Update created dates for communities based on their oldest record.

    This is the main orchestration method that:
    1. Finds all communities with kcr:commons_group_id
    2. For each community, finds the oldest record
    3. Updates the community's created date to start of that day
    4. Performs health checks between batches
    5. Tracks statistics and errors

    Args:
        batch_size: Number of communities to process before health check
        dry_run: If True, log what would be done without making changes
        verbose: If True, log detailed progress

    Returns:
        dict: Statistics about the operation
            {
                'total_found': int,
                'updated': int,
                'skipped': int,
                'no_records': int,
                'errors': list[dict],
                'stopped_early': bool (optional - True if stopped due to health check),
                'stopped_at_community': int (optional - number processed before stopping)
            }
    """
```

**Implementation Details:**
- Call `find_communities_needing_created_date_update()` to get list
- Process in batches
- For each batch:
  - Process all communities in the batch
  - After batch completion, perform OpenSearch health check
  - If unhealthy: pause 30 seconds, retry health check
  - If still unhealthy: stop processing and return stats
  - Add 1 second delay between batches
- For each community:
  - Call `find_oldest_record_for_community(group_id)`
  - If no records found, skip and count in `no_records`
  - If oldest date is earlier than current, update
  - Log if verbose
  - Skip actual update if dry_run (but count)
  - Catch and log errors, continue processing
- Track statistics:
  - `total_found`: Total communities with kcr:commons_group_id
  - `updated`: Successfully updated
  - `skipped`: Already had earlier date
  - `no_records`: No records found for this community
  - `errors`: List of `{'community_id': str, 'slug': str, 'error': str}` dicts
  - `stopped_early`: True if stopped due to health check failure
  - `stopped_at_community`: Number of communities processed before stopping

**Processing Logic:**
```python
for community in communities:
    try:
        oldest_date = self.find_oldest_record_for_community(community['group_id'])

        if oldest_date is None:
            stats['no_records'] += 1
            if verbose:
                app.logger.info(f"No records found for community {community['slug']}")
            continue

        if dry_run:
            current = arrow.get(community['current_created'])
            if oldest_date < current:
                stats['updated'] += 1
                app.logger.info(
                    f"[DRY RUN] Would update {community['slug']} "
                    f"from {current} to {oldest_date}"
                )
            else:
                stats['skipped'] += 1
        else:
            updated = self.update_single_community_created_date(
                community['id'],
                oldest_date
            )
            if updated:
                stats['updated'] += 1
            else:
                stats['skipped'] += 1

    except Exception as e:
        app.logger.error(f"Error updating community {community['id']}: {str(e)}")
        stats['errors'].append({
            'community_id': community['id'],
            'slug': community['slug'],
            'error': str(e)
        })
```

### 4. CLI Command (`cli.py`)

#### Command: `update_created_dates`

```python
@cli.command(name="update_created_dates")
@click.option(
    "--records-only",
    is_flag=True,
    default=False,
    help="Only update record created dates, not communities"
)
@click.option(
    "--communities-only",
    is_flag=True,
    default=False,
    help="Only update community created dates, not records"
)
@click.option(
    "--start-date",
    default=None,
    help=(
        "Only update records created after this date (YYYY-MM-DD format). "
        "Only applies to records, not communities."
    )
)
@click.option(
    "--end-date",
    default=None,
    help=(
        "Only update records created before this date (YYYY-MM-DD format). "
        "Only applies to records, not communities."
    )
)
@click.option(
    "--batch-size",
    default=100,
    type=int,
    help="Number of records/communities to process in each batch"
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show what would be updated without making changes"
)
@click.option(
    "--background",
    is_flag=True,
    default=False,
    help="Run the update as a background Celery task"
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    help="Print detailed progress information"
)
@with_appcontext
def update_created_dates(
    records_only: bool,
    communities_only: bool,
    start_date: Optional[str],
    end_date: Optional[str],
    batch_size: int,
    dry_run: bool,
    background: bool,
    verbose: bool
):
    """
    Update created dates for migrated records and communities.

    This command updates the 'created' timestamp for:
    1. Records with custom_fields.hclegacy:record_creation_date
    2. Communities based on their oldest record's creation date

    Records are always updated before communities to ensure accurate data.

    Examples:

        Update all records and communities:

            invenio importer update_created_dates

        Update only records created in 2020:

            invenio importer update_created_dates --records-only \\
                --start-date 2020-01-01 --end-date 2020-12-31

        Update only communities:

            invenio importer update_created_dates --communities-only

        Run in background as Celery task:

            invenio importer update_created_dates --background

        Dry run to see what would be updated:

            invenio importer update_created_dates --dry-run --verbose
    """
```

**Implementation Details:**

##### Validation
```python
# Validate mutually exclusive options
if records_only and communities_only:
    click.echo("Error: Cannot use both --records-only and --communities-only")
    return

# Validate date formats
if start_date:
    try:
        arrow.get(start_date)
    except arrow.parser.ParserError:
        click.echo(f"Error: Invalid start-date format: {start_date}")
        return

if end_date:
    try:
        arrow.get(end_date)
    except arrow.parser.ParserError:
        click.echo(f"Error: Invalid end-date format: {end_date}")
        return
```

##### Background Execution
```python
if background:
    click.echo("Starting background tasks...")

    tasks = []

    if not communities_only:
        click.echo("Queuing record update task...")
        task = update_record_created_dates_task.delay(
            start_date=start_date,
            end_date=end_date,
            batch_size=batch_size,
            dry_run=dry_run,
            verbose=verbose
        )
        tasks.append(('records', task))
        click.echo(f"Record update task queued: {task.id}")

    if not records_only:
        click.echo("Queuing community update task...")
        # If updating both, chain the tasks so communities run after records
        if not communities_only:
            # Wait for record task to complete first
            task = update_community_created_dates_task.apply_async(
                kwargs={
                    'batch_size': batch_size,
                    'dry_run': dry_run,
                    'verbose': verbose
                },
                link=tasks[0][1]  # Chain after record task
            )
        else:
            task = update_community_created_dates_task.delay(
                batch_size=batch_size,
                dry_run=dry_run,
                verbose=verbose
            )
        tasks.append(('communities', task))
        click.echo(f"Community update task queued: {task.id}")

    click.echo("\nTasks queued successfully!")
    click.echo("Monitor progress with: celery -A invenio_app.celery inspect active")
    for name, task in tasks:
        click.echo(f"  {name}: {task.id}")

    return
```

##### Foreground Execution
```python
# Run in foreground
if not communities_only:
    click.echo("=" * 70)
    click.echo("Updating record created dates...")
    click.echo("=" * 70)

    records_helper = RecordsHelper()
    records_stats = records_helper.update_record_created_dates(
        start_date=start_date,
        end_date=end_date,
        batch_size=batch_size,
        dry_run=dry_run,
        verbose=verbose
    )

    click.echo("\nRecord Update Results:")
    click.echo(f"  Total found: {records_stats['total_found']}")
    click.echo(f"  Updated: {records_stats['updated']}")
    click.echo(f"  Skipped: {records_stats['skipped']}")
    click.echo(f"  Errors: {len(records_stats['errors'])}")

    if records_stats['errors']:
        click.echo("\nErrors:")
        for error in records_stats['errors'][:10]:  # Show first 10
            click.echo(f"  - {error['record_id']}: {error['error']}")
        if len(records_stats['errors']) > 10:
            click.echo(f"  ... and {len(records_stats['errors']) - 10} more")

if not records_only:
    click.echo("\n" + "=" * 70)
    click.echo("Updating community created dates...")
    click.echo("=" * 70)

    communities_helper = CommunitiesHelper()
    communities_stats = communities_helper.update_community_created_dates(
        batch_size=batch_size,
        dry_run=dry_run,
        verbose=verbose
    )

    click.echo("\nCommunity Update Results:")
    click.echo(f"  Total found: {communities_stats['total_found']}")
    click.echo(f"  Updated: {communities_stats['updated']}")
    click.echo(f"  Skipped: {communities_stats['skipped']}")
    click.echo(f"  No records: {communities_stats['no_records']}")
    click.echo(f"  Errors: {len(communities_stats['errors'])}")

    if communities_stats['errors']:
        click.echo("\nErrors:")
        for error in communities_stats['errors'][:10]:
            click.echo(f"  - {error['slug']}: {error['error']}")
        if len(communities_stats['errors']) > 10:
            click.echo(f"  ... and {len(communities_stats['errors']) - 10} more")

click.echo("\n" + "=" * 70)
click.echo("Update complete!")
click.echo("=" * 70)

# Alert sound
print("\a")
```

## Python Version and Type Hints

**Python Version**: 3.12

**Type Hinting Convention**: Use modern Python 3.12 type hints:
- Use built-in types: `dict`, `list`, `tuple`, `set` instead of `Dict`, `List`, `Tuple`, `Set`
- Use `|` for union types instead of `Optional` or `Union`
- Example: `str | None` instead of `Optional[str]`
- Example: `list[dict]` instead of `List[Dict]`
- Example: `dict[str, str | int]` instead of `Dict[str, Union[str, int]]`

## Import Requirements

### `tasks.py`
```python
import time
from celery import shared_task
from flask import current_app as app
from flask import render_template
from flask_mail import Message
from invenio_search.proxies import current_search_client
from invenio_stats.proxies import current_stats
from opensearchpy.exceptions import ConnectionTimeout, ConnectionError
from typing import Optional
```

### `services/records.py`
Add to existing imports:
```python
import arrow
import time
from opensearchpy.exceptions import ConnectionTimeout, ConnectionError
from opensearchpy.helpers.search import Search
```

### `services/communities.py`
Add to existing imports:
```python
import arrow
import time
from opensearchpy.exceptions import ConnectionTimeout, ConnectionError
from opensearchpy.helpers.search import Search
from invenio_records_resources.services.uow import RecordCommitOp
```

### `cli.py`
Add to existing imports:
```python
import arrow
from invenio_record_importer_kcworks.tasks import (
    update_record_created_dates_task,
    update_community_created_dates_task,
)
```

## Unit of Work Pattern

The Invenio unit of work pattern is crucial for ensuring data consistency:

### How It Works

1. **Decorator**: `@unit_of_work()` on method
2. **Parameter**: Method receives `uow: Optional[UnitOfWork] = None`
3. **Registration**: Changes registered with `uow.register(RecordCommitOp(record))`
4. **Automatic Handling**:
   - Database transaction committed
   - Search index updated
   - Changes are atomic (all or nothing)

### Example

```python
@unit_of_work()
def update_single_record_created_date(
    self,
    record_id: str,
    new_created_date: str,
    uow: Optional[UnitOfWork] = None
) -> bool:
    record = records_service.read(system_identity, id_=record_id)._record

    if record.model.created != new_created_date:
        record.model.created = new_created_date
        uow.register(RecordCommitOp(record))  # Register the change
        return True
    return False
    # When method returns, decorator commits transaction and updates index
```

## OpenSearch Health Checks

To prevent overwhelming the OpenSearch cluster during batch processing, we implement health checks between batches. This ensures the system remains responsive and prevents failures due to resource exhaustion.

### Health Check Implementation

```python
def check_opensearch_health(self) -> dict:
    """
    Check OpenSearch cluster health.

    Returns:
        dict: Health check result with keys:
            - is_healthy (bool): Whether the cluster is healthy
            - reason (str): Explanation of health status
            - status (str): Cluster status (green/yellow/red) if available
    """
    try:
        health = current_search_client.cluster.health(timeout=5)
        status = health.get('status', 'unknown')

        if status == 'red':
            return {
                'is_healthy': False,
                'reason': f'Cluster status is RED',
                'status': status
            }
        elif status == 'yellow':
            # Yellow is acceptable but log a warning
            app.logger.warning(f'OpenSearch cluster status is YELLOW')
            return {
                'is_healthy': True,
                'reason': 'Cluster status is YELLOW (acceptable)',
                'status': status
            }
        else:  # green
            return {
                'is_healthy': True,
                'reason': 'Cluster status is GREEN',
                'status': status
            }

    except (ConnectionTimeout, ConnectionError) as e:
        return {
            'is_healthy': False,
            'reason': f'OpenSearch not responsive: {str(e)}',
            'status': 'unreachable'
        }
    except Exception as e:
        app.logger.error(f'Error checking OpenSearch health: {str(e)}')
        return {
            'is_healthy': False,
            'reason': f'Health check error: {str(e)}',
            'status': 'error'
        }
```

### Health Check Usage in Batch Processing

```python
def update_record_created_dates(
    self,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    batch_size: int = 100,
    dry_run: bool = False,
    verbose: bool = False
) -> dict:
    """Update created dates with health checks between batches."""

    records = self.find_records_needing_created_date_update(start_date, end_date)
    stats = {'total_found': len(records), 'updated': 0, 'skipped': 0, 'errors': []}

    # Process in batches
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(records) + batch_size - 1) // batch_size

        app.logger.info(f'Processing batch {batch_num} of {total_batches}')

        # Process batch
        for record in batch:
            # ... update logic ...

        # Health check after each batch (except the last one)
        if i + batch_size < len(records):
            health = self.check_opensearch_health()

            if not health['is_healthy']:
                app.logger.warning(
                    f"OpenSearch health check failed: {health['reason']}. "
                    f"Pausing for 30 seconds..."
                )
                time.sleep(30)

                # Check again after pause
                health = self.check_opensearch_health()
                if not health['is_healthy']:
                    app.logger.error(
                        f"OpenSearch still unhealthy after pause: {health['reason']}. "
                        f"Stopping updates. Processed {i + len(batch)} of {len(records)} records."
                    )
                    stats['stopped_early'] = True
                    stats['stopped_at_record'] = i + len(batch)
                    break
            elif verbose:
                app.logger.info(f"OpenSearch health: {health['status']}")

            # Small delay between batches to avoid overwhelming the cluster
            time.sleep(1)

    return stats
```

### Health Check Configuration

The health check behavior can be configured:

- **Timeout**: 5 seconds for cluster health check
- **Pause Duration**: 30 seconds if cluster is unhealthy
- **Retry**: One retry after pause before stopping
- **Inter-batch Delay**: 1 second between batches (even when healthy)

### Health Status Interpretation

- **GREEN**: All primary and replica shards are allocated. Cluster is fully operational.
- **YELLOW**: All primary shards are allocated, but some replica shards are not. Cluster is operational but not fully redundant. **This is acceptable for our purposes.**
- **RED**: Some primary shards are not allocated. Cluster is not fully operational. **We pause and retry, then stop if still red.**
- **UNREACHABLE**: Cannot connect to cluster. **We pause and retry, then stop if still unreachable.**

### Why Health Checks Matter

1. **Prevent Index Overload**: Batch updates can overwhelm the search index with indexing requests
2. **Avoid Cascading Failures**: If the cluster becomes unhealthy, continuing to send requests makes it worse
3. **Data Integrity**: Stopping when unhealthy prevents partial updates that might not be indexed
4. **Graceful Degradation**: Allows the system to recover before continuing
5. **Monitoring**: Provides visibility into cluster health during long-running operations

## OpenSearch Query Patterns

### Basic Search Setup
```python
from opensearchpy.helpers.search import Search
from invenio_search.proxies import current_search_client

prefix = app.config.get("SEARCH_INDEX_PREFIX", "")
index = f"{prefix}rdmrecords" if prefix else "rdmrecords"

search = Search(
    using=current_search_client,
    index=index
)
```

### Filter by Field Existence
```python
search = search.filter("exists", field="custom_fields.hclegacy:record_creation_date")
```

### Filter by Term (Exact Match)
```python
search = search.filter("term", **{"field.name": "value"})
```

### Filter by Date Range
```python
search = search.filter("range", created={"gte": start_date, "lte": end_date})
```

### Sort Results
```python
search = search.sort({"custom_fields.hclegacy:record_creation_date": {"order": "asc"}})
```

### Limit Results
```python
search = search[:10]  # First 10 results
```

### Scan All Results
```python
for hit in search.scan():
    # Process each result
    record_id = hit.meta.id
    custom_fields = hit.custom_fields
```

### Execute and Get Results
```python
results = search.execute()
total = results.hits.total.value
first_hit = results.hits[0] if results.hits else None
```

### Array Field Queries
For array fields like `groups_for_deposit`, use dot notation:
```python
search = search.filter(
    "term",
    **{"custom_fields.hclegacy:groups_for_deposit.group_identifier": group_id}
)
```

## Date Handling with Arrow

### Parse Date String
```python
import arrow

# Parse ISO date
date = arrow.get("2020-01-15T10:30:00Z")

# Parse date string
date = arrow.get("2020-01-15")
```

### Floor to Start of Day
```python
start_of_day = arrow.get(date_string).floor('day')
# Returns: 2020-01-15T00:00:00+00:00
```

### Convert to Datetime
```python
dt = arrow_date.datetime
```

### Compare Dates
```python
if date1 < date2:
    # date1 is earlier
```

### Format for Display
```python
formatted = arrow_date.format('YYYY-MM-DD HH:mm:ss')
```

## Error Handling Strategy

### Record Updates
```python
stats = {
    'total_found': 0,
    'updated': 0,
    'skipped': 0,
    'errors': []
}

for record in records_to_update:
    try:
        updated = self.update_single_record_created_date(
            record['id'],
            record['new_created']
        )
        if updated:
            stats['updated'] += 1
        else:
            stats['skipped'] += 1
    except Exception as e:
        app.logger.error(f"Error updating record {record['id']}: {str(e)}")
        stats['errors'].append({
            'record_id': record['id'],
            'pid': record.get('pid', 'unknown'),
            'error': str(e)
        })
        # Continue processing other records
```

### Community Updates
```python
stats = {
    'total_found': 0,
    'updated': 0,
    'skipped': 0,
    'no_records': 0,
    'errors': []
}

for community in communities:
    try:
        oldest_date = self.find_oldest_record_for_community(community['group_id'])

        if oldest_date is None:
            stats['no_records'] += 1
            continue

        updated = self.update_single_community_created_date(
            community['id'],
            oldest_date
        )
        if updated:
            stats['updated'] += 1
        else:
            stats['skipped'] += 1

    except Exception as e:
        app.logger.error(f"Error updating community {community['id']}: {str(e)}")
        stats['errors'].append({
            'community_id': community['id'],
            'slug': community.get('slug', 'unknown'),
            'group_id': community.get('group_id', 'unknown'),
            'error': str(e)
        })
```

## Testing Strategy

### Unit Tests

#### Test Record Finding
```python
def test_find_records_needing_update(app, db, sample_records):
    """Test finding records with hclegacy:record_creation_date."""
    helper = RecordsHelper()
    records = helper.find_records_needing_created_date_update()

    assert len(records) > 0
    assert all('id' in r for r in records)
    assert all('new_created' in r for r in records)
```

#### Test Record Update
```python
def test_update_single_record(app, db, sample_record):
    """Test updating a single record's created date."""
    helper = RecordsHelper()
    new_date = "2020-01-15T00:00:00Z"

    result = helper.update_single_record_created_date(
        sample_record['id'],
        new_date
    )

    assert result is True

    # Verify update
    record = records_service.read(system_identity, id_=sample_record['id'])
    assert str(record._record.model.created) == new_date
```

#### Test Community Finding
```python
def test_find_communities_needing_update(app, db, sample_communities):
    """Test finding communities with kcr:commons_group_id."""
    helper = CommunitiesHelper()
    communities = helper.find_communities_needing_created_date_update()

    assert len(communities) > 0
    assert all('group_id' in c for c in communities)
```

#### Test Oldest Record Finding
```python
def test_find_oldest_record(app, db, sample_community, sample_records):
    """Test finding oldest record for a community."""
    helper = CommunitiesHelper()
    group_id = sample_community['custom_fields']['kcr:commons_group_id']

    oldest = helper.find_oldest_record_for_community(group_id)

    assert oldest is not None
    assert isinstance(oldest, arrow.Arrow)
```

### Integration Tests

#### Test Full Record Update
```python
def test_update_all_records(app, db, search_clear, sample_records):
    """Test updating all records end-to-end."""
    helper = RecordsHelper()

    stats = helper.update_record_created_dates(
        batch_size=10,
        verbose=True
    )

    assert stats['total_found'] > 0
    assert stats['updated'] + stats['skipped'] == stats['total_found']
    assert len(stats['errors']) == 0
```

#### Test Full Community Update
```python
def test_update_all_communities(app, db, search_clear, sample_communities):
    """Test updating all communities end-to-end."""
    helper = CommunitiesHelper()

    stats = helper.update_community_created_dates(
        batch_size=10,
        verbose=True
    )

    assert stats['total_found'] > 0
    assert stats['updated'] + stats['skipped'] + stats['no_records'] == stats['total_found']
```

#### Test CLI Command
```python
def test_cli_update_records_only(app, db, cli_runner):
    """Test CLI command with --records-only flag."""
    result = cli_runner.invoke(
        update_created_dates,
        ['--records-only', '--dry-run', '--verbose']
    )

    assert result.exit_code == 0
    assert 'Updating record created dates' in result.output
    assert 'Updating community created dates' not in result.output
```

#### Test Background Tasks
```python
def test_background_task_records(app, db, celery_worker):
    """Test record update as background task."""
    task = update_record_created_dates_task.delay(
        batch_size=10,
        verbose=True
    )

    result = task.get(timeout=60)

    assert result['total_found'] >= 0
    assert 'updated' in result
    assert 'errors' in result
```

### Edge Cases to Test

1. **Records without hclegacy:record_creation_date**
   - Should be ignored

2. **Records with invalid date format**
   - Should log error and continue

3. **Communities with no matching records**
   - Should count in `no_records` statistic

4. **Communities with no kcr:commons_group_id**
   - Should not be included in search results

5. **Date already correct**
   - Should skip update, count in `skipped`

6. **Multiple records in same community**
   - Should use oldest record's date

7. **Dry run mode**
   - Should not make any changes
   - Should still count what would be updated

8. **Empty date range**
   - Should process all records

9. **Invalid date range**
   - Should validate and show error

## Performance Considerations

### Batch Processing
- Process records in batches to avoid memory exhaustion
- Default batch size: 100
- Adjustable via CLI flag
- Log progress every batch

### Search Index Load
- Use `scan()` for large result sets (more efficient than pagination)
- Avoid overwhelming search index with too many simultaneous queries
- Process sequentially, not in parallel

### Database Connections
- Unit of work pattern manages transactions efficiently
- Each update commits immediately
- No long-running transactions

### Estimated Performance
- **Records**: ~100-200 records/minute
  - Depends on: batch size, search index performance, database speed
- **Communities**: ~10-20 communities/minute
  - Slower due to nested searches for each community
  - Each community requires searching all records

### Recommendations for Large Datasets
1. Run as background task (`--background` flag)
2. Use date ranges to process in chunks
3. Monitor Celery worker logs
4. Run during low-traffic periods
5. Consider running records and communities separately

## Usage Examples

### Basic Usage

Update all records and communities:
```bash
invenio importer update_created_dates
```

### Dry Run

See what would be updated without making changes:
```bash
invenio importer update_created_dates --dry-run --verbose
```

### Records Only

Update only records from 2020:
```bash
invenio importer update_created_dates \
    --records-only \
    --start-date 2020-01-01 \
    --end-date 2020-12-31
```

### Communities Only

Update only communities:
```bash
invenio importer update_created_dates --communities-only
```

### Background Execution

Run as background Celery task:
```bash
invenio importer update_created_dates --background
```

Monitor task progress:
```bash
celery -A invenio_app.celery inspect active
```

### Custom Batch Size

Process 50 records at a time:
```bash
invenio importer update_created_dates --batch-size 50
```

### Verbose Output

See detailed progress:
```bash
invenio importer update_created_dates --verbose
```

### Combined Options

Dry run for 2020 records with verbose output:
```bash
invenio importer update_created_dates \
    --records-only \
    --start-date 2020-01-01 \
    --end-date 2020-12-31 \
    --dry-run \
    --verbose \
    --batch-size 50
```

## Production Deployment

### Pre-Deployment Checklist

1. **Backup Database**
   ```bash
   pg_dump -h localhost -U postgres knowledge_commons_works > backup.sql
   ```

2. **Test on Development Data**
   - Run with `--dry-run` first
   - Verify output looks correct
   - Test with small date range

3. **Check Celery Workers**
   ```bash
   celery -A invenio_app.celery inspect stats
   ```

4. **Monitor Disk Space**
   - Ensure adequate space for search index updates

### Deployment Steps

1. **Deploy Code**
   ```bash
   cd /path/to/knowledge-commons-works
   git pull
   pipenv install
   ```

2. **Restart Services**
   ```bash
   # Restart web workers
   sudo systemctl restart invenio-web

   # Restart Celery workers
   sudo systemctl restart invenio-worker
   ```

3. **Run Updates**

   Option A: Foreground (for smaller datasets)
   ```bash
   pipenv run invenio importer update_created_dates --verbose
   ```

   Option B: Background (recommended for large datasets)
   ```bash
   pipenv run invenio importer update_created_dates --background
   ```

4. **Monitor Progress**

   Check Celery tasks:
   ```bash
   celery -A invenio_app.celery inspect active
   ```

   Check logs:
   ```bash
   tail -f /path/to/logs/invenio.log
   ```

5. **Verify Results**

   Check a few records:
   ```bash
   pipenv run invenio importer read <record-id>
   ```

   Check search index:
   ```bash
   curl -X GET "localhost:9200/rdmrecords/_search?q=custom_fields.hclegacy:record_creation_date:*&size=5"
   ```

### Post-Deployment Verification

1. **Spot Check Records**
   - Verify created dates match hclegacy:record_creation_date
   - Check that search index is updated

2. **Spot Check Communities**
   - Verify created dates are at start of day
   - Verify dates match oldest record in community

3. **Check for Errors**
   - Review error logs
   - Investigate any failed updates

4. **Performance Check**
   - Verify search index is responsive
   - Check database performance

### Rollback Plan

If issues are discovered:

1. **Stop Any Running Tasks**
   ```bash
   celery -A invenio_app.celery control revoke <task-id>
   ```

2. **Restore Database**
   ```bash
   psql -h localhost -U postgres knowledge_commons_works < backup.sql
   ```

3. **Reindex Search**
   ```bash
   pipenv run invenio index reindex --yes-i-know
   pipenv run invenio index run
   ```

## Monitoring and Logging

### Log Levels

- **INFO**: Normal progress updates
- **WARNING**: Skipped items, no records found
- **ERROR**: Failed updates, invalid data

### Key Log Messages

#### Records
```
INFO: Starting record created date updates
INFO: Found 1234 records needing update
INFO: Processing batch 1 of 13 (100 records)
INFO: Updated record abc-123 from 2024-01-15 to 2020-03-10
WARNING: Skipped record xyz-789 (dates already match)
ERROR: Failed to update record def-456: Invalid timestamp format
INFO: Record updates complete: 1200 updated, 30 skipped, 4 errors
```

#### Communities
```
INFO: Starting community created date updates
INFO: Found 45 communities needing update
INFO: Processing community 'my-group' (group_id: 12345)
INFO: Found oldest record for group 12345: 2019-05-20
INFO: Updated community 'my-group' from 2024-01-15 to 2019-05-20
WARNING: No records found for community 'empty-group'
ERROR: Failed to update community 'bad-group': Community not found
INFO: Community updates complete: 40 updated, 3 skipped, 2 no records, 0 errors
```

### Monitoring Commands

Check Celery worker status:
```bash
celery -A invenio_app.celery inspect active
celery -A invenio_app.celery inspect stats
```

Check task results:
```bash
celery -A invenio_app.celery result <task-id>
```

Monitor logs in real-time:
```bash
tail -f /path/to/logs/invenio.log | grep "created date"
```

## Troubleshooting

### Issue: Task Not Starting

**Symptoms**: Background task queued but not running

**Possible Causes**:
- Celery workers not running
- Wrong queue configuration

**Solutions**:
```bash
# Check worker status
celery -A invenio_app.celery inspect active

# Restart workers
sudo systemctl restart invenio-worker

# Check queue
celery -A invenio_app.celery inspect registered
```

### Issue: Search Index Not Updating

**Symptoms**: Database updated but search shows old dates

**Possible Causes**:
- Indexer not running
- Unit of work not committing

**Solutions**:
```bash
# Manually reindex
invenio index reindex --yes-i-know
invenio index run

# Check indexer status
celery -A invenio_app.celery inspect active | grep indexer
```

### Issue: Memory Errors

**Symptoms**: Process crashes with out of memory error

**Possible Causes**:
- Batch size too large
- Too many results loaded at once

**Solutions**:
- Reduce batch size: `--batch-size 50`
- Process in date ranges
- Increase worker memory limit

### Issue: Invalid Dates

**Symptoms**: Errors about invalid timestamp format

**Possible Causes**:
- Malformed hclegacy:record_creation_date
- Wrong timezone

**Solutions**:
- Check date format in source data
- Verify timezone is UTC
- Add validation before update

### Issue: Slow Performance

**Symptoms**: Updates taking very long

**Possible Causes**:
- Large dataset
- Search index overloaded
- Database slow

**Solutions**:
- Run during off-peak hours
- Use background tasks
- Process in smaller batches
- Check database indexes

## Maintenance

### Regular Checks

After initial deployment, periodically verify:

1. **New Records**: Check if newly imported records have correct dates
2. **New Communities**: Verify new communities get correct dates
3. **Search Index**: Ensure index stays in sync with database

### Future Improvements

Potential enhancements for future versions:

1. **Progress Bar**: Add progress bar for CLI output
2. **Email Notifications**: Send email when background task completes
3. **Webhooks**: Trigger webhooks for monitoring systems
4. **Retry Logic**: Automatic retry for failed updates
5. **Parallel Processing**: Process multiple communities in parallel
6. **Incremental Updates**: Only update records changed since last run
7. **Audit Trail**: Log all changes to separate audit table

## Appendix: File Locations

### Source Files
- `/site/kcworks/dependencies/invenio-record-importer-kcworks/invenio_record_importer_kcworks/tasks.py`
- `/site/kcworks/dependencies/invenio-record-importer-kcworks/invenio_record_importer_kcworks/services/records.py`
- `/site/kcworks/dependencies/invenio-record-importer-kcworks/invenio_record_importer_kcworks/services/communities.py`
- `/site/kcworks/dependencies/invenio-record-importer-kcworks/invenio_record_importer_kcworks/cli.py`

### Test Files
- `/site/kcworks/dependencies/invenio-record-importer-kcworks/tests/test_created_dates_records.py` (to be created)
- `/site/kcworks/dependencies/invenio-record-importer-kcworks/tests/test_created_dates_communities.py` (to be created)
- `/site/kcworks/dependencies/invenio-record-importer-kcworks/tests/test_created_dates_cli.py` (to be created)

### Configuration Files
- `/site/kcworks/dependencies/invenio-record-importer-kcworks/invenio_record_importer_kcworks/config.py` (may need updates)

### Documentation Files
- `/site/kcworks/dependencies/invenio-record-importer-kcworks/CREATED_DATES_UPDATE_PLAN.md` (this file)
- `/site/kcworks/dependencies/invenio-record-importer-kcworks/README.md` (update with new command)

## Summary

This implementation provides a comprehensive solution for updating created dates on migrated records and communities. The key features are:

1. **Flexible CLI**: Options for records-only, communities-only, date ranges, dry-run, and background execution
2. **Background Tasks**: Celery integration for long-running operations
3. **Atomic Updates**: Unit of work pattern ensures data consistency
4. **Error Handling**: Robust error handling with detailed logging
5. **Performance**: Batch processing and efficient search queries
6. **Testing**: Comprehensive test coverage for all components
7. **Monitoring**: Detailed logging and progress reporting

The implementation follows Invenio best practices and integrates seamlessly with the existing record importer infrastructure.
