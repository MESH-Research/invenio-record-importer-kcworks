from celery import shared_task
from flask import current_app as app
from flask import render_template
from flask_mail import Message
from invenio_stats.proxies import current_stats


@shared_task(ignore_result=False)
def aggregate_events(
    aggregations: list,
    start_date: str = "",
    end_date: str = "",
    update_bookmark: bool = True,
    bookmark_override: str = "",
) -> list:
    """Aggregate indexed events.

    Identical to the function in invenio_stats.tasks, but allows
    for the `bookmark_override` parameter to be passed in to facilitate
    re-indexing older (artificially created) view and download events.

    This relies on a subclass of the StatAggregator class to be defined
    that can use the `previous_bookmark` parameter to override the
    `previous_bookmark` value derived from the search index.
    """
    results = []
    for aggr_name in aggregations:
        aggr_cfg = current_stats.aggregations[aggr_name]
        aggregator = aggr_cfg.cls(name=aggr_cfg.name, **aggr_cfg.params)
        app.logger.warning(f"Aggregator task running: {start_date} - {end_date}")
        results.append(
            aggregator.run(
                start_date,
                end_date,
                update_bookmark,
                previous_bookmark=bookmark_override,
            )
        )
        app.logger.warning(f"Aggregator task complete {aggr_name}")

    return results


@shared_task(ignore_result=False)
def send_security_email(
    subject: str,
    recipients: list[str],
    user: dict,
    community_url: str,
    record_data: dict,
    collection_config: dict,
):
    """Send a security email as a background task.

    Args:
        subject: The subject of the email.
        recipients: A list of email addresses to send the email to.
        user: The user to send the email to.
        community_record: The community record.
        record_data: The record data.
        collection_config: The collection configuration.
    """
    sender = app.config.get("MAIL_DEFAULT_SENDER")

    template_variables = {
        "user": user,
        "community_page_url": community_url,
        "record": record_data,
    }
    template_name = collection_config.get("email_template_register")
    txt_body = render_template(
        f"security/email/{template_name}.txt",
        **template_variables,
    )
    html_body = render_template(
        f"security/email/{template_name}.html",
        **template_variables,
    )
    msg = Message(subject, sender=sender, recipients=recipients)
    msg.body = txt_body
    msg.html = html_body

    mail = app.extensions.get("mail")
    if mail:
        mail.send(msg)
    else:
        app.logger.error("Mail extension not found")


@shared_task(ignore_result=False, bind=True)
def update_record_created_dates_task(
    self,
    start_date: str | None = None,
    end_date: str | None = None,
    batch_size: int = 100,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """Celery task to update record created dates in background.

    This task updates the 'created' timestamp for records that have
    custom_fields.hclegacy:record_creation_date set. It processes records
    in batches and performs health checks between batches to avoid
    overwhelming the OpenSearch cluster.

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
                'errors': list[dict],
                'stopped_early': bool (optional),
                'stopped_at_record': int (optional)
            }
    """
    from invenio_record_importer_kcworks.services.records import RecordsHelper

    app.logger.info("Starting background task: update_record_created_dates")
    app.logger.info(
        f"Parameters: start_date={start_date}, end_date={end_date}, "
        f"batch_size={batch_size}, dry_run={dry_run}, verbose={verbose}"
    )

    # Update task state to show progress
    self.update_state(state="PROGRESS", meta={"status": "Initializing..."})

    try:
        helper = RecordsHelper()
        stats = helper.update_record_created_dates(
            start_date=start_date,
            end_date=end_date,
            batch_size=batch_size,
            dry_run=dry_run,
            verbose=verbose,
        )

        app.logger.info(f"Background task completed: {stats}")
        return stats

    except Exception as e:
        app.logger.error(f"Background task failed: {str(e)}")
        self.update_state(state="FAILURE", meta={"error": str(e)})
        raise


@shared_task(ignore_result=False, bind=True)
def update_community_created_dates_task(
    self,
    batch_size: int = 100,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """Celery task to update community created dates in background.

    This task updates the 'created' timestamp for communities based on
    the oldest record in each community. It processes communities in
    batches and performs health checks between batches.

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
                'errors': list[dict],
                'stopped_early': bool (optional),
                'stopped_at_community': int (optional)
            }
    """
    from invenio_record_importer_kcworks.services.communities import CommunitiesHelper

    app.logger.info("Starting background task: update_community_created_dates")
    app.logger.info(
        f"Parameters: batch_size={batch_size}, dry_run={dry_run}, verbose={verbose}"
    )

    # Update task state to show progress
    self.update_state(state="PROGRESS", meta={"status": "Initializing..."})

    try:
        helper = CommunitiesHelper()
        stats = helper.update_community_created_dates(
            batch_size=batch_size,
            dry_run=dry_run,
            verbose=verbose,
        )

        app.logger.info(f"Background task completed: {stats}")
        return stats

    except Exception as e:
        app.logger.error(f"Background task failed: {str(e)}")
        self.update_state(state="FAILURE", meta={"error": str(e)})
        raise
