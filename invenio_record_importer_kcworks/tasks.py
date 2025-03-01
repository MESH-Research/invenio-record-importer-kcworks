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
    app.logger.debug(f"sending security email to {recipients}...")
    app.logger.debug(f"msg: {msg}")

    mail = app.extensions.get("mail")
    if mail:
        mail.send(msg)
    else:
        app.logger.error("Mail extension not found")
