from celery import shared_task
from flask import current_app as app
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
