from celery import shared_task
import datetime
from dateutil.parser import parse as dateutil_parse
from invenio_stats.proxies import current_stats


@shared_task(ignore_result=False)
def aggregate_events(
    aggregations: list,
    start_date: str = None,
    end_date: str = None,
    update_bookmark: bool = True,
    bookmark_override: datetime.datetime = None,
) -> list:
    """Aggregate indexed events.

    Identical to the function in invenio_stats.tasks, but allows
    for the `bookmark_override` parameter to be passed in to facilitate
    re-indexing older (artificially created) view and download events.

    This relies on a subclass of the StatAggregator class to be defined
    that can use the `previous_bookmark` parameter to override the
    `previous_bookmark` value derived from the search index.
    """
    start_date = dateutil_parse(start_date) if start_date else None
    end_date = dateutil_parse(end_date) if end_date else None
    results = []
    for aggr_name in aggregations:
        aggr_cfg = current_stats.aggregations[aggr_name]
        aggregator = aggr_cfg.cls(name=aggr_cfg.name, **aggr_cfg.params)
        results.append(
            aggregator.run(
                start_date,
                end_date,
                update_bookmark,
                previous_bookmark=bookmark_override,
            )
        )

    return results
