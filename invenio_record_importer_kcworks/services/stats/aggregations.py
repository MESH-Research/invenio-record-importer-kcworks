import arrow
from datetime import datetime
from flask import current_app
from invenio_search.engine import dsl, search

# from invenio_search.proxies import current_search_client
from invenio_stats.aggregations import StatAggregator

# from invenio_stats.bookmark import format_range_dt
from typing import Union


class StatAggregatorOverridable(StatAggregator):
    """A subclass of StatAggregator that allows for overriding the
    bookmark datetime to re-aggregate earlier stats.

    Primarily needed because of re-aggregation after importing older
    records with existing stats.
    """

    def run(
        self,
        start_date: Union[str, None, datetime] = None,
        end_date: Union[str, None, datetime] = None,
        update_bookmark: bool = True,
        previous_bookmark: Union[str, None, datetime] = None,
    ):
        """Calculate statistics aggregations."""
        # If no events have been indexed there is nothing to aggregate
        if not dsl.Index(self.event_index, using=self.client).exists():
            return

        current_app.logger.warning(
            "Gathering aggregated stats for %s", self.event_index
        )
        # Handle either string or datetime inputs -- invenio_stats
        # aggregate_events calls with datetime objects.
        # Convert to naive datetime because the invenio_stats builders
        # provide iso datetime strings without offset information
        start_date = arrow.get(start_date).naive if start_date else None
        end_date = arrow.get(end_date).naive if end_date else None

        # bookmark_api.get_bookmark() returns a datetime object or string
        previous_bookmark = (
            self.bookmark_api.get_bookmark()
            if not previous_bookmark
            else previous_bookmark
        )
        if previous_bookmark:
            previous_bookmark = arrow.get(previous_bookmark).naive
        # return from _get_oldest_event_timestamp() is datetime object
        lower_limit = (
            start_date
            or previous_bookmark
            or self._get_oldest_event_timestamp()  # noqa: E501
        )
        # FIXME: this is to handle accidentally recording bookmark
        # with timezone awareness
        lower_limit = arrow.get(lower_limit).naive if lower_limit else None
        # Stop here if no bookmark could be estimated.
        if lower_limit is None:
            return

        upper_limit = self._upper_limit(end_date)
        dates = self._split_date_range(lower_limit, upper_limit)
        # Let's get the timestamp before we start the aggregation.
        # This will be used for the next iteration. Some events might
        # be processed twice
        if not end_date:
            end_date = arrow.utcnow().naive

        current_app.logger.warning("upper_limit: %s", upper_limit)
        current_app.logger.warning("lower_limit: %s", lower_limit)
        current_app.logger.warning("end_date: %s", end_date)
        current_app.logger.warning("previous_bookmark: %s", previous_bookmark)
        results = []
        for dt_key, dt in sorted(dates.items()):
            results.append(
                search.helpers.bulk(
                    self.client,
                    self.agg_iter(dt, previous_bookmark),
                    stats_only=True,
                    chunk_size=50,
                )
            )
        if update_bookmark:
            self.bookmark_api.set_bookmark(upper_limit)
        return results

    # NOTE: debugging statements in delete() may be useful again
    #       but the logic is unchanged from the superclass
    #
    # def delete(self, start_date=None, end_date=None, skip_bookmark=False):
    #     """Delete aggregation documents."""
    #     aggs_query = dsl.Search(
    #         using=self.client,
    #         index=self.index,
    #     ).extra(_source=False)

    #     range_args = {}
    #     if start_date:
    #         range_args["gte"] = format_range_dt(start_date, self.interval)
    #     if end_date:
    #         range_args["lte"] = format_range_dt(end_date, self.interval)
    #     if range_args:
    #         aggs_query = aggs_query.filter("range", timestamp=range_args)

    #     from flask import current_app

    #     current_app.logger.warning(f"deleting for range: {range_args}")

    #     bookmarks_query = (
    #         dsl.Search(
    #             using=self.client,
    #             index=self.bookmark_api.bookmark_index,
    #         )
    #         .filter("term", aggregation_type=self.name)
    #         .sort({"date": {"order": "desc"}})
    #     )

    #     if range_args:
    #         bookmarks_query = bookmarks_query.filter("range",
    #                                                  date=range_args)

    #     def _delete_actions():
    #         for query in (aggs_query, bookmarks_query):
    #             affected_indices = set()
    #             for doc in query.scan():
    #                 affected_indices.add(doc.meta.index)
    #                 yield {
    #                     "_index": doc.meta.index,
    #                     "_op_type": "delete",
    #                     "_id": doc.meta.id,
    #                 }
    #             current_search_client.indices.flush(
    #                 index=",".join(affected_indices), wait_if_ongoing=True
    #             )

    #     result = search.helpers.bulk(
    #         self.client, _delete_actions(), refresh=True
    #     )
    #     print("delete result: ", result)
