from flask import current_app as app
from invenio_search.proxies import current_search_client
from opensearchpy.helpers.search import Search


# def view_events_search(recid):
#     my_search = RecordsSearch(index="events-stats-record-view").query(
#         dsl.QueryString(q=f"record_id:{recid}")
#     )
#     return my_search.execute()


def view_events_search(recid, dt=None):

    # views_query = {
    #     "query": {
    #         "bool": {
    #             "filter": [
    #                 {"term": {"country": "imported"}},
    #                 {"term": {"recid": recid}},
    #             ]
    #         }
    #     }
    # }

    prefix = app.config.get("SEARCH_INDEX_PREFIX", "")
    search = (
        Search(
            using=current_search_client,
            index=f"{prefix}events-stats-record-view",
        )
        .filter({"term": {"country": "imported"}})
        .filter({"term": {"recid": recid}})
    )

    if dt:
        search = (
            Search(
                using=current_search_client,
                index=f"{prefix}events-stats-record-view",
            )
            .filter({"term": {"country": "imported"}})
            .filter({"term": {"recid": recid}})
        )
        search.filter({"term": {"timestamp": dt}})
        terms = search.aggs.bucket("terms", "terms", field="unique_id")
        terms.metric(
            "top_hit", "top_hits", size=10, sort={"timestamp": "desc"}
        )
        terms.metric(
            "unique_count",
            "cardinality",
            field="unique_session_id",
            precision_threshold=20000,
        )

    response = list(search.scan())

    return response


def download_events_search(file_id):

    # downloads_query = {
    #     "query": {
    #         "bool": {
    #             "filter": [
    #                 {"term": {"country": "imported"}},
    #                 {"term": {"file_id": file_id}},
    #             ]
    #         }
    #     }
    # }

    prefix = app.config.get("SEARCH_INDEX_PREFIX", "")
    search = (
        Search(
            using=current_search_client,
            index=f"{prefix}events-stats-file-download",
        )
        .filter({"term": {"country": "imported"}})
        .filter({"term": {"file_id": file_id}})
    )
    response = list(search.scan())

    return response


def aggregations_search(record_id):
    prefix = app.config.get("SEARCH_INDEX_PREFIX", "")
    views_search = Search(
        using=current_search_client,
        index=f"{prefix}stats-record-view",
    )
    views_search = views_search.filter({"term": {"recid": record_id}})
    views_response = list(views_search.scan())

    downloads_search = Search(
        using=current_search_client,
        index=f"{prefix}stats-file-download",
    )
    downloads_search = downloads_search.filter({"term": {"recid": record_id}})
    downloads_response = list(downloads_search.scan())

    return views_response, downloads_response
