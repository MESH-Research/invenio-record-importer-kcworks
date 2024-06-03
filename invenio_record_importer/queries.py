from invenio_search import current_search_client
from invenio_search.api import RecordsSearchV2 as RecordsSearch
from invenio_search.engine import dsl


# def view_events_search(recid):
#     my_search = RecordsSearch(index="events-stats-record-view").query(
#         dsl.QueryString(q=f"record_id:{recid}")
#     )
#     return my_search.execute()


def view_events_search(recid):

    views_query = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"country": "imported"}},
                    {"term": {"recid": recid}},
                ]
            }
        }
    }

    response = current_search_client.search(
        index="kcworks-events-stats-record-view", body=views_query
    )

    # my_search = (
    #     RecordsSearch(index="events-stats-record-view")
    #     .filter("term", record_id=recid)
    #     .query("match", country="imported")
    # )
    # return my_search.execute()

    return response


# def download_events_search(file_id):
#     my_search = RecordsSearch(index="events-stats-file-download").query(
#         dsl.QueryString(q=f"file_id:{file_id}")
#     )
#     return my_search.execute()


def download_events_search(file_id):

    downloads_query = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"country": "imported"}},
                    {"term": {"file_id": file_id}},
                ]
            }
        }
    }

    # my_search = (
    #     RecordsSearch(index="events-stats-file-download")
    #     .filter("term", file_id=file_id)
    #     .query("match", country="imported")
    # )
    # return my_search.execute()

    response = current_search_client.search(
        index="kcworks-events-stats-file-download", body=downloads_query
    )

    return response
