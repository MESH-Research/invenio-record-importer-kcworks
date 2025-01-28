from flask import current_app
from werkzeug.local import LocalProxy

current_record_importer = LocalProxy(
    lambda: current_app.extensions["invenio-record-importer-kcworks"]
)

current_record_importer_service = LocalProxy(
    lambda: current_app.extensions["invenio-record-importer-kcworks"].service
)
