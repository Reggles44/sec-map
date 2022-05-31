import json
import os

from flask import Flask

from sec_map.config import configure_logging

RESOURCES = os.path.join(os.path.dirname(__file__), 'resources')


META_FILE_PATH = os.path.join(RESOURCES, 'meta.json')
META_MAPPING = json.load(open(META_FILE_PATH)) if os.path.isfile(META_FILE_PATH) else {}

TICKER_FILE_PATH = os.path.join(RESOURCES, 'ticker.json')
TICKER_MAPPING = json.load(open(TICKER_FILE_PATH)) if os.path.isfile(TICKER_FILE_PATH) else {}

INDEX_FILE_PATH = os.path.join(RESOURCES, 'index.json')
INDEX_MAPPING = json.load(open(INDEX_FILE_PATH)) if os.path.isfile(INDEX_FILE_PATH) else {}
"""
Index schema

{
    # CIK is primary index
    "0000001": {
        "ticker": "ABC",
        "company_name": "ABC Inc.",
        "forms": {
            "10-K": {
                "2022-02-09": "0123456789-12-123456"
            }
        }
    }
}
"""


def create_app(*args, **kwargs):
    app = Flask(__name__, instance_relative_config=True)

    @app.route('/', methods=['GET'])
    def index():
        return INDEX_MAPPING

    from . import lookup
    app.register_blueprint(lookup.bp)

    from . import assemble
    app.register_blueprint(assemble.bp)

    return app