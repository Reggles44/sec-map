import pytest

from sec_map import create_app, INDEX_MAPPING
from sec_map.build import build


@pytest.fixture
def app():
    app = create_app({"TESTING": True})
    yield app


@pytest.fixture()
def client(app):
    return app.test_client()


@pytest.fixture
def runner(app):
    """A test runner for the app's Click commands."""
    return app.test_cli_runner()


if not INDEX_MAPPING:
    build(tickers=False)