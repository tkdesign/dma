import pytest
from app import create_app
from tests.utils import basic_auth_header

@pytest.fixture
def auth_headers():
    return basic_auth_header("employee","123456")

@pytest.fixture
def app():
    app = create_app({
        "TESTING": True,
        "WTF_CSRF_ENABLED": False,
        "SECRET_KEY": "test-key"
    })

    with app.app_context():
        yield app

@pytest.fixture
def client(app):
    return app.test_client()