import base64
from flask_login import login_user
from models import User

def basic_auth_header(username, password):
    credentials = f"{username}:{password}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return {"Authorization": f"Basic {encoded}"}

def autologin_user(app, client):
    with app.app_context():
        user = User.query.filter_by(email='peter.novak@eshop.sk').first()
        assert user is not None
        assert user.is_active()

        with app.test_request_context():
            login_user(user)

            with client.session_transaction() as sess:
                sess['_user_id'] = str(user.id)
                sess['_fresh'] = True