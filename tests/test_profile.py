from auth.forms import ProfileForm
from models import User
from tests.utils import autologin_user

def test_auth_profile(app, client, auth_headers):
    autologin_user(app, client)
    response = client.get("/profile", headers=auth_headers)
    assert response.status_code == 200

def test_auth_profile_update(app, client, auth_headers):
    autologin_user(app, client)

    with app.app_context():
        user = User.query.filter_by(email="peter.novak@eshop.sk").first()
        assert user is not None

    form = ProfileForm()
    form.email.data = user.email
    form.current_password.data = ''
    form.new_password.data = ''
    form.first_name.data = user.first_name
    form.last_name.data = user.last_name
    form.occupation.data = user.occupation
    form.department.data = user.department

    response = client.post("/profile", headers=auth_headers, data={
        "email": form.email.data,
        "current_password": form.current_password.data,
        "new_password": form.new_password.data,
        "first_name": form.first_name.data,
        "last_name": form.last_name.data,
        "occupation": form.occupation.data,
        "department": form.department.data
    })

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/profile")