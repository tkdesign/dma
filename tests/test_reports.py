from tests.utils import autologin_user

def test_reports_index(app, client, auth_headers):
    autologin_user(app, client)
    response = client.get("/reports", headers=auth_headers)
    assert response.status_code == 200

