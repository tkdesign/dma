def test_home(client, auth_headers):
    response = client.get("/", headers=auth_headers)
    assert response.status_code == 200

def test_login(app, client, auth_headers):
    response = client.post("/login", headers=auth_headers, data={"email": "peter.novak@eshop.sk", "password": "456789"}, follow_redirects=True)
    assert response.status_code == 200
