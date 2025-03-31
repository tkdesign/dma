from tests.utils import autologin_user

def test_dashboard(app, client, auth_headers):
    autologin_user(app, client)
    response = client.get("/dashboard", headers=auth_headers)
    assert response.status_code == 200


def test_get_summary(app, client, auth_headers):
    autologin_user(app, client)

    response = client.get('/get-summary', headers=auth_headers)

    assert response.status_code == 200
    assert response.is_json
    data = response.get_json()

    assert 'carts_count' in data
    assert 'orders_count' in data
    assert 'orders_paid_count' in data
    assert 'conversion_rate' in data
    assert 'conversion_rate_paid' in data
    assert 'total_revenue' in data

def test_get_period_revenue(app, client, auth_headers):
    autologin_user(app, client)

    response = client.get('/get-period-revenue', headers=auth_headers)
    assert response.status_code == 200
    assert response.is_json

    data = response.get_json()
    assert 'data' in data
    assert 'layout' in data

def test_get_orders_heatmap(app, client, auth_headers):
    autologin_user(app, client)

    response = client.get('/get-orders-heatmap', headers=auth_headers)
    assert response.status_code == 200
    assert response.is_json

    data = response.get_json()
    assert 'data' in data
    assert 'layout' in data

def test_get_carrier_revenue_orders_distribution(app, client, auth_headers):
    autologin_user(app, client)

    response = client.get('/get-carrier-revenue-orders-distribution', headers=auth_headers)
    assert response.status_code == 200
    assert response.is_json

    data = response.get_json()
    assert 'data' in data
    assert 'layout' in data

def test_get_top_manufacturer_revenue_distribution(app, client, auth_headers):
    autologin_user(app, client)

    response = client.get('/get-top-manufacturer-revenue-distribution', headers=auth_headers)
    assert response.status_code == 200
    assert response.is_json

    data = response.get_json()
    assert 'data' in data
    assert 'layout' in data


def test_get_market_group_revenue_distribution(app, client, auth_headers):
    autologin_user(app, client)

    response = client.get('/get-top-market-group-revenue-distribution', headers=auth_headers)
    assert response.status_code == 200
    assert response.is_json

    data = response.get_json()
    assert 'data' in data
    assert 'layout' in data


def test_gender_distribution(app, client, auth_headers):
    autologin_user(app, client)

    response = client.get('/get-gender-distribution', headers=auth_headers)
    assert response.status_code == 200
    assert response.is_json

    data = response.get_json()
    assert 'data' in data
    assert 'layout' in data