import datetime

from flask import Blueprint, request, jsonify, render_template, redirect, url_for
from flask_login import current_user, login_required
from sqlalchemy import create_engine, text
import pandas as pd
import plotly.express as px
import gc
from celeryconfig import PROD_DB_URI, DWH_DB_URI

from auth.base_auth import check_auth, authenticate
from reportsconfig import dashboard_queries, filter_queries
dashboard_blueprint = Blueprint('dashboard', __name__)

prod_engine = create_engine(PROD_DB_URI)
dwh_engine = create_engine(DWH_DB_URI)

def apply_period_filter(original_query, filter_type, filter_value, range_start, range_end):
    current_date = datetime.datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    current_quarter = (current_month - 1) // 3 + 1
    if filter_type == "month" and filter_value:
        try:
            filter_value_parts = filter_value.split("-")
            filter_value_year = int(filter_value_parts[0])
            filter_value_month = int(filter_value_parts[1])
        except ValueError:
            filter_value_month = current_month
            filter_value_year = current_year
        query = original_query.format(filter="month = " + str(filter_value_month) + " AND year = " + str(filter_value_year), filter_raw="'month = " + str(filter_value_month) + " AND year = " + str(filter_value_year) + "'")
    elif filter_type == "quarter" and filter_value:
        try:
            filter_value_parts = filter_value.split("-")
            filter_value_year = int(filter_value_parts[0])
            filter_value_quarter = int(filter_value_parts[1])
        except ValueError:
            filter_value_quarter = current_quarter
            filter_value_year = current_year
        query = original_query.format(
            filter="quarter = " + str(filter_value_quarter) + " AND year = " + str(filter_value_year), filter_raw="'quarter = " + str(filter_value_quarter) + " AND year = " + str(filter_value_year) + "'")
    elif filter_type == "year" and filter_value:
        try:
            filter_value = int(filter_value)
        except ValueError:
            filter_value = 0
        query = original_query.format(filter="year = " + str(filter_value), filter_raw="'year = " + str(filter_value) + "'")
    elif filter_type == "range" and range_start or range_end:
        try:
            date_start = range_start
            filter_value_start = datetime.datetime.strptime(date_start, "%Y-%m-%d").date()
            date_end = range_end
            filter_value_end = datetime.datetime.strptime(date_end, "%Y-%m-%d").date()
        except ValueError:
            filter_value_start = filter_value_end = datetime.datetime.strptime(current_date, "%Y-%m-%d").date()
        query = original_query.format(filter="date BETWEEN '" + str(filter_value_start) + "' AND '" + str(filter_value_end) + "'", filter_raw="'date BETWEEN ''" + str(filter_value_start) + "'' AND ''" + str(filter_value_end) + "'''")
    else:
        current_year = datetime.datetime.now().year
        query = original_query.format(filter="year = " + str(current_year), filter_raw="'year = " + str(current_year) + "'")
    return query

@dashboard_blueprint.before_request
def require_http_auth():
    auth = request.authorization
    if not auth or not check_auth(auth.username, auth.password):
        return authenticate()\

@dashboard_blueprint.route('/dashboard', methods=['GET'])
@login_required
def dashboard_index():
    if not current_user.is_authenticated:
        return redirect(url_for('auth.auth_login_form'))

    with dwh_engine.connect() as conn:
        min_date_row = conn.execute(text(filter_queries["min_date"])).fetchone()
        months = conn.execute(text(filter_queries["months"]), parameters={"min_date": min_date_row.min_date}).fetchall()
        quarters = conn.execute(text(filter_queries["quarters"]), parameters = {"min_date": min_date_row.min_date}).fetchall()
        years = conn.execute(text(filter_queries["years"]), parameters ={"min_date": min_date_row.min_date}).fetchall()

    return render_template('dashboard/dashboard.html', title='DMA - Dashboard', page='dashboard', months=months, quarters=quarters, years=years)

@dashboard_blueprint.route('/get-summary', methods=['GET'])
@login_required
def get_summary():
    filter_type = request.args.get("filter_type")
    filter_value = request.args.get("filter_value")

    filter_type = "year" if filter_type is None else filter_type
    filter_value = str(datetime.datetime.now().year) if filter_value is None else filter_value

    range_start = range_end = None

    if (filter_type == "range"):
        range_start = request.args.get("filter_value_start")
        range_end = request.args.get("filter_value_end")

    query = apply_period_filter(dashboard_queries["summary"], filter_type, filter_value, range_start, range_end)

    with dwh_engine.connect() as conn:
        summary_df = pd.read_sql_query(text(query), conn)
    if summary_df.empty:
        return jsonify({})

    summary_df['orders_count'] = summary_df['orders_count'].astype('int64')
    summary_df['total_revenue'] = summary_df['total_revenue'].astype('float64')
    summary_df['avg_order_value'] = summary_df['avg_order_value'].astype('float64')
    summary_df['carts_count'] = summary_df['carts_count'].astype('int64')
    summary_df['avg_age'] = summary_df['avg_age'].astype('float64')
    summary_df = summary_df.fillna({'orders_count': 0, 'total_revenue': 0.0, 'avg_order_value': 0.0, 'carts_count': 0, 'conversion_rate': 0.0, 'avg_age': 0.0})
    summary_df = summary_df.astype({'orders_count': 'int64', 'total_revenue': 'float64', 'avg_order_value': 'float64', 'carts_count': 'int64', 'conversion_rate': 'float64', 'avg_age': 'float64'})
    summary_data = summary_df.iloc[0].to_dict()
    del summary_df
    gc.collect()
    return jsonify(summary_data)


@dashboard_blueprint.route('/get-period-revenue', methods=['GET'])
@login_required
def get_period_revenue():
    filter_type = request.args.get("filter_type")
    filter_value = request.args.get("filter_value")

    filter_type = "year" if filter_type is None else filter_type
    filter_value = str(datetime.datetime.now().year) if filter_value is None else filter_value

    range_start = range_end = None

    if (filter_type == "range"):
        range_start = request.args.get("filter_value_start")
        range_end = request.args.get("filter_value_end")

    query = apply_period_filter(dashboard_queries["period_revenue"], filter_type, filter_value, range_start, range_end)

    with dwh_engine.connect() as conn:
        revenue_df = pd.read_sql_query(text(query), conn)

    try:
        fig = px.bar(revenue_df, x='period', y='total_revenue', title='Príjmy')
        fig.update_layout(
            xaxis=dict(
                tickmode="linear",
                dtick=1,
                title="Period",
                type="category"
            ),
            yaxis=dict(title="Príjmy"),
            margin=dict(l=40, r=40, t=40, b=40)
        )
    except Exception as e:
        print(filter_type)
        print(filter_value)
        print(query)
        print(revenue_df.head())
        return jsonify({})

    del revenue_df
    gc.collect()
    return fig.to_json()

@dashboard_blueprint.route('/get-orders-heatmap', methods=['GET'])
@login_required
def get_orders_heatmap():
    filter_type = request.args.get("filter_type")
    filter_value = request.args.get("filter_value")

    filter_type = "year" if filter_type is None else filter_type
    filter_value = str(datetime.datetime.now().year) if filter_value is None else filter_value

    range_start = range_end = None

    if (filter_type == "range"):
        range_start = request.args.get("filter_value_start")
        range_end = request.args.get("filter_value_end")

    query = apply_period_filter(dashboard_queries["orders_heatmap"], filter_type, filter_value, range_start, range_end)

    with dwh_engine.connect() as conn:
        heatmap_data = pd.read_sql_query(text(query), conn)
    # if heatmap_data.empty:
    #     return jsonify({})

    pivot_table = heatmap_data.pivot(index="time_of_day", columns="day_of_week", values="order_count").fillna(0)

    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    pivot_table = pivot_table.reindex(columns=day_order)

    time_order = ["Morning", "Afternoon", "Evening", "Night"]
    pivot_table = pivot_table.reindex(index=time_order)

    try:
        fig = px.imshow(
            pivot_table,
            labels=dict(x="", y="", color="Počet objednávok"),
            title="Rozdelenie objednávok",
            color_continuous_scale="RdBu_r"
        )

        fig.update_layout(
            xaxis=dict(tickangle=45, dtick=1),
            yaxis=dict(title="", dtick=1),
            #margin=dict(l=40, r=40, t=40, b=40)
        )
    except Exception as e:
        print(filter_type)
        print(filter_value)
        print(query)
        print(heatmap_data.head())
        return jsonify({})

    del heatmap_data, pivot_table
    gc.collect()

    return fig.to_json()

@dashboard_blueprint.route('/get-gender-distribution', methods=['GET'])
@login_required
def get_gender_distribution():
    filter_type = request.args.get("filter_type")
    filter_value = request.args.get("filter_value")

    filter_type = "year" if filter_type is None else filter_type
    filter_value = str(datetime.datetime.now().year) if filter_value is None else filter_value

    range_start = range_end = None

    if (filter_type == "range"):
        range_start = request.args.get("filter_value_start")
        range_end = request.args.get("filter_value_end")

    query = apply_period_filter(dashboard_queries["gender_distribution"], filter_type, filter_value, range_start, range_end)

    with dwh_engine.connect() as conn:
        gender_df = pd.read_sql_query(text(query), conn)
    # if gender_df.empty:
    #     return jsonify({})

    try:
        fig = px.pie(gender_df, values='customers_count', names='gender', title='Rozdelenie podľa pohlavia')
    except Exception as e:
        print(filter_type)
        print(filter_value)
        print(gender_df.head())
        print(query)
        return jsonify({})

    del gender_df
    gc.collect()

    return fig.to_json()

@dashboard_blueprint.route('/get-age-distribution', methods=['GET'])
@login_required
def get_age_distribution():
    filter_type = request.args.get("filter_type")
    filter_value = request.args.get("filter_value")

    filter_type = "year" if filter_type is None else filter_type
    filter_value = str(datetime.datetime.now().year) if filter_value is None else filter_value

    range_start = range_end = None

    if (filter_type == "range"):
        range_start = request.args.get("filter_value_start")
        range_end = request.args.get("filter_value_end")

    query = apply_period_filter(dashboard_queries["age_distribution"], filter_type, filter_value, range_start, range_end)
    # query = dashboard_queries["age_distribution"]

    with dwh_engine.connect() as conn:
        age_df = pd.read_sql_query(text(query), conn)
    # if age_df.empty:
    #     return jsonify({})

    try:
        fig = px.bar(age_df, x='age_range', y='avg_order_value', title='Priemerná suma objednávky podľa vekového rozpätia')
    except Exception as e:
        print("age_distribution")
        print(e)
        print(filter_type)
        print(filter_value)
        print(query)
        print (age_df.head())
        return jsonify({})

    # print("age_distribution")
    # print(filter_type)
    # print(filter_value)
    # print(query)
    # print (age_df.head())

    del age_df
    gc.collect()

    return fig.to_json()

@dashboard_blueprint.route('/get-gender-quartile-distribution', methods=['GET'])
@login_required
def get_gender_quartile_distribution():
    filter_type = request.args.get("filter_type")
    filter_value = request.args.get("filter_value")

    filter_type = "year" if filter_type is None else filter_type
    filter_value = str(datetime.datetime.now().year) if filter_value is None else filter_value

    range_start = range_end = None

    if (filter_type == "range"):
        range_start = request.args.get("filter_value_start")
        range_end = request.args.get("filter_value_end")

    query = apply_period_filter(dashboard_queries["gender_quartile_distribution"], filter_type, filter_value, range_start, range_end)

    with dwh_engine.connect() as conn:
        quartile_df = pd.read_sql_query(text(query), conn)
    # if quartile_df.empty:
    #     return jsonify({})

    try:
        fig = px.bar(
            quartile_df,
            x="percentage",
            y="quartile",
            color="gender",
            orientation="h",
            title="Pohlavné rozdelenie podľa platobného kvartilu",
            labels={"quartile": "Quartile", "percentage": "Percentage", "gender": "Gender"},
            color_discrete_map={"Male": "teal", "Female": "coral", "Unknown": "gray"}
        )
    except Exception as e:
        print(filter_type)
        print(filter_value)
        print(query)
        print (quartile_df.head())
        return jsonify({})

    fig.update_layout(
        barmode="stack",
        xaxis_title="Percentage",
        yaxis_title="Quartile"
    )

    del quartile_df
    gc.collect()

    return fig.to_json()

@dashboard_blueprint.route('/get-order-status-heatmap', methods=['GET'])
@login_required
def get_order_status_heatmap():
    filter_type = request.args.get("filter_type")
    filter_value = request.args.get("filter_value")

    filter_type = "year" if filter_type is None else filter_type
    filter_value = str(datetime.datetime.now().year) if filter_value is None else filter_value

    range_start = range_end = None

    if (filter_type == "range"):
        range_start = request.args.get("filter_value_start")
        range_end = request.args.get("filter_value_end")

    query = apply_period_filter(dashboard_queries["order_status_heatmap"], filter_type, filter_value, range_start, range_end)

    with dwh_engine.connect() as conn:
        heatmap_df = pd.read_sql_query(text(query), conn)

    try:
        heatmap_pivot = heatmap_df.pivot(index="status", columns="month", values="orders_count").fillna(0)
        fig = px.imshow(
            heatmap_pivot,
            labels=dict(x="Month", y="Order Status", color="Počet objednávok"),
            title="Teplotná mapa stavu objednávok"
        )
    except Exception as e:
        print(filter_type)
        print(filter_value)
        print(query)
        print (heatmap_df.head())
        return jsonify({})

    del heatmap_df, heatmap_pivot
    gc.collect()

    return fig.to_json()