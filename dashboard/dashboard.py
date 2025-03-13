import datetime
import calendar

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

def apply_period_filter(original_query, current_date, filter_type, filter_value, range_start, range_end):
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

        end_day = calendar.monthrange(filter_value_year, filter_value_month)[1]

        date_filter = f"month = {filter_value_month} AND year = {filter_value_year}"
        valid_filter = "{alias}.valid_from <= '{filter_value_year}-{filter_value_month}-{end_day}' AND {alias}.valid_to >= '{filter_value_year}-{filter_value_month}-01'".format(alias="{alias}", filter_value_year=filter_value_year, filter_value_month=filter_value_month, end_day=end_day)
        date_format = "YYYY-MM-DD"
    elif filter_type == "quarter" and filter_value:
        try:
            filter_value_parts = filter_value.split("-")
            filter_value_year = int(filter_value_parts[0])
            filter_value_quarter = int(filter_value_parts[1])
        except ValueError:
            filter_value_quarter = current_quarter
            filter_value_year = current_year

        start_month = (filter_value_quarter-1)*3 + 1
        end_month = filter_value_quarter*3
        end_day = calendar.monthrange(filter_value_year, end_month)[1]

        date_filter = f"quarter = {filter_value_quarter} AND year = {filter_value_year}"
        valid_filter = "{alias}.valid_from <= '{filter_value_year}-{end_month}-{end_day}' AND {alias}.valid_to >= '{filter_value_year}-{start_month}-01'".format(alias="{alias}", filter_value_year=filter_value_year, start_month=start_month, end_month=end_month, end_day=end_day)
        date_format = "MM"
    elif filter_type == "year" and filter_value:
        try:
            filter_value = int(filter_value)
        except ValueError:
            filter_value = current_year

        date_filter = f"year = {filter_value}"
        valid_filter = "{alias}.valid_from <= '{filter_value}-12-31' AND {alias}.valid_to >= '{filter_value}-01-01'".format(alias="{alias}", filter_value=filter_value)
        date_format = "MM"
    elif filter_type == "range" and range_start or range_end:
        try:
            date_start = range_start
            filter_value_start = datetime.datetime.strptime(date_start, "%Y-%m-%d").date()
            date_end = range_end
            filter_value_end = datetime.datetime.strptime(date_end, "%Y-%m-%d").date()
        except ValueError:
            filter_value_start = filter_value_end = datetime.datetime.strptime(current_date, "%Y-%m-%d").date()

        date_filter = f"date BETWEEN '{filter_value_start}' AND '{filter_value_end}'"
        valid_filter = "{alias}.valid_from <= '{range_end}' AND {alias}.valid_to >= '{range_start}'".format(alias="{alias}", range_start=range_start, range_end=range_end)
        date_format = "YYYY-MM-DD"
    else:
        try:
            filter_value = int(filter_value)
        except ValueError:
            filter_value = current_year

        date_filter = f"year = {filter_value}"
        valid_filter = "{alias}.valid_from <= '{filter_value}-12-31' AND {alias}.valid_to >= '{filter_value}-01-01'".format(
            alias="{alias}", filter_value=filter_value)
        date_format = "MM"

    query = original_query.format(filter=date_filter, filter_raw=f"'{date_filter}'", valid_customer_filter=valid_filter.format(alias="dc"), valid_product_filter=valid_filter.format(alias="dp"), valid_order_state_filter=valid_filter.format(alias="dos"), group_filter="{group_filter}", date_format=date_format)
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

    return render_template('dashboard/dashboard.html', title='DMA - Informačný panel', page='dashboard', months=months, quarters=quarters, years=years)

@dashboard_blueprint.route('/get-summary', methods=['GET'])
@login_required
def get_summary():
    filter_type = request.args.get("filter_type")
    filter_value = request.args.get("filter_value")

    current_date = datetime.datetime.now()

    filter_type = "year" if filter_type is None else filter_type
    filter_value = str(current_date.year) if filter_value is None else filter_value

    range_start = range_end = None

    if (filter_type == "range"):
        range_start = request.args.get("filter_value_start")
        range_end = request.args.get("filter_value_end")

    with dwh_engine.connect() as conn:
        carts_query = apply_period_filter(dashboard_queries["carts_query"], current_date, filter_type, filter_value, range_start, range_end)

        carts_df = pd.read_sql_query(text(carts_query), conn)
        carts_count = carts_df['carts_count'].iloc[0]
        del carts_df

        orders_query = apply_period_filter(dashboard_queries["orders_query"], current_date, filter_type, filter_value, range_start, range_end)
        orders_df = pd.read_sql_query(text(orders_query), conn)

        customer_query = apply_period_filter(dashboard_queries["customer_query"], current_date, filter_type, filter_value, range_start, range_end)
        customers_df = pd.read_sql_query(text(customer_query), conn)

        merged_df = orders_df.merge(customers_df,
                                  left_on='customer_sk',
                                  right_on='customer_key',
                                  how='left')

        del orders_df, customers_df

        merged_df['birthdate'] = pd.to_datetime(merged_df['birthdate'], errors='coerce')

        summary = {
            'orders_count': merged_df['orderid_bk'].nunique(),
            'total_revenue': round(merged_df['total_paid_tax_incl'].sum(), 2),
            'avg_order_value': 0.0 if merged_df.empty else merged_df['total_paid_tax_incl'].mean(),
            'carts_count': int(carts_count),
            'conversion_rate': (merged_df['orderid_bk'].nunique() * 100.0 / carts_count
                              if carts_count > 0 else 0.0),
            'avg_age': (pd.Timestamp.now().year - merged_df['birthdate'].dt.year).mean()
                       if not merged_df['birthdate'].isna().all() else 0.0
        }

        summary['orders_count'] = int(summary['orders_count'])
        summary['total_revenue'] = float(summary['total_revenue'])
        summary['avg_order_value'] = float(summary['avg_order_value'])
        summary['conversion_rate'] = round(float(summary['conversion_rate']), 2)
        summary['avg_age'] = float(summary['avg_age']) if summary['avg_age'] else 0.0
        del merged_df

    gc.collect()
    return jsonify(summary if summary else {})

@dashboard_blueprint.route('/get-period-revenue', methods=['GET'])
@login_required
def get_period_revenue():
    filter_type = request.args.get("filter_type")
    filter_value = request.args.get("filter_value")

    current_date = datetime.datetime.now()

    filter_type = "year" if filter_type is None else filter_type
    filter_value = str(current_date.year) if filter_value is None else filter_value

    range_start = range_end = None

    if (filter_type == "range"):
        range_start = request.args.get("filter_value_start")
        range_end = request.args.get("filter_value_end")

    query = apply_period_filter(dashboard_queries["period_revenue"], current_date, filter_type, filter_value, range_start, range_end)

    with dwh_engine.connect() as conn:
        revenue_df = pd.read_sql_query(text(query), conn)

    try:
        fig = px.bar(revenue_df, x='period', y='total_revenue', title='Príjmy')
        fig.update_layout(xaxis=dict(tickmode="linear", dtick=1, title="Period", type="category"), yaxis=dict(title="Príjmy"), margin=dict(l=40, r=40, t=40, b=40))
    except Exception as e:
        return jsonify({})

    del revenue_df
    gc.collect()
    return fig.to_json()

@dashboard_blueprint.route('/get-orders-heatmap', methods=['GET'])
@login_required
def get_orders_heatmap():
    filter_type = request.args.get("filter_type")
    filter_value = request.args.get("filter_value")

    current_date = datetime.datetime.now()

    filter_type = "year" if filter_type is None else filter_type
    filter_value = str(current_date.year) if filter_value is None else filter_value

    range_start = range_end = None

    if (filter_type == "range"):
        range_start = request.args.get("filter_value_start")
        range_end = request.args.get("filter_value_end")

    query = apply_period_filter(dashboard_queries["orders_heatmap"], current_date, filter_type, filter_value, range_start, range_end)

    with dwh_engine.connect() as conn:
        heatmap_data = pd.read_sql_query(text(query), conn)

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
        return jsonify({})

    del heatmap_data, pivot_table
    gc.collect()

    return fig.to_json()

@dashboard_blueprint.route('/get-gender-distribution', methods=['GET'])
@login_required
def get_gender_distribution():
    filter_type = request.args.get("filter_type")
    filter_value = request.args.get("filter_value")

    current_date = datetime.datetime.now()

    filter_type = "year" if filter_type is None else filter_type
    filter_value = str(current_date.year) if filter_value is None else filter_value

    range_start = range_end = None

    if (filter_type == "range"):
        range_start = request.args.get("filter_value_start")
        range_end = request.args.get("filter_value_end")

    query = apply_period_filter(dashboard_queries["gender_distribution"], current_date, filter_type, filter_value, range_start, range_end)

    with dwh_engine.connect() as conn:
        gender_df = pd.read_sql_query(text(query), conn)

    try:
        fig = px.pie(gender_df, values='customers_count', names='gender', title='Rozdelenie podľa pohlavia')
    except Exception as e:
        return jsonify({})

    del gender_df
    gc.collect()

    return fig.to_json()

@dashboard_blueprint.route('/get-age-distribution', methods=['GET'])
@login_required
def get_age_distribution():
    filter_type = request.args.get("filter_type")
    filter_value = request.args.get("filter_value")

    current_date = datetime.datetime.now()

    filter_type = "year" if filter_type is None else filter_type
    filter_value = str(current_date.year) if filter_value is None else filter_value

    range_start = range_end = None

    if (filter_type == "range"):
        range_start = request.args.get("filter_value_start")
        range_end = request.args.get("filter_value_end")

    query = apply_period_filter(dashboard_queries["age_distribution"], current_date, filter_type, filter_value, range_start, range_end)

    with dwh_engine.connect() as conn:
        age_df = pd.read_sql_query(text(query), conn)

    try:
        fig = px.bar(age_df, x='age_range', y='avg_order_value', title='Priemerná suma objednávky podľa vekového rozpätia')
    except Exception as e:
        return jsonify({})

    del age_df
    gc.collect()

    return fig.to_json()

@dashboard_blueprint.route('/get-gender-quartile-distribution', methods=['GET'])
@login_required
def get_gender_quartile_distribution():
    filter_type = request.args.get("filter_type")
    filter_value = request.args.get("filter_value")

    current_date = datetime.datetime.now()

    filter_type = "year" if filter_type is None else filter_type
    filter_value = str(current_date.year) if filter_value is None else filter_value

    range_start = range_end = None

    if (filter_type == "range"):
        range_start = request.args.get("filter_value_start")
        range_end = request.args.get("filter_value_end")

    query = apply_period_filter(dashboard_queries["gender_quartile_distribution"], current_date, filter_type, filter_value, range_start, range_end)

    with dwh_engine.connect() as conn:
        quartile_df = pd.read_sql_query(text(query), conn)

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

    current_date = datetime.datetime.now()

    filter_type = "year" if filter_type is None else filter_type
    filter_value = str(current_date.year) if filter_value is None else filter_value

    range_start = range_end = None

    if (filter_type == "range"):
        range_start = request.args.get("filter_value_start")
        range_end = request.args.get("filter_value_end")

    query = apply_period_filter(dashboard_queries["order_status_heatmap"], current_date, filter_type, filter_value, range_start, range_end)

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
        return jsonify({})

    del heatmap_df, heatmap_pivot
    gc.collect()

    return fig.to_json()