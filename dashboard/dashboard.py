import datetime
import calendar

import numpy as np
from flask import Blueprint, request, jsonify, render_template, redirect, url_for, Response
from flask_login import current_user, login_required
from sqlalchemy import create_engine, text
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import gc
from celeryconfig import PROD_DB_URI, DWH_DB_URI

from auth.base_auth import check_auth, authenticate
from reportsconfig import dashboard_queries, filter_queries
dashboard_blueprint = Blueprint('dashboard', __name__)

prod_engine = create_engine(PROD_DB_URI)
dwh_engine = create_engine(DWH_DB_URI)

def get_date_range_filter():
    filter_type = request.args.get("filter_type")
    filter_value = request.args.get("filter_value")
    current_date = datetime.datetime.now()
    filter_type = "year" if filter_type is None else filter_type
    filter_value = str(current_date.year) if filter_value is None else filter_value
    range_start = range_end = None
    if filter_type == "range":
        range_start = request.args.get("filter_value_start")
        range_end = request.args.get("filter_value_end")
    return current_date, filter_type, filter_value, range_end, range_start

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
    current_date, filter_type, filter_value, range_end, range_start = get_date_range_filter()

    query = apply_period_filter(dashboard_queries["carts_query"], current_date, filter_type, filter_value, range_start, range_end)

    with dwh_engine.connect() as conn:
        carts_df = pd.read_sql_query(text(query), conn)
        carts_df['carts_count'] = carts_df['carts_count'].fillna(0)
        carts_count = carts_df['carts_count'].iloc[0]
        del carts_df

        orders_query = apply_period_filter(dashboard_queries["orders_query"], current_date, filter_type, filter_value, range_start, range_end)
        orders_df = pd.read_sql_query(text(orders_query), conn)
        orders_df['orders_count'] = orders_df['orders_count'].fillna(0)
        orders_count = int(orders_df['orders_count'].iloc[0])
        del orders_df

        orders_paid_query = apply_period_filter(dashboard_queries["orders_paid_query"], current_date, filter_type, filter_value, range_start, range_end)
        orders_paid_df = pd.read_sql_query(text(orders_paid_query), conn)
        orders_paid_df['orders_paid_count'] = orders_paid_df['orders_paid_count'].fillna(0)
        orders_paid_count = int(orders_paid_df['orders_paid_count'].iloc[0])
        orders_paid_df['total_revenue'] = pd.to_numeric(orders_paid_df['total_revenue'], errors='coerce').fillna(0)
        total_revenue = float(orders_paid_df['total_revenue'].iloc[0])
        del orders_paid_df

        summary = {
            'orders_count': orders_count,
            'total_revenue': round(total_revenue, 2),
            'orders_paid_count': orders_paid_count,
            'carts_count': int(carts_count),
            'conversion_rate': (orders_count * 100.0 / carts_count if carts_count > 0 else 0.0),
            'conversion_rate_paid': (orders_paid_count * 100.0 / carts_count if carts_count > 0 else 0.0),
        }

        summary['conversion_rate'] = round(float(summary['conversion_rate']), 2)

    gc.collect()
    return jsonify(summary if summary else {})

@dashboard_blueprint.route('/get-period-revenue', methods=['GET'])
@login_required
def get_period_revenue():
    current_date, filter_type, filter_value, range_end, range_start = get_date_range_filter()

    query = apply_period_filter(dashboard_queries["period_revenue"], current_date, filter_type, filter_value, range_start, range_end)

    with dwh_engine.connect() as conn:
        revenue_df = pd.read_sql_query(text(query), conn)
        revenue_df['total_revenue'] = revenue_df['total_revenue'].round(0)

    try:
        bar_trace = go.Bar(
            x=revenue_df['period'],
            y=revenue_df['total_revenue'],
            name='Príjmy',
            # marker=dict(color='rgb(55, 83, 109)')
        )

        x = np.arange(len(revenue_df))
        y = revenue_df['total_revenue'].values

        slope, intercept = 0, 0

        if len(x) < 2:
            intercept = y[0] if len(y) > 0 else 0
        elif len(y) > 0:
            slope, intercept = np.polyfit(x, y, 1)

        revenue_df['lin_reg'] = slope * x + intercept

        trend_trace_lr = go.Scatter(
            x=revenue_df['period'],
            y=revenue_df['lin_reg'],
            mode='lines',
            name='Trend (lineárna reg.)',
            line=dict(color='green', dash='dot')
        )

        data = [bar_trace, trend_trace_lr,]

        layout = go.Layout(
            title='Príjmy',
            height=400,
            grid=dict(rows=1, columns=1, pattern='independent'),
            xaxis=dict(
                tickmode="linear",
                dtick=1,
                title="Obdobie",
                type="category",
                domain=[0, 1]
            ),
            yaxis=dict(
                title="Príjmy",
                domain=[0, 1],
            ),
            autosize=True,
        )

        fig = go.Figure(data=data, layout=layout)


    except Exception as e:
        return jsonify({})

    del revenue_df
    gc.collect()
    return Response(fig.to_json(), content_type='application/json')

@dashboard_blueprint.route('/get-orders-heatmap', methods=['GET'])
@login_required
def get_orders_heatmap():
    current_date, filter_type, filter_value, range_end, range_start = get_date_range_filter()

    query = apply_period_filter(dashboard_queries["orders_heatmap"], current_date, filter_type, filter_value, range_start, range_end)

    with dwh_engine.connect() as conn:
        heatmap_data = pd.read_sql_query(text(query), conn)

    pivot_table = heatmap_data.pivot(index="time_of_day", columns="day_of_week", values="order_count").fillna(0)

    day_order_labels = ["Pondelok", "Utorok", "Streda", "Štvrtok", "Piatok", "Sobota", "Nedeľa"]
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    pivot_table = pivot_table.reindex(columns=day_order)

    time_order_labels = ["Ráno", "Popoludnie", "Večer", "Noc"]
    time_order = ["Morning", "Afternoon", "Evening", "Night"]
    pivot_table = pivot_table.reindex(index=time_order)

    try:
        heatmap_trace = go.Heatmap(
            z=pivot_table.values,
            x=day_order,
            y=time_order,
            colorscale="RdBu_r",
            colorbar=dict(title="Objednávky")
        )

        data = [heatmap_trace]

        layout = go.Layout(
            title="Distribúcia objednávok počas týždňa",
            height=400,
            xaxis=dict(
                title="",
                tickangle=45,
                tickvals=day_order,
                ticktext=day_order_labels,
                dtick=1,
                domain=[0, 1],
            ),
            yaxis=dict(
                title="",
                dtick=1,
                tickvals=time_order,
                ticktext=time_order_labels,
                domain=[0, 1],
                autorange="reversed",
            ),
            autosize=True
        )

        fig = go.Figure(data=data, layout=layout)
    except Exception as e:
        return jsonify({})

    del heatmap_data, pivot_table
    gc.collect()

    return Response(fig.to_json(), content_type='application/json')

@dashboard_blueprint.route('/get-carrier-revenue-orders-distribution', methods=['GET'])
@login_required
def get_carrier_revenue_orders_distribution():
    current_date, filter_type, filter_value, range_end, range_start = get_date_range_filter()

    query = apply_period_filter(dashboard_queries["carrier_revenue_orders_distribution"], current_date, filter_type, filter_value, range_start, range_end)

    with dwh_engine.connect() as conn:
        carrier_df = pd.read_sql_query(text(query), conn)
        carrier_df['carrier'] = carrier_df['carrier'].fillna('Neuvedené')

    try:
        area_revenue = go.Scatter(
            x=carrier_df['carrier'],
            y=carrier_df['total_revenue'],
            fill='tozeroy',
            mode='lines+markers',
            name='Príjmy',
            line=dict(color='rgba(0, 100, 200, 0.7)', width=2)
        )

        area_count = go.Scatter(
            x=carrier_df['carrier'],
            y=carrier_df['total_count'],
            fill='tozeroy',
            mode='lines+markers',
            name='Objednávky',
            line=dict(color='rgba(255, 150, 0, 0.6)', width=2),
            yaxis='y2',
        )

        data = [area_revenue, area_count,]

        layout = go.Layout(
            title='Príjmy / objednávky podľa dopravcu',
            height=400,
            xaxis=dict(title="Dopravca"),
            yaxis=dict(title="Príjmy"),
            yaxis2=dict(
                title="Objednávky",
                overlaying='y',
                side='right'
            ),
            autosize=True,
        )

        fig = go.Figure(data=data, layout=layout)

    except Exception as e:
        return jsonify({})

    del carrier_df
    gc.collect()

    return Response(fig.to_json(), content_type='application/json')

@dashboard_blueprint.route('/get-top-manufacturer-revenue-distribution', methods=['GET'])
@login_required
def get_top_manufacturer_revenue_distribution():
    current_date, filter_type, filter_value, range_end, range_start = get_date_range_filter()

    query = apply_period_filter(dashboard_queries["top_manufacturer_revenue_distribution"], current_date, filter_type, filter_value,
                                range_start, range_end)

    with dwh_engine.connect() as conn:
        tmr_df = pd.read_sql_query(text(query), conn)

    try:
        h_bar_trace = go.Bar(
            x=tmr_df['total_revenue'],
            y=tmr_df['manufacturer'],
            orientation='h',
            name='Príjmy',
            # marker=dict(color='rgb(55, 83, 109)')
        )

        data = [h_bar_trace,]

        layout = go.Layout(
            title='Top 10 značiek podľa objemu predaja',
            height=400,
            grid=dict(rows=1, columns=1, pattern='independent'),
            xaxis=dict(
                title="Príjmy",
                domain=[0, 1]
            ),
            yaxis=dict(
                title="Značka",
                domain=[0, 1],
                autorange="reversed",
            ),
            autosize=True,
        )

        fig = go.Figure(data=data, layout=layout)
    except Exception as e:
        return jsonify({})

    gc.collect()

    return Response(fig.to_json(), content_type='application/json')

@dashboard_blueprint.route('/get-top-market-group-revenue-distribution', methods=['GET'])
@login_required
def get_market_group_revenue_distribution():
    current_date, filter_type, filter_value, range_end, range_start = get_date_range_filter()

    query = apply_period_filter(dashboard_queries["market_group_revenue_distribution"], current_date, filter_type, filter_value,
                                range_start, range_end)

    with dwh_engine.connect() as conn:
        tmgr_df = pd.read_sql_query(text(query), conn)
        tmgr_df['total_revenue'] = tmgr_df['total_revenue'].round(0)
        tmgr_df['parent'] = ''


    try:
        treemap_trace = go.Treemap(
            labels=tmgr_df['market_group'],
            parents=tmgr_df['parent'],
            values=tmgr_df['total_revenue'],
            hoverinfo="label+value+percent parent",
            textinfo="label+value+percent parent",
            marker=dict(
                line=dict(width=0),
            ),
        )

        data = [treemap_trace,]

        layout = go.Layout(
            title='Rozdelenie príjmov podľa marketingovej kategórie',
            height=400,
            autosize=True,
        )

        fig = go.Figure(data=data, layout=layout)
    except Exception as e:
        return jsonify({})

    gc.collect()

    return Response(fig.to_json(), content_type='application/json')

@dashboard_blueprint.route('/get-gender-distribution', methods=['GET'])
@login_required
def get_gender_distribution():
    current_date, filter_type, filter_value, range_end, range_start = get_date_range_filter()

    query = apply_period_filter(dashboard_queries["gender_distribution"], current_date, filter_type, filter_value, range_start, range_end)

    with dwh_engine.connect() as conn:
        gender_df = pd.read_sql_query(text(query), conn)
        gender_df['gender'] = gender_df['gender'].fillna('Neuvedené')

    try:
        pie_trace = go.Pie(
            labels=gender_df['gender'],
            values=gender_df['customers_count'],
            name="Rozdelenie zákazníkov podľa pohlavia",
            textinfo="label+percent",
            hoverinfo="label+value+percent",
            domain=dict(row=0, column=0),
            hole=0.5,
        )

        data = [pie_trace]

        layout = go.Layout(
            title="Rozdelenie zákazníkov podľa pohlavia",
            height=400,
            grid=dict(rows=1, columns=1, pattern='independent'),
            autosize=True
        )

        fig = go.Figure(data=data, layout=layout)
    except Exception as e:
        return jsonify({})

    del gender_df
    gc.collect()

    return Response(fig.to_json(), content_type='application/json')