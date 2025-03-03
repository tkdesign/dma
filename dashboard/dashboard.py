import datetime

from flask import Blueprint, request, jsonify, render_template, redirect, url_for
from flask_login import current_user, login_required
from sqlalchemy import create_engine, text
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
import json
import gc
from celeryconfig import PROD_DB_URI, STAGE_DB_URI, DWH_DB_URI

from auth.base_auth import check_auth, authenticate

dashboard_blueprint = Blueprint('dashboard', __name__)

@dashboard_blueprint.before_request
def require_http_auth():
    auth = request.authorization
    if not auth or not check_auth(auth.username, auth.password):
        return authenticate()

prod_engine = create_engine(PROD_DB_URI)
dwh_engine = create_engine(DWH_DB_URI)

queries = {
    "min_date": """
        SELECT MIN(date) AS min_date FROM (
            SELECT MIN(d.date) AS date FROM fact_cart_line fc
            JOIN dim_date d ON fc.date_sk = d.date_key
            UNION ALL
            SELECT MIN(d.date) AS date FROM fact_order_line fo
            JOIN dim_date d ON fo.date_sk = d.date_key
        ) AS min_dates;
    """,
    "months": """
        SELECT DISTINCT month_name, year, month
        FROM dim_date
        WHERE date >= :min_date AND date <= CURRENT_DATE
        ORDER BY year DESC, month;
    """,
        "quarters": """
        SELECT DISTINCT quarter, year
        FROM dim_date
        WHERE date >= :min_date AND date <= CURRENT_DATE
        ORDER BY year DESC, quarter;
    """,
        "years": """
        SELECT DISTINCT year
        FROM dim_date
        WHERE date >= :min_date AND date <= CURRENT_DATE
        ORDER BY year DESC;
    """,
    "summary": """
    WITH total_carts AS (
        SELECT COUNT(DISTINCT cartid_bk) AS carts_count
        FROM fact_cart_line
        WHERE date_sk IN (SELECT date_key FROM dim_date WHERE {filter})
    )
    SELECT 
        COUNT(DISTINCT fo.orderid_bk) AS orders_count,
        CAST(SUM(fo.paid_tax_incl) AS NUMERIC(20,2)) AS total_revenue,
        AVG(fo.paid_tax_incl) AS avg_order_value,
        (SELECT carts_count FROM total_carts) AS carts_count,
        CASE 
            WHEN (SELECT carts_count FROM total_carts) = 0 THEN 0
            ELSE ROUND(COUNT(DISTINCT fo.orderid_bk) * 100.0 / (SELECT carts_count FROM total_carts), 2)
        END AS conversion_rate,
        AVG(EXTRACT(YEAR FROM AGE(dc.birthdate))) AS avg_age
    FROM fact_order_line fo
    LEFT JOIN dim_customer dc ON fo.customer_sk = dc.customer_key
    WHERE fo.date_sk IN (SELECT date_key FROM dim_date WHERE {filter});
    """,
    "monthly_revenue": """
    SELECT 
        d.month, 
        d.year,
        SUM(fo.paid_tax_incl) AS total_revenue
    FROM fact_order_line fo
    JOIN dim_date d ON fo.date_sk = d.date_key
    WHERE d.year = 2024
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month;
    """,
    "orders_heatmap": """
    WITH time_categories AS (
        SELECT 
            fo.orderid_bk,
            CASE
                WHEN dt.hour BETWEEN 6 AND 11 THEN 'Morning'
                WHEN dt.hour BETWEEN 12 AND 17 THEN 'Afternoon'
                WHEN dt.hour BETWEEN 18 AND 23 THEN 'Evening'
                ELSE 'Night'
            END AS time_of_day,
            dd.day_of_week,
            dd.day_name
        FROM fact_order_line fo
        JOIN dim_time dt ON fo.time_sk = dt.time_key
        JOIN dim_date dd ON fo.date_sk = dd.date_key
        WHERE dd.year = 2024
    )
    SELECT
        time_of_day,
        day_name AS day_of_week,
        COUNT(*) AS order_count
    FROM time_categories
    GROUP BY time_of_day, day_name, day_of_week
    ORDER BY 
        CASE time_of_day 
            WHEN 'Morning' THEN 1
            WHEN 'Afternoon' THEN 2
            WHEN 'Evening' THEN 3
            WHEN 'Night' THEN 4
        END,
        CASE day_name 
            WHEN 'Monday' THEN 1
            WHEN 'Tuesday' THEN 2
            WHEN 'Wednesday' THEN 3
            WHEN 'Thursday' THEN 4
            WHEN 'Friday' THEN 5
            WHEN 'Saturday' THEN 6
            WHEN 'Sunday' THEN 7
        END;
    """,
    "gender_distribution": """
    SELECT 
        dc.gender, 
        COUNT(dc.customer_key) AS customers_count
    FROM dim_customer dc
    WHERE dc.active = TRUE
    GROUP BY dc.gender;
    """,
    "age_distribution": """
    SELECT 
        FLOOR(EXTRACT(YEAR FROM AGE(CURRENT_DATE, dc.birthdate)) / 10) * 10 AS age_range,
        AVG(fo.paid_tax_incl) AS avg_order_value
    FROM fact_order_line fo
    JOIN dim_customer dc ON fo.customer_sk = dc.customer_key
    WHERE fo.date_sk IN (SELECT date_key FROM dim_date WHERE year = 2024)
    GROUP BY age_range
    ORDER BY age_range;
    """,
    "gender_quartile_distribution": """
    WITH quartiles AS (
        SELECT 
            fo.customer_sk,
            dc.gender,
            fo.paid_tax_incl,
            NTILE(4) OVER (ORDER BY fo.paid_tax_incl DESC) AS quartile
        FROM fact_order_line fo
        JOIN dim_customer dc ON fo.customer_sk = dc.customer_key
        WHERE fo.date_sk IN (SELECT date_key FROM dim_date WHERE year = 2024)
    ),
    quartile_gender_count AS (
        SELECT 
            quartile,
            gender,
            COUNT(*) AS count
        FROM quartiles
        GROUP BY quartile, gender
    ),
    quartile_totals AS (
        SELECT 
            quartile,
            SUM(count) AS total_count
        FROM quartile_gender_count
        GROUP BY quartile
    )
    SELECT 
        qgc.quartile,
        qgc.gender,
        ROUND(qgc.count * 100.0 / qt.total_count, 2) AS percentage
    FROM quartile_gender_count qgc
    JOIN quartile_totals qt ON qgc.quartile = qt.quartile
    ORDER BY qgc.quartile, qgc.gender;
    """,
    "order_status_heatmap": """
    SELECT
        dos.current_state AS status,
        dd.month,
        COUNT(foh.orderid_bk) AS orders_count
    FROM fact_order_history foh
    JOIN dim_order_state dos ON foh.orderstate_sk = dos.orderstate_key
    JOIN dim_date dd ON foh.date_sk = dd.date_key
    WHERE dd.year = 2024
    GROUP BY dos.current_state, dd.month
    ORDER BY dd.month;
""",
}

@dashboard_blueprint.route('/dashboard', methods=['GET'])
@login_required
def dashboard_index():
    if not current_user.is_authenticated:
        return redirect(url_for('auth.auth_login_form'))

    with dwh_engine.connect() as conn:
        min_date_row = conn.execute(text(queries["min_date"])).fetchone()
        # min_date = min_date_row.min_date.strftime('%Y-%m-%d')
        months = conn.execute(text(queries["months"]), parameters={"min_date": min_date_row.min_date}).fetchall()
        quarters = conn.execute(text(queries["quarters"]), parameters = {"min_date": min_date_row.min_date}).fetchall()
        years = conn.execute(text(queries["years"]), parameters ={"min_date": min_date_row.min_date}).fetchall()

    return render_template('dashboard/dashboard.html', title='DMA - Dashboard', page='dashboard', months=months, quarters=quarters, years=years)

@dashboard_blueprint.route('/get-summary', methods=['GET'])
def get_summary():
    filter_type = request.args.get("filter_type")
    filter_value = request.args.get("filter_value")

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
        query = queries["summary"].format(filter="month = " + str(filter_value_month) + " AND year = " + str(filter_value_year))
    elif filter_type == "quarter" and filter_value:
        try:
            filter_value_parts = filter_value.split("-")
            filter_value_year = int(filter_value_parts[0])
            filter_value_quarter = int(filter_value_parts[1])
        except ValueError:
            filter_value_quarter = current_quarter
            filter_value_year = current_year
        query = queries["summary"].format(filter="quarter = " + str(filter_value_quarter) + " AND year = " + str(filter_value_year))
    elif filter_type == "year" and filter_value:
        try:
            filter_value = int(filter_value)
        except ValueError:
            filter_value = 0
        query = queries["summary"].format(filter="year = " + str(filter_value))
    elif filter_type == "range" and (request.args.get("filter_value_start") or request.args.get("filter_value_end")):
        try:
            date_start = request.args.get("filter_value_start")
            filter_value_start = datetime.datetime.strptime(date_start, "%Y-%m-%d").date()
            date_end = request.args.get("filter_value_end")
            filter_value_end = datetime.datetime.strptime(date_end, "%Y-%m-%d").date()
        except ValueError:
            filter_value_start = filter_value_end = datetime.datetime.strptime(current_date, "%Y-%m-%d").date()
        query = queries["summary"].format(
            filter="date BETWEEN '" + str(filter_value_start) + "' AND '" + str(filter_value_end) + "'")
    else:
        current_year = datetime.datetime.now().year
        query = queries["summary"].format(filter="year = " + str(current_year))
    with dwh_engine.connect() as conn:
        summary_df = pd.read_sql_query(query, conn)
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

@dashboard_blueprint.route('/get-monthly-revenue', methods=['GET'])
def get_monthly_revenue():
    with dwh_engine.connect() as conn:
        revenue_df = pd.read_sql_query(queries["monthly_revenue"], conn)
    fig = px.bar(revenue_df, x='month', y='total_revenue', title='Mesačné príjmy')
    fig.update_layout(
        xaxis=dict(
            tickmode="linear",
            dtick=1,
            title="Mesiac",
        ),
        yaxis=dict(title="Príjmy"),
        margin=dict(l=40, r=40, t=40, b=40)
    )
    del revenue_df
    gc.collect()
    return fig.to_json()

@dashboard_blueprint.route('/get-orders-heatmap', methods=['GET'])
def get_orders_heatmap():
    with dwh_engine.connect() as conn:
        heatmap_data = pd.read_sql_query(queries["orders_heatmap"], conn)

    pivot_table = heatmap_data.pivot(index="time_of_day", columns="day_of_week", values="order_count").fillna(0)

    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    pivot_table = pivot_table.reindex(columns=day_order)

    time_order = ["Morning", "Afternoon", "Evening", "Night"]
    pivot_table = pivot_table.reindex(index=time_order)

    fig = px.imshow(
        pivot_table,
        labels=dict(x="", y="", color="Počet objednávok"),
        title="Rozdelenie objednávok",
        color_continuous_scale="RdBu_r"
    )

    fig.update_layout(
        xaxis=dict(tickangle=45),
        yaxis=dict(title=""),
        #margin=dict(l=40, r=40, t=40, b=40)
    )

    del heatmap_data, pivot_table
    gc.collect()

    return fig.to_json()

@dashboard_blueprint.route('/get-gender-distribution', methods=['GET'])
def get_gender_distribution():
    with dwh_engine.connect() as conn:
        gender_df = pd.read_sql_query(queries["gender_distribution"], conn)
    fig = px.pie(gender_df, values='customers_count', names='gender', title='Rozdelenie podľa pohlavia')

    del gender_df
    gc.collect()

    return fig.to_json()

@dashboard_blueprint.route('/get-age-distribution', methods=['GET'])
def get_age_distribution():
    with dwh_engine.connect() as conn:
        age_df = pd.read_sql_query(queries["age_distribution"], conn)
    fig = px.bar(age_df, x='age_range', y='avg_order_value', title='Priemerná suma objednávky podľa vekového rozpätia')

    del age_df
    gc.collect()

    return fig.to_json()

@dashboard_blueprint.route('/get-gender-quartile-distribution', methods=['GET'])
def get_gender_quartile_distribution():
    with dwh_engine.connect() as conn:
        quartile_df = pd.read_sql_query(queries["gender_quartile_distribution"], conn)

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

    fig.update_layout(
        barmode="stack",
        xaxis_title="Percentage",
        yaxis_title="Quartile"
    )

    del quartile_df
    gc.collect()

    return fig.to_json()

@dashboard_blueprint.route('/get-order-status-heatmap', methods=['GET'])
def get_order_status_heatmap():
    with dwh_engine.connect() as conn:
        heatmap_df = pd.read_sql_query(queries["order_status_heatmap"], conn)

    heatmap_pivot = heatmap_df.pivot(index="status", columns="month", values="orders_count").fillna(0)

    fig = px.imshow(
        heatmap_pivot,
        labels=dict(x="Month", y="Order Status", color="Počet objednávok"),
        title="Teplotná mapa stavu objednávok"
    )

    del heatmap_df, heatmap_pivot
    gc.collect()

    return fig.to_json()