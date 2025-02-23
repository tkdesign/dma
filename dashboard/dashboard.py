from flask import Blueprint, request, jsonify, render_template, redirect, url_for
from flask_login import current_user, login_required
from sqlalchemy import create_engine, text
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
import json
from celeryconfig import PROD_DB_URI, STAGE_DB_URI, DWH_DB_URI

dashboard_blueprint = Blueprint('dashboard', __name__)

prod_engine = create_engine(PROD_DB_URI)

queries = {
    "summary": """
WITH total_carts AS (
    SELECT COUNT(*) AS carts_count
    FROM ps_cart
    WHERE YEAR(date_add) = 2024
)
SELECT 
    COUNT(DISTINCT o.id_order) AS orders_count,
    CAST(SUM(o.total_paid) AS UNSIGNED) AS total_revenue,
    AVG(o.total_paid) AS avg_order_value,
    (SELECT carts_count FROM total_carts) AS carts_count,
    ROUND(COUNT(DISTINCT o.id_order) / (SELECT carts_count FROM total_carts) * 100, 0) AS conversion_rate,
    AVG(TIMESTAMPDIFF(YEAR, cu.birthday, CURDATE())) AS avg_age
FROM ps_orders o
LEFT JOIN ps_customer cu ON o.id_customer = cu.id_customer
WHERE YEAR(o.date_add) = 2024;
    """,
    "monthly_revenue": """
        SELECT 
            MONTH(o.date_add) AS month, 
            SUM(o.total_paid) AS total_revenue
        FROM ps_orders o
        WHERE YEAR(o.date_add) = 2024
        GROUP BY MONTH(o.date_add);
    """,
    "gender_distribution": """
SELECT 
    g.name AS gender, 
    COUNT(cu.id_customer) AS customers_count
FROM ps_customer cu
LEFT JOIN ps_gender g ON COALESCE(NULLIF(cu.id_gender, 0), 3) = g.id_gender
WHERE cu.active = 1
GROUP BY g.name;

    """,
    "order_status_heatmap": """
        SELECT 
            os.name AS status,
            MONTH(o.date_add) AS month,
            COUNT(o.id_order) AS orders_count
        FROM ps_orders o
        LEFT JOIN ps_order_state os ON o.current_state = os.id_order_state
        WHERE YEAR(o.date_add) = 2024
        GROUP BY os.name, MONTH(o.date_add);
    """,
    "age_distribution": """
        SELECT 
            FLOOR(TIMESTAMPDIFF(YEAR, cu.birthday, CURDATE()) / 10) * 10 AS age_range,
            AVG(o.total_paid) AS avg_order_value
        FROM ps_orders o
        LEFT JOIN ps_customer cu ON o.id_customer = cu.id_customer
        WHERE YEAR(o.date_add) = 2024
        GROUP BY age_range;
    """,
    "gender_quartile_distribution": """
    WITH quartiles AS (
    SELECT 
        cu.id_customer,
        cu.id_gender,
        o.total_paid,
        NTILE(4) OVER (ORDER BY o.total_paid DESC) AS quartile
    FROM ps_customer cu
    JOIN ps_orders o ON cu.id_customer = o.id_customer
    WHERE YEAR(o.date_add) = 2024
),
quartile_gender_count AS (
    SELECT 
        quartile,
        CASE 
            WHEN COALESCE(NULLIF(cu.id_gender, 0), 3) = 1 THEN 'Male'
            WHEN COALESCE(NULLIF(cu.id_gender, 0), 3) = 2 THEN 'Female'
            ELSE 'Unknown'
        END AS gender,
        COUNT(*) AS count
    FROM quartiles cu
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
    ROUND(qgc.count / qt.total_count * 100, 2) AS percentage
FROM quartile_gender_count qgc
JOIN quartile_totals qt ON qgc.quartile = qt.quartile
ORDER BY qgc.quartile, qgc.gender;
    """,
    "orders_heatmap": """
    WITH time_categories AS (
    SELECT
        id_order,
        CASE
            WHEN HOUR(date_add) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN HOUR(date_add) BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN HOUR(date_add) BETWEEN 18 AND 23 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day,
        DAYOFWEEK(date_add) AS day_of_week
    FROM ps_orders
    WHERE YEAR(date_add) = 2024
)
SELECT
    time_of_day,
    CASE
        WHEN day_of_week = 1 THEN 'Sunday'
        WHEN day_of_week = 2 THEN 'Monday'
        WHEN day_of_week = 3 THEN 'Tuesday'
        WHEN day_of_week = 4 THEN 'Wednesday'
        WHEN day_of_week = 5 THEN 'Thursday'
        WHEN day_of_week = 6 THEN 'Friday'
        WHEN day_of_week = 7 THEN 'Saturday'
    END AS day_of_week,
    COUNT(*) AS order_count
FROM time_categories
GROUP BY time_of_day, day_of_week
ORDER BY FIELD(time_of_day, 'Morning', 'Afternoon', 'Evening', 'Night'),
         FIELD(day_of_week, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday');
    """
}

@dashboard_blueprint.route('/dashboard', methods=['GET'])
@login_required
def dashboard_index():
    if not current_user.is_authenticated:
        return redirect(url_for('auth.auth_login_form'))
    return render_template('dashboard/dashboard.html', title='DMA - Dashboard', page='dashboard')

@dashboard_blueprint.route('/get-summary', methods=['GET'])
def get_summary():
    with prod_engine.connect() as conn:
        summary_df = pd.read_sql_query(queries["summary"], conn)
    summary_data = summary_df.iloc[0].to_dict()
    return jsonify(summary_data)


@dashboard_blueprint.route('/get-monthly-revenue', methods=['GET'])
def get_monthly_revenue():
    with prod_engine.connect() as conn:
        revenue_df = pd.read_sql_query(queries["monthly_revenue"], conn)
    fig = px.bar(revenue_df, x='month', y='total_revenue', title='Mesačné príjmy')
    # Обновляем ось X
    fig.update_layout(
        xaxis=dict(
            tickmode="linear",
            dtick=1,
            title="Mesiac",
        ),
        yaxis=dict(title="Príjmy"),
        margin=dict(l=40, r=40, t=40, b=40)
    )
    return fig.to_json()

@dashboard_blueprint.route('/get-gender-distribution', methods=['GET'])
def get_gender_distribution():
    with prod_engine.connect() as conn:
        gender_df = pd.read_sql_query(queries["gender_distribution"], conn)
    fig = px.pie(gender_df, values='customers_count', names='gender', title='Rozdelenie podľa pohlavia')
    return fig.to_json()


@dashboard_blueprint.route('/get-order-status-heatmap', methods=['GET'])
def get_order_status_heatmap():
    with prod_engine.connect() as conn:
        heatmap_df = pd.read_sql_query(queries["order_status_heatmap"], conn)

    heatmap_pivot = heatmap_df.pivot(index="status", columns="month", values="orders_count").fillna(0)

    fig = px.imshow(
        heatmap_pivot,
        labels=dict(x="Month", y="Order Status", color="Počet objednávok"),
        title="Teplotná mapa stavu objednávok"
    )
    return fig.to_json()


@dashboard_blueprint.route('/get-age-distribution', methods=['GET'])
def get_age_distribution():
    with prod_engine.connect() as conn:
        age_df = pd.read_sql_query(queries["age_distribution"], conn)
    fig = px.bar(age_df, x='age_range', y='avg_order_value', title='Priemerná suma objednávky podľa vekového rozpätia')
    return fig.to_json()


@dashboard_blueprint.route('/get-gender-quartile-distribution', methods=['GET'])
def get_gender_quartile_distribution():
    with prod_engine.connect() as conn:
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

    return fig.to_json()


@dashboard_blueprint.route('/get-orders-heatmap', methods=['GET'])
def get_orders_heatmap():
    with prod_engine.connect() as conn:
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

    return fig.to_json()