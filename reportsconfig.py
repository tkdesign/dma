filter_queries = {
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
}

dashboard_queries = {
    "carts_query": """
    SELECT COUNT(DISTINCT cartid_bk) AS carts_count
    FROM fact_cart_line
    WHERE date_sk IN (SELECT date_key FROM dim_date WHERE {filter})
    """,
    "orders_query": """
    SELECT DISTINCT
        orderid_bk,
        customer_sk,
        MAX(paid_tax_incl) AS total_paid_tax_incl
    FROM fact_order_line
    WHERE date_sk IN (SELECT date_key FROM dim_date WHERE {filter})
    GROUP BY orderid_bk, customer_sk
    """,
    "customer_query": """
    SELECT dc.customer_key, dc.birthdate
    FROM dim_customer dc
    WHERE {valid_customer_filter}
    """,
    "period_revenue":"""
    SELECT
        TO_CHAR(d.date, '{date_format}') AS period,
        SUM(fo.amount_tax_incl) AS total_revenue
    FROM fact_order_line fo
    JOIN dim_date d ON fo.date_sk = d.date_key
    JOIN dim_product dp ON fo.product_sk = dp.product_key
    WHERE {filter} AND {valid_product_filter}
    GROUP BY period
    ORDER BY period;
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
        WHERE {filter}
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
    WHERE dc.active = TRUE AND {valid_customer_filter}
    GROUP BY dc.gender;
    """,
    "age_distribution": """
    SELECT 
            CASE 
                WHEN dc.birthdate IS NULL THEN 'Neuvedené'
                WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, dc.birthdate)) < 0 THEN 'Neuvedené'
                ELSE FLOOR(EXTRACT(YEAR FROM AGE(CURRENT_DATE, dc.birthdate)) / 10) * 10 || '-' || 
                     (FLOOR(EXTRACT(YEAR FROM AGE(CURRENT_DATE, dc.birthdate)) / 10) * 10 + 9)
            END AS age_range,
            AVG(fo.paid_tax_incl) AS avg_order_value
        FROM fact_order_line fo
        JOIN dim_customer dc ON fo.customer_sk = dc.customer_key
        WHERE fo.date_sk IN (SELECT date_key FROM dim_date WHERE {filter}) AND {valid_customer_filter}
        GROUP BY 
            CASE 
                WHEN dc.birthdate IS NULL THEN 'Neuvedené'
                WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, dc.birthdate)) < 0 THEN 'Neuvedené'
                ELSE FLOOR(EXTRACT(YEAR FROM AGE(CURRENT_DATE, dc.birthdate)) / 10) * 10 || '-' || 
                     (FLOOR(EXTRACT(YEAR FROM AGE(CURRENT_DATE, dc.birthdate)) / 10) * 10 + 9)
            END
        ORDER BY age_range;
    """,
}

reports_queries = {
    "gender_distribution": {
        "title": "Rozdelenie zákazníkov podľa pohlavia",
        "data_type": "diagram",
        "diagram_type": "bar",
        "show_diagram_table": True,
        "query": """
        SELECT 
            dc.gender, 
            COUNT(dc.customer_key) AS customers_count
        FROM dim_customer dc
        WHERE dc.active = TRUE AND {valid_customer_filter}
        GROUP BY dc.gender;
        """,
    },
    "age_distribution": {
        "title": "Priemerná suma objednávky podľa veku zákazníkov",
        "data_type": "diagram",
        "diagram_type": "pie",
        "show_diagram_table": True,
        "query": """
        SELECT 
            FLOOR(EXTRACT(YEAR FROM AGE(CURRENT_DATE, dc.birthdate)) / 10) * 10 AS age_range,
            AVG(fo.paid_tax_incl) AS avg_order_value
        FROM fact_order_line fo
        JOIN dim_customer dc ON fo.customer_sk = dc.customer_key
        WHERE fo.date_sk IN (SELECT date_key FROM dim_date WHERE {filter}) AND {valid_customer_filter}
        GROUP BY age_range
        ORDER BY age_range;
        """
    },
    "product_group_revenue": {
        "title": "Výnosy z marketingových kampaní",
        "data_type": "diagram",
        "diagram_type": "bar",
        "show_diagram_table": True,
        "query": """
        SELECT
            TO_CHAR(d.date, '{date_format}') AS period,
            SUM(fo.amount_tax_incl) AS total_revenue
        FROM fact_order_line fo
        JOIN dim_date d ON fo.date_sk = d.date_key
        JOIN dim_product dp ON fo.product_sk = dp.product_key
        WHERE {filter} AND {valid_product_filter}
        AND (
            {group_filter}
        )
        GROUP BY period
        ORDER BY period;
        """,
        "subfilters": {
            "market_group": {
                "title": "Marketingová kampaň",
                "menu_query": """
                SELECT market_group
                FROM dim_product p
                WHERE {filter}
                GROUP BY market_group
                """
            },
            "market_subgroup": {
                "title": "Marketingová podkampaň",
                "menu_query": """
                SELECT market_subgroup
                FROM dim_product p
                WHERE {filter}
                GROUP BY market_subgroup;
                """
            },
        }
    },
    "product_gender_revenue": {
        "title": "Výnosy podľa rodovej kategórie tovaru",
        "data_type": "diagram",
        "diagram_type": "bar",
        "show_diagram_table": True,
        "query": """
        SELECT
            TO_CHAR(d.date, '{date_format}') AS period,
            SUM(fo.amount_tax_incl) AS total_revenue
        FROM fact_order_line fo
        JOIN dim_date d ON fo.date_sk = d.date_key
        JOIN dim_product dp ON fo.product_sk = dp.product_key
        WHERE {filter} AND {valid_product_filter}
        AND (
            {group_filter}
        )
        GROUP BY period
        ORDER BY period;
        """,
        "subfilters": {
            "market_gender": {
                "title": "Rodová kategória",
                "menu_query": """
                SELECT market_gender
                FROM dim_product p
                WHERE {filter}
                GROUP BY p.market_gender;
                """
            },
        }
    },
}

