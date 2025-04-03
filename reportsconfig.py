filter_queries = {
    "min_date": """
    SELECT MIN(date) AS min_date FROM (
        SELECT MIN(d.date) AS date FROM fact_cart_line fc
        JOIN dim_date d ON fc.date_sk = d.date_key
        UNION ALL
        SELECT MIN(dd.date) AS date FROM fact_order_line fo
        JOIN dim_date dd ON fo.date_sk = dd.date_key
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
    "orders_paid_query": """
    SELECT
        COUNT(fo.orderid_bk) AS orders_paid_count,
        SUM(fo.paid_tax_incl) AS total_revenue
    FROM fact_order_history foh
	JOIN fact_order fo ON foh.orderid_bk = fo.orderid_bk
	JOIN dim_date dd ON fo.date_sk = dd.date_key
    WHERE {filter} AND foh.orderstateid_bk = 2
    """,
    "orders_query": """
    SELECT
        COUNT(fo.orderid_bk) AS orders_count
    FROM fact_order fo
	JOIN dim_date dd ON fo.date_sk = dd.date_key
    WHERE {filter}
    """,
    "period_revenue":"""
    SELECT
        TO_CHAR(dd.date, '{date_format}') AS period,
        SUM(fo.paid_tax_incl) AS total_revenue
    FROM fact_order_history foh
    JOIN dim_date dd ON foh.date_sk = dd.date_key
    JOIN fact_order fo ON foh.orderid_bk = fo.orderid_bk
    WHERE {filter} AND foh.orderstateid_bk = 2
    GROUP BY period
    ORDER BY period;
    """,
    "orders_heatmap": """
    WITH time_categories AS (
        SELECT 
            fo.orderid_bk,
            dd.date,
            CASE
                WHEN dt.hour BETWEEN 6 AND 11 THEN 'Morning'
                WHEN dt.hour BETWEEN 12 AND 17 THEN 'Afternoon'
                WHEN dt.hour BETWEEN 18 AND 23 THEN 'Evening'
                ELSE 'Night'
            END AS time_of_day,
            dd.day_name
        FROM fact_order fo
        JOIN dim_time dt ON fo.time_sk = dt.time_key
        JOIN dim_date dd ON fo.date_sk = dd.date_key
        WHERE {filter}
    ),
    daily_counts AS (
        SELECT
            date,
            time_of_day,
            day_name,
            COUNT(*) AS order_count_per_day
        FROM time_categories
        GROUP BY date, time_of_day, day_name
    )
    SELECT
        time_of_day,
        day_name AS day_of_week,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY order_count_per_day) AS order_count
    FROM daily_counts
    GROUP BY time_of_day, day_name
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
    "carrier_revenue_orders_distribution": """
    SELECT
        fo.carrier,
        SUM(fo.paid_tax_incl) AS total_revenue,
        COUNT(fo.orderid_bk) as total_count
    FROM fact_order_history foh
    JOIN dim_date dd 
        ON foh.date_sk = dd.date_key
    JOIN fact_order fo
        ON fo.orderid_bk = foh.orderid_bk
    WHERE {filter}
      AND foh.orderstateid_bk = 2
    GROUP BY fo.carrier
    ORDER BY total_revenue DESC;
    """,
    "top_manufacturer_revenue_distribution": """
    SELECT
        dp.manufacturer,
        SUM(fol.amount_tax_incl) AS total_revenue
    FROM fact_order_history foh
    JOIN dim_date dd 
        ON foh.date_sk = dd.date_key
    JOIN fact_order_line fol 
        ON fol.orderid_bk = foh.orderid_bk
    JOIN dim_product dp 
        ON fol.product_sk = dp.product_key
    WHERE {filter}
      AND foh.orderstateid_bk = 2
    GROUP BY dp.manufacturer
    ORDER BY total_revenue DESC
    LIMIT 10;
    """,
    "market_group_revenue_distribution": """
    SELECT
        dp.market_group,
        SUM(fol.amount_tax_incl) AS total_revenue
    FROM fact_order_history foh
    JOIN dim_date dd 
        ON foh.date_sk = dd.date_key
    JOIN fact_order_line fol 
        ON fol.orderid_bk = foh.orderid_bk
    JOIN dim_product dp 
        ON fol.product_sk = dp.product_key
    WHERE {filter}
      AND foh.orderstateid_bk = 2
    GROUP BY dp.market_group
    ORDER BY total_revenue DESC;
    """,
    "gender_distribution": """
    SELECT 
        dc.gender, 
        COUNT(dc.customer_key) AS customers_count
    FROM dim_customer dc
    WHERE dc.active = TRUE AND {valid_customer_filter}
    GROUP BY dc.gender;
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
        FROM fact_order fo
        JOIN dim_date dd ON fo.date_sk = dd.date_key
        JOIN dim_customer dc ON fo.customer_sk = dc.customer_key
        WHERE {filter} AND {valid_customer_filter}
        GROUP BY age_range
        ORDER BY age_range;
        """
    },
    "product_group_revenue": {
        "title": "Príjmy z marketingových kampaní",
        "data_type": "diagram",
        "diagram_type": "bar",
        "show_diagram_table": True,
        "query": """
        SELECT
            TO_CHAR(dd.date, '{date_format}') AS period,
            SUM(fo.paid_tax_incl) AS total_revenue
        FROM fact_order_line fol
        JOIN fact_order fo ON fo.orderid_bk = fol.orderid_bk
        JOIN fact_order_history foh ON foh.orderid_bk = fo.orderid_bk AND foh.orderstateid_bk = 2
        JOIN dim_date dd ON fol.date_sk = dd.date_key
        JOIN dim_product dp ON fol.product_sk = dp.product_key
        WHERE {filter} AND {valid_product_filter} AND ({group_filter})
        GROUP BY period
        ORDER BY period;
        """,
        "subfilters": {
            "market_group": {
                "title": "Marketingová kampaň",
                "menu_query": """
                SELECT COALESCE(p.market_group, '[Not specified]') AS market_group
                FROM dim_product p
                WHERE {filter}
                GROUP BY market_group
                """
            },
            "market_subgroup": {
                "title": "Marketingová podkampaň",
                "menu_query": """
                SELECT COALESCE(p.market_subgroup, '[Not specified]') AS market_subgroup
                FROM dim_product p
                WHERE {filter}
                GROUP BY market_subgroup;
                """
            },
        }
    },
    "product_gender_revenue": {
        "title": "Príjmy podľa rodovej kategórie tovaru",
        "data_type": "diagram",
        "diagram_type": "bar",
        "show_diagram_table": True,
        "query": """
        SELECT
            TO_CHAR(dd.date, '{date_format}') AS period,
            SUM(fo.paid_tax_incl) AS total_revenue
        FROM fact_order_line fol
        JOIN fact_order fo ON fo.orderid_bk = fol.orderid_bk
        JOIN fact_order_history foh ON foh.orderid_bk = fo.orderid_bk AND foh.orderstateid_bk = 2
        JOIN dim_date dd ON fol.date_sk = dd.date_key
        JOIN dim_product dp ON fol.product_sk = dp.product_key
        WHERE {filter} AND {valid_product_filter} AND ({group_filter})
        GROUP BY period
        ORDER BY period;
        """,
        "subfilters": {
            "market_gender": {
                "title": "Rodová kategória",
                "menu_query": """
                SELECT COALESCE(p.market_gender, '[Not specified]') AS market_gender
                FROM dim_product p
                WHERE {filter}
                GROUP BY p.market_gender;
                """
            },
        },
    },
    "top_customers_above_median_csv": {
        "title": "Top zákazníci nad mediánom",
        "data_type": "table",
        "query": """
        WITH 
        median_calc AS (
            SELECT 
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fo.paid_tax_incl) AS median_order_total
            FROM fact_order fo
            JOIN dim_date dd ON fo.date_sk = dd.date_key
            WHERE {filter}
        )
        SELECT 
            dc.customerid_bk AS customer_id,
            SUM(fo.paid_tax_incl) AS total_spent,
            COUNT(fo.orderid_bk) AS order_count
        FROM fact_order fo
        JOIN median_calc mc ON fo.paid_tax_incl > mc.median_order_total
        JOIN dim_customer dc ON fo.customer_sk = dc.customer_key
        JOIN dim_date dd ON fo.date_sk = dd.date_key
        JOIN fact_order_history foh ON foh.orderid_bk = fo.orderid_bk AND foh.orderstateid_bk = 2
        WHERE {filter} AND {valid_customer_filter}
        GROUP BY dc.customerid_bk
        ORDER BY total_spent DESC;
        """,
    },
}

