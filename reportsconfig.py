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
    "summary": """
    WITH total_carts AS (
        SELECT COUNT(DISTINCT cartid_bk) AS carts_count
        FROM fact_cart_line
        WHERE date_sk IN (SELECT date_key FROM dim_date WHERE {filter})
    ),
    orders AS (
        SELECT DISTINCT
            orderid_bk,
            customer_sk,
            MAX(paid_tax_incl) AS total_paid_tax_incl
        FROM fact_order_line
        WHERE date_sk IN (SELECT date_key FROM dim_date WHERE {filter})
        GROUP BY orderid_bk, customer_sk
    )
    SELECT 
        COUNT(DISTINCT o.orderid_bk) AS orders_count,
        CAST(SUM(o.total_paid_tax_incl) AS NUMERIC(20,2)) AS total_revenue,
        AVG(o.total_paid_tax_incl) AS avg_order_value,
        (SELECT carts_count FROM total_carts) AS carts_count,
        CASE 
            WHEN (SELECT carts_count FROM total_carts) = 0 THEN 0
            ELSE ROUND(COUNT(o.orderid_bk) * 100.0 / (SELECT carts_count FROM total_carts), 2)
        END AS conversion_rate,
        AVG(EXTRACT(YEAR FROM AGE(dc.birthdate))) AS avg_age
    FROM orders o
    LEFT JOIN dim_customer dc ON o.customer_sk = dc.customer_key;
    """,
    "period_revenue": """    
    SELECT
        CASE
            WHEN {filter_raw} LIKE '%year%' AND {filter_raw} NOT LIKE '%month%' AND {filter_raw} NOT LIKE '%quarter%' THEN TO_CHAR(d.date, 'MM')
            WHEN {filter_raw} LIKE '%year%' AND {filter_raw} LIKE '%month%' THEN TO_CHAR(d.date, 'YYYY-MM-DD')
            WHEN {filter_raw} LIKE '%year%' AND {filter_raw} LIKE '%quarter%' THEN TO_CHAR(d.date, 'MM')
            WHEN {filter_raw} LIKE 'date BETWEEN%' THEN TO_CHAR(d.date, 'YYYY-MM-DD')
            ELSE TO_CHAR(d.date, 'YYYY-MM-DD')
        END AS period,
        SUM(fo.amount_tax_incl) AS total_revenue
    FROM fact_order_line fo
    JOIN dim_date d ON fo.date_sk = d.date_key
    JOIN dim_product dp ON fo.product_sk = dp.product_key
    WHERE {filter}
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
    WHERE dc.active = TRUE
    GROUP BY dc.gender;
    """,
    "age_distribution": """
    SELECT 
        FLOOR(EXTRACT(YEAR FROM AGE(CURRENT_DATE, dc.birthdate)) / 10) * 10 AS age_range,
        AVG(fo.paid_tax_incl) AS avg_order_value
    FROM fact_order_line fo
    JOIN dim_customer dc ON fo.customer_sk = dc.customer_key
    WHERE fo.date_sk IN (SELECT date_key FROM dim_date WHERE {filter})
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
        WHERE fo.date_sk IN (SELECT date_key FROM dim_date WHERE {filter})
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
    WHERE {filter}
    GROUP BY dos.current_state, dd.month
    ORDER BY dd.month;
""",
}

reports_queries = {
    "gender_distribution": {
        "title": "Gender distribution",
        "query": """
        SELECT 
            dc.gender, 
            COUNT(dc.customer_key) AS customers_count
        FROM dim_customer dc
        WHERE dc.active = TRUE
        AND ({filter})
        GROUP BY dc.gender;
        """
    },
    "age_distribution": {
        "title": "Age distribution",
        "query": """
        SELECT 
            FLOOR(EXTRACT(YEAR FROM AGE(CURRENT_DATE, dc.birthdate)) / 10) * 10 AS age_range,
            AVG(fo.paid_tax_incl) AS avg_order_value
        FROM fact_order_line fo
        JOIN dim_customer dc ON fo.customer_sk = dc.customer_key
        WHERE fo.date_sk IN (SELECT date_key FROM dim_date WHERE {filter})
        GROUP BY age_range
        ORDER BY age_range;
        """
    },
    "product_group_revenue": {
        "title": "Group revenue",
        "query": """
        SELECT
            CASE
                WHEN {filter_raw} LIKE '%year%' AND {filter_raw} NOT LIKE '%month%' AND {filter_raw} NOT LIKE '%quarter%' THEN TO_CHAR(d.date, 'MM')
                WHEN {filter_raw} LIKE '%year%' AND {filter_raw} LIKE '%month%' THEN TO_CHAR(d.date, 'YYYY-MM-DD')
                WHEN {filter_raw} LIKE '%year%' AND {filter_raw} LIKE '%quarter%' THEN TO_CHAR(d.date, 'MM')
                WHEN {filter_raw} LIKE 'date BETWEEN%' THEN TO_CHAR(d.date, 'YYYY-MM-DD')
                ELSE TO_CHAR(d.date, 'YYYY-MM-DD')
            END AS period,
            SUM(fo.amount_tax_incl) AS total_revenue
        FROM fact_order_line fo
        JOIN dim_date d ON fo.date_sk = d.date_key
        JOIN dim_product dp ON fo.product_sk = dp.product_key
        WHERE {filter}
        AND (
            {group_filter}
        )
        GROUP BY period
        ORDER BY period;
        """,
        "subfilters": {
            "market_group": {
                "title": "Group",
                "menu_query": """
                SELECT market_group
                FROM dim_product p
                WHERE {filter}
                GROUP BY market_group
                """
            },
            "market_subgroup": {
                "title": "Subgroup",
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
        "title": "Product gender revenue",
        "query": """
        """,
        "subfilters": {
            "market_gender": {
                "title": "Gender",
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

