"""
CREATE TABLE IF NOT EXISTS public.bridge_product_attribute
(
    product_sk bigint NOT NULL,
    attribute_sk bigint NOT NULL,
    productattributeid_bk bigint,
    attributeid_bk bigint,
    CONSTRAINT bridge_product_attribute_pkey PRIMARY KEY (product_sk, attribute_sk)
);

CREATE TABLE IF NOT EXISTS public.dim_address
(
    address_key bigserial NOT NULL,
    addressid_bk bigint NOT NULL,
    customerid_bk bigint,
    country character varying COLLATE pg_catalog."default",
    state character varying COLLATE pg_catalog."default",
    city character varying COLLATE pg_catalog."default",
    zipcode character varying COLLATE pg_catalog."default",
    valid_from date,
    valid_to date,
    CONSTRAINT dim_address_pkey PRIMARY KEY (address_key)
);

CREATE TABLE IF NOT EXISTS public.dim_attribute
(
    attribute_key bigserial NOT NULL,
    attributeid_bk bigint NOT NULL,
    attribute_name character varying COLLATE pg_catalog."default" NOT NULL,
    attribute_group character varying COLLATE pg_catalog."default",
    CONSTRAINT dim_attribute_pkey PRIMARY KEY (attribute_key)
);

CREATE TABLE IF NOT EXISTS public.dim_customer
(
    customer_key bigserial NOT NULL,
    customerid_bk bigint NOT NULL,
    hashedemail character varying COLLATE pg_catalog."default" NOT NULL,
    defaultgroup character varying COLLATE pg_catalog."default" NOT NULL,
    birthdate date,
    gender character varying COLLATE pg_catalog."default",
    businessaccount boolean,
    active boolean,
    valid_from date,
    valid_to date,
    CONSTRAINT dim_customer_pkey PRIMARY KEY (customer_key)
);

CREATE TABLE IF NOT EXISTS public.dim_date
(
    date_key bigserial NOT NULL,
    date date NOT NULL,
    year integer NOT NULL,
    quarter integer NOT NULL,
    month integer NOT NULL,
    month_name character varying COLLATE pg_catalog."default" NOT NULL,
    day integer NOT NULL,
    day_of_week integer NOT NULL,
    day_name character varying COLLATE pg_catalog."default" NOT NULL,
    week_of_year integer NOT NULL,
    is_weekend boolean DEFAULT false,
    CONSTRAINT dim_date_pkey PRIMARY KEY (date_key)
);

CREATE TABLE IF NOT EXISTS public.dim_order_state
(
    orderstate_key bigserial NOT NULL,
    orderstateid_bk bigint NOT NULL,
    current_state character varying COLLATE pg_catalog."default",
    valid_from date,
    valid_to date,
    CONSTRAINT dim_orderstate_pkey PRIMARY KEY (orderstate_key)
);

CREATE TABLE IF NOT EXISTS public.dim_product
(
    product_key bigserial NOT NULL,
    productid_bk bigint NOT NULL,
    productattributeid_bk bigint,
    productname character varying COLLATE pg_catalog."default" NOT NULL,
    manufacturer character varying COLLATE pg_catalog."default",
    defaultcategory character varying COLLATE pg_catalog."default",
    market_group character varying COLLATE pg_catalog."default",
    market_subgroup character varying COLLATE pg_catalog."default",
    market_gender character varying COLLATE pg_catalog."default",
    price numeric,
    active boolean,
    valid_from date,
    valid_to date,
    CONSTRAINT dim_product_pkey PRIMARY KEY (product_key)
);

CREATE TABLE IF NOT EXISTS public.dim_time
(
    time_key bigserial NOT NULL,
    "time" time without time zone NOT NULL,
    hour integer NOT NULL,
    CONSTRAINT dim_time_pkey PRIMARY KEY (time_key)
);

CREATE TABLE IF NOT EXISTS public.fact_cart_line
(
    cartline_key bigserial NOT NULL,
    cartid_bk bigint NOT NULL,
    product_sk bigint NOT NULL,
    customer_sk bigint NOT NULL,
    date_sk bigint NOT NULL,
    time_sk bigint NOT NULL,
    quantity integer NOT NULL,
    CONSTRAINT fact_cart_line_pkey PRIMARY KEY (cartline_key)
);

CREATE TABLE IF NOT EXISTS public.fact_order_history
(
    orderhistory_key bigserial NOT NULL,
    orderhistoryid_bk bigint NOT NULL,
    orderstate_sk bigint NOT NULL,
    orderid_bk bigint NOT NULL,
    orderstateid_bk bigint NOT NULL,
    date_sk bigint NOT NULL,
    time_sk bigint NOT NULL,
    CONSTRAINT fact_order_history_pkey PRIMARY KEY (orderhistory_key)
);

CREATE TABLE IF NOT EXISTS public.fact_order_line
(
    orderline_key bigserial NOT NULL,
    orderid_bk bigint NOT NULL,
    orderdetailid_bk bigint NOT NULL,
    cartid_bk bigint,
    product_sk bigint NOT NULL,
    customer_sk bigint NOT NULL,
    address_sk bigint,
    date_sk bigint NOT NULL,
    time_sk bigint NOT NULL,
    quantity integer NOT NULL,
    price numeric(20, 6),
    price_tax_incl numeric(20, 6),
    amount numeric(20, 6),
    amount_tax_incl numeric(20, 6),
    paid numeric(20, 6),
    paid_tax_incl numeric(20, 6),
    taxrate numeric(20, 6),
    conversion_rate numeric,
    paymenttype character varying(300) COLLATE pg_catalog."default",
    carrier character varying(300) COLLATE pg_catalog."default",
    CONSTRAINT fact_order_line_pkey PRIMARY KEY (orderline_key)
);
"""

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
                SELECT DISTINCT market_group
                FROM dim_product
                WHERE {filter};
                """
            },
            "market_subgroup": {
                "title": "Subgroup",
                "menu_query": """
                SELECT DISTINCT market_subgroup
                FROM dim_product
                WHERE {filter};
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
                SELECT DISTINCT market_gender
                FROM dim_product
                WHERE {filter};
                """
            },
        }
    },
}

