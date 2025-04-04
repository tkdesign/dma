import pandas as pd
from sqlalchemy import text
import hashlib
from datetime import date, datetime
import gc


def create_date_frame(start, end):
    dates_range = pd.date_range(start=start, end=end, freq='D')

    df = pd.DataFrame({'date': dates_range})

    df['year'] = df['date'].dt.year
    df['quarter'] = df['date'].dt.quarter
    df['month'] = df['date'].dt.month
    df['month_name'] = df['date'].dt.strftime('%B')
    df['day'] = df['date'].dt.day
    df['day_of_week'] = df['date'].dt.dayofweek + 1
    df['day_name'] = df['date'].dt.strftime('%A')
    df['week_of_year'] = df['date'].dt.isocalendar().week
    df['is_weekend'] = df['day_of_week'].isin([6, 7])

    return df

def load_dim_date(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Úloha zrušená")
        return

    print('Spracovanie `dim_date` sa začalo...')

    start = '2000-01-01'
    end = '2030-12-31'

    df_date = create_date_frame(start, end)

    with dwh_engine.begin() as conn:
        conn.execute(text('SET session_replication_role = replica;'))
        conn.execute(text('TRUNCATE TABLE public.dim_date RESTART IDENTITY CASCADE;'))
        conn.execute(text('SET session_replication_role = DEFAULT;'))
        df_date.to_sql('dim_date', conn, if_exists='append', index=False, schema='public')

    print('Spracovanie `dim_date` dokončené.')

def create_time_frame():
    start_time = '2000-01-01 00:00:00'
    end_time = '2000-01-01 23:00:00'
    times = pd.date_range(start=start_time, end=end_time, freq='h')

    df = pd.DataFrame({
        'time': times,
        'hour': times.hour
    })

    df['time'] = df['time'].dt.time

    return df

def load_dim_time(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Úloha zrušená")
        return

    print('Spracovanie `dim_time` sa začalo...')

    df_time = create_time_frame()

    with dwh_engine.begin() as conn:
        conn.execute(text('SET session_replication_role = replica;'))
        conn.execute(text('TRUNCATE TABLE public.dim_time RESTART IDENTITY CASCADE;'))
        conn.execute(text('SET session_replication_role = DEFAULT;'))
        df_time.to_sql('dim_time', conn, if_exists='append', index=False)

    print('Spracovanie `dim_time` dokončené.')

def calc_hash_dim_address(row):
    data = f"{row['addressid_bk']}-{row['country']}-{row['state']}-{row['city']}-{row['zipcode']}"
    return hashlib.md5(data.encode('utf-8')).hexdigest()

def load_dim_address(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Úloha zrušená")
        return

    stage_query = """
    SELECT
        a.id_address AS addressid_bk,
        a.id_customer AS customerid_bk,
        c.name AS country,
        s.name AS state,
        a.city,
        a.postcode AS zipcode,
        a.date_add AS valid_from
    FROM
        sg_address a
        LEFT JOIN sg_country c ON c.id_country = a.id_country
        LEFT JOIN sg_state s ON s.id_state = a.id_state
    ORDER BY
        a.id_address;
    """

    print('Spracovanie `dim_address` sa začalo...')

    today = date.today()
    valid_to = today - pd.DateOffset(days=1)
    min_date = datetime(2000, 1, 1)
    chunksize = 10000

    with stage_engine.connect().execution_options(stream_results=True) as conn:
        for chunk in pd.read_sql_query(text(stage_query), con=conn, chunksize=chunksize):
            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            print('Spracovanie bloku...')

            chunk['country'] = chunk['country'].replace('', None)
            chunk['state'] = chunk['state'].replace('', None)
            chunk['city'] = chunk['city'].replace('', None)

            chunk['row_hash_stage'] = chunk.apply(calc_hash_dim_address, axis=1)
            business_keys = chunk['addressid_bk'].unique().tolist()
            query_dim = text("SELECT * FROM dma_dwh.public.dim_address WHERE addressid_bk IN :keys and valid_to = '9999-12-31';")
            df_dim = pd.read_sql_query(query_dim, dwh_engine, params={"keys": tuple(business_keys)})

            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            if not df_dim.empty:
                df_dim['row_hash_dim'] = df_dim.apply(lambda row: calc_hash_dim_address({
                    'addressid_bk': row['addressid_bk'],
                    'customerid_bk': row['customerid_bk'],
                    'country': row['country'],
                    'state': row['state'],
                    'city': row['city'],
                    'zipcode': row['zipcode'],
                }), axis=1)
            else:
                df_dim['row_hash_dim'] = None

            merged = pd.merge(chunk, df_dim, on='addressid_bk', how='left', suffixes=('_stage', '_dim'))

            new_records = merged[merged['address_key'].isnull()] if 'address_key' in merged.columns else merged
            changed_records = merged[(merged['address_key'].notnull()) & (merged['row_hash_stage'] != merged['row_hash_dim'])]

            for idx, row in new_records.iterrows():
                insert_sql = text("""
                INSERT INTO dma_dwh.public.dim_address (addressid_bk, customerid_bk, country, state, city, zipcode, valid_from, valid_to)
                VALUES (:addressid_bk, :customerid_bk, :country, :state, :city, :zipcode, :valid_from, '9999-12-31');
                """)
                valid_from = row['valid_from_stage'] if not pd.isna(row['valid_from_stage']) else min_date
                with dwh_engine.begin() as conn:
                    conn.execute(insert_sql, {
                        'addressid_bk': row['addressid_bk'],
                        'customerid_bk': row['customerid_bk_stage'],
                        'country': row['country_stage'],
                        'state': row['state_stage'],
                        'city': row['city_stage'],
                        'zipcode': row['zipcode_stage'],
                        'valid_from': valid_from,
                    })

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

            for idx, row in changed_records.iterrows():
                update_sql = text("""
                UPDATE dma_dwh.public.dim_address
                SET valid_to = :valid_to
                WHERE address_key = :address_key;
                """)
                with dwh_engine.begin() as conn:
                    conn.execute(update_sql, {
                        'valid_to': valid_to,
                        'address_key': row['address_key']
                    })

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

                insert_sql = text("""
                INSERT INTO dma_dwh.public.dim_address (addressid_bk, customerid_bk, country, state, city, zipcode, valid_from, valid_to)
                VALUES (:addressid_bk, :customerid_bk, :country, :state, :city, :zipcode, :valid_from, '9999-12-31');
                """)
                with dwh_engine.begin() as conn:
                    conn.execute(insert_sql, {
                        'addressid_bk': row['addressid_bk'],
                        'customerid_bk': row['customerid_bk_stage'],
                        'country': row['country_stage'],
                        'state': row['state_stage'],
                        'city': row['city_stage'],
                        'zipcode': row['zipcode_stage'],
                        'valid_from': today,
                    })

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

            del new_records
            del changed_records
            del merged
            del df_dim
            del chunk
            gc.collect()

    print("Spracovanie `dim_address` dokončené.")
    return

def calc_hash_dim_customer(row):
    data = f"{row['customerid_bk']}-{row['hashedemail']}-{row['defaultgroup']}-{row['birthday']}-{row['gender']}-{row['businessaccount']}-{row['active']}"
    return hashlib.md5(data.encode('utf-8')).hexdigest()

def load_dim_customer(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Úloha zrušená")
        return
    stage_query = """
    SELECT
        c.id_customer AS customerid_bk,
        c.hashed_login AS hashedemail,
        (SELECT gr.name FROM sg_customer_group cg JOIN sg_group gr ON gr.id_group = cg.id_group WHERE cg.id_group = c.id_default_group ORDER BY gr.id_group DESC LIMIT 1) AS defaultgroup,
        c.birthday,
        (SELECT gen.name FROM sg_gender AS gen WHERE gen.id_gender = c.id_gender ORDER BY gen.id_gender DESC LIMIT 1) AS gender,
        ((SELECT cc.id_customer FROM sg_customer_company cc WHERE cc.id_customer = c.id_customer ORDER BY cc.id_customer DESC LIMIT 1) IS NOT NULL) AS businessaccount,
        c.active AS active,
        c.date_add AS valid_from
    FROM
        sg_customer AS c
    ORDER BY
        c.id_customer;
    """

    print('Spracovanie `dim_customer` sa začalo...')

    today = date.today()
    valid_to = today - pd.DateOffset(days=1)
    min_date = datetime(2000, 1, 1)
    chunksize = 10000

    with stage_engine.connect().execution_options(stream_results=True) as conn:
        for chunk in pd.read_sql_query(text(stage_query), con=conn, chunksize=chunksize):
            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            print('Spracovanie bloku...')

            chunk['gender'] = chunk['gender'].replace('[neuvádzam]', None)

            chunk['row_hash_stage'] = chunk.apply(calc_hash_dim_customer, axis=1)
            business_keys = chunk['customerid_bk'].unique().tolist()

            query_dim = text("SELECT * FROM dma_dwh.public.dim_customer WHERE customerid_bk IN :keys and valid_to = '9999-12-31';")
            df_dim = pd.read_sql_query(query_dim, dwh_engine, params={"keys": tuple(business_keys)})

            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            if not df_dim.empty:
                df_dim['row_hash_dim'] = df_dim.apply(lambda row: calc_hash_dim_customer({
                    'customerid_bk': row['customerid_bk'],
                    'hashedemail': row['hashedemail'],
                    'defaultgroup': row['defaultgroup'],
                    'birthday': row['birthdate'],
                    'gender': row['gender'],
                    'businessaccount': row['businessaccount'],
                    'active': row['active']
                }), axis=1)
            else:
                df_dim['row_hash_dim'] = None

            merged = pd.merge(chunk, df_dim, on='customerid_bk', how='left', suffixes=('_stage', '_dim'))

            new_records = merged[merged['customer_key'].isnull()] if 'customer_key' in merged.columns else merged
            changed_records = merged[(merged['customer_key'].notnull()) & (merged['row_hash_stage'] != merged['row_hash_dim'])]

            for idx, row in new_records.iterrows():
                insert_sql = text("""
                INSERT INTO dma_dwh.public.dim_customer (customerid_bk, hashedemail, defaultgroup, birthdate, gender, businessaccount, active, valid_from, valid_to)
                VALUES (:customerid_bk, :hashedemail, :defaultgroup, :birthdate, :gender, :businessaccount, :active, :valid_from, '9999-12-31');
                """)
                valid_from = row['valid_from_stage'] if not pd.isna(row['valid_from_stage']) else min_date
                with dwh_engine.begin() as conn:
                    conn.execute(insert_sql, {
                        'customerid_bk': row['customerid_bk'],
                        'hashedemail': row['hashedemail_stage'],
                        'defaultgroup': row['defaultgroup_stage'],
                        'birthdate': row['birthday'],
                        'gender': row['gender_stage'],
                        'businessaccount': row['businessaccount_stage'],
                        'active': row['active_stage'],
                        'valid_from': valid_from,
                    })

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

            for idx, row in changed_records.iterrows():
                update_sql = text("""
                UPDATE dma_dwh.public.dim_customer
                SET valid_to = :valid_to
                WHERE customer_key = :customer_key;
                """)
                with dwh_engine.begin() as conn:
                    conn.execute(update_sql, {
                        'valid_to': valid_to,
                        'customer_key': row['customer_key']
                    })

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

                insert_sql = text("""
                INSERT INTO dma_dwh.public.dim_customer (customerid_bk, hashedemail, defaultgroup, birthdate, gender, businessaccount, active, valid_from, valid_to)
                VALUES (:customerid_bk, :hashedemail, :defaultgroup, :birthdate, :gender, :businessaccount, :active, :valid_from, '9999-12-31');
                """)
                with dwh_engine.begin() as conn:
                    conn.execute(insert_sql, {
                        'customerid_bk': row['customerid_bk'],
                        'hashedemail': row['hashedemail_stage'],
                        'defaultgroup': row['defaultgroup_stage'],
                        'birthdate': row['birthday'],
                        'gender': row['gender_stage'],
                        'businessaccount': row['businessaccount_stage'],
                        'active': row['active_stage'],
                        'valid_from': today,
                    })

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

            del new_records
            del changed_records
            del merged
            del df_dim
            del chunk
            gc.collect()

    print("Spracovanie `dim_customer` dokončené.")
    return

def calc_hash_dim_attribute(row):
    data = f"{row['attributeid_bk']}-{row['attribute_name']}-{row['attribute_group']}"
    return hashlib.md5(data.encode('utf-8')).hexdigest()

def load_dim_attribute(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Úloha zrušená")
        return

    stage_query = """
    SELECT
        a.id_attribute AS attributeid_bk,
        a.name AS attribute_name,
        a.name AS attribute_group
    FROM
        sg_attribute a
    LEFT JOIN
        sg_attribute_group ag ON a.id_attribute_group = ag.id_attribute_group
    ORDER BY
        a.id_attribute;
    """

    print('Spracovanie `dim_attribute` sa začalo...')

    chunksize = 10000

    with stage_engine.connect().execution_options(stream_results=True) as conn:
        for chunk in pd.read_sql_query(text(stage_query), con=conn, chunksize=chunksize):
            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            print('Spracovanie bloku...')

            chunk['attribute_name'] = chunk['attribute_name'].replace('', 'Unknown').fillna('Unknown')
            chunk['attribute_group'] = chunk['attribute_group'].replace('', None)

            chunk['row_hash_stage'] = chunk.apply(calc_hash_dim_attribute, axis=1)
            business_keys = chunk['attributeid_bk'].unique().tolist()

            query_dim = text("SELECT * FROM dma_dwh.public.dim_attribute WHERE attributeid_bk IN :keys;")
            df_dim = pd.read_sql_query(query_dim, dwh_engine, params={"keys": tuple(business_keys)})

            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            if not df_dim.empty:
                df_dim['row_hash_dim'] = df_dim.apply(lambda row: calc_hash_dim_attribute({
                    'attributeid_bk': row['attributeid_bk'],
                    'attribute_name': row['attribute_name'],
                    'attribute_group': row['attribute_group'],
                }), axis=1)
            else:
                df_dim['row_hash_dim'] = None

            merged = pd.merge(chunk, df_dim, on='attributeid_bk', how='left', suffixes=('_stage', '_dim'))

            new_records = merged[merged['attribute_key'].isnull()] if 'attribute_key' in merged.columns else merged
            changed_records = merged[(merged['attribute_key'].notnull()) & (merged['row_hash_stage'] != merged['row_hash_dim'])]

            for idx, row in new_records.iterrows():
                insert_sql = text("""
                INSERT INTO dma_dwh.public.dim_attribute (attributeid_bk, attribute_name, attribute_group)
                VALUES (:attributeid_bk, :attribute_name, :attribute_group)
                """)
                with dwh_engine.begin() as conn:
                    conn.execute(insert_sql, {
                        'attributeid_bk': row['attributeid_bk'],
                        'attribute_name': row['attribute_name_stage'],
                        'attribute_group': row['attribute_group_stage'],
                    })

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

            for idx, row in changed_records.iterrows():
                update_sql = text("""
                UPDATE dma_dwh.public.dim_attribute
                SET attribute_name = :attribute_name, attribute_group = :attribute_group
                WHERE attribute_key = :attribute_key
                """)
                with dwh_engine.begin() as conn:
                    conn.execute(update_sql, {
                        'attribute_key': row['attribute_key'],
                        'attribute_name': row['attribute_name_stage'],
                        'attribute_group': row['attribute_group_stage'],
                    })

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

            del new_records
            del changed_records
            del merged
            del df_dim
            del chunk
            gc.collect()

    print("Spracovanie `dim_attribute` dokončené.")
    return

def calc_hash_dim_product(row):
    data = f"{row['productid_bk']}-{row['productattributeid_bk']}-{row['productname']}-{row['manufacturer']}-{row['defaultcategory']}-{row['market_group']}-{row['market_subgroup']}-{row['market_gender']}-{row['price']}-{row['active']}"
    return hashlib.md5(data.encode('utf-8')).hexdigest()

def load_dim_product(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Úloha zrušená")
        return

    stage_query = """
    SELECT
        p.id_product AS productid_bk,
        p.id_product_attribute AS productattributeid_bk,
        p.name AS productname,
        m.name AS manufacturer,
        c.name AS defaultcategory,
        p.group AS market_group,
        p.subgroup AS market_subgroup,
        p.gender AS market_gender,
        p.price,
        p.active,
        p.date_add AS valid_from
    FROM
        sg_product AS p
    LEFT JOIN 
        sg_manufacturer m ON p.id_manufacturer = m.id_manufacturer
    LEFT JOIN
        sg_category c ON p.id_category_default = c.id_category
    ORDER BY
        p.id_product;
    """

    print('Spracovanie `dim_product` sa začalo...')

    today = date.today()
    valid_to = today - pd.DateOffset(days=1)
    min_date = datetime(2000, 1, 1)
    chunksize = 10000

    with stage_engine.connect().execution_options(stream_results=True) as conn:
        for chunk in pd.read_sql_query(text(stage_query), con=conn, chunksize=chunksize):
            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            print('Spracovanie bloku...')

            chunk['manufacturer'] = chunk['manufacturer'].replace('', None)
            chunk['defaultcategory'] = chunk['defaultcategory'].replace('', None)
            chunk['market_group'] = chunk['market_group'].replace('', None)
            chunk['market_subgroup'] = chunk['market_subgroup'].replace('', None)
            chunk['market_gender'] = chunk['market_gender'].replace('', None)
            chunk['productattributeid_bk'] = chunk['productattributeid_bk'].fillna(0).astype('int64')

            chunk['row_hash_stage'] = chunk.apply(calc_hash_dim_product, axis=1)
            keys_pairs = list(zip(chunk['productid_bk'], chunk['productattributeid_bk']))
            values_clause = ", ".join(f"({a}, {b})" for a, b in keys_pairs)

            query_dim = f"""
            SELECT * FROM dma_dwh.public.dim_product
            WHERE (productid_bk, productattributeid_bk) IN (
                VALUES {values_clause}
            )
            """
            df_dim = pd.read_sql_query(text(query_dim), dwh_engine)

            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            if not df_dim.empty:
                df_dim['row_hash_dim'] = df_dim.apply(lambda row: calc_hash_dim_product({
                    'productid_bk': row['productid_bk'],
                    'productattributeid_bk': row['productattributeid_bk'],
                    'productname': row['productname'],
                    'manufacturer': row['manufacturer'],
                    'defaultcategory': row['defaultcategory'],
                    'market_group': row['market_group'],
                    'market_subgroup': row['market_subgroup'],
                    'market_gender': row['market_gender'],
                    'price': row['price'],
                    'active': row['active']
                }), axis=1)
            else:
                df_dim['row_hash_dim'] = None

            merged = pd.merge(chunk, df_dim, on=['productid_bk', 'productattributeid_bk'], how='left', suffixes=('_stage', '_dim'))

            new_records = merged[merged['product_key'].isnull()] if 'product_key' in merged.columns else merged
            changed_records = merged[(merged['product_key'].notnull()) & (merged['row_hash_stage'] != merged['row_hash_dim'])]

            for idx, row in new_records.iterrows():
                insert_sql = text("""
                INSERT INTO dma_dwh.public.dim_product (productid_bk, productattributeid_bk, productname, manufacturer, defaultcategory, market_group, market_subgroup, market_gender, price, active, valid_from, valid_to)
                VALUES (:productid_bk, :productattributeid_bk, :productname, :manufacturer, :defaultcategory, :market_group, :market_subgroup, :market_gender, :price, :active, :valid_from, '9999-12-31');
                """)
                valid_from = row['valid_from_stage'] if not pd.isna(row['valid_from_stage']) else min_date
                with dwh_engine.begin() as conn:
                    conn.execute(insert_sql, {
                        'productid_bk': row['productid_bk'],
                        'productattributeid_bk': row['productattributeid_bk'] if not pd.isna(row['productattributeid_bk']) else None,
                        'productname': row['productname_stage'],
                        'manufacturer': row['manufacturer_stage'],
                        'defaultcategory': row['defaultcategory_stage'],
                        'market_group': row['market_group_stage'],
                        'market_subgroup': row['market_subgroup_stage'],
                        'market_gender': row['market_gender_stage'],
                        'price': row['price_stage'],
                        'active': row['active_stage'],
                        'valid_from': valid_from,
                    })

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

            for idx, row in changed_records.iterrows():
                update_sql = text("""
                UPDATE dma_dwh.public.dim_product
                SET valid_to = :valid_to
                WHERE product_key = :product_key;
                """)
                with dwh_engine.begin() as conn:
                    conn.execute(update_sql, {
                        'valid_to': valid_to,
                        'product_key': row['product_key']
                    })

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

                insert_sql = text("""
                INSERT INTO dma_dwh.public.dim_product (productid_bk, productattributeid_bk, productname, manufacturer, defaultcategory, market_group, market_subgroup, market_gender, price, active, valid_from, valid_to)
                VALUES (:productid_bk, :productattributeid_bk, :productname, :manufacturer, :defaultcategory, :market_group, :market_subgroup, :market_gender, :price, :active, :valid_from, '9999-12-31');
                """)
                with dwh_engine.begin() as conn:
                    conn.execute(insert_sql, {
                        'productid_bk': row['productid_bk'],
                        'productattributeid_bk': row['productattributeid_bk_stage'],
                        'productname': row['productname_stage'],
                        'manufacturer': row['manufacturer_stage'],
                        'defaultcategory': row['defaultcategory_stage'],
                        'market_group': row['market_group_stage'],
                        'market_subgroup': row['market_subgroup_stage'],
                        'market_gender': row['market_gender_stage'],
                        'price': row['price_stage'],
                        'active': row['active_stage'],
                        'valid_from': today,
                    })

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

            del new_records
            del changed_records
            del merged
            del df_dim
            del chunk
            gc.collect()

    print("Spracovanie `dim_product` dokončené.")
    return

def load_bridge_product_attribute(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Úloha zrušená")
        return

    query = """
    SELECT
        pac.id_product_attribute,
        pac.id_attribute,
        dp.product_key,
        da.attribute_key,
        bpa.product_sk,
        bpa.attribute_sk
    FROM
        dma_stage.dma_db_stage.sg_product_attribute_combination pac
    JOIN
        dma_stage.public.dim_product_fdw dp ON pac.id_product_attribute = dp.productattributeid_bk
    JOIN
        dma_stage.public.dim_attribute_fdw da ON pac.id_attribute = da.attributeid_bk
    LEFT JOIN
        dma_stage.public.bridge_product_attribute_fdw bpa ON dp.product_key = bpa.product_sk AND da.attribute_key = bpa.attribute_sk
    WHERE bpa.product_sk IS NULL
    ORDER BY pac.id_product_attribute;
    """

    print('Spracovanie `bridge_product_attribute` sa začalo...')

    chunksize = 10000

    with stage_engine.connect().execution_options(stream_results=True) as conn:
        for chunk in pd.read_sql_query(text(query), con=conn, chunksize=chunksize):
            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            print('Spracovanie bloku...')

            insert_sql = text("""
            INSERT INTO dma_dwh.public.bridge_product_attribute (product_sk, attribute_sk, productattributeid_bk, attributeid_bk)
            VALUES (:product_sk, :attribute_sk, :id_product_attribute, :id_attribute)
            """)

            with dwh_engine.begin() as conn:
                for _, row in chunk.iterrows():
                    conn.execute(insert_sql, {
                        'product_sk': int(row['product_key']),
                        'attribute_sk': int(row['attribute_key']),
                        'id_product_attribute': int(row['id_product_attribute']),
                        'id_attribute': int(row['id_attribute'])
                    })

                    if self is not None and self.is_aborted():
                        print("Úloha zrušená")
                        return

            del chunk
            gc.collect()

    print("Spracovanie `bridge_product_attribute` dokončené.")
    return

def calc_hash_load_dim_order_state(row):
    data = f"{row['orderstateid_bk']}-{row['current_state']}"
    return hashlib.md5(data.encode('utf-8')).hexdigest()

def load_dim_order_state(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Úloha zrušená")
        return

    stage_query = """
    SELECT
        os.id_order_state AS orderstateid_bk,
        os.name AS current_state
    FROM
        sg_order_state os
    ORDER BY
        os.id_order_state;
    """

    print('Spracovanie `dim_order_state` sa začalo...')

    today = date.today()
    valid_to = today - pd.DateOffset(days=1)
    min_date = datetime(2000, 1, 1)
    chunksize = 10000

    with stage_engine.connect().execution_options(stream_results=True) as conn:
        for chunk in pd.read_sql_query(text(stage_query), con=conn, chunksize=chunksize):

            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            print('Spracovanie bloku...')

            chunk['row_hash_stage'] = chunk.apply(calc_hash_load_dim_order_state, axis=1)
            business_keys = chunk['orderstateid_bk'].unique().tolist()

            query_dim = text("SELECT * FROM dma_dwh.public.dim_order_state WHERE orderstateid_bk IN :keys and valid_to = '9999-12-31';")
            df_dim = pd.read_sql_query(query_dim, dwh_engine, params={"keys": tuple(business_keys)})

            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            if not df_dim.empty:
                df_dim['row_hash_dim'] = df_dim.apply(lambda row: calc_hash_load_dim_order_state({'orderstateid_bk': row['orderstateid_bk'], 'current_state': row['current_state'], }), axis=1)
            else:
                df_dim['row_hash_dim'] = None

            merged = pd.merge(chunk, df_dim, on='orderstateid_bk', how='left', suffixes=('_stage', '_dim'))

            new_records = merged[merged['orderstate_key'].isnull()] if 'orderstate_key' in merged.columns else merged
            changed_records = merged[(merged['orderstate_key'].notnull()) & (merged['row_hash_stage'] != merged['row_hash_dim'])]

            for idx, row in new_records.iterrows():
                insert_sql = text("""
                INSERT INTO dma_dwh.public.dim_order_state (orderstateid_bk, current_state, valid_from, valid_to)
                VALUES (:orderstateid_bk, :current_state, :valid_from, '9999-12-31');
                """)
                with dwh_engine.begin() as conn:
                    conn.execute(insert_sql, {
                        'orderstateid_bk': row['orderstateid_bk'],
                        'current_state': row['current_state_stage'],
                        'valid_from': min_date,
                    })

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

            for idx, row in changed_records.iterrows():
                update_sql = text("""
                UPDATE dma_dwh.public.dim_order_state
                SET valid_to = :valid_to
                WHERE orderstate_key = :orderstate_key;
                """)
                with dwh_engine.begin() as conn:
                    conn.execute(update_sql, {
                        'valid_to': valid_to,
                        'orderstate_key': row['orderstate_key']
                    })

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

                insert_sql = text("""
                INSERT INTO dma_dwh.public.dim_order_state (orderstateid_bk, current_state, valid_from, valid_to)
                VALUES (:orderstateid_bk, :current_state, :valid_from, '9999-12-31');
                """)
                with dwh_engine.begin() as conn:
                    conn.execute(insert_sql, {
                        'orderstateid_bk': row['orderstateid_bk'],
                        'current_state': row['current_state_stage'],
                        'valid_from': today,
                    })

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

            del merged
            del new_records
            del changed_records
            del df_dim
            del chunk
            gc.collect()

    print("Spracovanie `dim_order_state` dokončené.")
    return

def load_fact_cart_line(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Úloha zrušená")
        return

    stage_query = """
    SELECT
        sgcp.id_cart AS sgcp_id_cart,
        sgcp.quantity AS sgcp_quantity,
        sgc.date_add AS sgc_date_add,
        (SELECT dp.product_key FROM dma_stage.public.dim_product_fdw dp
         WHERE dp.productid_bk = sgcp.id_product 
         AND dp.productattributeid_bk = sgcp.id_product_attribute
         AND dp.valid_to = '9999-12-31'
         LIMIT 1) AS dp_product_key,
        (SELECT dc.customer_key FROM dma_stage.public.dim_customer_fdw dc
         WHERE dc.customerid_bk = sgc.id_customer
         AND dc.valid_to = '9999-12-31'
         LIMIT 1) AS dc_customer_key
    FROM dma_stage.dma_db_stage.sg_cart_product sgcp 
    JOIN dma_stage.dma_db_stage.sg_cart sgc 
        ON sgc.id_cart = sgcp.id_cart
    ORDER BY sgc.date_add;
    """

    print('Spracovanie `fact_cart_line` sa začalo...')

    chunksize = 10000

    with stage_engine.connect().execution_options(stream_results=True) as conn:
        for chunk in pd.read_sql_query(text(stage_query), con=conn, chunksize=chunksize):

            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            print('Spracovanie bloku...')

            chunk = chunk[chunk['dp_product_key'].notnull() & chunk['dc_customer_key'].notnull()]
            chunk['dp_product_key'] = chunk['dp_product_key'].fillna(0).astype('int64')
            chunk['dc_customer_key'] = chunk['dc_customer_key'].fillna(0).astype('int64')

            key_list = list(zip(chunk['sgcp_id_cart'], chunk['dp_product_key'], chunk['dc_customer_key']))
            values_clause = ", ".join(f"({int(a)}, {int(b)}, {int(c)})" for a, b, c in key_list)
            query_fact = f"""
            SELECT st.cartid_bk, st.product_sk, st.customer_sk
            FROM (
                VALUES {values_clause}
            ) AS st(cartid_bk, product_sk, customer_sk)
            LEFT JOIN dma_dwh.public.fact_cart_line fc
              ON st.cartid_bk = fc.cartid_bk
              AND st.product_sk = fc.product_sk
              AND st.customer_sk = fc.customer_sk
            WHERE fc.cartid_bk IS NULL;
            """

            df_fact = pd.read_sql_query(text(query_fact), dwh_engine)
            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            if not df_fact.empty:
                existing_keys = set(df_fact[['cartid_bk', 'product_sk', 'customer_sk']].itertuples(index=False, name=None))
                chunk = chunk[chunk.apply(lambda row: (row['sgcp_id_cart'], row['dp_product_key'], row['dc_customer_key']) in existing_keys, axis=1)]

                chunk['sgc_date_add'] = pd.to_datetime(chunk['sgc_date_add'], utc=True)
                date_add_list = chunk['sgc_date_add'].dt.date.tolist()

                query_date = text("SELECT date_key, date FROM dma_dwh.public.dim_date WHERE date IN :keys")
                df_date = pd.read_sql_query(query_date, dwh_engine, params={"keys": tuple(date_add_list)})

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

                time_add_list = chunk['sgc_date_add'].dt.floor('h').dt.time.tolist()

                query_time = text("SELECT time_key, time FROM dma_dwh.public.dim_time WHERE time IN :keys")
                df_time = pd.read_sql_query(query_time, dwh_engine, params={"keys": tuple(time_add_list)})

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

                chunk['sg_date'] = chunk['sgc_date_add'].dt.date
                chunk['sg_time'] = chunk['sgc_date_add'].dt.floor('h').dt.time

                merged = pd.merge(chunk, df_date, left_on='sg_date', right_on='date', how='left')
                merged = pd.merge(merged, df_time, left_on='sg_time', right_on='time', how='left')

                merged['date_key'] = merged['date_key'].astype('float64')
                merged['time_key'] = merged['time_key'].astype('float64')
                merged = merged.fillna({'date_key': 0, 'time_key': 0})
                merged = merged.astype({'date_key': 'int64', 'time_key': 'int64'})

                for _, row in merged.iterrows():
                    insert_sql = text("""
                    INSERT INTO dma_dwh.public.fact_cart_line (cartid_bk, product_sk, customer_sk, date_sk, time_sk, quantity)
                    VALUES (:cartid_bk, :product_sk, :customer_sk, :date_sk, :time_sk, :quantity);
                    """)
                    with dwh_engine.begin() as conn:
                        conn.execute(insert_sql, {
                            'cartid_bk': row['sgcp_id_cart'],
                            'product_sk': row['dp_product_key'],
                            'customer_sk': row['dc_customer_key'],
                            'date_sk': row['date_key'],
                            'time_sk': row['time_key'],
                            'quantity': row['sgcp_quantity'],
                        })

                    if self is not None and self.is_aborted():
                        print("Úloha zrušená")
                        return

                del df_date
                del df_time
                del merged
            del chunk
            gc.collect()

    print("Spracovanie `fact_cart_line` dokončené.")
    return

def load_fact_order_line(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Úloha zrušená")
        return

    stage_query = """
    SELECT
        sgod.id_order AS sgod_id_order,
        sgod.id_order_detail AS sgod_id_order_detail,
        sgo.id_cart AS sgo_id_cart,
        (SELECT dp.product_key FROM dma_stage.public.dim_product_fdw dp
         WHERE dp.productid_bk = sgod.product_id
         AND dp.productattributeid_bk = sgod.product_attribute_id
         AND dp.valid_to = '9999-12-31'
         LIMIT 1) AS dp_product_key,
        (SELECT dc.customer_key FROM dma_stage.public.dim_customer_fdw dc
         WHERE dc.customerid_bk = sgo.id_customer
         AND dc.valid_to = '9999-12-31'
         LIMIT 1) AS dc_customer_key,
        (SELECT dadr.address_key FROM dma_stage.public.dim_address_fdw dadr
         WHERE dadr.addressid_bk = sgo.id_address_delivery
         LIMIT 1) AS dadr_address_key,
        sgo.date_add AS sgo_date_add,
        sgod.product_quantity AS sgod_product_quantity,
        sgod.unit_price_tax_excl AS sgod_unit_price_tax_excl,
        sgod.unit_price_tax_incl AS sgod_unit_price_tax_incl,
        sgod.total_price_tax_excl AS sgod_total_price_tax_excl,
        sgod.total_price_tax_incl AS sgod_total_price_tax_incl,
        sgo.total_paid_tax_excl AS sgo_total_paid_tax_excl,
        sgo.total_paid_tax_incl AS sgo_total_paid_tax_incl,
        sgod.tax_rate AS sgod_tax_rate,
        sgo.conversion_rate AS sgo_conversion_rate,
        sgo.carrier AS sgo_carrier,
        sgo.payment AS sgo_payment
    FROM dma_stage.dma_db_stage.sg_order_detail sgod 
    JOIN dma_stage.dma_db_stage.sg_orders sgo 
        ON sgo.id_order = sgod.id_order
    ORDER BY sgo.date_add;
    """

    print('Spracovanie `fact_order_line` sa začalo...')

    chunksize = 10000

    with stage_engine.connect().execution_options(stream_results=True) as conn:
        for chunk in pd.read_sql_query(text(stage_query), con=conn, chunksize=chunksize):
            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            print('Spracovanie bloku...')

            chunk = chunk[chunk['dp_product_key'].notnull() & chunk['dc_customer_key'].notnull()]
            chunk['dp_product_key'] = chunk['dp_product_key'].fillna(0).astype('int64')

            key_list = list(zip(chunk['sgod_id_order'], chunk['sgod_id_order_detail'], chunk['dp_product_key']))
            values_clause = ", ".join(f"({int(a)}, {int(b)}, {int(c)})" for a, b, c in key_list)
            query_fact = f"""
            SELECT st.orderid_bk, st.orderdetailid_bk, st.product_sk
            FROM (
                VALUES {values_clause}
            ) AS st(orderid_bk, orderdetailid_bk, product_sk)
            LEFT JOIN dma_dwh.public.fact_order_line fol
              ON st.orderid_bk = fol.orderid_bk
              AND st.orderdetailid_bk = fol.orderdetailid_bk
              AND st.product_sk = fol.product_sk
            WHERE fol.orderid_bk IS NULL;
            """

            df_fact = pd.read_sql_query(text(query_fact), dwh_engine)

            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            if not df_fact.empty:
                existing_keys = set(df_fact[['orderid_bk', 'orderdetailid_bk', 'product_sk']].itertuples(index=False, name=None))
                chunk = chunk[chunk.apply(lambda row: (row['sgod_id_order'], row['sgod_id_order_detail'], row['dp_product_key']) in existing_keys, axis=1)]

                chunk['sgo_carrier'] = chunk['sgo_carrier'].replace('', None)

                chunk['sgo_date_add'] = pd.to_datetime(chunk['sgo_date_add'], utc=True)
                date_add_list = chunk['sgo_date_add'].dt.date.tolist()

                query_date = text("SELECT date_key, date FROM dma_dwh.public.dim_date WHERE date IN :keys")
                df_date = pd.read_sql_query(query_date, dwh_engine, params={"keys": tuple(date_add_list)})

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

                time_add_list = chunk['sgo_date_add'].dt.floor('h').dt.time.tolist()

                query_time = text("SELECT time_key, time FROM dma_dwh.public.dim_time WHERE time IN :keys")
                df_time = pd.read_sql_query(query_time, dwh_engine, params={"keys": tuple(time_add_list)})

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

                chunk['sg_date'] = chunk['sgo_date_add'].dt.date
                chunk['sg_time'] = chunk['sgo_date_add'].dt.floor('h').dt.time

                merged = pd.merge(chunk, df_date, left_on='sg_date', right_on='date', how='left')
                merged = pd.merge(merged, df_time, left_on='sg_time', right_on='time', how='left')

                merged['date_key'] = merged['date_key'].astype('float64')
                merged['time_key'] = merged['time_key'].astype('float64')
                merged = merged.fillna({'date_key': 0, 'time_key': 0})
                merged = merged.astype({'date_key': 'int64', 'time_key': 'int64'})

                for _, row in merged.iterrows():
                    insert_sql = text("""
                    INSERT INTO dma_dwh.public.fact_order_line (orderid_bk, orderdetailid_bk, cartid_bk, product_sk, customer_sk, address_sk, date_sk, time_sk, quantity, price, price_tax_incl, amount, amount_tax_incl, paid, paid_tax_incl, taxrate, conversion_rate, carrier, paymenttype)
                    VALUES (:orderid_bk, :orderdetailid_bk, :cartid_bk, :product_sk, :customer_sk, :address_sk, :date_sk, :time_sk, :quantity, :price, :price_tax_incl, :amount, :amount_tax_incl, :paid, :paid_tax_incl, :taxrate, :conversion_rate, :carrier, :paymenttype);
                    """)
                    with dwh_engine.begin() as conn:
                        conn.execute(insert_sql, {
                            'orderid_bk': row['sgod_id_order'],
                            'orderdetailid_bk': row['sgod_id_order_detail'],
                            'cartid_bk': row['sgo_id_cart'],
                            'product_sk': row['dp_product_key'],
                            'customer_sk': row['dc_customer_key'],
                            'address_sk': None if pd.isna(row['dadr_address_key']) else row['dadr_address_key'],
                            'date_sk': row['date_key'],
                            'time_sk': row['time_key'],
                            'quantity': row['sgod_product_quantity'],
                            'price': row['sgod_unit_price_tax_excl'],
                            'price_tax_incl': row['sgod_unit_price_tax_incl'],
                            'amount': row['sgod_total_price_tax_excl'],
                            'amount_tax_incl': row['sgod_total_price_tax_incl'],
                            'paid': row['sgo_total_paid_tax_excl'],
                            'paid_tax_incl': row['sgo_total_paid_tax_incl'],
                            'taxrate': row['sgod_tax_rate'],
                            'conversion_rate': row['sgo_conversion_rate'],
                            'carrier': row['sgo_carrier'],
                            'paymenttype': row['sgo_payment'],
                        })

                    if self is not None and self.is_aborted():
                        print("Úloha zrušená")
                        return

                del df_date
                del df_time
                del merged
            del chunk
            gc.collect()

    print("Spracovanie `fact_order_line` dokončené.")
    return

def load_fact_order_history(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Úloha zrušená")
        return

    stage_query = """
    SELECT
        sgoh.id_order_history AS sgoh_id_order_history,
        (SELECT dos.orderstate_key FROM dma_stage.public.dim_order_state_fdw dos
         WHERE dos.orderstateid_bk = sgoh.id_order_state
         LIMIT 1) AS dos_orderstate_key,
        sgoh.id_order AS sgoh_id_order,
        sgoh.id_order_state AS sgoh_id_order_state,
        sgoh.date_add AS sgoh_date_add
    FROM dma_stage.dma_db_stage.sg_order_history sgoh
    ORDER BY sgoh.id_order_history;
    """

    print('Spracovanie `fact_order_history` sa začalo...')

    chunksize = 10000

    with stage_engine.connect().execution_options(stream_results=True) as conn:
        for chunk in pd.read_sql_query(text(stage_query), con=conn, chunksize=chunksize):
            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            print('Spracovanie bloku...')

            key_list = list(zip(chunk['sgoh_id_order_history'], chunk['sgoh_id_order'], chunk['sgoh_id_order_state']))
            values_clause = ", ".join(f"({int(a)}, {int(b)}, {int(c)})" for a, b, c in key_list)
            query_fact = f"""
            SELECT st.orderhistoryid_bk, st.orderid_bk, st.orderstateid_bk
            FROM (
                VALUES {values_clause}
            ) AS st(orderhistoryid_bk, orderid_bk, orderstateid_bk)
            LEFT JOIN dma_dwh.public.fact_order_history fo
                ON st.orderhistoryid_bk = fo.orderhistoryid_bk
                AND st.orderid_bk = fo.orderid_bk
                AND st.orderstateid_bk = fo.orderstateid_bk
                WHERE fo.orderhistoryid_bk IS NULL;
            """

            df_fact = pd.read_sql_query(text(query_fact), dwh_engine)
            if self is not None and self.is_aborted():
                print("Úloha zrušená")
                return

            if not df_fact.empty:
                existing_keys = set(df_fact[['orderhistoryid_bk', 'orderid_bk', 'orderstateid_bk']].itertuples(index=False, name=None))
                chunk = chunk[chunk.apply(lambda row: (row['sgoh_id_order_history'], row['sgoh_id_order'], row['sgoh_id_order_state']) in existing_keys, axis=1)]

                chunk['sgoh_date_add'] = pd.to_datetime(chunk['sgoh_date_add'], utc=True)
                date_add_list = chunk['sgoh_date_add'].dt.date.tolist()

                query_date = text("SELECT date_key, date FROM dma_dwh.public.dim_date WHERE date IN :keys")
                df_date = pd.read_sql_query(query_date, dwh_engine, params={"keys": tuple(date_add_list)})

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

                time_add_list = chunk['sgoh_date_add'].dt.floor('h').dt.time.tolist()

                query_time = text("SELECT time_key, time FROM dma_dwh.public.dim_time WHERE time IN :keys")
                df_time = pd.read_sql_query(query_time, dwh_engine, params={"keys": tuple(time_add_list)})

                if self is not None and self.is_aborted():
                    print("Úloha zrušená")
                    return

                chunk['sg_date'] = chunk['sgoh_date_add'].dt.date
                chunk['sg_time'] = chunk['sgoh_date_add'].dt.floor('h').dt.time

                merged = pd.merge(chunk, df_date, left_on='sg_date', right_on='date', how='left')
                merged = pd.merge(merged, df_time, left_on='sg_time', right_on='time', how='left')

                merged['date_key'] = merged['date_key'].astype('float64')
                merged['time_key'] = merged['time_key'].astype('float64')
                merged = merged.fillna({'date_key': 0, 'time_key': 0})
                merged = merged.astype({'date_key': 'int64', 'time_key': 'int64'})

                for _, row in merged.iterrows():
                    insert_sql = text("""
                    INSERT INTO dma_dwh.public.fact_order_history (orderhistoryid_bk, orderstate_sk, orderid_bk, orderstateid_bk, date_sk, time_sk)
                    VALUES (:orderhistoryid_bk, :orderstate_sk, :orderid_bk, :orderstateid_bk, :date_sk, :time_sk);
                    """)
                    with dwh_engine.begin() as conn:
                        conn.execute(insert_sql, {
                            'orderhistoryid_bk': row['sgoh_id_order_history'],
                            'orderstate_sk': row['dos_orderstate_key'],
                            'orderid_bk': row['sgoh_id_order'],
                            'orderstateid_bk': row['sgoh_id_order_state'],
                            'date_sk': row['date_key'],
                            'time_sk': row['time_key'],
                        })

                    if self is not None and self.is_aborted():
                        print("Úloha zrušená")
                        return

                del df_date
                del df_time
                del merged
            del chunk
            gc.collect()

    print("Spracovanie `fact_order_history` dokončené.")
    return

def load_fact_order(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Úloha zrušená")
        return

    query = """
    INSERT INTO fact_order (orderid_bk, customer_sk, address_sk, date_sk, time_sk, paid, paid_tax_incl, taxrate, conversion_rate, paymenttype, carrier)
    SELECT 
        fol.orderid_bk, 
        fol.customer_sk, 
        fol.address_sk, 
        fol.date_sk, 
        fol.time_sk, 
        MAX(fol.paid) AS paid,  
        MAX(fol.paid_tax_incl) AS paid_tax_incl,  
        MAX(fol.taxrate) AS taxrate,  
        MAX(fol.conversion_rate) AS conversion_rate,  
        MAX(fol.paymenttype) AS paymenttype,  
        MAX(fol.carrier) AS carrier  
    FROM fact_order_line fol
    WHERE NOT EXISTS (
        SELECT 1 FROM fact_order fo WHERE fo.orderid_bk = fol.orderid_bk
    )
    GROUP BY fol.orderid_bk, fol.customer_sk, fol.address_sk, fol.date_sk, fol.time_sk;
    """

    print('Spracovanie `fact_order` sa začalo...')

    with dwh_engine.begin() as conn:
        conn.execute(text(query))

    if self is not None and self.is_aborted():
        print("Úloha zrušená")
        return

    print("Spracovanie `fact_order` dokončené.")