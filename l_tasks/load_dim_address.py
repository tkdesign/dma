import pandas as pd
from sqlalchemy import text
import hashlib
from datetime import date, datetime
import gc

def calc_hash(row):
    data = f"{row['addressid_bk']}-{row['country']}-{row['state']}-{row['city']}-{row['zipcode']}"
    return hashlib.md5(data.encode('utf-8')).hexdigest()

def load_dim_address(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Task aborted.")
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

    print('Start processing...')

    today = date.today()
    min_date = datetime(2000, 1, 1)
    chunksize = 10000

    for chunk in pd.read_sql_query(stage_query, stage_engine, chunksize=chunksize):
        if self is not None and self.is_aborted():
            print("Task aborted.")
            return

        print('Processing chunk...')

        chunk['row_hash_stage'] = chunk.apply(calc_hash, axis=1)
        business_keys = chunk['addressid_bk'].unique().tolist()
        query_dim = text("SELECT * FROM dma_dwh.public.dim_address WHERE addressid_bk IN :keys")
        df_dim = pd.read_sql_query(query_dim, dwh_engine, params={"keys": tuple(business_keys)})

        if self is not None and self.is_aborted():
            print("Task aborted.")
            return

        if not df_dim.empty:
            df_dim['row_hash_dim'] = df_dim.apply(lambda row: calc_hash({
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
                print("Task aborted.")
                return

        for idx, row in changed_records.iterrows():
            update_sql = text("""
            UPDATE dma_dwh.public.dim_address
            SET valid_to = :valid_to
            WHERE address_key = :address_key;
            """)
            valid_to = today - pd.DateOffset(days=1)
            with dwh_engine.begin() as conn:
                conn.execute(update_sql, {
                    'valid_to': valid_to,
                    'address_key': row['address_key']
                })

            if self is not None and self.is_aborted():
                print("Task aborted.")
                return

            insert_sql = text("""
            INSERT INTO dma_dwh.public.dim_address (addressid_bk, customerid_bk, country, state, city, zipcode, valid_from, valid_to)
            VALUES (:addressid_bk, :customerid_bk, :country, :state, :city, :zipcode, :valid_from, '9999-12-31');
            """)
            valid_from = row['valid_from'] if not pd.isna(row['valid_from']) else min_date
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
                print("Task aborted.")
                return

        del new_records
        del changed_records
        del merged
        del df_dim
        del chunk
        gc.collect()

    print("Processing completed.")
    return