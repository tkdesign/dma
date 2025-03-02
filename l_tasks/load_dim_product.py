import pandas as pd
from sqlalchemy import text
import hashlib
from datetime import date, datetime
import gc

def calc_hash(row):
    data = f"{row['productid_bk']}-{row['productattributeid_bk']}-{row['productname']}-{row['manufacturer']}-{row['defaultcategory']}-{row['market_group']}-{row['market_subgroup']}-{row['market_gender']}-{row['price']}-{row['active']}"
    return hashlib.md5(data.encode('utf-8')).hexdigest()

def load_dim_product(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Task aborted.")
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
        chunk['productattributeid_bk'] = chunk['productattributeid_bk'].fillna(0).astype('int64')
        keys_pairs = list(zip(chunk['productid_bk'], chunk['productattributeid_bk']))

        query_dim = text("""
        SELECT * FROM dma_dwh.public.dim_product
        WHERE (productid_bk, productattributeid_bk) IN (
            SELECT * FROM unnest(:keys_pairs) AS t(productid_bk int, productattributeid_bk int)
        )
        """)
        df_dim = pd.read_sql_query(query_dim, dwh_engine, params={"keys_pairs": keys_pairs})

        if self is not None and self.is_aborted():
            print("Task aborted.")
            return

        if not df_dim.empty:
            df_dim['row_hash_dim'] = df_dim.apply(lambda row: calc_hash({
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

        merged = pd.merge(chunk, df_dim, on='productid_bk', how='left', suffixes=('_stage', '_dim'))

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
                    'productattributeid_bk': row['productattributeid_bk_stage'] if not pd.isna(row['productattributeid_bk_stage']) else None,
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
                print("Task aborted.")
                return

        for idx, row in changed_records.iterrows():
            update_sql = text("""
            UPDATE dma_dwh.public.dim_product
            SET valid_to = :valid_to
            WHERE product_key = :product_key;
            """)
            valid_to = today - pd.DateOffset(days=1)
            with dwh_engine.begin() as conn:
                conn.execute(update_sql, {
                    'valid_to': valid_to,
                    'product_key': row['product_key']
                })

            if self is not None and self.is_aborted():
                print("Task aborted.")
                return

            insert_sql = text("""
            INSERT INTO dma_dwh.public.dim_product (productid_bk, productattributeid_bk, productname, manufacturer, defaultcategory, market_group, market_subgroup, market_gender, price, active, valid_from, valid_to)
            VALUES (:productid_bk, :productattributeid_bk, :productname, :manufacturer, :defaultcategory, :market_group, :market_subgroup, :market_gender, :price, :active, :valid_from, '9999-12-31');
            """)
            valid_from = row['valid_from_stage'] if not pd.isna(row['valid_from_stage']) else min_date
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