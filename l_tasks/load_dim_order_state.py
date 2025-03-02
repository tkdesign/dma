import pandas as pd
from sqlalchemy import text
import hashlib
from datetime import date, datetime
import gc

def calc_hash(row):
    data = f"{row['orderstateid_bk']}-{row['current_state']}"
    return hashlib.md5(data.encode('utf-8')).hexdigest()

def load_dim_order_state(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Task aborted.")
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
        business_keys = chunk['orderstateid_bk'].unique().tolist()

        query_dim = text("SELECT * FROM dma_dwh.public.dim_order_state WHERE orderstateid_bk IN :keys")
        df_dim = pd.read_sql_query(query_dim, dwh_engine, params={"keys": tuple(business_keys)})

        if self is not None and self.is_aborted():
            print("Task aborted.")
            return

        if not df_dim.empty:
            df_dim['row_hash_dim'] = df_dim.apply(lambda row: calc_hash({'orderstateid_bk': row['orderstateid_bk'], 'current_state': row['current_state'],}), axis=1)
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
                print("Task aborted.")
                return

        for idx, row in changed_records.iterrows():
            update_sql = text("""
            UPDATE dma_dwh.public.dim_order_state
            SET valid_to = :valid_to
            WHERE orderstate_key = :orderstate_key;
            """)
            valid_to = today - pd.DateOffset(days=1)
            with dwh_engine.begin() as conn:
                conn.execute(update_sql, {
                    'valid_to': valid_to,
                    'orderstate_key': row['orderstate_key']
                })

            if self is not None and self.is_aborted():
                print("Task aborted.")
                return

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
                print("Task aborted.")
                return

        del merged
        del new_records
        del changed_records
        del df_dim
        del chunk
        gc.collect()

    print("Processing completed.")
    return