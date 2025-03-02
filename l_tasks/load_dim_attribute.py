import pandas as pd
from sqlalchemy import text
import hashlib
import gc

def calc_hash(row):
    data = f"{row['attributeid_bk']}-{row['attribute_name']}-{row['attribute_group']}"
    return hashlib.md5(data.encode('utf-8')).hexdigest()

def load_dim_attribute(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Task aborted.")
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

    print('Start processing...')

    chunksize = 10000

    for chunk in pd.read_sql_query(stage_query, stage_engine, chunksize=chunksize):
        if self is not None and self.is_aborted():
            print("Task aborted.")
            return

        print('Processing chunk...')

        chunk['row_hash_stage'] = chunk.apply(calc_hash, axis=1)
        business_keys = chunk['attributeid_bk'].unique().tolist()

        query_dim = text("SELECT * FROM dma_dwh.public.dim_attribute WHERE attributeid_bk IN :keys")
        df_dim = pd.read_sql_query(query_dim, dwh_engine, params={"keys": tuple(business_keys)})

        if self is not None and self.is_aborted():
            print("Task aborted.")
            return

        if not df_dim.empty:
            df_dim['row_hash_dim'] = df_dim.apply(lambda row: calc_hash({
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
                print("Task aborted.")
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