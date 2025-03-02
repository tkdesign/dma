import pandas as pd
from sqlalchemy import text
import gc

def load_bridge_product_attribute(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Task aborted.")
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
    LEFT JOIN
        dma_stage.public.dim_product_fdw dp ON pac.id_product_attribute = dp.productattributeid_bk
    LEFT JOIN
        dma_stage.public.dim_attribute_fdw da ON pac.id_attribute = da.attributeid_bk
    LEFT JOIN
        dma_stage.public.bridge_product_attribute_fdw bpa ON dp.product_key = bpa.product_sk AND da.attribute_key = bpa.attribute_sk
    WHERE bpa.product_sk IS NULL
    ORDER BY pac.id_product_attribute
    """

    print('Start processing...')

    chunksize = 10000
    for chunk in pd.read_sql_query(query, stage_engine, chunksize=chunksize):
        if self is not None and self.is_aborted():
            print("Task aborted.")
            return

        print('Processing chunk...')

        chunk = chunk.drop_duplicates()

        insert_sql = text("""
        INSERT INTO dma_dwh.public.bridge_product_attribute (product_sk, attribute_sk, productattributeid_bk, attributeid_bk)
        VALUES (:product_sk, :attribute_sk, :id_product_attribute, :id_attribute)
        """)

        with dwh_engine.begin() as conn:
            for _, row in chunk.iterrows():
                conn.execute(insert_sql, {
                    'product_sk': int(row['product_sk']),
                    'attribute_sk': int(row['attribute_sk']),
                    'id_product_attribute': int(row['id_product_attribute']),
                    'id_attribute': int(row['id_attribute'])
                })

                if self is not None and self.is_aborted():
                    print("Task aborted.")
                    return

        del chunk
        gc.collect()

    print("Processing completed.")
    return