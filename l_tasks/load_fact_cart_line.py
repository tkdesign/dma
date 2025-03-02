import pandas as pd
from sqlalchemy import text
import gc

def load_fact_cart_line(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Task aborted.")
        return
    
    stage_query = """
    SELECT
        sgcp.id_cart AS sgcp_id_cart,
        sgcp.quantity AS sgcp_quantity,
        sgc.date_add AS sgc_date_add,
        dp.product_key AS dp_product_key,
        dc.customer_key AS dc_customer_key,
        fcl.cartline_key AS fcl_cartline_key
    FROM dma_stage.dma_db_stage.sg_cart_product sgcp 
    JOIN dma_stage.dma_db_stage.sg_cart sgc ON sgc.id_cart = sgcp.id_cart
    LEFT JOIN dma_stage.public.dim_product_fdw dp ON sgcp.id_product = dp.productid_bk AND sgcp.id_product_attribute = dp.productattributeid_bk
    LEFT JOIN dma_stage.public.dim_customer_fdw dc ON sgc.id_customer = dc.customerid_bk
    LEFT JOIN dma_stage.public.fact_cart_line_fdw fcl ON fcl.cartid_bk = sgc.id_cart AND fcl.product_sk = dp.product_key
	WHERE fcl.cartline_key IS NULL
    ORDER BY sgcp.date_add;
    """

    print('Start processing...')

    chunksize = 10000

    for chunk in pd.read_sql_query(stage_query, stage_engine, chunksize=chunksize):
        if self is not None and self.is_aborted():
            print("Task aborted.")
            return

        print('Processing chunk...')

        chunk = chunk.drop_duplicates()
        chunk = chunk[chunk['dp_product_key'].notnull() & chunk['dc_customer_key'].notnull()]

        chunk['sgc_date_add'] = pd.to_datetime(chunk['sgc_date_add'], utc=True)
        date_add_list = chunk['sgc_date_add'].dt.date.tolist()

        query_date = text("SELECT date_key, date FROM dma_dwh.public.dim_date WHERE date IN :keys")
        df_date = pd.read_sql_query(query_date, dwh_engine, params={"keys": tuple(date_add_list)})

        if self is not None and self.is_aborted():
            print("Task aborted.")
            return

        time_add_list = chunk['sgc_date_add'].dt.floor('h').dt.time.tolist()

        query_time = text("SELECT time_key, time FROM dma_dwh.public.dim_time WHERE time IN :keys")
        df_time = pd.read_sql_query(query_time, dwh_engine, params={"keys": tuple(time_add_list)})

        if self is not None and self.is_aborted():
            print("Task aborted.")
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
                print("Task aborted.")
                return

        del df_date
        del df_time
        del merged
        del chunk
        gc.collect()

    print("Processing completed.")
    return