import pandas as pd
from sqlalchemy import text
import gc

def load_fact_order_history(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Task aborted.")
        return

    stage_query = """
SELECT
		sgoh.id_order_history AS sgoh_id_order_history,
		dos.orderstate_key AS dos_orderstate_key,
		sgoh.id_order AS sgoh_id_order,
		sgoh.id_order_state AS sgoh_id_order_state,
		sgoh.date_add AS sgoh_date_add,
		foh.orderhistory_key AS foh_orderhistory_key
FROM dma_stage.dma_db_stage.sg_order_history sgoh 
LEFT JOIN dma_stage.public.dim_order_state_fdw dos ON dos.orderstateid_bk = sgoh.id_order_state
LEFT JOIN dma_stage.public.fact_order_history_fdw foh ON foh.orderhistoryid_bk = sgoh.id_order_history
WHERE foh.orderhistory_key IS NULL
ORDER BY sgoh.id_order_history;
    """

    print('Start processing...')

    chunksize = 10000

    for chunk in pd.read_sql_query(stage_query, stage_engine, chunksize=chunksize):
        if self is not None and self.is_aborted():
            print("Task aborted.")
            return

        print('Processing chunk...')

        chunk['sgoh_date_add'] = pd.to_datetime(chunk['sgoh_date_add'], utc=True)
        date_add_list = chunk['sgoh_date_add'].dt.date.tolist()

        query_date = text("SELECT date_key, date FROM dma_dwh.public.dim_date WHERE date IN :keys")
        df_date = pd.read_sql_query(query_date, dwh_engine, params={"keys": tuple(date_add_list)})

        if self is not None and self.is_aborted():
            print("Task aborted.")
            return

        time_add_list = chunk['sgoh_date_add'].dt.floor('h').dt.time.tolist()

        query_time = text("SELECT time_key, time FROM dma_dwh.public.dim_time WHERE time IN :keys")
        df_time = pd.read_sql_query(query_time, dwh_engine, params={"keys": tuple(time_add_list)})

        if self is not None and self.is_aborted():
            print("Task aborted.")
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
                print("Task aborted.")
                return

        del df_date
        del df_time
        del merged
        del chunk
        gc.collect()

    print("Processing completed.")
    return