import pandas as pd
from sqlalchemy import text
import gc

def load_fact_order_line(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Task aborted.")
        return

    stage_query = """
SELECT
		sgod.id_order AS sgod_id_order,
		sgod.id_order_detail AS sgod_id_order_detail,
		sgo.id_cart AS sgo_id_cart,
		dp.product_key AS dp_product_key,
		dc.customer_key AS dc_customer_key,
		dadr.address_key AS dadr_address_key,
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
		sgo.payment AS sgo_payment,
		fol.orderline_key AS fol_orderline_key
FROM dma_stage.dma_db_stage.sg_order_detail sgod 
JOIN dma_stage.dma_db_stage.sg_orders sgo ON sgo.id_order = sgod.id_order
LEFT JOIN dma_stage.public.dim_product_fdw dp ON sgod.product_id = dp.productid_bk AND sgod.product_attribute_id = dp.productattributeid_bk
LEFT JOIN dma_stage.public.dim_customer_fdw dc ON sgo.id_customer = dc.customerid_bk
LEFT JOIN dma_stage.public.dim_address_fdw dadr ON dadr.addressid_bk = sgo.id_address_delivery
LEFT JOIN dma_stage.public.fact_order_line_fdw fol ON fol.cartid_bk = sgo.id_order AND fol.product_sk = dp.product_key
WHERE fol.orderline_key IS NULL
ORDER BY sgo.date_add;
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

        chunk['sgo_date_add'] = pd.to_datetime(chunk['sgo_date_add'], utc=True)
        date_add_list = chunk['sgo_date_add'].dt.date.tolist()

        query_date = text("SELECT date_key, date FROM dma_dwh.public.dim_date WHERE date IN :keys")
        df_date = pd.read_sql_query(query_date, dwh_engine, params={"keys": tuple(date_add_list)})

        if self is not None and self.is_aborted():
            print("Task aborted.")
            return

        time_add_list = chunk['sgo_date_add'].dt.floor('h').dt.time.tolist()

        query_time = text("SELECT time_key, time FROM dma_dwh.public.dim_time WHERE time IN :keys")
        df_time = pd.read_sql_query(query_time, dwh_engine, params={"keys": tuple(time_add_list)})

        if self is not None and self.is_aborted():
            print("Task aborted.")
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
                print("Task aborted.")
                return

        del df_date
        del df_time
        del merged
        del chunk
        gc.collect()

    print("Processing completed.")
    return