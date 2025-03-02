import pandas as pd
from sqlalchemy import text
import hashlib
from datetime import date, datetime
import gc

def calc_hash(row):
    data = f"{row['customerid_bk']}-{row['hashedemail']}-{row['defaultgroup']}-{row['birthday']}-{row['gender']}-{row['businessaccount']}-{row['active']}"
    return hashlib.md5(data.encode('utf-8')).hexdigest()

def load_dim_customer(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Task aborted.")
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

    print('Start processing...')

    chunksize = 10000
    today = date.today()
    min_date = datetime(2000, 1, 1)

    for chunk in pd.read_sql_query(stage_query, stage_engine, chunksize=chunksize):
        if self is not None and self.is_aborted():
            print("Task aborted.")
            return

        print('Processing chunk...')

        chunk['row_hash_stage'] = chunk.apply(calc_hash, axis=1)
        business_keys = chunk['customerid_bk'].unique().tolist()

        query_dim = text("SELECT * FROM dma_dwh.public.dim_customer WHERE customerid_bk IN :keys")
        df_dim = pd.read_sql_query(query_dim, dwh_engine, params={"keys": tuple(business_keys)})

        if self is not None and self.is_aborted():
            print("Task aborted.")
            return

        if not df_dim.empty:
            df_dim['row_hash_dim'] = df_dim.apply(lambda row: calc_hash({
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
                print("Task aborted.")
                return

        for idx, row in changed_records.iterrows():
            update_sql = text("""
            UPDATE dma_dwh.public.dim_customer
            SET valid_to = :valid_to
            WHERE customer_key = :customer_key;
            """)
            valid_to = today - pd.DateOffset(days=1)
            with dwh_engine.begin() as conn:
                conn.execute(update_sql, {
                    'valid_to': valid_to,
                    'customer_key': row['customer_key']
                })

            if self is not None and self.is_aborted():
                print("Task aborted.")
                return

            insert_sql = text("""
            INSERT INTO dma_dwh.public.dim_customer (customerid_bk, hashedemail, defaultgroup, birthdate, gender, businessaccount, active, valid_from, valid_to)
            VALUES (:customerid_bk, :hashedemail, :defaultgroup, :birthdate, :gender, :businessaccount, :active, :valid_from, '9999-12-31');
            """)
            valid_from = row['valid_from'] if not pd.isna(row['valid_from']) else min_date
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