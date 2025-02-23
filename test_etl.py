import pandas as pd
from sqlalchemy import create_engine, text
import hashlib
from datetime import date

STAGE_DB_URI = 'postgresql+psycopg2://postgres:lurcakah@localhost/dma_stage?options=-csearch_path=dma_db_stage'
DWH_DB_URI = 'postgresql+psycopg2://postgres:lurcakah@localhost/dma_dwh'


def calc_hash(row):
    data = f"{row['customerid_bk']}-{row['hashedemail']}-{row['defaultgroup']}-{row['birthday']}-{row['gender']}-{row['businessaccount']}-{row['active']}"
    return hashlib.md5(data.encode('utf-8')).hexdigest()


stage_engine = create_engine(STAGE_DB_URI)
dwh_engine = create_engine(DWH_DB_URI)

stage_query = """
SELECT DISTINCT
    c.id_customer AS customerid_bk,
    c.hashed_login AS hashedemail,
    gr.name AS defaultgroup,
    c.birthday,
    gen.name AS gender,
    CASE WHEN cc.id_customer IS NOT NULL THEN true ELSE false END AS businessaccount,
    c.active AS active,
    c.date_add AS valid_from
FROM sg_customer c
LEFT JOIN sg_customer_company cc ON cc.id_customer = c.id_customer
LEFT JOIN sg_customer_group cg ON cg.id_group = c.id_default_group
LEFT JOIN sg_group gr ON gr.id_group = cg.id_group
LEFT JOIN sg_gender gen ON gen.id_gender = c.id_gender
ORDER BY c.id_customer
"""

chunksize = 50000
today = date.today()
print('Start processing...')
for chunk in pd.read_sql_query(stage_query, stage_engine, chunksize=chunksize):
    chunk['row_hash_stage'] = chunk.apply(calc_hash, axis=1)
    print('Processing chunk...')

    business_keys = chunk['customerid_bk'].unique().tolist()

    query_dim = text("SELECT * FROM dma_dwh.public.dim_customer WHERE customerid_bk IN :keys")
    df_dim = pd.read_sql_query(query_dim, dwh_engine, params={"keys": tuple(business_keys)})

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
            INSERT INTO dim_customer (
                customerid_bk, hashedemail, defaultgroup, birthdate, gender, businessaccount, active, valid_from, valid_to
            ) VALUES (
                :customerid_bk, :hashedemail, :defaultgroup, :birthdate, :gender, :businessaccount, :active, :valid_from, '9999-12-31'
            )
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
                'valid_from': today
            })
    for idx, row in changed_records.iterrows():
        update_sql = text("""
            UPDATE dim_customer
            SET valid_to = :valid_to
            WHERE customer_key = :customer_key
        """)
        with dwh_engine.begin() as conn:
            conn.execute(update_sql, {
                'valid_to': today,
                'customer_key': row['customer_key']
            })

        insert_sql = text("""
            INSERT INTO dim_customer (
                customerid_bk, hashedemail, defaultgroup, birthdate, gender, businessaccount, active, valid_from, valid_to
            ) VALUES (
                :customerid_bk, :hashedemail, :defaultgroup, :birthdate, :gender, :businessaccount, :active, :valid_from, '9999-12-31'
            )
        """)
        with dwh_engine.begin() as conn:
            conn.execute(insert_sql, {
                'customerid_bk': row['customerid_bk'],
                'hashedemail': row['hashedemail'],
                'defaultgroup': row['defaultgroup'],
                'birthdate': row['birthday'],
                'gender': row['gender'],
                'businessaccount': row['businessaccount'],
                'active': row['active'],
                'valid_from': today
            })
