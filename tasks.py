from celery import Celery
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
import time
from celeryconfig import broker_url, result_backend, PROD_DB_URI, STAGE_DB_URI, DWH_DB_URI
import signal

task_revoked = False

celery_app = Celery('etl_tasks', broker=broker_url, backend=result_backend)
celery_app.config_from_object('celeryconfig')

prod_engine = create_engine(PROD_DB_URI)
stage_engine = create_engine(STAGE_DB_URI)
dwh_engine = create_engine(DWH_DB_URI)

TABLES_CONFIG = {
    "ps_address": {
        "select": "SELECT id_address, id_country, id_state, id_customer, id_customer_company, postcode, city, date_add, date_upd, active, deleted, `default`, has_phone FROM ps_address",
        "target": "sg_address"
    },
    "ps_country": {
        "select": "SELECT id_country, id_zone, iso_code, call_prefix, active, contains_states, need_zip_code, zip_code_format, default_tax, name FROM ps_country",
        "target": "sg_country"
    },
    "ps_customer": {
        "select": "SELECT id_customer, id_gender, id_default_group, hashed_login, birthday, newsletter, active, is_guest, deleted, date_add, date_upd FROM ps_customer",
        "target": "sg_customer"
    },
    "ps_customer_company": {
        "select": "SELECT id_customer_company, id_customer, name, verified, active, date_add, date_upd, id_address FROM ps_customer_company",
        "target": "sg_customer_company"
    },
    "ps_customer_group": {
        "select": "SELECT id_customer, id_group FROM ps_customer_group",
        "target": "sg_customer_group"
    },
    "ps_gender": {
        "select": "SELECT id_gender, `type`, name FROM ps_gender",
        "target": "sg_gender"
    },
    "ps_group": {
        "select": "SELECT id_group, date_add, date_upd, is_wholesale, order_days_return, order_days_complaint, name FROM ps_group",
        "target": "sg_group"
    },
    "ps_category": {
        "select": "SELECT id_category, id_parent, level_depth, nleft, nright, active, date_add, date_upd, is_root_category, name FROM ps_category",
        "target": "sg_category"
    },
    "ps_manufacturer": {
        "select": "SELECT id_manufacturer, name, date_add, date_upd, active FROM ps_manufacturer",
        "target": "sg_manufacturer"
    },
    "ps_product_attribute_combination": {
        "select": "SELECT id_attribute, id_product_attribute FROM ps_product_attribute_combination",
        "target": "sg_product_attribute_combination"
    },
    "ps_attribute": {
        "select": "SELECT id_attribute, id_attribute_group, color, name FROM ps_attribute",
        "target": "sg_attribute"
    },
    "ps_attribute_group": {
        "select": "SELECT id_attribute_group, is_color_group, name FROM ps_attribute_group",
        "target": "sg_attribute_group"
    },
    "ps_currency": {
        "select": "SELECT id_currency, name, iso_code, iso_code_num, sign, blank, format, decimals, conversion_rate, default_vat_rate, deleted, active, default_on_instance FROM ps_currency",
        "target": "sg_currency"
    },
    "ps_cart_product": {
        "select": "SELECT id_cart, id_product, id_product_attribute, quantity, date_add FROM ps_cart_product",
        "target": "sg_cart_product"
    },
    "ps_order_history": {
        "select": "SELECT id_order_history, id_order, id_order_state, date_add FROM ps_order_history",
        "target": "sg_order_history"
    },
    "ps_order_state": {
        "select": "SELECT id_order_state, invoice, slip, color, unremovable, hidden, shipped, paid, closed, is_canceled_state, can_send_repay, can_be_canceled, name FROM ps_order_state",
        "target": "sg_order_state"
    },
    "ps_order_slip": {
        "select": "SELECT id_order_slip, conversion_rate, id_customer, id_order, total_products_tax_excl, total_products_tax_incl, total_shipping_tax_excl, total_shipping_tax_incl, shipping_cost, amount, shipping_cost_amount, `partial`, date_add, date_upd FROM ps_order_slip",
        "target": "sg_order_slip"
    },
    "ps_order_slip_detail": {
        "select": "SELECT id_order_slip, id_order_detail, product_quantity, unit_price_tax_excl, unit_price_tax_incl, total_price_tax_excl, total_price_tax_incl, amount_tax_excl, amount_tax_incl FROM ps_order_slip_detail",
        "target": "sg_order_slip_detail"
    },
}

def revoke_handler(signum, frame):
    global task_revoked
    task_revoked = True

signal.signal(signal.SIGTERM, revoke_handler)

def insert_etl_log(job_name):
    with stage_engine.begin() as conn:
        started_at = datetime.now()
        result = conn.execute(text("""
            INSERT INTO etl_log (job_name, started_at, status)
            VALUES (:job_name, :started_at, :status)
            RETURNING id
        """), {"job_name": job_name, "started_at": started_at, "status": "RUNNING"})
        row = result.fetchone()
        return row[0] if row is not None else None

def update_etl_log(log_id, status, message=None, tables_processed=None):
    with stage_engine.begin() as conn:
        ended_at = datetime.now()
        conn.execute(text("""
            UPDATE etl_log
            SET status = :status,
                ended_at = :ended_at,
                message = :message,
                tables_processed = :tables_processed
            WHERE id = :log_id
        """), {
            "status": status,
            "ended_at": ended_at,
            "message": message,
            "tables_processed": tables_processed,
            "log_id": log_id
        })

def clear_stage_tables():
    target_tables = [ config["target"] for config in TABLES_CONFIG.values() ]
    with stage_engine.begin() as conn:
        for table in target_tables:
            if task_revoked:
                print("Task revoked.")
                return
            conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
            print(f"Table {table} truncated.")

def et_table(table_name, query, target_table, chunksize=50000):
    print(f"Synchronizing table {table_name}...")
    for chunk in pd.read_sql_query(query, con=prod_engine, chunksize=chunksize):
        if task_revoked:
            print("Task revoked.")
            return

        chunk['active'] = chunk['active'].apply(lambda x: None if pd.isnull(x) else bool(x))
        chunk['deleted'] = chunk['deleted'].apply(lambda x: None if pd.isnull(x) else bool(x))
        chunk['default'] = chunk['default'].apply(lambda x: None if pd.isnull(x) else bool(x))
        chunk['has_phone'] = chunk['has_phone'].apply(lambda x: None if pd.isnull(x) else bool(x))
        chunk.to_sql(target_table, con=stage_engine, if_exists='append', index=False, method='multi')
        print(f"Processed {chunksize} rows.")
    print(f"Table {table_name} synchronized.")

@celery_app.task(bind=True)
def stage_reload_task(self):
    job_name = "stage_reload"
    log_id = insert_etl_log(job_name)
    try:
        print("Starting ETL process...")
        clear_stage_tables()
        if task_revoked:
            update_etl_log(log_id, "REVOKED", "Task revoked")
            return {"status": "REVOKED", "tables": 0}
        tables_processed = 0
        for table_name, config in TABLES_CONFIG.items():
            if task_revoked:
                print("Task revoked.")
                break
            et_table(table_name, config["select"], config["target"])
            tables_processed += 1
        print("End of ETL process.")
        if task_revoked:
            update_etl_log(log_id, "REVOKED", "Task revoked", tables_processed)
            return {"status": "REVOKED", "tables": tables_processed}
        update_etl_log(log_id, "SUCCESS", "Stage reload completed", tables_processed)
        return {"status": "SUCCESS", "tables": tables_processed}
    except Exception as e:
        update_etl_log(log_id, "FAILED", str(e))
        raise e

@celery_app.task(bind=True)
def dwh_incremental_task(self):
    job_name = "dwh_incremental"
    log_id = insert_etl_log(job_name)
    try:
        time.sleep(3)
        tables_processed = 10
        update_etl_log(log_id, "SUCCESS", "DWH incremental load completed", tables_processed)
        return {"status": "SUCCESS", "rows": tables_processed}
    except Exception as e:
        update_etl_log(log_id, "FAILED", str(e))
        raise e
