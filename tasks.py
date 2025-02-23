import hashlib

from celery import Celery
from celery.signals import task_revoked
from celery.contrib.abortable import AbortableTask
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
import time
from celeryconfig import broker_url, result_backend, PROD_DB_URI, STAGE_DB_URI, DWH_DB_URI

celery_app = Celery('etl_tasks', broker=broker_url, backend=result_backend)
celery_app.config_from_object('celeryconfig')

prod_engine = create_engine(PROD_DB_URI)
stage_engine = create_engine(STAGE_DB_URI)
dwh_engine = create_engine(DWH_DB_URI)

ET_TABLES_CONFIG = {
    "ps_address": {
        "select": "SELECT id_address, id_country, id_state, id_customer, id_customer_company, postcode, city, date_add, date_upd, active, deleted, `default`, has_phone FROM ps_address",
        "convert_fields": {
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "date_upd": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "active": lambda x: None if pd.isnull(x) else bool(x),
            "deleted": lambda x: None if pd.isnull(x) else bool(x),
            "default": lambda x: None if pd.isnull(x) else bool(x),
            "has_phone": lambda x: None if pd.isnull(x) else bool(x)
        },
        "target": "sg_address"
    },
    "ps_country": {
        "select": "SELECT id_country, id_zone, iso_code, call_prefix, active, contains_states, need_zip_code, zip_code_format, default_tax, name FROM ps_country",
        "convert_fields": {
            "active": lambda x: None if pd.isnull(x) else bool(x),
            "contains_states": lambda x: None if pd.isnull(x) else bool(x),
            "need_zip_code": lambda x: None if pd.isnull(x) else bool(x),
        },
        "target": "sg_country"
    },
    "ps_customer": {
        "select": "SELECT id_customer, id_gender, id_default_group, hashed_login, birthday, newsletter, active, is_guest, deleted, date_add, date_upd FROM ps_customer",
        "convert_fields": {
            "birthday": lambda x: None if pd.isnull(x) or x == "0000-00-00" else x,
            "newsletter": lambda x: None if pd.isnull(x) else bool(x),
            "active": lambda x: None if pd.isnull(x) else bool(x),
            "is_guest": lambda x: None if pd.isnull(x) else bool(x),
            "deleted": lambda x: None if pd.isnull(x) else bool(x),
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "date_upd": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
        },
        "target": "sg_customer"
    },
    "ps_customer_company": {
        "select": "SELECT id_customer_company, id_customer, name, verified, active, date_add, date_upd, id_address FROM ps_customer_company",
        "convert_fields": {
            "name": lambda x: None if pd.isnull(x) else hashlib.sha256(x.encode('utf-8')).hexdigest(),
            "verified": lambda x: None if pd.isnull(x) else bool(x),
            "active": lambda x: None if pd.isnull(x) else bool(x),
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "date_upd": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
        },
        "target": "sg_customer_company"
    },
    "ps_customer_group": {
        "select": "SELECT id_customer, id_group FROM ps_customer_group",
        "convert_fields": {},
        "target": "sg_customer_group"
    },
    "ps_gender": {
        "select": "SELECT id_gender, `type`, name FROM ps_gender",
        "convert_fields": {},
        "target": "sg_gender"
    },
    "ps_group": {
        "select": "SELECT id_group, date_add, date_upd, is_wholesale, order_days_return, order_days_complaint, name FROM ps_group",
        "convert_fields": {
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "date_upd": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "is_wholesale": lambda x: None if pd.isnull(x) else bool(x),
        },
        "target": "sg_group"
    },
    "ps_category": {
        "select": "SELECT id_category, id_parent, level_depth, nleft, nright, active, date_add, date_upd, is_root_category, name FROM ps_category",
        "convert_fields": {
            "level_depth": lambda x: None if pd.isnull(x) else int(x),
            "active": lambda x: None if pd.isnull(x) else bool(x),
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "date_upd": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "is_root_category": lambda x: None if pd.isnull(x) else bool(x),
        },
        "target": "sg_category"
    },
    "ps_manufacturer": {
        "select": "SELECT id_manufacturer, name, date_add, date_upd, active FROM ps_manufacturer",
        "convert_fields": {
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "date_upd": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "active": lambda x: None if pd.isnull(x) else bool(x),
        },
        "target": "sg_manufacturer"
    },
    "ps_product_attribute_combination": {
        "select": "SELECT id_attribute, id_product_attribute FROM ps_product_attribute_combination",
        "convert_fields": {},
        "target": "sg_product_attribute_combination"
    },
    "ps_attribute": {
        "select": "SELECT id_attribute, id_attribute_group, color, name FROM ps_attribute",
        "convert_fields": {},
        "target": "sg_attribute"
    },
    "ps_attribute_group": {
        "select": "SELECT id_attribute_group, is_color_group, name FROM ps_attribute_group",
        "convert_fields": {
            "is_color_group": lambda x: None if pd.isnull(x) else bool(x),
        },
        "target": "sg_attribute_group"
    },
    "ps_currency": {
        "select": "SELECT id_currency, name, iso_code, iso_code_num, sign, blank, format, decimals, conversion_rate, default_vat_rate, deleted, active, default_on_instance FROM ps_currency",
        "convert_fields": {
            "blank": lambda x: None if pd.isnull(x) else bool(x),
            "format": lambda x: None if pd.isnull(x) else bool(x),
            "decimals": lambda x: None if pd.isnull(x) else int(x),
            "deleted": lambda x: None if pd.isnull(x) else bool(x),
            "active": lambda x: None if pd.isnull(x) else bool(x),
        },
        "target": "sg_currency"
    },
    "ps_cart_product": {
        "select": "SELECT id_cart, id_product, id_product_attribute, quantity, date_add FROM ps_cart_product",
        "convert_fields": {
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
        },
        "target": "sg_cart_product"
    },
    "ps_order_history": {
        "select": "SELECT id_order_history, id_order, id_order_state, date_add FROM ps_order_history",
        "convert_fields": {
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
        },
        "target": "sg_order_history"
    },
    "ps_order_state": {
        "select": "SELECT id_order_state, invoice, slip, color, unremovable, hidden, shipped, paid, closed, is_canceled_state, can_send_repay, can_be_canceled, name FROM ps_order_state",
        "convert_fields": {
            "invoice": lambda x: None if pd.isnull(x) else bool(x),
            "slip": lambda x: None if pd.isnull(x) else bool(x),
            "unremovable": lambda x: None if pd.isnull(x) else bool(x),
            "hidden": lambda x: None if pd.isnull(x) else bool(x),
            "shipped": lambda x: None if pd.isnull(x) else bool(x),
            "paid": lambda x: None if pd.isnull(x) else bool(x),
            "is_canceled_state": lambda x: None if pd.isnull(x) else bool(x),
            "can_send_repay": lambda x: None if pd.isnull(x) else bool(x),
            "can_be_canceled": lambda x: None if pd.isnull(x) else bool(x),
        },
        "target": "sg_order_state"
    },
    "ps_order_slip": {
        "select": "SELECT id_order_slip, conversion_rate, id_customer, id_order, total_products_tax_excl, total_products_tax_incl, total_shipping_tax_excl, total_shipping_tax_incl, shipping_cost, amount, shipping_cost_amount, `partial`, date_add, date_upd FROM ps_order_slip",
        "convert_fields": {
            "partial": lambda x: None if pd.isnull(x) else bool(x),
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "date_upd": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
        },
        "target": "sg_order_slip"
    },
    "ps_order_slip_detail": {
        "select": "SELECT id_order_slip, id_order_detail, product_quantity, unit_price_tax_excl, unit_price_tax_incl, total_price_tax_excl, total_price_tax_incl, amount_tax_excl, amount_tax_incl FROM ps_order_slip_detail",
        "convert_fields": {},
        "target": "sg_order_slip_detail"
    },

    # joined tables
    "ps_product": {
        "select": "SELECT p.id_product, pa.id_product_attribute, p.id_manufacturer, p.id_category_default, p.price, p.wholesale_price, p.active, p.available_for_order, p.date_add, p.date_upd, p.price_type, p.force_disable, p.only_for_loyalty, p.has_loyalty_price, p.is_rental, p.rental_price, p.name, p.season, p.`group`, p.subgroup, p.gender FROM ps_product p LEFT JOIN ps_product_attribute pa ON pa.id_product=p.id_product;",
        "convert_fields": {
            "active": lambda x: None if pd.isnull(x) else bool(x),
            "available_for_order": lambda x: None if pd.isnull(x) else bool(x),
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "date_upd": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
        },
        "target": "sg_product"
    },
    "ps_stock_available": {
        "select": "SELECT sa.id_stock_available, sa.id_product, sa.id_product_attribute, sa.quantity, sa.date_add, sa.date_upd FROM ps_stock_available sa;",
        "convert_fields": {
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "date_upd": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
        },
        "target": "sg_stock_available"
    },
    "ps_cart": {
        "select": "SELECT DISTINCT crt.id_cart, crr.name as carrier, crt.id_address_invoice, crt.id_currency, crt.id_customer, crt.free_shipping, crt.date_add, crt.date_upd FROM ps_cart crt LEFT JOIN ps_carrier crr ON crr.id_carrier=crt.id_carrier;",
        "convert_fields": {
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "date_upd": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
        },
        "target": "sg_cart"
    },
    "ps_orders": {
        "select": "SELECT DISTINCT o.id_order, o.id_customer, o.id_cart, o.id_currency, crr.name as carrier, o.id_address_delivery, o.current_state, o.payment, o.conversion_rate, o.total_discounts, o.total_discounts_tax_incl, o.total_discounts_tax_excl, o.total_paid, o.total_paid_tax_incl, o.total_paid_tax_excl, o.total_paid_real, o.total_products, o.total_products_wt, o.total_shipping, o.total_shipping_tax_incl, o.total_shipping_tax_excl, o.carrier_tax_rate, o.total_cod_tax_incl, o.valid, o.date_add, o.date_upd, o.split_number, o.main_order_id, o.ip, o.review_mail_sent FROM ps_orders o LEFT JOIN ps_order_carrier ocrr ON ocrr.id_order=o.id_order LEFT JOIN ps_carrier crr ON crr.id_carrier=ocrr.id_carrier;",
        "convert_fields": {
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "date_upd": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "review_mail_sent": lambda x: None if pd.isnull(x) else bool(x),
        },
        "target": "sg_orders",
    },
    "ps_order_detail": {
        "select": "SELECT DISTINCT od.id_order_detail, od.id_order, od.product_id, od.product_attribute_id, od.product_name, od.product_quantity, od.product_quantity_in_stock, od.product_price, od.reduction_amount, od.reduction_amount_tax_incl, od.reduction_amount_tax_excl, od.tax_computation_method, od.total_price_tax_incl, od.total_price_tax_excl, od.unit_price_tax_incl, od.unit_price_tax_excl, od.purchase_supplier_price, tax.rate as tax_rate, tax.name as tax_name FROM ps_order_detail od LEFT JOIN ps_order_detail_tax odt ON odt.id_order_detail=od.id_order_detail LEFT JOIN ps_tax tax ON tax.id_tax=odt.id_tax;",
        "convert_fields": {
            "tax_rate": lambda x: 0.0 if pd.isnull(x) else x,
        },
        "target": "sg_order_detail"
    },
    "ps_order_payment": {
        "select": "SELECT DISTINCT op.id_order_payment, o.id_order, op.id_currency, op.amount, op.payment_method, op.date_add FROM ps_order_payment op LEFT JOIN ps_orders o ON o.reference=op.order_reference;",
        "convert_fields": {
            "id_order": lambda x: 0 if pd.isnull(x) else int(x),
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
        },
        "target": "sg_order_payment"
    },
    "ps_state": {
        "select": "SELECT state.id_state, state.id_country, state.name, state.iso_code FROM ps_state state;",
        "convert_fields": {},
        "target": "sg_state"
    },
}

@task_revoked.connect
def revoke_handler(*args, **kwargs):
    if "request" in kwargs:
        revoke_etl_log(kwargs["request"].id)
    print("Task revoked.")

def insert_etl_log(job_name, task_id):
    with stage_engine.begin() as conn:
        started_at = datetime.now()
        result = conn.execute(text("""
            INSERT INTO etl_log (job_name, started_at, status, task_id)
            VALUES (:job_name, :started_at, :status, :task_id)
            RETURNING id
        """), {"job_name": job_name, "started_at": started_at, "status": "RUNNING", "task_id": task_id})
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

def revoke_etl_log(task_id):
    with stage_engine.begin() as conn:
        conn.rollback()
        conn.execute(text("""
            UPDATE etl_log
            SET status = 'REVOKED'
            WHERE task_id = :task_id
        """), {"task_id": task_id})

def clear_stage_tables(self, tables):
    target_tables = [ config["target"] for config in tables ]
    with stage_engine.begin() as conn:
        for table in target_tables:
            if self.is_aborted():
                print("Task revoked.")
                return
            # Test some tables
            # if table != "sg_customer_company":
            #     continue
            # //Test some tables
            conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
            print(f"Table {table} truncated.")

def et_table(self, table_name, query, target_table, convert_items, chunksize=50000):
    if self.is_aborted():
        print("Task aborted.")
        return
    # Test some tables
    # if table_name != "ps_customer_company":
    #     return
    # //Test some tables
    print(f"Synchronizing table {table_name}...")
    for chunk in pd.read_sql_query(query, con=prod_engine, chunksize=chunksize):
        for field, convert_func in convert_items:
            chunk[field] = chunk[field].apply(convert_func)
        if self.is_aborted():
            print("Task aborted.")
            return
        chunk.to_sql(target_table, con=stage_engine, if_exists='append', index=False, method='multi')
        print(f"Processed {chunksize} rows.")
        if self.is_aborted():
            print("Task aborted.")
            return
        # test short cycle
        # break
        # //test short cycle
    print(f"Table {table_name} synchronized.")

@celery_app.task(bind=True, base=AbortableTask)
def stage_reload_task(self, *args, **kwargs):
    if self.is_aborted():
        return {"status": "REVOKED", "tables": 0}
    job_name = "stage_reload"
    log_id = insert_etl_log(job_name, self.request.id)
    try:
        print("Starting ETL process...")
        clear_stage_tables(self, ET_TABLES_CONFIG.values())
        if self.is_aborted():
            return {"status": "REVOKED", "tables": 0}
        tables_processed = 0
        for table_name, config in ET_TABLES_CONFIG.items():
            if self.is_aborted():
                print("Task revoked.")
                break
            et_table(self, table_name, config["select"], config["target"], ET_TABLES_CONFIG[table_name].get("convert_fields", {}).items())
            tables_processed += 1
            # test short cycle
            # if tables_processed == 3:
            #     break
            # //test short cycle
        print("End of ETL process.")
        if self.is_aborted():
            return {"status": "REVOKED", "tables": tables_processed}
        update_etl_log(log_id, "SUCCESS", "Stage reload completed", tables_processed)
        return {"status": "SUCCESS", "tables": tables_processed}
    except Exception as e:
        update_etl_log(log_id, "FAILED", str(e))
        # raise e
        return {"status": "FAILED", "tables": 0}

@celery_app.task(bind=True, base=AbortableTask)
def dwh_incremental_task(self, *args, **kwargs):
    if self.is_aborted():
        return {"status": "REVOKED", "tables": 0}
    job_name = "dwh_incremental"
    if args:
        print("Positional arguments:", args)
    if kwargs:
        print("Keyword arguments:", kwargs)
    log_id = insert_etl_log(job_name, self.request.id)
    try:
        time.sleep(3)
        tables_processed = 10
        update_etl_log(log_id, "SUCCESS", "DWH incremental load completed", tables_processed)
        return {"status": "SUCCESS", "rows": tables_processed}
    except Exception as e:
        update_etl_log(log_id, "FAILED", str(e))
        # raise e
