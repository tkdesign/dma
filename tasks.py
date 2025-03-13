import hashlib
from celery import Celery
from celery.signals import task_revoked
from celery.contrib.abortable import AbortableTask
from sqlalchemy import create_engine, text
import pandas as pd
import plotly.express as px
from datetime import datetime
import time
import gc
from celeryconfig import broker_url, result_backend, PROD_DB_URI, STAGE_DB_URI, DWH_DB_URI

from load_to_dwh import load_dim_date, load_dim_time, load_dim_address, load_dim_customer, load_dim_attribute, load_dim_product, load_bridge_product_attribute, load_dim_order_state, load_fact_cart_line, load_fact_order_line, load_fact_order_history
celery_app = Celery('etl_tasks', broker=broker_url, backend=result_backend)
celery_app.config_from_object('celeryconfig')

prod_engine = create_engine(PROD_DB_URI)
stage_engine = create_engine(STAGE_DB_URI)
dwh_engine = create_engine(DWH_DB_URI)

ET_TABLES_CONFIG = {
    "ps_address": {
        "select": "SELECT id_address, id_country, id_state, id_customer, id_customer_company, postcode, city, date_add, date_upd, active, deleted, `default`, has_phone FROM ps_address;",
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
        "select": "SELECT id_country, id_zone, iso_code, call_prefix, active, contains_states, need_zip_code, zip_code_format, default_tax, name FROM ps_country;",
        "convert_fields": {
            "active": lambda x: None if pd.isnull(x) else bool(x),
            "contains_states": lambda x: None if pd.isnull(x) else bool(x),
            "need_zip_code": lambda x: None if pd.isnull(x) else bool(x),
        },
        "target": "sg_country"
    },
    "ps_customer": {
        "select": "SELECT id_customer, id_gender, id_default_group, hashed_login, birthday, newsletter, active, is_guest, deleted, date_add, date_upd FROM ps_customer;",
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
        "select": "SELECT id_customer_company, id_customer, name, verified, active, date_add, date_upd, id_address FROM ps_customer_company;",
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
        "select": "SELECT id_customer, id_group FROM ps_customer_group;",
        "convert_fields": {},
        "target": "sg_customer_group"
    },
    "ps_gender": {
        "select": "SELECT id_gender, `type`, name FROM ps_gender;",
        "convert_fields": {},
        "target": "sg_gender"
    },
    "ps_group": {
        "select": "SELECT id_group, date_add, date_upd, is_wholesale, order_days_return, order_days_complaint, name FROM ps_group;",
        "convert_fields": {
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "date_upd": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "is_wholesale": lambda x: None if pd.isnull(x) else bool(x),
        },
        "target": "sg_group"
    },
    "ps_category": {
        "select": "SELECT id_category, id_parent, level_depth, nleft, nright, active, date_add, date_upd, is_root_category, name FROM ps_category;",
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
        "select": "SELECT id_manufacturer, name, date_add, date_upd, active FROM ps_manufacturer;",
        "convert_fields": {
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "date_upd": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "active": lambda x: None if pd.isnull(x) else bool(x),
        },
        "target": "sg_manufacturer"
    },
    "ps_product_attribute_combination": {
        "select": "SELECT id_attribute, id_product_attribute FROM ps_product_attribute_combination;",
        "convert_fields": {},
        "target": "sg_product_attribute_combination"
    },
    "ps_attribute": {
        "select": "SELECT id_attribute, id_attribute_group, color, name FROM ps_attribute;",
        "convert_fields": {},
        "target": "sg_attribute"
    },
    "ps_attribute_group": {
        "select": "SELECT id_attribute_group, is_color_group, name FROM ps_attribute_group;",
        "convert_fields": {
            "is_color_group": lambda x: None if pd.isnull(x) else bool(x),
        },
        "target": "sg_attribute_group"
    },
    "ps_currency": {
        "select": "SELECT id_currency, name, iso_code, iso_code_num, sign, blank, format, decimals, conversion_rate, default_vat_rate, deleted, active, default_on_instance FROM ps_currency;",
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
        "select": "SELECT id_cart, id_product, id_product_attribute, quantity, date_add FROM ps_cart_product;",
        "convert_fields": {
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
        },
        "target": "sg_cart_product"
    },
    "ps_order_history": {
        "select": "SELECT id_order_history, id_order, id_order_state, date_add FROM ps_order_history;",
        "convert_fields": {
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
        },
        "target": "sg_order_history"
    },
    "ps_order_state": {
        "select": "SELECT id_order_state, invoice, slip, color, unremovable, hidden, shipped, paid, closed, is_canceled_state, can_send_repay, can_be_canceled, name FROM ps_order_state;",
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
        "select": "SELECT id_order_slip, conversion_rate, id_customer, id_order, total_products_tax_excl, total_products_tax_incl, total_shipping_tax_excl, total_shipping_tax_incl, shipping_cost, amount, shipping_cost_amount, `partial`, date_add, date_upd FROM ps_order_slip;",
        "convert_fields": {
            "partial": lambda x: None if pd.isnull(x) else bool(x),
            "date_add": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
            "date_upd": lambda x: None if pd.isnull(x) or x == "0000-00-00 00:00:00" else x,
        },
        "target": "sg_order_slip"
    },
    "ps_order_slip_detail": {
        "select": "SELECT id_order_slip, id_order_detail, product_quantity, unit_price_tax_excl, unit_price_tax_incl, total_price_tax_excl, total_price_tax_incl, amount_tax_excl, amount_tax_incl FROM ps_order_slip_detail;",
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

L_TABLES_CONFIG = {
    "dim_date": load_dim_date,
    "dim_time": load_dim_time,
    "dim_customer": load_dim_customer,
    "dim_address": load_dim_address,
    "dim_attribute": load_dim_attribute,
    "dim_product": load_dim_product,
    "bridge_product_attribute": load_bridge_product_attribute,
    "dim_order_state": load_dim_order_state,
    "fact_cart_line": load_fact_cart_line,
    "fact_order_line": load_fact_order_line,
    "fact_order_history": load_fact_order_history,
}

@task_revoked.connect
def revoke_handler(*args, **kwargs):
    if "request" in kwargs:
        revoke_etl_log(kwargs["request"].id)
    print("Úloha zrušená")

def revoke_etl_log(task_id):
    # with stage_engine.connect() as conn:
    with stage_engine.begin() as conn:
    #     conn.rollback()
        conn.execute(text("""
            UPDATE etl_log
            SET status = 'REVOKED', ended_at = :ended_at
            WHERE task_id = :task_id
        """), {"task_id": task_id, "ended_at": datetime.now()})

def clear_stage_tables(self, tables):
    print("Vyprázdnenie tabuliek dočasného úložiska...")
    target_tables = [ config["target"] for config in tables ]
    with stage_engine.begin() as conn:
        for table in target_tables:
            if self.is_aborted():
                print("Úloha zrušená")
                return
            conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
            print(f"Tabuľka {table} bola vyprázdnená.")
    print("Vyprázdnenie tabuliek dočasného úložiska dokončené.")

def et_table(self, table_name, query, target_table, convert_items, chunksize=10000):
    if self.is_aborted():
        print("Úloha zrušená")
        return
    print(f"Synchronizácia tabuľky {table_name}...")

    # offset = 0

    # while True:
    #     chunk = pd.read_sql_query(text(query.format(chunksize=str(chunksize), offset=str(offset))), con=prod_engine)
    with prod_engine.connect().execution_options(stream_results=True) as conn:
        result = conn.execution_options(yield_per=chunksize).execute(text(query))
        chunk = pd.read_sql_query(result, con=prod_engine, chunksize=chunksize)

        # if chunk.empty:
        #     break
        #
        # offset += chunksize

        for field, convert_func in convert_items:
            chunk[field] = chunk[field].apply(convert_func)

        if self.is_aborted():
            print("Úloha zrušená")
            return

        chunk.to_sql(target_table, con=stage_engine, if_exists='append', index=False, method='multi')

        print(f"Spracovaných  {chunksize} riadkov.")

        if self.is_aborted():
            print("Úloha zrušená")
            return

        del chunk
        gc.collect()

    print(f"Tabuľka {table_name} bola synchronizovaná.")

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

@celery_app.task(bind=True, base=AbortableTask)
def stage_reload_task(self, *args, **kwargs):
    if self.is_aborted():
        return {"status": "REVOKED", "tables": 0}
    job_name = "stage_reload"
    log_id = insert_etl_log(job_name, self.request.id)
    print("Načítanie dočasného úložiska spustené.")
    pass_mode = False
    if pass_mode:
        print("Vyprázdňovanie tabuliek dočasného úložiska...")
        time.sleep(10)
        print("Vyprázdňovanie tabuliek dočasného úložiska dokončené.")
        print("Extracting data from production...")
        time.sleep(10)
        print("End of extracting data from production.")
        print("Stage reload completed.")
        update_etl_log(log_id, "SUCCESS", "Stage reload completed", 0)
        return {"status": "SUCCESS", "tables": 0}
    try:
        clear_stage_tables(self, ET_TABLES_CONFIG.values())
        if self.is_aborted():
            return {"status": "REVOKED", "tables": 0}
        tables_processed = 0
        for table_name, config in ET_TABLES_CONFIG.items():
            if self.is_aborted():
                print("Úloha zrušená")
                break
            et_table(self, table_name, config["select"], config["target"], ET_TABLES_CONFIG[table_name].get("convert_fields", {}).items())
            tables_processed += 1
        if self.is_aborted():
            return {"status": "REVOKED", "tables": tables_processed}
        update_etl_log(log_id, "SUCCESS", "Načítanie dočasného úložiska dokončené.", tables_processed)
        ret_status = {"status": "SUCCESS", "tables": tables_processed}
    except Exception as e:
        update_etl_log(log_id, "FAILED", str(e))
        # raise e
        ret_status = {"status": "FAILED", "tables": 0}

    return ret_status

@celery_app.task(bind=True, base=AbortableTask)
def dwh_incremental_task(self, *args, **kwargs):
    if self.is_aborted():
        return {"status": "REVOKED", "tables": 0}
    job_name = "dwh_incremental"
    print("Načítanie do dátového skladu spustené.")
    log_id = insert_etl_log(job_name, self.request.id)
    pass_mode = False
    if pass_mode:
        time.sleep(10)
        print("Načítanie do dátového skladu dokončené.")
        update_etl_log(log_id, "SUCCESS", "Načítanie do dátového skladu dokončené.", 0)
        return {"status": "SUCCESS", "tables": 0}
    try:
        tables_processed = 0
        for table_name, load_function in L_TABLES_CONFIG.items():
            if table_name == "dim_date":
                with dwh_engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 FROM public.dim_date LIMIT 1"))
                    if result.rowcount > 0:
                        continue
            elif table_name == "dim_time":
                with dwh_engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 FROM public.dim_time LIMIT 1"))
                    if result.rowcount > 0:
                        continue
            if self.is_aborted():
                print("Úloha zrušená")
                break
            load_function(self, stage_engine, dwh_engine)
            tables_processed += 1
        print("Načítanie do dátového skladu dokončené.")
        if self.is_aborted():
            return {"status": "REVOKED", "tables": tables_processed}
        update_etl_log(log_id, "SUCCESS", "Načítanie do dátového skladu dokončené.", tables_processed)
        return {"status": "SUCCESS", "rows": tables_processed}
    except Exception as e:
        print(e)
        update_etl_log(log_id, "FAILED", str(e))
        # raise e

def insert_report(user_id, report_type, parameters, task_id):
    with dwh_engine.begin() as conn:
        started_at = datetime.now()
        result = conn.execute(text("""
            INSERT INTO report (user_id, report_type, parameters, started_at, status, task_id)
            VALUES (:user_id, :report_type, :parameters, :started_at, :status, :task_id)
            RETURNING id
        """),{"user_id": user_id, "report_type": report_type, "parameters": parameters, "started_at": started_at, "status": "RUNNING", "task_id": task_id})
        row = result.fetchone()
        return row[0] if row is not None else None

def update_report(report_id, status, message=None, result='{}'):
    with dwh_engine.begin() as conn:
        ended_at = datetime.now()
        conn.execute(text("""
            UPDATE report
            SET status = :status,
                ended_at = :ended_at,
                message = :message,
                result = :result
            WHERE id = :report_id
        """), {
            "status": status,
            "ended_at": ended_at,
            "message": message,
            "result": result,
            "report_id": report_id
        })

@celery_app.task(bind=True, base=AbortableTask)
def build_report_task(self, *args, **kwargs):
    if self.is_aborted():
        print("Úloha zrušená")
        return
    if args is not None and len(args) == 0:
        print("Úloha zrušená")
        return

    user_id = args[0].get("user_id")
    report_type = args[0].get("report_type")
    query = args[0].get("query")
    print(f"Generovanie reportu {report_type}...")
    report_id = insert_report(user_id, report_type, "{}", self.request.id)

    with dwh_engine.connect() as conn:
        df = pd.read_sql_query(text(query), conn)
    if self.is_aborted():
        print("Úloha zrušená")
        return

    result = '{}'
    status = 'FAILED'
    message = 'Neznámy typ reportu.'

    try:
        if report_type == 'gender_distribution':
            fig = px.pie(df, values='customers_count', names='gender', title='Rozdelenie podľa pohlavia')
            result = fig.to_json()
            status = "SUCCESS"
            message = 'Report bol úspešne vytvorený.'
        elif report_type == 'age_distribution':
            fig = px.bar(df, x='age_range', y='avg_order_value', title='Priemerná suma objednávky podľa vekového rozpätia')
            result = fig.to_json()
            status = "SUCCESS"
            message = 'Report bol úspešne vytvorený.'
        elif report_type == 'product_group_revenue':
            fig = px.bar(df, x='period', y='total_revenue', title='Tržby podľa skupín produktov')
            result = fig.to_json()
            status = "SUCCESS"
            message = 'Report bol úspešne vytvorený.'
        elif report_type == 'product_gender_revenue':
            status = "SUCCESS"
            message = 'Report bol úspešne vytvorený.'
    except Exception as e:
        print(e)
        message = str(e)
    finally:
        update_report(report_id=report_id, status=status, message=message, result=result)
        del df
        gc.collect()

