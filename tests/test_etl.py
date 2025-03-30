import pytest_check as check
from sqlalchemy import text, create_engine
import pandas as pd
import datetime
from load_to_dwh import load_fact_cart_line, load_fact_order_line, load_fact_order_history, load_fact_order
from celeryconfig import STAGE_DB_URI, DWH_DB_URI

pass_mode = True

def test_fact_etl(app, client, auth_headers):
    stage_engine = create_engine(STAGE_DB_URI)
    dwh_engine = create_engine(DWH_DB_URI)

    cart_last_id_query = "SELECT MAX(id_cart) AS last_id FROM dma_stage.dma_db_stage.sg_cart;"
    cart_product_last_id_cart_query = "SELECT MAX(id_cart) AS last_id FROM dma_stage.dma_db_stage.sg_cart_product;"
    cartline_last_key_query = "SELECT MAX(cartline_key) AS last_key FROM dma_dwh.public.fact_cart_line;"
    cartline_test_rows_query = """
    SELECT fcl.cartid_bk, dp.productid_bk, dp.productattributeid_bk, dc.customerid_bk, fcl.quantity
    FROM dma_dwh.public.fact_cart_line fcl
    JOIN dma_dwh.public.dim_customer dc ON fcl.customer_sk = dc.customer_key
    JOIN dma_dwh.public.dim_product dp ON fcl.product_sk = dp.product_key
    WHERE fcl.cartline_key > :last_cartline_key;
    """

    order_detail_last_id_query = "SELECT MAX(id_order_detail) AS last_id_order_detail, MAX(id_order) AS last_id_order FROM dma_stage.dma_db_stage.sg_order_detail;"
    orderline_last_key_query = "SELECT MAX(orderline_key) AS last_key FROM dma_dwh.public.fact_order_line;"
    orderline_test_rows_query = """
    SELECT fol.orderdetailid_bk, fol.orderid_bk, fol.cartid_bk, dp.productid_bk, dp.productattributeid_bk, dc.customerid_bk, da.addressid_bk, fol.quantity, fol.price, fol.price_tax_incl, fol.amount, fol.amount_tax_incl, fol.paid, fol.paid_tax_incl, fol.taxrate, fol.conversion_rate, fol.paymenttype, fol.carrier
    FROM dma_dwh.public.fact_order_line fol
    JOIN dma_dwh.public.dim_customer dc ON fol.customer_sk = dc.customer_key
    JOIN dma_dwh.public.dim_product dp ON fol.product_sk = dp.product_key
    JOIN dma_dwh.public.dim_address da ON fol.address_sk = da.address_key
    WHERE fol.orderline_key > :last_orderline_key;

    """

    order_history_last_id_query = "SELECT MAX(id_order_history) AS last_id FROM dma_stage.dma_db_stage.sg_order_history;"
    orderhistory_last_key_query = "SELECT MAX(orderhistory_key) AS last_key FROM dma_dwh.public.fact_order_history;"
    orderhistory_test_rows_query = """
    SELECT foh.orderhistoryid_bk, foh.orderid_bk, foh.orderstateid_bk
    FROM dma_dwh.public.fact_order_history foh
    WHERE foh.orderhistory_key > :last_orderhistory_key;
    """

    order_last_id_query = "SELECT MAX(id_order) AS last_id FROM dma_stage.dma_db_stage.sg_orders;"
    order_last_key_query = "SELECT MAX(order_key) AS last_key FROM dma_dwh.public.fact_order;"
    order_test_rows_query = """
    SELECT fo.orderid_bk, dc.customerid_bk, da.addressid_bk, fo.paid, fo.paid_tax_incl, fo.taxrate, fo.conversion_rate, fo.paymenttype, fo.carrier 
    FROM dma_dwh.public.fact_order fo
    JOIN dma_dwh.public.dim_customer dc ON fo.customer_sk = dc.customer_key
    JOIN dma_dwh.public.dim_address da ON fo.address_sk = da.address_key
    WHERE fo.order_key > :last_order_key;
    """

    id_cart = id_order = id_order_detail = 0
    id_order_history = []

    id_product = 102435
    id_product_attribute = 151227

    cart = pd.DataFrame({
        'carrier': ['Packeta - Výdajné miesto a Z-BOX'],
        'id_address_invoice': [825820],
        'id_currency': [1],
        'id_customer': [288988],
        'free_shipping': [0],
        'date_add': [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]],
        'date_upd': [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]],
    })
    sg_cart_insert_query = """
    INSERT INTO dma_stage.dma_db_stage.sg_cart (id_cart, carrier,id_address_invoice,id_currency,id_customer,free_shipping,date_add,date_upd) VALUES
    	 (:id_cart, :carrier, :id_address_invoice, :id_currency, :id_customer, :free_shipping, :date_add, :date_upd)
         RETURNING id_cart;
    """

    cart_product = pd.DataFrame({
        'id_cart': [None],
        'id_product': [id_product],
        'id_product_attribute': [id_product_attribute],
        'quantity': [1],
        'date_add': [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]],
    })
    sg_cart_product_insert_query = """
    INSERT INTO dma_stage.dma_db_stage.sg_cart_product (id_cart,id_product,id_product_attribute,quantity,date_add) VALUES
    	 (:id_cart, :id_product, :id_product_attribute, :quantity, :date_add)
    """

    order = pd.DataFrame({
        'id_customer': [288988],
        'id_cart': [None],
        'id_currency': [1],
        'carrier': ['Packeta - Výdajné miesto a Z-BOX'],
        'id_address_delivery': [825820],
        'current_state': [2],
        'payment': ['CardPay'],
        'conversion_rate': [1.000000],
        'total_discounts': [12.500000],
        'total_discounts_tax_incl': [12.500000],
        'total_discounts_tax_excl': [10.420000],
        'total_paid': [112.490000],
        'total_paid_tax_incl': [112.490000],
        'total_paid_tax_excl': [93.740000],
        'total_paid_real': [0.000000],
        'total_products': [104.160000],
        'total_products_wt': [124.990000],
        'total_shipping': [0.000000],
        'total_shipping_tax_incl': [0.000000],
        'total_shipping_tax_excl': [0.000000],
        'carrier_tax_rate': [20.000],
        'total_cod_tax_incl': [0.000000],
        'valid': [1],
        'date_add': [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]],
        'date_upd': [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]],
        'split_number': [0],
        'main_order_id': [0],
        'ip': ['193.245.40.73'],
        'review_mail_sent': [False],
    })
    sg_orders_insert_query = """
    INSERT INTO dma_stage.dma_db_stage.sg_orders (id_order,id_customer,id_cart,id_currency,carrier,id_address_delivery,current_state,payment,conversion_rate,total_discounts,total_discounts_tax_incl,total_discounts_tax_excl,total_paid,total_paid_tax_incl,total_paid_tax_excl,total_paid_real,total_products,total_products_wt,total_shipping,total_shipping_tax_incl,total_shipping_tax_excl,carrier_tax_rate,total_cod_tax_incl,"valid",date_add,date_upd,split_number,main_order_id,ip,review_mail_sent) VALUES
    	 (:id_order, :id_customer, :id_cart, :id_currency, :carrier, :id_address_delivery, :current_state, :payment, :conversion_rate, :total_discounts, :total_discounts_tax_incl, :total_discounts_tax_excl, :total_paid, :total_paid_tax_incl, :total_paid_tax_excl, :total_paid_real, :total_products, :total_products_wt, :total_shipping, :total_shipping_tax_incl, :total_shipping_tax_excl, :carrier_tax_rate, :total_cod_tax_incl,:valid,:date_add,:date_upd,:split_number,:main_order_id,:ip,:review_mail_sent)
         RETURNING id_order;
    """

    order_detail = pd.DataFrame({
        'id_order': [None],
        'product_id': [id_product],
        'product_attribute_id': [id_product_attribute],
        'product_name': ['Pánska bežecká trailová obuv NIKE-Pegasus Trail 4 GTX black/reflect silver/wolf grey - Farba : Čierna, Veľkosť EU : 44'],
        'product_quantity': [1],
        'product_quantity_in_stock': [1],
        'product_price': [141.658333],
        'reduction_amount': [37.500000],
        'reduction_amount_tax_incl': [45.000000],
        'reduction_amount_tax_excl': [37.500000],
        'tax_computation_method': [0],
        'total_price_tax_incl': [124.990000],
        'total_price_tax_excl': [104.158333],
        'unit_price_tax_incl': [124.990000],
        'unit_price_tax_excl': [104.158333],
        'purchase_supplier_price': [80.000000],
        'tax_rate': [20.000],
        'tax_name': ['DPH SK 20%'],
    })
    sg_order_detail_insert_query = """
    INSERT INTO dma_stage.dma_db_stage.sg_order_detail (id_order_detail,id_order,product_id,product_attribute_id,product_name,product_quantity,product_quantity_in_stock,product_price,reduction_amount,reduction_amount_tax_incl,reduction_amount_tax_excl,tax_computation_method,total_price_tax_incl,total_price_tax_excl,unit_price_tax_incl,unit_price_tax_excl,purchase_supplier_price,tax_rate,tax_name) VALUES
    	 (:id_order_detail, :id_order, :product_id, :product_attribute_id, :product_name, :product_quantity, :product_quantity_in_stock, :product_price, :reduction_amount, :reduction_amount_tax_incl, :reduction_amount_tax_excl, :tax_computation_method, :total_price_tax_incl, :total_price_tax_excl, :unit_price_tax_incl, :unit_price_tax_excl, :purchase_supplier_price, :tax_rate, :tax_name)
    	 RETURNING id_order_detail;
    """

    order_history = pd.DataFrame([
        {
            'id_order': None,
            'id_order_state': 18,
            'date_add': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        },
        {
            'id_order': None,
            'id_order_state': 2,
            'date_add': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        }
    ])
    sg_order_history_insert_query = """
    INSERT INTO dma_stage.dma_db_stage.sg_order_history (id_order_history,id_order,id_order_state,date_add) VALUES
       (:id_order_history, :id_order, :id_order_state, :date_add)
       RETURNING id_order_history;
    """

    try:
        df = pd.read_sql_query(text(cart_product_last_id_cart_query), stage_engine).fillna(0)
        last_id_cart = int(df['last_id'][0])

        df = pd.read_sql_query(text(cart_last_id_query), stage_engine).fillna(0)
        tmp_last_id_cart = int(df['last_id'][0])
        last_id_cart = last_id_cart if last_id_cart > tmp_last_id_cart else tmp_last_id_cart

        df = pd.read_sql_query(text(order_detail_last_id_query), stage_engine).fillna(0)
        last_id_order_detail = int(df['last_id_order_detail'][0])
        last_id_order = int(df['last_id_order'][0])

        df = pd.read_sql_query(text(order_history_last_id_query), stage_engine).fillna(0)
        last_id_order_history = int(df['last_id'][0])

        df = pd.read_sql_query(text(order_last_id_query), stage_engine).fillna(0)
        tmp_last_id_order = int(df['last_id'][0])
        last_id_order = last_id_order if last_id_order > tmp_last_id_order else tmp_last_id_order
    except Exception as e:
        print(f"Error fetching last id's from staging database: {e}")
        raise

    try:
        df = pd.read_sql_query(text(cartline_last_key_query), dwh_engine).fillna(0)
        last_cartline_key = int(df['last_key'][0])

        df = pd.read_sql_query(text(orderline_last_key_query), dwh_engine).fillna(0)
        last_orderline_key = int(df['last_key'][0])

        df = (pd.read_sql_query(text(orderhistory_last_key_query), dwh_engine).fillna(0))
        last_orderhistory_key = int(df['last_key'])

        df = pd.read_sql_query(text(order_last_key_query), dwh_engine).fillna(0)
        last_order_key = int(df['last_key'][0])
    except Exception as e:
        print(f"Error fetching last id's from DWH database: {e}")
        raise

    if pass_mode:
        try:
            with stage_engine.begin() as conn:
                cart['id_cart'] = last_id_cart + 1
                result = conn.execute(text(sg_cart_insert_query), cart.iloc[0].to_dict())
                id_cart = result.scalar()
                if id_cart is None:
                    raise ValueError("Failed to insert data into sg_cart table")

                cart_product['id_cart'] = id_cart
                conn.execute(text(sg_cart_product_insert_query), cart_product.iloc[0].to_dict())

                order['id_order'] = last_id_order + 1
                order['id_cart'] = id_cart
                result = conn.execute(text(sg_orders_insert_query), order.iloc[0].to_dict())
                id_order = result.scalar()
                if id_order is None:
                    raise ValueError("Failed to insert data into sg_orders table")

                conn.execute(text("UPDATE dma_stage.dma_db_stage.sg_orders SET main_order_id = :id_order WHERE id_order = :id_order"),
                             {'id_order': id_order})

                order_detail['id_order_detail'] = last_id_order_detail + 1
                order_detail['id_order'] = id_order
                result = conn.execute(text(sg_order_detail_insert_query), order_detail.iloc[0].to_dict())
                id_order_detail = result.scalar()
                if id_order_detail is None:
                    raise ValueError("Failed to insert data into sg_order_detail table")

                order_history['id_order'] = id_order
                for i, row in order_history.iterrows():
                    row['id_order_history'] = last_id_order_history + i + 1
                    result = conn.execute(text(sg_order_history_insert_query), row.to_dict())
                    if result is None:
                        raise ValueError("Failed to insert data into sg_order_history table")
                    id_order_history.append(result.scalar())
        except Exception as e:
            print(f"Error inserting data into stage database: {e}")
            raise

        try:
            load_fact_cart_line(self=None, stage_engine=stage_engine, dwh_engine=dwh_engine)
            load_fact_order_line(self=None, stage_engine=stage_engine, dwh_engine=dwh_engine)
            load_fact_order_history(self=None, stage_engine=stage_engine, dwh_engine=dwh_engine)
            load_fact_order(self=None, stage_engine=stage_engine, dwh_engine=dwh_engine)
        except Exception as e:
            reset_db(stage_engine, dwh_engine, id_cart, id_order, last_cartline_key, last_order_key, last_orderhistory_key, last_orderline_key)
            print(f"Error loading data into DWH database: {e}")
            raise

    try:
        new_cartline = pd.read_sql_query(text(cartline_test_rows_query), dwh_engine, params={"last_cartline_key": last_cartline_key})
        new_orderline = pd.read_sql_query(text(orderline_test_rows_query), dwh_engine, params={"last_orderline_key": last_orderline_key})
        new_orderhistory = pd.read_sql_query(text(orderhistory_test_rows_query), dwh_engine, params={"last_orderhistory_key": last_orderhistory_key})
        new_order = pd.read_sql_query(text(order_test_rows_query), dwh_engine, params={"last_order_key": last_order_key})
    except Exception as e:
        reset_db(stage_engine, dwh_engine, id_cart, id_order, last_cartline_key, last_order_key, last_orderhistory_key, last_orderline_key)
        print(f"Error fetching data from DWH database: {e}")
        raise
    check.equal(len(new_cartline), 1, "Expected 1 new test row in cart line, got {len(new_cartline)}")
    if not new_cartline.empty:
        check.equal(new_cartline['cartid_bk'][0], cart['id_cart'][0], "Expected cart id to be the same")
        check.equal(new_cartline['productid_bk'][0], cart_product['id_product'][0], "Expected product id to be the same")
        check.equal(new_cartline['productattributeid_bk'][0], cart_product['id_product_attribute'][0], "Expected product attribute id to be the same")
        check.equal(new_cartline['customerid_bk'][0], cart['id_customer'][0], "Expected customer id to be the same")
        check.equal(new_cartline['quantity'][0], cart_product['quantity'][0], "Expected quantity to be the same")
        check.equal(new_orderline['orderid_bk'][0], order['id_cart'][0], "Expected order id to be the same")
        check.equal(new_orderline['cartid_bk'][0], order['id_cart'][0], "Expected cart id to be the same")
        check.equal(new_orderline['productid_bk'][0], order_detail['product_id'][0], "Expected product id to be the same")
    check.equal(len(new_orderline), 1, "Expected 1 new test row in order line, got {len(new_orderline)}")
    if not new_order.empty:
        check.equal(new_orderline['productattributeid_bk'][0], order_detail['product_attribute_id'][0], "Expected product attribute id to be the same")
        check.equal(new_orderline['customerid_bk'][0], order['id_customer'][0], "Expected customer id to be the same")
        check.equal(new_orderline['addressid_bk'][0], order['id_address_delivery'][0], "Expected address id to be the same")
        check.equal(new_orderline['quantity'][0], order_detail['product_quantity'][0], "Expected quantity to be the same")
        check.equal(new_orderline['price'][0], order_detail['product_price'][0], "Expected price to be the same")
        check.equal(new_orderline['price_tax_incl'][0], order_detail['total_price_tax_incl'][0], "Expected price tax incl to be the same")
        check.equal(new_orderline['amount'][0], order_detail['total_price_tax_incl'][0], "Expected amount to be the same")
        check.equal(new_orderline['amount_tax_incl'][0], order_detail['total_price_tax_incl'][0], "Expected amount tax incl to be the same")
        check.equal(new_orderline['paid'][0], order['total_paid'][0], "Expected paid to be the same")
        check.equal(new_orderline['paid_tax_incl'][0], order['total_paid_tax_incl'][0], "Expected paid tax incl to be the same")
        check.equal(new_orderline['taxrate'][0], order['carrier_tax_rate'][0], "Expected tax rate to be the same")
        check.equal(new_orderline['conversion_rate'][0], order['conversion_rate'][0], "Expected conversion rate to be the same")
        check.equal(new_orderline['paymenttype'][0], order['payment'][0], "Expected payment type to be the same")
        check.equal(new_orderline['carrier'][0], order['carrier'][0], "Expected carrier to be the same")
    check.equal(len(new_orderhistory), 2, "Expected 2 test rows in order history, got {len(new_orderhistory)}")
    if not new_orderhistory.empty:
        for _,row in new_orderhistory.iterrows():
            check.equal(row['orderhistoryid_bk'], order_history['id_order'][0], "Expected order history id to be the same")
            check.equal(row['orderid_bk'], order['id_cart'][0], "Expected order id to be the same")
            check.equal(row['orderstateid_bk'], order_history['id_order_state'][0], "Expected order state id to be the same")
    check.equal(len(new_order), 1)
    if not new_order.empty:
        check.equal(new_order['orderid_bk'][0], order['id_cart'][0], "Expected order id to be the same")
        check.equal(new_order['customerid_bk'][0], order['id_customer'][0], "Expected customer id to be the same")
        check.equal(new_order['addressid_bk'][0], order['id_address_delivery'][0], "Expected address id to be the same")
        check.equal(new_order['paid'][0], order['total_paid'][0], "Expected paid to be the same")
        check.equal(new_order['paid_tax_incl'][0], order['total_paid_tax_incl'][0], "Expected paid tax incl to be the same")
        check.equal(new_order['taxrate'][0], order['carrier_tax_rate'][0], "Expected tax rate to be the same")
        check.equal(new_order['conversion_rate'][0], order['conversion_rate'][0], "Expected conversion rate to be the same")
        check.equal(new_order['paymenttype'][0], order['payment'][0], "Expected payment type to be the same")
        check.equal(new_order['carrier'][0], order['carrier'][0], "Expected carrier to be the same")

    reset_db(stage_engine, dwh_engine, id_cart, id_order, last_cartline_key, last_order_key, last_orderhistory_key, last_orderline_key)

def reset_db(stage_engine, dwh_engine, id_cart, id_order, last_cartline_key, last_order_key, last_orderhistory_key,
             last_orderline_key):
    if pass_mode:
        with stage_engine.begin() as conn:
            conn.execute(text("DELETE FROM dma_stage.dma_db_stage.sg_cart WHERE id_cart = :id_cart"), id_cart=id_cart)
            conn.execute(text("DELETE FROM dma_stage.dma_db_stage.sg_cart_product WHERE id_cart = :id_cart"), id_cart=id_cart)
            conn.execute(text("DELETE FROM dma_stage.dma_db_stage.sg_orders WHERE id_order = :id_order"), id_order=id_order)
            conn.execute(text("DELETE FROM dma_stage.dma_db_stage.sg_order_detail WHERE id_order = :id_order"), id_order=id_order)
            conn.execute(text("DELETE FROM dma_stage.dma_db_stage.sg_order_history WHERE id_order = :id_order"), id_order=id_order)
        with dwh_engine.begin() as conn:
            conn.execute(text("DELETE FROM dma_dwh.public.fact_cart_line WHERE cartline_key > :cartline_key"), cartline_key=last_cartline_key)
            conn.execute(text("DELETE FROM dma_dwh.public.fact_order_line WHERE orderline_key > :orderline_key"), orderline_key=last_orderline_key)
            conn.execute(text("DELETE FROM dma_dwh.public.fact_order_history WHERE orderhistory_key > :orderhistory_key"), orderhistory_key=last_orderhistory_key)
            conn.execute(text("DELETE FROM dma_dwh.public.fact_order WHERE order_key > :order_key"), order_key=last_order_key)
