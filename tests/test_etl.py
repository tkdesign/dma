from sqlalchemy import text, create_engine
import pandas as pd
import datetime
from load_to_dwh import load_fact_cart_line, load_fact_order_line, load_fact_order_history, load_fact_order
from celeryconfig import STAGE_DB_URI, DWH_DB_URI


def test_fact_etl(app, client, auth_headers):
    stage_engine = create_engine(STAGE_DB_URI)
    dwh_engine = create_engine(DWH_DB_URI)

    # cart_line_last_count = pd.read_sql_query("SELECT COUNT(*) FROM dma_db_dwh.fact_cart_line", dwh_engine).iloc[0, 0]
    cartline_current_state_query = "SELECT COUNT(*) AS rows_count, MAX(cartline_key) AS last_key FROM dma_db_dwh.fact_cart_line;"
    cartline_test_rows_query = """
    SELECT fcl.cartid_bk, dp.productid_bk, dp.product_attributeid_bk, dc.customerid_bk, fcl.quantity
    FROM dma_db_dwh.fact_cart_line fcl
    JOIN dma_db_dwh.dim_customer dc ON fcl.customer_sk = dc.customer_key
    JOIN dma_db_dwh.dim_product dp ON fcl.product_sk = dp.product_key
    WHERE fcl.cartline_key > :last_cartline_key;
    """

    orderline_current_state_query = "SELECT COUNT(*) AS rows_count, MAX(orderline_key) AS last_key FROM dma_db_dwh.fact_order_line;"
    orderline_test_rows_query = """
    SELECT fol.orderdetailid_bk, fol.orderid_bk, fol.cartid_bk, dp.productid_bk, dp.product_attributeid_bk, dc.customerid_bk, da.addressid_bk, fol.quantity, fol.price, fol.price_tax_incl, fol.amount, fol.amount_tax_incl, fol.paid, fol.paid_tax_incl, fol.taxrate, fol.conversion_rate, fol.paymenttype, fol.carrier
    FROM dma_db_dwh.fact_order_line fol
    JOIN dma_db_dwh.dim_customer dc ON fol.customer_sk = dc.customer_key
    JOIN dma_db_dwh.dim_product dp ON fol.product_sk = dp.product_key
    JOIN dma_db_dwh.dim_address da ON fol.address_sk = da.address_key
    WHERE fol.orderline_key > :last_orderline_key;

    """

    orderhistory_current_state_query = "SELECT COUNT(*) AS rows_count, MAX(orderhistory_key) AS last_key FROM dma_db_dwh.fact_order_history;"
    orderhistory_test_rows_query = """
    SELECT foh.orderhistoryid_bk, foh.orderid_bk, foh.orderstateid_bk
    FROM dma_db_dwh.fact_order_history foh
    WHERE foh.orderhistory_key > :last_orderhistory_key;
    """

    order_current_state_query = "SELECT COUNT(*) AS rows_count, MAX(order_key) AS last_key FROM dma_db_dwh.fact_order;"
    order_test_rows_query = """
    SELECT fo.orderid_bk, dc.customerid_bk, da.addressid_bk, fo.paid, fo.paid_tax_incl, fo.taxrate, fo.conversion_rate, fo.paymenttype. fo.carrier 
    FROM dma_db_dwh.fact_order fo
    JOIN dma_db_dwh.dim_customer dc ON fo.customer_sk = dc.customer_key
    JOIN dma_db_dwh.dim_address da ON fo.address_sk = da.address_key
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
    INSERT INTO dma_db_stage.sg_cart (carrier,id_address_invoice,id_currency,id_customer,free_shipping,date_add,date_upd) VALUES
    	 (:carrier, :id_address_invoice, :id_currency, :id_customer, :free_shipping, :date_add, :date_upd)
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
    INSERT INTO dma_db_stage.sg_cart_product (id_cart,id_product,id_product_attribute,quantity,date_add) VALUES
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
        'main_order_id': [387858],
        'ip': ['193.245.40.73'],
        'review_mail_sent': [False],
    })
    sg_orders_insert_query = """
    INSERT INTO dma_db_stage.sg_orders (id_customer,id_cart,id_currency,carrier,id_address_delivery,current_state,payment,conversion_rate,total_discounts,total_discounts_tax_incl,total_discounts_tax_excl,total_paid,total_paid_tax_incl,total_paid_tax_excl,total_paid_real,total_products,total_products_wt,total_shipping,total_shipping_tax_incl,total_shipping_tax_excl,carrier_tax_rate,total_cod_tax_incl,"valid",date_add,date_upd,split_number,main_order_id,ip,review_mail_sent) VALUES
    	 (:id_customer, :id_cart, :id_currency, :carrier, :id_address_delivery, :current_state, :payment, :conversion_rate, :total_discounts, :total_discounts_tax_incl, :total_discounts_tax_excl, :total_paid, :total_paid_tax_incl, :total_paid_tax_excl, :total_paid_real, :total_products, :total_products_wt, :total_shipping, :total_shipping_tax_incl, :total_shipping_tax_excl, :carrier_tax_rate, :total_cod_tax_incl,:valid,:date_add,:date_upd,:split_number,:main_order_id,:ip,:review_mail_sent)
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
    INSERT INTO dma_db_stage.sg_order_detail (id_order,product_id,product_attribute_id,product_name,product_quantity,product_quantity_in_stock,product_price,reduction_amount,reduction_amount_tax_incl,reduction_amount_tax_excl,tax_computation_method,total_price_tax_incl,total_price_tax_excl,unit_price_tax_incl,unit_price_tax_excl,purchase_supplier_price,tax_rate,tax_name) VALUES
    	 (:id_order, :product_id, :product_attribute_id, :product_name, :product_quantity, :product_quantity_in_stock, :product_price, :reduction_amount, :reduction_amount_tax_incl, :reduction_amount_tax_excl, :tax_computation_method, :total_price_tax_incl, :total_price_tax_excl, :unit_price_tax_incl, :unit_price_tax_excl, :purchase_supplier_price, :tax_rate, :tax_name)
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
    INSERT INTO dma_db_stage.sg_order_history (id_order,id_order_state,date_add) VALUES
       (:id_order, :id_order_state, :date_add)
       RETURNING id_order_history;
    """

    last_cartline_key = last_orderline_key = last_order_key = 0
    last_orderhistory_key = []
    try:
        with dwh_engine.connect() as conn:
            # cartline_current_state_query = "SELECT COUNT(*) AS rows_count, MAX(cartline_key) AS last_key FROM dma_db_dwh.fact_cart_line;"
            cartline_current_state = conn.execute(text(cartline_current_state_query)).fetchone()
            cartline_current_state = cartline_current_state.
            # cartline_rows_count = cartline_current_state['rows_count'] if cartline_current_state['rows_count'] not 0 else 0
            # last_cartline_key = cartline_current_state['last_key'] if cartline_current_state['rows_count'] > 0 else 0


    except Exception as e:
        print(f"Error fetching data from DWH database: {e}")
        raise

    try:
        with stage_engine.begin() as conn:
            result = conn.execute(text(sg_cart_insert_query), **cart.iloc[0].to_dict())
            id_cart = result.scalar()
            if id_cart is None:
                raise ValueError("Failed to insert data into sg_cart table")
            cart_product['id_cart'] = id_cart
            order['id_cart'] = id_cart

            conn.execute(text(sg_cart_product_insert_query), **cart_product.iloc[0].to_dict())

            result = conn.execute(text(sg_orders_insert_query), **order.iloc[0].to_dict())
            id_order = result.scalar()
            order_detail['id_order'] = id_order
            order_history['id_order'] = id_order
            if id_order is None:
                raise ValueError("Failed to insert data into sg_orders table")

            result = conn.execute(text(sg_order_detail_insert_query), **order_detail.iloc[0].to_dict())
            id_order_detail = result.scalar()
            if id_order_detail is None:
                raise ValueError("Failed to insert data into sg_order_detail table")

            for _, row in order_history.iterrows():
                result = conn.execute(text(sg_order_history_insert_query), **row.to_dict())
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
        reset_db(dwh_engine, id_cart, id_order, last_cartline_key, last_order_key, last_orderhistory_key,
                 last_orderline_key, stage_engine)
        print(f"Error loading data into DWH database: {e}")
        raise

    try:
        new_cartline = pd.read_sql_query(text(cartline_test_rows_query), dwh_engine, params={"last_cartline_key": last_cartline_key})
        new_orderline = pd.read_sql_query(text(orderline_test_rows_query), dwh_engine, params={"last_orderline_key": last_orderline_key})
        new_orderhistory = pd.read_sql_query(text(orderhistory_test_rows_query), dwh_engine, params={"last_orderhistory_key": last_orderhistory_key})
        new_order = pd.read_sql_query(text(order_test_rows_query), dwh_engine, params={"last_order_key": last_order_key})

        assert len(new_cartline) == 1
        assert len(new_orderline) == 1
        assert len(new_orderhistory) == 2
        assert len(new_order) == 1
        assert new_cartline['cartid_bk'][0] == cart['id_cart'][0]
        assert new_cartline['productid_bk'][0] == cart_product['id_product'][0]
        assert new_cartline['product_attributeid_bk'][0] == cart_product['id_product_attribute'][0]
        assert new_cartline['customerid_bk'][0] == cart['id_customer'][0]
        assert new_cartline['quantity'][0] == cart_product['quantity'][0]
        assert new_orderline['orderid_bk'][0] == order['id_cart'][0]
        assert new_orderline['cartid_bk'][0] == order['id_cart'][0]
        assert new_orderline['productid_bk'][0] == order_detail['product_id'][0]
        assert new_orderline['product_attributeid_bk'][0] == order_detail['product_attribute_id'][0]
        assert new_orderline['customerid_bk'][0] == order['id_customer'][0]
        assert new_orderline['addressid_bk'][0] == order['id_address_delivery'][0]
        assert new_orderline['quantity'][0] == order_detail['product_quantity'][0]
        assert new_orderline['price'][0] == order_detail['product_price'][0]
        assert new_orderline['price_tax_incl'][0] == order_detail['total_price_tax_incl'][0]
        assert new_orderline['amount'][0] == order_detail['total_price_tax_incl'][0]
        assert new_orderline['amount_tax_incl'][0] == order_detail['total_price_tax_incl'][0]
        assert new_orderline['paid'][0] == order['total_paid'][0]
        assert new_orderline['paid_tax_incl'][0] == order['total_paid_tax_incl'][0]
        assert new_orderline['taxrate'][0] == order['carrier_tax_rate'][0]
        assert new_orderline['conversion_rate'][0] == order['conversion_rate'][0]
        assert new_orderline['paymenttype'][0] == order['payment'][0]
        assert new_orderline['carrier'][0] == order['carrier'][0]
        assert new_orderhistory['orderhistoryid_bk'][0] == order_history['id_order'][0]
        assert new_orderhistory['orderid_bk'][0] == order['id_cart'][0]
        assert new_orderhistory['orderstateid_bk'][0] == order_history['id_order_state'][0]
        assert new_order['orderid_bk'][0] == order['id_cart'][0]
        assert new_order['customerid_bk'][0] == order['id_customer'][0]
        assert new_order['addressid_bk'][0] == order['id_address_delivery'][0]
        assert new_order['paid'][0] == order['total_paid'][0]
        assert new_order['paid_tax_incl'][0] == order['total_paid_tax_incl'][0]
        assert new_order['taxrate'][0] == order['carrier_tax_rate'][0]
        assert new_order['conversion_rate'][0] == order['conversion_rate'][0]
        assert new_order['paymenttype'][0] == order['payment'][0]
        assert new_order['carrier'][0] == order['carrier'][0]
    except AssertionError as e:
        print(f"Data validation failed: {e}")
        raise
    except Exception as e:
        print(f"Error fetching data from DWH database: {e}")
        raise
    finally:
        reset_db(dwh_engine, id_cart, id_order, last_cartline_key, last_order_key, last_orderhistory_key,
                 last_orderline_key, stage_engine)


def reset_db(dwh_engine, id_cart, id_order, last_cartline_key, last_order_key, last_orderhistory_key,
             last_orderline_key, stage_engine):
    with stage_engine.begin() as conn:
        conn.execute(text("DELETE FROM dma_db_stage.sg_cart WHERE id_cart = :id_cart"), id_cart=id_cart)
        conn.execute(text("DELETE FROM dma_db_stage.sg_cart_product WHERE id_cart = :id_cart"), id_cart=id_cart)
        conn.execute(text("DELETE FROM dma_db_stage.sg_orders WHERE id_order = :id_order"), id_order=id_order)
        conn.execute(text("DELETE FROM dma_db_stage.sg_order_detail WHERE id_order = :id_order"), id_order=id_order)
        conn.execute(text("DELETE FROM dma_db_stage.sg_order_history WHERE id_order = :id_order"), id_order=id_order)
    with dwh_engine.begin() as conn:
        conn.execute(text("DELETE FROM dma_db_dwh.fact_cart_line WHERE cartline_key > :cartline_key"), cartline_key=last_cartline_key)
        conn.execute(text("DELETE FROM dma_db_dwh.fact_order_line WHERE orderline_key > :orderline_key"), orderline_key=last_orderline_key)
        conn.execute(text("DELETE FROM dma_db_dwh.fact_order_history WHERE orderhistory_key > :orderhistory_key"), orderhistory_key=last_orderhistory_key)
        conn.execute(text("DELETE FROM dma_db_dwh.fact_order WHERE order_key > :order_key"), order_key=last_order_key)
