-- dma_db_stage.etl_log

-- Drop table

-- DROP TABLE dma_db_stage.etl_log;

CREATE TABLE dma_db_stage.etl_log (
	id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
	job_name varchar(255) NOT NULL,
	started_at timestamp NOT NULL,
	ended_at timestamp NULL,
	status varchar(50) NOT NULL,
	message text NULL,
	tables_processed int4 NULL,
	task_id varchar(36) NULL,
	created_at timestamp DEFAULT now() NULL,
	CONSTRAINT etl_log_pk PRIMARY KEY (id)
);


-- dma_db_stage.sg_address

-- Drop table

-- DROP TABLE dma_db_stage.sg_address;

CREATE TABLE dma_db_stage.sg_address (
	id_address bigserial NOT NULL,
	id_country int8 NOT NULL,
	id_state int8 NULL,
	id_customer int8 DEFAULT '0'::bigint NOT NULL,
	id_customer_company int8 DEFAULT '0'::bigint NULL,
	postcode varchar(12) DEFAULT NULL::character varying NULL,
	city varchar(64) NOT NULL,
	date_add timestamptz NULL,
	date_upd timestamptz NULL,
	active bool DEFAULT true NOT NULL,
	deleted bool DEFAULT false NOT NULL,
	"default" bool DEFAULT false NOT NULL,
	has_phone bool DEFAULT false NOT NULL,
	CONSTRAINT sg_address_pk PRIMARY KEY (id_address)
);
CREATE INDEX id_country ON dma_db_stage.sg_address USING btree (id_country);
CREATE INDEX id_state ON dma_db_stage.sg_address USING btree (id_state);


-- dma_db_stage.sg_attribute

-- Drop table

-- DROP TABLE dma_db_stage.sg_attribute;

CREATE TABLE dma_db_stage.sg_attribute (
	id_attribute bigserial NOT NULL,
	id_attribute_group int8 NOT NULL,
	color varchar(32) DEFAULT NULL::character varying NULL,
	"name" varchar(128) NOT NULL,
	CONSTRAINT sg_attribute_pk PRIMARY KEY (id_attribute)
);
CREATE INDEX attribute_group ON dma_db_stage.sg_attribute USING btree (id_attribute_group);


-- dma_db_stage.sg_attribute_group

-- Drop table

-- DROP TABLE dma_db_stage.sg_attribute_group;

CREATE TABLE dma_db_stage.sg_attribute_group (
	id_attribute_group bigserial NOT NULL,
	is_color_group bool DEFAULT false NOT NULL,
	"name" varchar(128) NOT NULL,
	CONSTRAINT sg_attribute_group_pk PRIMARY KEY (id_attribute_group)
);


-- dma_db_stage.sg_cart

-- Drop table

-- DROP TABLE dma_db_stage.sg_cart;

CREATE TABLE dma_db_stage.sg_cart (
	id_cart bigserial NOT NULL,
	carrier varchar(64) NULL,
	id_address_invoice int8 NOT NULL,
	id_currency int8 NOT NULL,
	id_customer int8 NOT NULL,
	free_shipping int4 DEFAULT 0 NULL,
	date_add timestamptz NULL,
	date_upd timestamptz NULL,
	CONSTRAINT sg_cart_pk PRIMARY KEY (id_cart)
);


-- dma_db_stage.sg_cart_product

-- Drop table

-- DROP TABLE dma_db_stage.sg_cart_product;

CREATE TABLE dma_db_stage.sg_cart_product (
	id_cart int8 NOT NULL,
	id_product int8 NOT NULL,
	id_product_attribute int8 DEFAULT '0'::bigint NOT NULL,
	quantity int8 DEFAULT '0'::bigint NOT NULL,
	date_add timestamptz NULL,
	CONSTRAINT sg_cart_product_pk PRIMARY KEY (id_cart, id_product, id_product_attribute)
);
CREATE INDEX idx_cart_product_date_add ON dma_db_stage.sg_cart_product USING btree (date_add);
CREATE INDEX idx_cart_product_id_cart ON dma_db_stage.sg_cart_product USING btree (id_cart);


-- dma_db_stage.sg_category

-- Drop table

-- DROP TABLE dma_db_stage.sg_category;

CREATE TABLE dma_db_stage.sg_category (
	id_category bigserial NOT NULL,
	id_parent int8 NOT NULL,
	level_depth int2 DEFAULT '0'::smallint NOT NULL,
	nleft int8 DEFAULT '0'::bigint NOT NULL,
	nright int8 DEFAULT '0'::bigint NOT NULL,
	active bool DEFAULT false NOT NULL,
	date_add timestamptz NULL,
	date_upd timestamptz NULL,
	is_root_category bool DEFAULT false NOT NULL,
	"name" varchar(128) NOT NULL,
	CONSTRAINT sg_category_pk PRIMARY KEY (id_category)
);


-- dma_db_stage.sg_country

-- Drop table

-- DROP TABLE dma_db_stage.sg_country;

CREATE TABLE dma_db_stage.sg_country (
	id_country bigserial NOT NULL,
	id_zone int8 NOT NULL,
	iso_code varchar(3) NOT NULL,
	call_prefix int8 DEFAULT '0'::bigint NOT NULL,
	active bool DEFAULT false NOT NULL,
	contains_states bool DEFAULT false NOT NULL,
	need_zip_code bool DEFAULT true NOT NULL,
	zip_code_format varchar(12) DEFAULT ''::character varying NOT NULL,
	default_tax int8 NULL,
	"name" varchar(64) NOT NULL,
	CONSTRAINT sg_country_pk PRIMARY KEY (id_country)
);


-- dma_db_stage.sg_currency

-- Drop table

-- DROP TABLE dma_db_stage.sg_currency;

CREATE TABLE dma_db_stage.sg_currency (
	id_currency bigserial NOT NULL,
	"name" varchar(32) NOT NULL,
	iso_code varchar(3) DEFAULT '0'::character varying NOT NULL,
	iso_code_num varchar(3) DEFAULT '0'::character varying NOT NULL,
	sign varchar(8) NOT NULL,
	blank bool DEFAULT false NOT NULL,
	format bool DEFAULT false NOT NULL,
	decimals int2 DEFAULT '0'::smallint NOT NULL,
	conversion_rate numeric(13, 6) NOT NULL,
	default_vat_rate int8 NULL,
	deleted bool DEFAULT false NOT NULL,
	active bool DEFAULT true NOT NULL,
	default_on_instance int8 DEFAULT '0'::bigint NOT NULL,
	CONSTRAINT sg_currency_pk PRIMARY KEY (id_currency)
);


-- dma_db_stage.sg_customer

-- Drop table

-- DROP TABLE dma_db_stage.sg_customer;

CREATE TABLE dma_db_stage.sg_customer (
	id_customer bigserial NOT NULL,
	id_gender int8 NOT NULL,
	id_default_group int8 DEFAULT '1'::bigint NOT NULL,
	hashed_login varchar(128) NOT NULL,
	birthday date NULL,
	newsletter bool DEFAULT false NOT NULL,
	active bool DEFAULT false NOT NULL,
	is_guest bool DEFAULT false NOT NULL,
	deleted bool DEFAULT false NOT NULL,
	date_add timestamptz NULL,
	date_upd timestamptz NULL,
	CONSTRAINT sg_customer_pk PRIMARY KEY (id_customer)
);


-- dma_db_stage.sg_customer_company

-- Drop table

-- DROP TABLE dma_db_stage.sg_customer_company;

CREATE TABLE dma_db_stage.sg_customer_company (
	id_customer_company bigserial NOT NULL,
	id_customer int8 NOT NULL,
	"name" varchar(255) NOT NULL,
	verified bool DEFAULT false NULL,
	active bool DEFAULT true NULL,
	date_add timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	date_upd timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	id_address int8 DEFAULT '0'::bigint NULL,
	CONSTRAINT sg_customer_company_pk PRIMARY KEY (id_customer_company)
);
CREATE INDEX idx_customer_company_id_customer ON dma_db_stage.sg_customer_company USING btree (id_customer);


-- dma_db_stage.sg_customer_group

-- Drop table

-- DROP TABLE dma_db_stage.sg_customer_group;

CREATE TABLE dma_db_stage.sg_customer_group (
	id_customer int8 NOT NULL,
	id_group int8 NOT NULL,
	CONSTRAINT sg_customer_group_pk PRIMARY KEY (id_customer, id_group)
);
CREATE INDEX idx_customer_group_id_group ON dma_db_stage.sg_customer_group USING btree (id_group);


-- dma_db_stage.sg_gender

-- Drop table

-- DROP TABLE dma_db_stage.sg_gender;

CREATE TABLE dma_db_stage.sg_gender (
	id_gender bigserial NOT NULL,
	"type" int2 DEFAULT '0'::smallint NOT NULL,
	"name" varchar(20) NOT NULL,
	CONSTRAINT sg_gender_pk PRIMARY KEY (id_gender)
);


-- dma_db_stage.sg_group

-- Drop table

-- DROP TABLE dma_db_stage.sg_group;

CREATE TABLE dma_db_stage.sg_group (
	id_group bigserial NOT NULL,
	date_add timestamptz NULL,
	date_upd timestamptz NULL,
	is_wholesale bool DEFAULT false NOT NULL,
	order_days_return int8 DEFAULT '0'::bigint NULL,
	order_days_complaint int8 DEFAULT '0'::bigint NULL,
	"name" varchar(32) NOT NULL,
	CONSTRAINT sg_group_pk PRIMARY KEY (id_group)
);


-- dma_db_stage.sg_manufacturer

-- Drop table

-- DROP TABLE dma_db_stage.sg_manufacturer;

CREATE TABLE dma_db_stage.sg_manufacturer (
	id_manufacturer bigserial NOT NULL,
	"name" varchar(64) NOT NULL,
	date_add timestamptz NULL,
	date_upd timestamptz NULL,
	active bool DEFAULT false NOT NULL,
	CONSTRAINT sg_manufacturer_pk PRIMARY KEY (id_manufacturer)
);


-- dma_db_stage.sg_order_detail

-- Drop table

-- DROP TABLE dma_db_stage.sg_order_detail;

CREATE TABLE dma_db_stage.sg_order_detail (
	id_order_detail bigserial NOT NULL,
	id_order int8 NOT NULL,
	product_id int8 NOT NULL,
	product_attribute_id int8 NULL,
	product_name varchar(255) NOT NULL,
	product_quantity int8 DEFAULT '0'::bigint NOT NULL,
	product_quantity_in_stock int8 DEFAULT '0'::bigint NOT NULL,
	product_price numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	reduction_amount numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	reduction_amount_tax_incl numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	reduction_amount_tax_excl numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	tax_computation_method int2 DEFAULT '0'::smallint NOT NULL,
	total_price_tax_incl numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	total_price_tax_excl numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	unit_price_tax_incl numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	unit_price_tax_excl numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	purchase_supplier_price numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	tax_rate numeric(10, 3) NULL,
	tax_name varchar(32) NULL,
	CONSTRAINT sg_order_detail_pk PRIMARY KEY (id_order_detail)
);
CREATE INDEX idx_order_detail_id_order ON dma_db_stage.sg_order_detail USING btree (id_order);


-- dma_db_stage.sg_order_history

-- Drop table

-- DROP TABLE dma_db_stage.sg_order_history;

CREATE TABLE dma_db_stage.sg_order_history (
	id_order_history bigserial NOT NULL,
	id_order int8 NOT NULL,
	id_order_state int8 NOT NULL,
	date_add timestamptz NULL,
	CONSTRAINT sg_order_history_pk PRIMARY KEY (id_order_history)
);


-- dma_db_stage.sg_order_payment

-- Drop table

-- DROP TABLE dma_db_stage.sg_order_payment;

CREATE TABLE dma_db_stage.sg_order_payment (
	id_order_payment bigserial NOT NULL,
	id_order int8 NOT NULL,
	id_currency int8 NOT NULL,
	amount numeric(10, 2) NOT NULL,
	payment_method varchar(255) NOT NULL,
	date_add timestamptz NULL,
	CONSTRAINT sg_order_payment_pk PRIMARY KEY (id_order_payment)
);


-- dma_db_stage.sg_order_slip

-- Drop table

-- DROP TABLE dma_db_stage.sg_order_slip;

CREATE TABLE dma_db_stage.sg_order_slip (
	id_order_slip bigserial NOT NULL,
	conversion_rate numeric(13, 6) DEFAULT 1.000000 NOT NULL,
	id_customer int8 NOT NULL,
	id_order int8 NOT NULL,
	total_products_tax_excl numeric(20, 6) DEFAULT NULL::numeric NULL,
	total_products_tax_incl numeric(20, 6) DEFAULT NULL::numeric NULL,
	total_shipping_tax_excl numeric(20, 6) DEFAULT NULL::numeric NULL,
	total_shipping_tax_incl numeric(20, 6) DEFAULT NULL::numeric NULL,
	shipping_cost int2 DEFAULT '0'::smallint NOT NULL,
	amount numeric(10, 2) NOT NULL,
	shipping_cost_amount numeric(10, 2) NOT NULL,
	"partial" bool NOT NULL,
	date_add timestamptz NULL,
	date_upd timestamptz NULL,
	CONSTRAINT sg_order_slip_pk PRIMARY KEY (id_order_slip)
);


-- dma_db_stage.sg_order_slip_detail

-- Drop table

-- DROP TABLE dma_db_stage.sg_order_slip_detail;

CREATE TABLE dma_db_stage.sg_order_slip_detail (
	id_order_slip int8 NOT NULL,
	id_order_detail int8 NOT NULL,
	product_quantity int8 DEFAULT '0'::bigint NOT NULL,
	unit_price_tax_excl numeric(20, 6) DEFAULT NULL::numeric NULL,
	unit_price_tax_incl numeric(20, 6) DEFAULT NULL::numeric NULL,
	total_price_tax_excl numeric(20, 6) DEFAULT NULL::numeric NULL,
	total_price_tax_incl numeric(20, 6) DEFAULT NULL::numeric NULL,
	amount_tax_excl numeric(20, 6) DEFAULT NULL::numeric NULL,
	amount_tax_incl numeric(20, 6) DEFAULT NULL::numeric NULL,
	CONSTRAINT sg_order_slip_detail_pk PRIMARY KEY (id_order_slip, id_order_detail)
);


-- dma_db_stage.sg_order_state

-- Drop table

-- DROP TABLE dma_db_stage.sg_order_state;

CREATE TABLE dma_db_stage.sg_order_state (
	id_order_state bigserial NOT NULL,
	invoice bool DEFAULT false NULL,
	slip bool DEFAULT false NOT NULL,
	color varchar(32) DEFAULT NULL::character varying NULL,
	unremovable bool NOT NULL,
	hidden bool DEFAULT false NOT NULL,
	shipped bool DEFAULT false NOT NULL,
	paid bool DEFAULT false NOT NULL,
	closed int4 DEFAULT 0 NOT NULL,
	is_canceled_state bool DEFAULT false NOT NULL,
	can_send_repay bool DEFAULT false NOT NULL,
	can_be_canceled bool DEFAULT false NOT NULL,
	"name" varchar(64) NOT NULL,
	CONSTRAINT sg_order_state_pk PRIMARY KEY (id_order_state)
);


-- dma_db_stage.sg_orders

-- Drop table

-- DROP TABLE dma_db_stage.sg_orders;

CREATE TABLE dma_db_stage.sg_orders (
	id_order bigserial NOT NULL,
	id_customer int8 NOT NULL,
	id_cart int8 NOT NULL,
	id_currency int8 NOT NULL,
	carrier varchar(64) DEFAULT NULL::character varying NULL,
	id_address_delivery int8 NOT NULL,
	current_state int8 NOT NULL,
	payment varchar(255) NOT NULL,
	conversion_rate numeric(13, 6) DEFAULT 1.000000 NOT NULL,
	total_discounts numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	total_discounts_tax_incl numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	total_discounts_tax_excl numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	total_paid numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	total_paid_tax_incl numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	total_paid_tax_excl numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	total_paid_real numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	total_products numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	total_products_wt numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	total_shipping numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	total_shipping_tax_incl numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	total_shipping_tax_excl numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	carrier_tax_rate numeric(10, 3) DEFAULT 0.000 NOT NULL,
	total_cod_tax_incl numeric(20, 6) DEFAULT 0.000000 NULL,
	"valid" int4 DEFAULT 0 NOT NULL,
	date_add timestamptz NULL,
	date_upd timestamptz NULL,
	split_number int8 NULL,
	main_order_id int8 NULL,
	ip varchar(40) DEFAULT NULL::character varying NULL,
	review_mail_sent bool DEFAULT false NULL,
	CONSTRAINT sg_orders_pk PRIMARY KEY (id_order)
);
CREATE INDEX idx_sg_orders_date_add ON dma_db_stage.sg_orders USING btree (date_add);


-- dma_db_stage.sg_product

-- Drop table

-- DROP TABLE dma_db_stage.sg_product;

CREATE TABLE dma_db_stage.sg_product (
	id bigserial NOT NULL,
	id_product int8 NOT NULL,
	id_product_attribute int8 NULL,
	id_manufacturer int8 NULL,
	id_category_default int8 NULL,
	price numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	wholesale_price numeric(20, 6) DEFAULT 0.000000 NOT NULL,
	active bool DEFAULT false NOT NULL,
	available_for_order bool DEFAULT true NOT NULL,
	date_add timestamptz NULL,
	date_upd timestamptz NULL,
	price_type varchar(15) DEFAULT 'standard'::character varying NOT NULL,
	force_disable int4 DEFAULT 0 NOT NULL,
	only_for_loyalty int4 DEFAULT 0 NOT NULL,
	has_loyalty_price int4 DEFAULT 0 NOT NULL,
	is_rental int4 DEFAULT 0 NOT NULL,
	rental_price numeric(20, 6) DEFAULT 0.000000 NULL,
	"name" varchar(128) NOT NULL,
	season varchar(300) DEFAULT NULL::character varying NULL,
	"group" varchar(300) DEFAULT NULL::character varying NULL,
	subgroup varchar(300) DEFAULT NULL::character varying NULL,
	gender varchar(300) DEFAULT NULL::character varying NULL,
	CONSTRAINT sg_product_pk PRIMARY KEY (id)
);


-- dma_db_stage.sg_product_attribute_combination

-- Drop table

-- DROP TABLE dma_db_stage.sg_product_attribute_combination;

CREATE TABLE dma_db_stage.sg_product_attribute_combination (
	id_product_attribute int8 NOT NULL,
	id_attribute int8 NOT NULL,
	CONSTRAINT sg_product_attribute_combination_pk PRIMARY KEY (id_product_attribute, id_attribute)
);


-- dma_db_stage.sg_state

-- Drop table

-- DROP TABLE dma_db_stage.sg_state;

CREATE TABLE dma_db_stage.sg_state (
	id_state bigserial NOT NULL,
	id_country int8 NOT NULL,
	"name" varchar(64) NOT NULL,
	iso_code varchar(7) NOT NULL,
	CONSTRAINT sg_state_pk PRIMARY KEY (id_state)
);


-- dma_db_stage.sg_stock_available

-- Drop table

-- DROP TABLE dma_db_stage.sg_stock_available;

CREATE TABLE dma_db_stage.sg_stock_available (
	id_stock_available bigserial NOT NULL,
	id_product int8 NOT NULL,
	id_product_attribute int8 NOT NULL,
	quantity int8 DEFAULT '0'::bigint NOT NULL,
	date_add timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	date_upd timestamptz NULL,
	CONSTRAINT sg_stock_available_pk PRIMARY KEY (id_stock_available)
);