-- public.dim_address

-- Drop table

-- DROP TABLE public.dim_address;

CREATE TABLE public.dim_address (
	address_key bigserial NOT NULL,
	addressid_bk int8 NOT NULL,
	customerid_bk int8 NULL,
	country varchar NULL,
	state varchar NULL,
	city varchar NULL,
	zipcode varchar NULL,
	valid_from date NULL,
	valid_to date NULL,
	CONSTRAINT dim_address_pkey PRIMARY KEY (address_key)
);
CREATE INDEX idx_dim_address_addressid_bk ON public.dim_address USING btree (addressid_bk);


-- public.dim_attribute

-- Drop table

-- DROP TABLE public.dim_attribute;

CREATE TABLE public.dim_attribute (
	attribute_key bigserial NOT NULL,
	attributeid_bk int8 NOT NULL,
	attribute_name varchar NOT NULL,
	attribute_group varchar NULL,
	CONSTRAINT dim_attribute_pkey PRIMARY KEY (attribute_key)
);


-- public.dim_customer

-- Drop table

-- DROP TABLE public.dim_customer;

CREATE TABLE public.dim_customer (
	customer_key bigserial NOT NULL,
	customerid_bk int8 NOT NULL,
	hashedemail varchar NOT NULL,
	defaultgroup varchar NOT NULL,
	birthdate date NULL,
	gender varchar NULL,
	businessaccount bool NULL,
	active bool NULL,
	valid_from date NULL,
	valid_to date NULL,
	CONSTRAINT dim_customer_pkey PRIMARY KEY (customer_key)
);
CREATE INDEX idx_dim_customer_lookup ON public.dim_customer USING btree (customerid_bk, valid_to);
CREATE INDEX idx_dim_customer_valid ON public.dim_customer USING btree (valid_to);


-- public.dim_date

-- Drop table

-- DROP TABLE public.dim_date;

CREATE TABLE public.dim_date (
	date_key bigserial NOT NULL,
	"date" date NOT NULL,
	"year" int4 NOT NULL,
	quarter int4 NOT NULL,
	"month" int4 NOT NULL,
	month_name varchar NOT NULL,
	"day" int4 NOT NULL,
	day_of_week int4 NOT NULL,
	day_name varchar NOT NULL,
	week_of_year int4 NOT NULL,
	is_weekend bool DEFAULT false NULL,
	CONSTRAINT dim_date_pkey PRIMARY KEY (date_key)
);


-- public.dim_order_state

-- Drop table

-- DROP TABLE public.dim_order_state;

CREATE TABLE public.dim_order_state (
	orderstate_key bigserial NOT NULL,
	orderstateid_bk int8 NOT NULL,
	current_state varchar NULL,
	valid_from date NULL,
	valid_to date NULL,
	CONSTRAINT dim_orderstate_pkey PRIMARY KEY (orderstate_key)
);
CREATE INDEX idx_dim_order_state_orderstateid_bk ON public.dim_order_state USING btree (orderstateid_bk);


-- public.dim_product

-- Drop table

-- DROP TABLE public.dim_product;

CREATE TABLE public.dim_product (
	product_key bigserial NOT NULL,
	productid_bk int8 NOT NULL,
	productattributeid_bk int8 NULL,
	productname varchar NOT NULL,
	manufacturer varchar NULL,
	defaultcategory varchar NULL,
	market_group varchar NULL,
	market_subgroup varchar NULL,
	market_gender varchar NULL,
	price numeric NULL,
	active bool NULL,
	valid_from date NULL,
	valid_to date NULL,
	CONSTRAINT dim_product_pkey PRIMARY KEY (product_key)
);
CREATE INDEX idx_dim_product_lookup ON public.dim_product USING btree (productid_bk, productattributeid_bk, valid_to);


-- public.dim_time

-- Drop table

-- DROP TABLE public.dim_time;

CREATE TABLE public.dim_time (
	time_key bigserial NOT NULL,
	"time" time NOT NULL,
	"hour" int4 NOT NULL,
	CONSTRAINT dim_time_pkey PRIMARY KEY (time_key)
);


-- public.report

-- Drop table

-- DROP TABLE public.report;

CREATE TABLE public.report (
	id bigserial NOT NULL,
	user_id int8 NOT NULL,
	report_type varchar(255) NOT NULL,
	parameters jsonb NOT NULL,
	"result" jsonb NULL,
	started_at timestamp NOT NULL,
	ended_at timestamp NULL,
	status varchar(50) NOT NULL,
	message text NULL,
	task_id varchar(36) NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT reports_pkey PRIMARY KEY (id)
);


-- public."user"

-- Drop table

-- DROP TABLE public."user";

CREATE TABLE public."user" (
	id bigserial NOT NULL,
	email varchar(255) NOT NULL,
	"password" varchar(255) NOT NULL,
	"role" int4 DEFAULT 2 NOT NULL,
	first_name varchar(255) NOT NULL,
	last_name varchar(255) NOT NULL,
	department varchar(255) NULL,
	occupation varchar(255) NULL,
	active bool DEFAULT true NOT NULL,
	created_at timestamp DEFAULT now() NOT NULL,
	updated_at timestamp DEFAULT now() NOT NULL,
	CONSTRAINT users_email_key UNIQUE (email),
	CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_active ON public."user" USING btree (active);
CREATE INDEX idx_users_last_first ON public."user" USING btree (last_name, first_name);
CREATE INDEX idx_users_role ON public."user" USING btree (role);


-- public.bridge_product_attribute

-- Drop table

-- DROP TABLE public.bridge_product_attribute;

CREATE TABLE public.bridge_product_attribute (
	product_sk int8 NOT NULL,
	attribute_sk int8 NOT NULL,
	productattributeid_bk int8 NULL,
	attributeid_bk int8 NULL,
	CONSTRAINT bridge_product_attribute_pkey PRIMARY KEY (product_sk, attribute_sk),
	CONSTRAINT fk_bridge_product_attribute_attribute FOREIGN KEY (attribute_sk) REFERENCES public.dim_attribute(attribute_key),
	CONSTRAINT fk_bridge_product_attribute_product FOREIGN KEY (product_sk) REFERENCES public.dim_product(product_key)
);


-- public.fact_cart_line

-- Drop table

-- DROP TABLE public.fact_cart_line;

CREATE TABLE public.fact_cart_line (
	cartline_key bigserial NOT NULL,
	cartid_bk int8 NOT NULL,
	product_sk int8 NOT NULL,
	customer_sk int8 NOT NULL,
	date_sk int8 NOT NULL,
	time_sk int8 NOT NULL,
	quantity int4 NOT NULL,
	CONSTRAINT fact_cart_line_pkey PRIMARY KEY (cartline_key),
	CONSTRAINT fk_fact_cart_line_customer FOREIGN KEY (customer_sk) REFERENCES public.dim_customer(customer_key),
	CONSTRAINT fk_fact_cart_line_date FOREIGN KEY (date_sk) REFERENCES public.dim_date(date_key),
	CONSTRAINT fk_fact_cart_line_product FOREIGN KEY (product_sk) REFERENCES public.dim_product(product_key),
	CONSTRAINT fk_fact_cart_line_time FOREIGN KEY (time_sk) REFERENCES public.dim_time(time_key)
);


-- public.fact_order

-- Drop table

-- DROP TABLE public.fact_order;

CREATE TABLE public.fact_order (
	order_key bigserial NOT NULL,
	orderid_bk int8 NOT NULL,
	customer_sk int8 NOT NULL,
	address_sk int8 NULL,
	date_sk int8 NOT NULL,
	time_sk int8 NOT NULL,
	paid numeric(20, 6) NULL,
	paid_tax_incl numeric(20, 6) NULL,
	taxrate numeric(20, 6) NULL,
	conversion_rate numeric NULL,
	paymenttype varchar(300) NULL,
	carrier varchar(300) NULL,
	CONSTRAINT fact_order_pkey PRIMARY KEY (order_key),
	CONSTRAINT fk_fact_order_address FOREIGN KEY (address_sk) REFERENCES public.dim_address(address_key),
	CONSTRAINT fk_fact_order_customer FOREIGN KEY (customer_sk) REFERENCES public.dim_customer(customer_key),
	CONSTRAINT fk_fact_order_date FOREIGN KEY (date_sk) REFERENCES public.dim_date(date_key),
	CONSTRAINT fk_fact_order_time FOREIGN KEY (time_sk) REFERENCES public.dim_time(time_key)
);
CREATE INDEX fki_fk_fact_order_address ON public.fact_order USING btree (address_sk);
CREATE INDEX fki_fk_fact_order_customer ON public.fact_order USING btree (customer_sk);
CREATE INDEX fki_fk_fact_order_date ON public.fact_order USING btree (date_sk);
CREATE INDEX fki_fk_fact_order_time ON public.fact_order USING btree (time_sk);
CREATE UNIQUE INDEX idx_fact_order_orderid_bk ON public.fact_order USING btree (orderid_bk);


-- public.fact_order_history

-- Drop table

-- DROP TABLE public.fact_order_history;

CREATE TABLE public.fact_order_history (
	orderhistory_key bigserial NOT NULL,
	orderhistoryid_bk int8 NOT NULL,
	orderstate_sk int8 NOT NULL,
	orderid_bk int8 NOT NULL,
	orderstateid_bk int8 NOT NULL,
	date_sk int8 NOT NULL,
	time_sk int8 NOT NULL,
	CONSTRAINT fact_order_history_pkey PRIMARY KEY (orderhistory_key),
	CONSTRAINT fk_order_history_date FOREIGN KEY (date_sk) REFERENCES public.dim_date(date_key),
	CONSTRAINT fk_order_history_state FOREIGN KEY (orderstate_sk) REFERENCES public.dim_order_state(orderstate_key),
	CONSTRAINT fk_order_history_time FOREIGN KEY (time_sk) REFERENCES public.dim_time(time_key)
);
CREATE INDEX idx_fact_order_history_lookup ON public.fact_order_history USING btree (orderhistoryid_bk, orderid_bk, orderstateid_bk);


-- public.fact_order_line

-- Drop table

-- DROP TABLE public.fact_order_line;

CREATE TABLE public.fact_order_line (
	orderline_key bigserial NOT NULL,
	orderid_bk int8 NOT NULL,
	orderdetailid_bk int8 NOT NULL,
	cartid_bk int8 NULL,
	product_sk int8 NOT NULL,
	customer_sk int8 NOT NULL,
	address_sk int8 NULL,
	date_sk int8 NOT NULL,
	time_sk int8 NOT NULL,
	quantity int4 NOT NULL,
	price numeric(20, 6) NULL,
	price_tax_incl numeric(20, 6) NULL,
	amount numeric(20, 6) NULL,
	amount_tax_incl numeric(20, 6) NULL,
	paid numeric(20, 6) NULL,
	paid_tax_incl numeric(20, 6) NULL,
	taxrate numeric(20, 6) NULL,
	conversion_rate numeric NULL,
	paymenttype varchar(300) NULL,
	carrier varchar(300) NULL,
	CONSTRAINT fact_order_line_pkey PRIMARY KEY (orderline_key),
	CONSTRAINT fk_order_line_address FOREIGN KEY (address_sk) REFERENCES public.dim_address(address_key),
	CONSTRAINT fk_order_line_customer FOREIGN KEY (customer_sk) REFERENCES public.dim_customer(customer_key),
	CONSTRAINT fk_order_line_date FOREIGN KEY (date_sk) REFERENCES public.dim_date(date_key),
	CONSTRAINT fk_order_line_product FOREIGN KEY (product_sk) REFERENCES public.dim_product(product_key),
	CONSTRAINT fk_order_line_time FOREIGN KEY (time_sk) REFERENCES public.dim_time(time_key)
);
CREATE INDEX idx_fact_order_line_lookup ON public.fact_order_line USING btree (orderid_bk, orderdetailid_bk, product_sk);