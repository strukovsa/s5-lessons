CREATE TABLE dds.dm_products (id SERIAL PRIMARY KEY,
							product_id VARCHAR NOT NULL,
							product_name VARCHAR NOT NULL,
 product_price NUMERIC(14, 2) DEFAULT 0 NOT NULL CONSTRAINT dm_products_price_check CHECK (product_price >= 0 AND product_price <= 999000000000.99),
restaurant_id INTEGER NOT NULL,
 	active_from timestamp NOT NULL,
 	active_to timestamp NOT NULL)