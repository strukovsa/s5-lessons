CREATE TABLE dds.dm_orders (id SERIAL PRIMARY KEY,
			user_id INTEGER NOT NULL,
			restaurant_id INTEGER NOT NULL,
			timestamp_id INTEGER NOT NULL,
			order_key VARCHAR NOT NULL,
			order_status VARCHAR NOT NULL)