CREATE TABLE dds.dm_restaurants (id SERIAL PRIMARY KEY,
 				restaurant_id VARCHAR NOT NULL,
 				restaurant_name VARCHAR NOT NULL,
 				active_from TIMESTAMP NOT NULL,
 				active_to TIMESTAMP NOT NULL)