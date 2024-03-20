CREATE TABLE stg.ordersystem_users (id SERIAL PRIMARY KEY,
                                    object_id varchar NOT NULL,
                                    object_value text NOT NULL,
                                    update_ts timestamp NOT NULL);

CREATE TABLE stg.ordersystem_orders (id SERIAL PRIMARY KEY,
                                    object_id varchar NOT NULL,
                                    object_value text NOT NULL,
                                    update_ts timestamp NOT NULL);

CREATE TABLE stg.ordersystem_restaurants (id SERIAL PRIMARY KEY,
                                    object_id varchar NOT NULL,
                                    object_value text NOT NULL,
                                    update_ts timestamp NOT NULL)
               
