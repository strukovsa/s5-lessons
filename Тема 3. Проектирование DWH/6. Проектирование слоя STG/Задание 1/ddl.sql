CREATE TABLE stg.bonussystem_users (
	id integer NOT NULL,
	order_user_id text NOT NULL
);

CREATE TABLE stg.bonussystem_ranks (
	id int4 NOT NULL,
	"name" varchar(2048) NOT NULL,
	bonus_percent numeric(19, 5) NOT NULL DEFAULT 0,
	min_payment_threshold numeric(19, 5) NOT NULL DEFAULT 0);

CREATE TABLE stg.bonussystem_events (
	id int4 NOT NULL,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL);

CREATE INDEX idx_bonussystem_events__event_ts
ON stg.bonussystem_events (event_ts)