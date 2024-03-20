CREATE TABLE public.outbox(id INTEGER PRIMARY KEY,
						object_id INTEGER NOT NULL,
						record_ts TIMESTAMP NOT NULL,
						type VARCHAR NOT NULL,
					    payload TEXT NOT NULL)