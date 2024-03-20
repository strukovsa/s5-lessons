CREATE TABLE dds.dm_timestamps (id SERIAL PRIMARY KEY,
					ts timestamp NOT NULL,
	year SMALLINT CHECK(year>=2022 AND year<2500) NOT NULL,
	month SMALLINT CHECK(month>=1 AND month<=12) NOT NULL,
	day SMALLINT CHECK(day>=1 AND day<=31) NOt NULL,
	time TIME NOT NULL,
	date DATE NOT NULL)