/**CREATE TABLE IF NOT EXISTS datamart_star.route (
    route_id int primary key,
    route_long_name text,
    route_short_name text,
    type text
);

CREATE TABLE IF NOT EXISTS datamart_star.buss_stopp (
    bkey text primary key,
    buss_stopp_id int,
    buss_stopp_name text,
    latitude float,
    longitude float

);

CREATE TABLE IF NOT EXISTS datamart_star.route_busstop (
    bkey text primary key references buss_stopp(bkey),
    route_id int referencesroute(route_id)
);

CREATE TABLE IF NOT EXISTS datamart_star.weatherstation_s (
    sourceid text primary key,
    name text,
    latitude float,
    longitude float
)

CREATE TABLE IF NOT EXISTS datamart_star.facttable (
    skey serial primary key,
    buss_stop_id text references buss_stopp(bkey),
    sourceid text references weatherstation_s(sourceId),
    ktime int references times(ktime),
    kdate int references date_s(k_date)
)

/**

/** Getting stopnames and their coordinates filtering the stops only in central oslo**/
create view sbstop_coor as (select stop_name, min(b.lat) lat, min(b.lon) lon from frostentur.busstoplatlon b
join public.oslostop o using(stop_name)
group by stop_name);

/**Getting valid weather stations and their coordinates**/
create view weather_station_coor as (
select w.sourceid, w.lat, w.lon from frostentur.weatherstationlatlon w 
join (select distinct(substring(t.vær_stasjon_id, 1, 7)) sourceid from public.temperatur t) h using(sourceid)
group by w.sourceid);

/**Only valid weather stations**/
select distinct(substring(t.vær_stasjon_id, 1, 7)) from public.temperatur t;

/** Pythagoras to get the closest weather station to each buss station
 ***/
create table if not exists frostentur.closest_buss_weather_station as (
with min_distance as (
		select  buss_stop.stop_name bid, MIN(SQRT(POWER(buss_stop.lat-t.lat ,2)+POWER(buss_stop.lon-t.lon, 2))) mdistance
		from sbstop_coor buss_stop
		cross join weather_station_coor t
		group by buss_stop.stop_name
	),
	distances as (
		select  buss_stop.stop_name bid, t.sourceid sid, SQRT(POWER(buss_stop.lat-t.lat, 2) + POWER(buss_stop.lon-t.lon, 2)) distance
		from sbstop_coor buss_stop
		cross join weather_station_coor t
	) 
	select * 
	from distances d 
	join min_distance md 
	using(bid)
	where md.mdistance = d.distance);

CREATE TABLE "star"."times" (
    id int4 NOT NULL,
    time time,
    hour int2,
    military_hour int2,
    minute int4,
    second int4,
    minute_of_day int4,
    second_of_day int4,
    quarter_hour varchar,
    am_pm varchar,
    day_night varchar,
    day_night_abbrev varchar,
    time_period varchar,
    time_period_abbrev varchar
)
WITH (OIDS=FALSE);


TRUNCATE TABLE star.times;

-- Unknown member
INSERT INTO star.times VALUES (
    -1, --id
    '0:0:0', -- time
    0, -- hour
    0, -- military_hour
    0, -- minute
    0, -- second
    0, -- minute_of_day
    0, -- second_of_day
    'Unknown', -- quarter_hour
    'Unknown', -- am_pm
    'Unknown', -- day_night
    'Unk', -- day_night_abbrev
    'Unknown', -- time_period
    'Unk' -- time_period_abbrev
);

INSERT INTO star.times
SELECT
  to_char(datum, 'HH24MISS')::integer AS id,
  datum::time AS time,

  to_char(datum, 'HH12')::integer AS hour,
  to_char(datum, 'HH24')::integer AS military_hour,

  extract(minute FROM datum)::integer AS minute,

  extract(second FROM datum) AS second,

  to_char(datum, 'SSSS')::integer / 60 AS minute_of_day,
  to_char(datum, 'SSSS')::integer AS second_of_day,

  to_char(datum - (extract(minute FROM datum)::integer % 15 || 'minutes')::interval, 'hh24:mi') ||
  ' – ' ||
  to_char(datum - (extract(minute FROM datum)::integer % 15 || 'minutes')::interval + '14 minutes'::interval, 'hh24:mi')
    AS quarter_hour,

  to_char(datum, 'AM') AS am_pm,

  CASE WHEN to_char(datum, 'hh24:mi') BETWEEN '08:00' AND '19:59' THEN 'Day (8AM-8PM)' ELSE 'Night (8PM-8AM)' END
  AS day_night,
  CASE WHEN to_char(datum, 'hh24:mi') BETWEEN '08:00' AND '19:59' THEN 'Day' ELSE 'Night' END
  AS day_night_abbrev,

  CASE
  WHEN to_char(datum, 'hh24:mi') BETWEEN '00:00' AND '03:59' THEN 'Late Night (Midnight-4AM)'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '04:00' AND '07:59' THEN 'Early Morning (4AM-8AM)'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '08:00' AND '11:59' THEN 'Morning (8AM-Noon)'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '12:00' AND '15:59' THEN 'Afternoon (Noon-4PM)'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '16:00' AND '19:59' THEN 'Evening (4PM-8PM)'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '20:00' AND '23:59' THEN 'Night (8PM-Midnight)'
  END AS time_period,

  CASE
  WHEN to_char(datum, 'hh24:mi') BETWEEN '00:00' AND '03:59' THEN 'Late Night'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '04:00' AND '07:59' THEN 'Early Morning'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '08:00' AND '11:59' THEN 'Morning'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '12:00' AND '15:59' THEN 'Afternoon'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '16:00' AND '19:59' THEN 'Evening'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '20:00' AND '23:59' THEN 'Night'
  END AS time_period_abbrev

FROM generate_series('2000-01-01 00:00:00'::timestamp, '2000-01-01 23:59:59'::timestamp, '1 hour') datum;



-- Dimension: Date
-- PK: k_date (YYYYMMDD)
-- Variable: all different time formats :-)

DROP TABLE if exists star.date_s;

CREATE TABLE star.date_s
(
  k_date              INT NOT NULL,
  date_actual              DATE NOT NULL,
  epoch                    BIGINT NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_week              INT NOT NULL,
  day_of_month             INT NOT NULL,
  day_of_quarter           INT NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             BOOLEAN NOT NULL
);

ALTER TABLE star.date_s ADD CONSTRAINT date_s_k_date_pk PRIMARY KEY (k_date);

CREATE INDEX date_s_date_actual_idx
  ON star.date_s(date_actual);

COMMIT;

INSERT INTO star.date_s
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS k_date,
       datum AS date_actual,
       EXTRACT(EPOCH FROM datum) AS epoch,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'Day') AS day_name,
       EXTRACT(ISODOW FROM datum) AS day_of_week,
       EXTRACT(DAY FROM datum) AS day_of_month,
       datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'Month') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(ISOYEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
           ELSE FALSE
           END AS weekend_indr
FROM (SELECT '2000-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;