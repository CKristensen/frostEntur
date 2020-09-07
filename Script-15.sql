/** Getting stopnames and their coordinates**/
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

create table if not exists datamart_star.closest_buss_weather_station as
select c.bid as stop_name, c.sid as sourceId
from frostentur.closest_buss_weather_station c;


ALTER TABLE datamart_star.closest_buss_weather_station ADD CONSTRAINT closest_buss_weather_station_fk FOREIGN KEY (sourceid) REFERENCES datamart_star.weatherstation_s(sourceid);
 



CREATE TABLE "datamart_star"."times" (
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


TRUNCATE TABLE datamart_star.times;

-- Unknown member
INSERT INTO datamart_star.times VALUES (
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

INSERT INTO datamart_star.times
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

DROP TABLE if exists datamart_star.date_s;

CREATE TABLE datamart_star.date_s
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

ALTER TABLE datamart_star.date_s ADD CONSTRAINT date_s_k_date_pk PRIMARY KEY (k_date);

CREATE INDEX date_s_date_actual_idx
  ON datamart_star.date_s(date_actual);

COMMIT;

INSERT INTO datamart_star.date_s
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


update datamart_star.times set ktime = ROUND(ktime/10000, 0) where ktime > 0;

/*Since buss stop have no unique key I create a new*/
create or replace function insert_bkey() returns trigger as $$
begin
    if NEW.bkey is null then
       NEW.bkey := concat(cast(NEW.buss_stopp_id as text), new.buss_stopp_name);
    end if;
    return new;
end;
$$ language plpgsql;

create trigger insert_bkey
before insert
on datamart_star.buss_stopp
for each row
execute procedure insert_bkey();

insert into datamart_star.buss_stopp (buss_stopp_id, buss_stopp_name, latitude, longitude) 
select stop_id, stop_name, lat, lon
from frostentur.busstoplatlon;

/**DELETE DUPLICATES**/
DELETE FROM frostentur.routes T1
    USING   frostentur.routes T2
WHERE   T1.ctid < T2.ctid  -- delete the older versions
    AND T1.route_id  = T2.route_id;
   
   
insert into datamart_star.route (route_id, route_short_name, route_long_name, type)
select route_id, route_short_name, route_long_name, route_type
from frostentur.routes r;


insert into datamart_star.weatherstation_s (sourceid, latitude, longitude, "name") 
select w.sourceid, w.lat, w.lon, w."name" from frostentur.weatherstationlatlon w;


select * from datamart_star.weatherstation_s;


select * from datamart_star.buss_stopp b
where bkey='102060Bjørkelangen skole';

insert into datamart_star.facttable (buss_stop_id, sourceid, temperatur, kdate, ktime)
with weather_data as (
		select temp_grader, replace(vær_stasjon_id, ':0', '') sid, 
		LEFT(replace(temp_date_time, '-',''), 8) kdate,
		cast(SUBSTRING(replace(temp_date_time, '-',''), 10, 2) as int) ktime from public.temperatur
		), buss_data as
		(
		select bkey, sid
		from frostentur.closest_buss_weather_station ws
		join datamart_star.buss_stopp bs on bs.buss_stopp_name = ws.bid
		)
	select w.bkey, wd.sid, wd.temp_grader, cast(wd.kdate as int), cast(wd.ktime as int)
	from weather_data wd
	join buss_data w using(sid);


select bkey, sid, bid 
from frostentur.closest_buss_weather_station ws
join datamart_star.buss_stopp bs on bs.buss_stopp_name = ws.bid;

select s.buss_stop_id , s.kdate, s.ktime, count(*)
from datamart_star.facttable s
group by s.buss_stop_id, s.kdate, s.ktime
order by count(*) desc;

select distinct(t.vær_stasjon_id)
from public.temperatur t;

insert into datamart_star.facttable_1 (buss_stop_id, sourceId, temperatur, kdate, ktime)
select bs.bkey, d.sourceid ,d.value ,d.datek, d.timek from frostentur.dailytemperatures d 
join datamart_star.closest_buss_weather_station cbws using(sourceId)
join datamart_star.buss_stopp bs on bs.buss_stopp_name = cbws.stop_name ;¨

truncate table datamart_star.facttable_1;


select r.route_long_name , max(f.temperatur) max_temperatur
from datamart_star.facttable_1 f 
join datamart_star.buss_stopp bs on f.buss_stop_id = bs.bkey
join datamart_star.route_busstop rb using(bkey)
join datamart_star.route r using(route_id)
group by r.route_long_name order by max_temperatur desc;

select bs.buss_stopp_name , f.temperatur
from datamart_star.facttable_1 f 
join datamart_star.buss_stopp bs on f.buss_stop_id = bs.bkey
join datamart_star.route_busstop rb using(bkey)
join datamart_star.route r using(route_id)
order by temperatur desc;

select mt.route_long_name, max(t.buss_stopp_name), max(t.temperatur)
from
		(select r.route_long_name , max(f.temperatur) max_temperatur
		from datamart_star.facttable_1 f 
		join datamart_star.buss_stopp bs on f.buss_stop_id = bs.bkey
		join datamart_star.route_busstop rb using(bkey)
		join datamart_star.route r using(route_id)
		group by r.route_long_name order by max_temperatur desc) as mt
	join
		(select bs.buss_stopp_name, r.route_long_name, f.temperatur
		from datamart_star.facttable_1 f 
		join datamart_star.buss_stopp bs on f.buss_stop_id = bs.bkey
		join datamart_star.route_busstop rb using(bkey)
		join datamart_star.route r using(route_id)
		order by temperatur desc) as t
	on t.route_long_name = mt.route_long_name
	where mt.max_temperatur = t.temperatur
	group by mt.route_long_name order by max(t.temperatur) desc;


select buss_stopp_id, count(*) from datamart_star.buss_stopp bs 
group by buss_stopp_id order by count(*) desc;

select * 
from datamart_star.buss_stopp bs
where bs.buss_stopp_id = '5644';






