/**CREATING THE REMAINING TABLES IN OUR DATAMART**/

-- drop table fe_star.route;

CREATE TABLE IF NOT EXISTS fe_star.route (
    routekey int primary key,
    route_long_name text,
    route_short_name text,
    type text
);

CREATE TABLE IF NOT EXISTS fe_star.buss_stopp (
    bkey text primary key,
    buss_stopp_id int,
    buss_stopp_name text,
    latitude float,
    longitude float
);

CREATE TABLE IF NOT EXISTS fe_star.route_busstop (
    bkey text references fe_star.buss_stopp(bkey),
    routekey int references fe_star.route(routekey)
);
ALTER TABLE fe_star.route_busstop ADD CONSTRAINT route_busstop_un UNIQUE (bkey,routekey);

CREATE TABLE IF NOT EXISTS fe_star.weatherstation_s (
    weatherskey text primary key,
    name text,
    latitude float,
    longitude float
);

CREATE TABLE IF NOT EXISTS fe_star.facttable (
    skey serial primary key,
    bkey text references fe_star.buss_stopp(bkey),
    weatherskey text references fe_star.weatherstation_s(weatherskey),
    ktime int references fe_star.times(timek),
    kdate int references fe_star.dates(datek)
);

ALTER TABLE fe_star.facttable ADD CONSTRAINT facttable_un UNIQUE (bkey, weatherskey, ktime, kdate);
alter table fe_star.facttable add column temperatur float;




truncate fe_star.buss_stopp cascade;
