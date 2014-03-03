drop table if exists distances_between_stations;
drop table if exists trips;
drop table if exists stations;

create table stations (
	station_id integer not null primary key,
	name text not null,
	latitude decimal(10, 8) not null,
	longitude decimal(10, 8) not null,
	capacity integer not null,
	start_date date not null
);
create index stations_station_id on stations(station_id);

create table trips (
	trip_id integer primary key,
	start_time timestamp without time zone not null,
	end_time timestamp without time zone not null,
	bike_id integer not null,
	from_station_id integer not null references stations (station_id),
	to_station_id integer not null references stations (station_id),
	user_type_cd varchar(16) check (user_type_cd in ('customer', 'subscriber') or user_type_cd is null),
	gender_cd varchar(8) check (gender_cd in ('female', 'male') or gender_cd is null),
	birthyear integer
);
create index trips_trip_id on trips(trip_id);
create index trips_from_station_id on trips(from_station_id);
create index trips_to_station_id on trips(to_station_id);

create table distances_between_stations (
    distance_between_station_id serial primary key,
    from_station_id integer not null references stations (station_id),
    to_station_id integer not null references stations (station_id),
    distance decimal(6, 2) not null check (distance >= 0.0),  --distance by bike in miles
    unique(from_station_id, to_station_id)
);
create index dbs_dbs_id on distances_between_stations (distance_between_station_id);
create index dbs_from_station_id on distances_between_stations (from_station_id);
create index dbs_to_station_id on distances_between_stations (to_station_id);