﻿drop table if exists pickups_of_interest;
create table pickups_of_interest as
(
select
	t.trip_id
	, case when t.gender_cd = 'male' then 0 else 1 end as gender
	, least(((2013 - t.birthyear) / 10) - 1, 6) as age
	, extract(month from t.start_time)-6 as "month"
	, extract(dow from t.start_time) as day_of_week
	, extract(hour from t.start_time)::int / 3 as hour_of_day
	, t.from_station_id
	, case
		when dbs.distance = 0 then 6 
		when dbs.distance <= 1 then 0
		when dbs.distance <= 2 then 1
		when dbs.distance <= 3 then 2
		when dbs.distance <= 5 then 3
		when dbs.distance <= 8 then 4
		when dbs.distance > 8 then 5
	end as distance
	, case
		when t.duration <= 60 * 2 then 0
		when t.duration <= 60 * 5 then 1
		when t.duration <= 60 * 10 then 2
		when t.duration <= 60 * 15 then 3
		when t.duration <= 60 * 30 then 4
		when t.duration <= 60 * 60 then 5
		when t.duration > 60 * 60 then 6
	end as duration		
    , case
		when (dbs.distance / t.duration * 3600/1609) = 0 then 0
		when (dbs.distance / t.duration * 3600/1609) <= 4 then 1
		when (dbs.distance / t.duration * 3600/1609) <= 6 then 2
		when (dbs.distance / t.duration * 3600/1609) <= 8 then 3
		when (dbs.distance / t.duration * 3600/1609) <= 10 then 4
		when (dbs.distance / t.duration * 3600/1609) <= 12 then 5
		when (dbs.distance / t.duration * 3600/1609) > 12 then 6
	end as speed -- miles per hour
from
	trips t
left join
	distances_between_stations dbs
on
	dbs.from_station_id = t.from_station_id and dbs.to_station_id = t.to_station_id
where
	t.gender_cd is not null
	and t.birthyear is not null
	and t.user_type_cd = 'subscriber'
	and t.start_time between '2013-06-01'::date and '2013-09-01'::date
);
create index poi_trip_id on pickups_of_interest(trip_id);

select gender, age, month, day_of_week as dow, hour_of_day as hod, from_station_id as station_id, distance, duration, speed from pickups_of_interest;
