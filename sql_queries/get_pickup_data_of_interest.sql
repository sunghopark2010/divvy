drop table if exists pickups_of_interest;
create table pickups_of_interest as
(
select
	t.trip_id
	, case when t.gender_cd = 'male' then 0 else 1 end as gender
	, least(((2013 - t.birthyear) / 10) - 1, 6) as age
	, extract(month from t.start_time)-6 as "month"
	, case when extract(dow from t.start_time) = 0 then 6
		else extract(dow from t.start_time) - 1 end as dow
	, extract(hour from t.start_time)::int / 3 as hod
	, t.from_station_id
	, case
		when dbs.distance = 0 then 0 
		when dbs.distance <= 1 then 1
		when dbs.distance <= 2 then 2
		when dbs.distance <= 3 then 3
		when dbs.distance <= 5 then 4
		when dbs.distance <= 8 then 5
		when dbs.distance > 8 then 6
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
		when (dbs.distance / t.duration * 3600) = 0 then 0
		when (dbs.distance / t.duration * 3600) <= 4 then 1
		when (dbs.distance / t.duration * 3600) <= 6 then 2
		when (dbs.distance / t.duration * 3600) <= 8 then 3
		when (dbs.distance / t.duration * 3600) <= 10 then 4
		when (dbs.distance / t.duration * 3600) <= 12 then 5
		when (dbs.distance / t.duration * 3600) > 12 then 6
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
	and (dbs.distance / t.duration * 3600) < 50 -- there exist errorneously too fast data points; according to wikipedia (http://en.wikipedia.org/wiki/Bicycle_performance), the official highest speed is 51.28 mph
);
create index poi_trip_id on pickups_of_interest(trip_id);

select gender, age, month, dow, hod, from_station_id as station_id, distance, duration, speed from pickups_of_interest;