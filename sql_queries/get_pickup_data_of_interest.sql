drop table if exists pickups_of_interest;
create table pickups_of_interest as
(
select
	t.trip_id
	, case when t.gender_cd = 'male' then 0 else 1 end as gender
	, 2013 - t.birthyear as age
	, extract(hour from t.start_time) as time_of_day
	, extract(dow from t.start_time) as weekday
	, (t.duration / 60)::Decimal(6, 2) as duration
	, dbs.distance
	, (dbs.distance / t.duration * 3600)::Decimal(5, 2) as velocity -- miles per hour
	
from
	trips t
left join distances_between_stations dbs on dbs.from_station_id = t.from_station_id and dbs.to_station_id = t.to_station_id
where
	t.gender_cd is not null
	and t.birthyear is not null
	and t.user_type_cd = 'subscriber'
	and t.start_time between '2013-08-01'::date and '2013-09-01'::date

	and t.duration is not null -- should be deleted later
);
create index poi_trip_id on pickups_of_interest(trip_id);

select max(duration), max(distance), max(velocity) from pickups_of_interest;
-- 84202, 14.29, 466.0274

select * from pickups_of_interest;