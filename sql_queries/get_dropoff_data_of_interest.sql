select
	case when t.gender_cd = 'male' then 0 else 1 end as gender
	, 2013 - t.birthyear as age
	, extract(hour from t.start_time) as time_of_day
	, to_char(extract(month from t.start_time), 'fm00') || to_char(extract(day from t.start_time), 'fm00') as "date"
from
	trips t
left join distances_between_stations dbs on dbs.from_station_id = t.from_station_id and dbs.to_station_id = t.to_station_id
where
	t.gender_cd is not null
	and t.birthyear is not null
	and t.user_type_cd = 'subscriber'
	and t.end_time between '2013-08-01'::date and '2013-09-01'::date
	