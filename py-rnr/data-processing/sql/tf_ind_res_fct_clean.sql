with tmp as (
	select name,
		pl,
		team,
		case
			when lower(year) like '%fr%'
			or lower(year) like '%freshman%' then 'FR'
			when lower(year) like '%so%'
			or lower(year) like '%sophomore%' then 'SO'
			when lower(year) like '%jr%'
			or lower(year) like '%junior%' then 'JR'
			when lower(year) like '%sr%'
			or lower(year) like '%senior%' then 'SR'
			else 'OTHER'
		end as class,
		case
			when lower(race_name) like '%55%'
			and lower(race_name) not like '%hurdles%' then '55m'
			when lower(race_name) like '%60%%'
			and lower(race_name) not like '%hurdles%'
			and lower(race_name) not like '%600%' then '60m'
			when lower(race_name) like '%55%'
			and lower(race_name) like '%hurdles%' then '55mH'
			when lower(race_name) like '%60%'
			and lower(race_name) like '%hurdles%' then '60mH'
			when lower(race_name) like '%100%'
			and lower(race_name) not like '%hurdles%'
			and lower(race_name) not like '%1000%' then '100m'
			when lower(race_name) like '%100%'
			and lower(race_name) like '%hurdles%'
			and lower(race_name) not like '%1000%' then '100mH'
			when lower(race_name) like '%110%'
			and lower(race_name) like '%hurdles%' then '110mH'
			when lower(race_name) like '%200%' then '200m'
			when lower(race_name) like '%300%'
			and lower(race_name) not like '%3000%' then '300m'
			when lower(race_name) like '%400%'
			and lower(race_name) not like '%hurdles%' then '400m'
			when lower(race_name) like '%400%'
			and lower(race_name) like '%hurdles%' then '400mH'
			when lower(race_name) like '%500%'
			and lower(race_name) not like '%5000%' then '500m'
			when lower(race_name) like '%600%' then '600m'
			when lower(race_name) like '%800%'
			and lower(race_name) not like '%8000%' then '800m'
			when (
				lower(race_name) like '%1000%'
				or lower(race_name) like '%1,000%'
			)
			and lower(race_name) not like '%10000%' then '1000m'
			when lower(race_name) like '%1500%'
			or lower(race_name) like '%1,500%' then '1500m'
			when lower(race_name) like '%mile%' then 'Mile'
			when (
				lower(race_name) like '%3000%'
				or lower(race_name) like '%3,000%'
			)
			and lower(race_name) not like '%steeple%' then '3000m'
			when (
				lower(race_name) like '%3000%'
				or lower(race_name) like '%3,000%'
			)
			and lower(race_name) like '%steeple%' then '3000mS'
			when lower(race_name) like '%5000%'
			or lower(race_name) like '%5k%'
			or lower(race_name) like '%5,000%' then '5000m'
			when lower(race_name) like '%10000%'
			or lower(race_name) like '%10k%'
			or lower(race_name) like '%10,000%' then '10km'
			else 'OTHER'
		end as event,
		-- keep raw time for display purposes
		REPLACE(time, 'h', '0') as time,
		meet_date,
		-- format meet date
		split_part(
			REGEXP_REPLACE(meet_date, '\s+', ' ', 'g'),
			' ',
			2
		) as meet_mnth,
		replace(
			split_part(
				REGEXP_REPLACE(meet_date, '\s+', ' ', 'g'),
				' ',
				3
			),
			',',
			''
		) as meet_day,
		split_part(
			REGEXP_REPLACE(meet_date, '\s+', ' ', 'g'),
			' ',
			4
		) as meet_yr,
		meet_location,
		meet_name,
		f.elevation,
		f.track_length,
		f.banked_or_flat
	from tf_ind_res_fct t
		left join facilities f on t.meet_location = f.meet_facility
	limit 10000
)

select name,
	pl,
	team,
	class,
	event,
	time,
	case
		-- Check if the string contains ':'
		when position(':' in time) > 0 then -- Extract minutes and convert to seconds
		(split_part(time, ':', 1)::INTEGER * 60) + -- Extract seconds and possibly milliseconds
		split_part(time, ':', 2)::FLOAT
		when time = 'DNF' then 0
		when time = 'FS' then 0
		when time = 'DNS' then 0
		when time = 'DQ' then 0
		when time = 'NT' then 0
		else -- If no ':', assume the value is already in seconds
		time::FLOAT
	end as time_in_seconds,
	case
		when position('-' in meet_day) > 0 then to_date(
			meet_mnth || ' ' || split_part(meet_day, '-', 1) || ' ' || meet_yr,
			'Month DD YYYY'
		)
		else to_date(
			meet_mnth || ' ' || meet_day || ' ' || meet_yr,
			'Month DD YYYY'
		)
	end as meet_start_dt,
	case
		when position('-' in meet_day) > 0 then to_date(
			meet_mnth || ' ' || split_part(meet_day, '-', 2) || ' ' || meet_yr,
			'Month DD YYYY'
		)
		else to_date(
			meet_mnth || ' ' || meet_day || ' ' || meet_yr,
			'Month DD YYYY'
		)
	end as meet_end_dt,
	meet_location,
	meet_name,
	elevation,
	track_length,
	banked_or_flat
from tmp