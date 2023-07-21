SELECT 
	concat(carrier, '-', flight) as flight,
	SUBSTRING(LPAD(CAST(sched_dep_time as VARCHAR), 4, '0') from 1 for 2) 
	|| ':'
	|| SUBSTRING(LPAD(CAST(sched_dep_time as VARCHAR), 4, '0') from 3 for 2) 
	as sched_dep_time
FROM public.flights;