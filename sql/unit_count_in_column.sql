select
	"column" , mixs_pref_unit,
	unit_name ,
	unit_count_in_column
from
	(
	select
		"column" , mixs_pref_unit,
		unit_name ,
		count(1) as unit_count_in_column
	from
		q3_lookup ql
	group by
		"column" ,
		unit_name )
order by
	"column" asc, unit_count_in_column desc
