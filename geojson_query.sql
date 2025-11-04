select 
		fd.fact_key, fd.ship_id, 
		fd.datetime_id, 
		"date", "year", "month", "day", "hour", "minute", quarter, week_of_year, day_of_week
		latitude, longitude, wind_direction, wind_speed, air_temperature, tank0_liquid_volume, tank0_max_volume, tank0_percentage, 
		tank1_liquid_volume, tank1_max_volume, tank1_percentage, tank2_liquid_volume, tank2_max_volume, tank2_percentage, 
		tank3_liquid_volume, tank3_max_volume, tank3_percentage, tank4_liquid_volume, tank4_max_volume, tank4_percentage, 
		tank0_vapor_pressure, tank0_vapor_temperature, tank1_vapor_pressure, tank1_vapor_temperature, tank2_vapor_pressure, 
		tank2_vapor_temperature, tank3_vapor_pressure, tank3_vapor_temperature, tank4_vapor_pressure, tank4_vapor_temperature, data_source, original_datetime, 
		fd.created_at
	from f_data fd 
	left join d_calendar dc on fd.datetime_id = dc.datetime_id
	left join d_ship ds on fd.ship_id = ds.ship_id 