DROP TABLE IF EXISTS datahub_analytics.dw_supply_geospatial;
CREATE TABLE datahub_analytics.dw_supply_geospatial
(
	row_id Integer IDENTITY NOT NULL
	,gis_id INTEGER   
	,polygon_code VARCHAR(50) 
	,polygon_description VARCHAR(256) 
	,cal_year INTEGER  
	,cal_month_no INTEGER
	,value DOUBLE
	,polygon_name VARCHAR(50) 
	,etl_src_from VARCHAR(256) 
	,etl_load_datetime TIMESTAMP 
)
;
