
DROP TABLE IF EXISTS datahub_analytics.dw_consumption_geospatial;
CREATE TABLE datahub_analytics.dw_consumption_geospatial
(
	row_id Integer IDENTITY NOT NULL
	,gis_id INTEGER 
	,polygon_code VARCHAR(50) 
	,polygon_description VARCHAR(256) 
	,cal_year INTEGER  
	,cal_month_no INTEGER 
	,consumption_type VARCHAR(100) 
	,consumption_category VARCHAR(100) 
	,value NUMERIC(38,9) 
	,polygon_name VARCHAR(50) 
)
;
