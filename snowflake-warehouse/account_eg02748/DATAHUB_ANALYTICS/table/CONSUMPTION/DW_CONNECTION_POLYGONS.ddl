
DROP TABLE IF EXISTS datahub_analytics.dw_connection_polygons;
CREATE TABLE datahub_analytics.dw_connection_polygons
(
	row_id INTEGER 
	,polygon_code VARCHAR(256) 
	,polygon_description VARCHAR(256) 
	,cal_year INTEGER 
	,cal_month_no INTEGER 
	,value INTEGER 
	,polygon_name VARCHAR(256) 
	,etl_src_from VARCHAR(256) 
	,etl_load_datetime TIMESTAMP
)
;