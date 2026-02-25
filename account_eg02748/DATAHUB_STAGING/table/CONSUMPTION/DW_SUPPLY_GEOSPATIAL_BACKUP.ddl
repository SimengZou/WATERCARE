DROP TABLE IF EXISTS datahub_staging.dw_supply_geospatial_backup;
CREATE TABLE datahub_staging.dw_supply_geospatial_backup
(
	row_id Integer 
	,gis_id INTEGER 
	,polygon_code VARCHAR(50)  
	,polygon_description VARCHAR(256)  
	,cal_year INTEGER   
	,cal_month_no INTEGER  
	,value double 
	,polygon_name VARCHAR(50)   
)
;
