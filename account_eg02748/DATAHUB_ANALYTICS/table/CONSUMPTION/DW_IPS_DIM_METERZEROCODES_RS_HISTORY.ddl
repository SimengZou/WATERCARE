DROP TABLE IF EXISTS datahub_analytics.dw_ips_dim_meterzerocodes_rs_history;
CREATE TABLE datahub_analytics.dw_ips_dim_meterzerocodes_rs_history
(
	stage_ips_meterzerocodes_key INTEGER
	,added_datetime TIMESTAMP   
	,code VARCHAR(10) NOT NULL  
	,description VARCHAR(300)   
	,modified_datetime TIMESTAMP   
	,etl_load_datetime TIMESTAMP   
)
;
