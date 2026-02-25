DROP TABLE IF EXISTS datahub_analytics.dw_ips_dim_serviceoptions_rs_history;
CREATE TABLE datahub_analytics.dw_ips_dim_serviceoptions_rs_history
(
	added_datetime TIMESTAMP 
	,asset_type INTEGER   
	,code VARCHAR(40)   
	,modified_datetime TIMESTAMP    
	,service_options_key INTEGER NOT NULL  
	,etl_load_datetime TIMESTAMP    
)
;
