DROP TABLE IF EXISTS datahub_analytics.dw_ips_dim_watertrackedreading_rs_history;
CREATE TABLE datahub_analytics.dw_ips_dim_watertrackedreading_rs_history
(
	dim_ips_watertrackedreading_key INTEGER
	,account_key INTEGER   
	,account_service_position_key INTEGER   
	,added_by VARCHAR(30)   
	,added_datetime TIMESTAMP    
	,bill_key INTEGER   
	,bill_type_key INTEGER   
	,modified_by VARCHAR(30)   
	,modified_datetime TIMESTAMP    
	,reading_key INTEGER   
	,water_tracked_reading_key INTEGER   
	,etl_load_datetime TIMESTAMP    
)
;
