DROP TABLE IF EXISTS datahub_analytics.dw_ips_dim_accountserviceposition_rs_history;
CREATE TABLE datahub_analytics.dw_ips_dim_accountserviceposition_rs_history
(
	dim_ips_accountserviceposition_key INTEGER 
	,account_service_key INTEGER NOT NULL  
	,account_service_position_key INTEGER NOT NULL  
	,added_by VARCHAR(30)   
	,added_datetime TIMESTAMP    
	,address_key INTEGER NOT NULL  
	,asset_type INTEGER   
	,consumption_percentage DOUBLE PRECISION   
	,graph_usage_flag VARCHAR(1)   
	,hide_readings_on_bill VARCHAR(1)   
	,meter_register_use INTEGER   
	,modified_by VARCHAR(30)   
	,modified_datetime TIMESTAMP    
	,move_in_reading_key INTEGER NOT NULL  
	,move_out_reading_key INTEGER NOT NULL  
	,position INTEGER   
	,start_datetime TIMESTAMP    
	,stop_datetime TIMESTAMP    
	,subtractive_flag VARCHAR(1)   
	,usedaysrds VARCHAR(1)   
	,use_uom VARCHAR(1)   
	,usghistinbilloutput VARCHAR(1)   
	,winter_average DOUBLE PRECISION   
	,etl_load_datetime TIMESTAMP    
)
;
