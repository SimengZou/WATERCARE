DROP TABLE IF EXISTS datahub_analytics.dw_ips_dim_xwatermeterdetail_rs_history;
CREATE TABLE datahub_analytics.dw_ips_dim_xwatermeterdetail_rs_history
(
	dim_ips_xwatermeterdetail_key INTEGER
	,added_datetime TIMESTAMP    
	,comp_key INTEGER   
	,modified_datetime TIMESTAMP    
	,meter_zero_code VARCHAR(10)   
	,etl_load_datetime TIMESTAMP    
)
;
