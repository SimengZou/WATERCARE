DROP TABLE IF EXISTS datahub_analytics.dw_ips_dim_meterreading_rs_history;
CREATE TABLE datahub_analytics.dw_ips_dim_meterreading_rs_history
(
	dim_ips_meterreading_key INTEGER
	,added_by VARCHAR(30)   
	,added_datetime TIMESTAMP    
	,audit_flag VARCHAR(1)   
	,billable_usage DOUBLE PRECISION   
	,billable_usage_in_cubic_feet DOUBLE PRECISION   
	,comp_key INTEGER   
	,corrected_flag VARCHAR(1)   
	,custprob INTEGER   
	,cycle VARCHAR(256)   
	,estmate_flag VARCHAR(1)   
	,field_notes VARCHAR(300)   
	,final_flag VARCHAR(1)   
	,group_project INTEGER   
	,high_billable_usage DOUBLE PRECISION   
	,high_billable_usage_in_cubic_feet DOUBLE PRECISION   
	,high_reading DOUBLE PRECISION   
	,high_usage DOUBLE PRECISION   
	,histkey INTEGER   
	,initial_flag VARCHAR(1)   
	,inspection_key INTEGER   
	,modified_by VARCHAR(256)   
	,modified_datetime TIMESTAMP    
	,no_read_flag VARCHAR(1)   
	,prob_code_work_order INTEGER   
	,prob_code_service_request_no INTEGER   
	,prob_code VARCHAR(10)   
	,reader_code_service_request_no INTEGER   
	,read_by VARCHAR(12)   
	,read_datetime TIMESTAMP    
	,reader_code VARCHAR(10)   
	,reader_code_work_order INTEGER   
	,reading DOUBLE PRECISION   
	,reading_key INTEGER   
	,read_reason VARCHAR(10)   
	,read_source VARCHAR(10)   
	,read_value1 VARCHAR(300)   
	,read_value2 VARCHAR(300)   
	,read_value3 VARCHAR(300)   
	,read_value4 VARCHAR(300)   
	,ready_flag VARCHAR(1)   
	,usage DOUBLE PRECISION   
	,etl_load_datetime TIMESTAMP    
)
;
