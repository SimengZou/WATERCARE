-- datahub_staging.dw_ips_stage_consump_stg6a definition

-- Drop table


DROP TABLE IF EXISTS datahub_staging.dw_ips_stage_consump_stg6a;

CREATE TRANSIENT TABLE IF NOT EXISTS datahub_staging.dw_ips_stage_consump_stg6a
(
	
	account_key INTEGER 
	,account_service_key INTEGER
	,address_key INTEGER 
	,start_datetime TIMESTAMP
	,stop_datetime TIMESTAMP 
	,consumption_percentage DOUBLE PRECISION
	,subtractive_flag VARCHAR(1) 
	,comp_key INTEGER 
	,comp_reading_key INTEGER 
	,rownum INTEGER 
	,prev_read_date TIMESTAMP
	,read_date TIMESTAMP
	,cal_month INTEGER  
	,month_start_date TIMESTAMP 
	,month_end_date TIMESTAMP 
	,avg_daily_usage DOUBLE PRECISION
	,actual_days INTEGER 
	,full_month_flag VARCHAR(1) 
	
)
;
-- ALTER TABLE datahub_staging.dw_ips_stage_consump_stg6a owner to talend_readwrite;