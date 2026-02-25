-- datahub_staging.dw_ips_stage_consump_stg7 definition

-- Drop table


DROP TABLE IF EXISTS datahub_staging.dw_ips_stage_consump_stg7;

CREATE TRANSIENT TABLE IF NOT EXISTS datahub_staging.dw_ips_stage_consump_stg7
(
	
	account_key INTEGER 
	,account_service_key INTEGER 
	,address_key INTEGER  
	,consumption_percentage DOUBLE PRECISION
	,subtractive_flag VARCHAR(1) 
	,comp_key INTEGER  	
	,cal_month INTEGER 
	,first_reading_date TIMESTAMP 
	,prev_reading_date TIMESTAMP 
	,month_start_date TIMESTAMP 
	,month_end_date TIMESTAMP 
	,avg_daily_usage DOUBLE PRECISION
	,actual_days INTEGER 
	,actual_volume DOUBLE PRECISION
	,full_month_flag VARCHAR(1)	
	
)
;
-- ALTER TABLE datahub_staging.dw_ips_stage_consump_stg7 owner to talend_readwrite;