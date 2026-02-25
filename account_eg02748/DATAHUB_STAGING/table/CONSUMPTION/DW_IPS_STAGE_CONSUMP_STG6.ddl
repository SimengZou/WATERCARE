-- datahub_staging.dw_ips_stage_consump_stg6 definition

-- Drop table


DROP TABLE IF EXISTS datahub_staging.dw_ips_stage_consump_stg6;

CREATE TRANSIENT TABLE IF NOT EXISTS datahub_staging.dw_ips_stage_consump_stg6
(
	
	account_key INTEGER 
	,account_service_key INTEGER 
	,address_key INTEGER  
	,start_datetime TIMESTAMP
	,stop_datetime TIMESTAMP
	,service_status VARCHAR(10) 
	,consumption_percentage DOUBLE PRECISION
	,subtractive_flag VARCHAR(1) 
	,comp_key INTEGER 
	,comp_reading_key INTEGER  
	,rownum INTEGER 
	,prev_read_date TIMESTAMP 
	,read_date TIMESTAMP 
	,reading INTEGER  
	,usage INTEGER  
	,reading_days INTEGER 
	,reporting_usage INTEGER
	,avg_daily_usage DOUBLE PRECISION
	,cal_month INTEGER 
	,calendar_date TIMESTAMP 
	,full_month_flag VARCHAR(1) 
	,start_dt DATE 
	,stop_dt DATE 
	
)
;
-- ALTER TABLE datahub_staging.dw_ips_stage_consump_stg6 owner to talend_readwrite;