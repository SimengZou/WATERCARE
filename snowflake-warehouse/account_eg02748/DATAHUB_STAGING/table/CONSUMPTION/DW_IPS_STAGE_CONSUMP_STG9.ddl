-- datahub_staging.dw_ips_stage_consump_stg9 definition

-- Drop table


DROP TABLE IF EXISTS datahub_staging.dw_ips_stage_consump_stg9;

CREATE TRANSIENT TABLE IF NOT EXISTS datahub_staging.dw_ips_stage_consump_stg9
(
	
	account_key INTEGER  
	,account_service_key INTEGER  
	,address_key INTEGER  
	,comp_key INTEGER  
	,month INTEGER  
	,cal_month_num INTEGER  
	,cal_year INTEGER  
	,act_daily_avg DOUBLE PRECISION  
	,actual_days INTEGER  
	,actual_volume DOUBLE PRECISION 
	,full_month_flag VARCHAR(1)  
	,est_daily_avg DOUBLE PRECISION 
	,est_days INTEGER  
	,est_volume DOUBLE PRECISION  
	,total_volume DOUBLE PRECISION 
	,estimate_flag VARCHAR(10)  
	,estimate_method VARCHAR(50)  
	,consumption_percentage DOUBLE PRECISION
	,ratio DOUBLE PRECISION 
)
;
-- ALTER TABLE datahub_staging.dw_ips_stage_consump_stg9 owner to talend_readwrite;