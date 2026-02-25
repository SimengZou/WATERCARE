-- datahub_staging.dw_ips_stage_consump_stg5 definition

-- Drop table

DROP TABLE IF EXISTS datahub_staging.dw_ips_stage_consump_stg5;

CREATE TRANSIENT TABLE IF NOT EXISTS datahub_staging.dw_ips_stage_consump_stg5
(
    comp_key INTEGER 
	,comp_reading_key INTEGER 
	,rownum INTEGER 
	,prev_read_date TIMESTAMP
	,read_date TIMESTAMP
	,reading INTEGER 
	,usage INTEGER 
	,reading_days INTEGER 
	,reporting_usage INTEGER 
	,avg_daily_usage DOUBLE PRECISION   	
)
;
-- ALTER TABLE datahub_staging.dw_ips_stage_consump_stg5 owner to talend_readwrite;