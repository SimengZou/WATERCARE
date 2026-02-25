-- datahub_staging.dw_ips_stage_consump_stg8 definition

-- Drop table


DROP TABLE IF EXISTS datahub_staging.dw_ips_stage_consump_stg8;

CREATE TRANSIENT TABLE IF NOT EXISTS datahub_staging.dw_ips_stage_consump_stg8
(
	
	comp_key INTEGER 
	,fullmonth INTEGER 
	,lastfullavg DOUBLE PRECISION 
	,ratio DOUBLE PRECISION
	,est_daily_avg DOUBLE PRECISION
	
)
;
-- ALTER TABLE datahub_staging.dw_ips_stage_consump_stg8 owner to talend_readwrite;