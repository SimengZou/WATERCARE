-- datahub_staging.dw_ips_stage_consump_stg4 definition

-- Drop table


DROP TABLE IF EXISTS datahub_staging.dw_ips_stage_consump_stg4;

CREATE TRANSIENT TABLE IF NOT EXISTS datahub_staging.dw_ips_stage_consump_stg4
(
	
	comp_key INTEGER 
	,comp_reading_key INTEGER 
	,rownum INTEGER
	,prev_read_date TIMESTAMP 
	,read_date TIMESTAMP 
	,reading INTEGER 
	,usage INTEGER 
	,reading_days INTEGER 
	
)
;
-- ALTER TABLE datahub_staging.dw_ips_stage_consump_stg4 owner to talend_readwrite;