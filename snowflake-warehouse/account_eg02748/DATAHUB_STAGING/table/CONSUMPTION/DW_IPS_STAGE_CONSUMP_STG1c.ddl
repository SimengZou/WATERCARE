-- datahub_staging.dw_ips_stage_consump_stg1c definition

-- Drop table


DROP TABLE IF EXISTS datahub_staging.dw_ips_stage_consump_stg1c;

CREATE TRANSIENT TABLE IF NOT EXISTS datahub_staging.dw_ips_stage_consump_stg1c
(
	seq INTEGER
	,compkey INTEGER  
	,readingkey INTEGER  
	,reading INTEGER  
	,maxread INTEGER  
	,consump INTEGER 
)
;
-- ALTER TABLE datahub_staging.dw_ips_stage_consump_stg1c owner to talend_readwrite;