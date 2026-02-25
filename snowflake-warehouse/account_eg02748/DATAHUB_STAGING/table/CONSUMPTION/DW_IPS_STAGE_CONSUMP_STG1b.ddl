-- datahub_staging.dw_ips_stage_consump_stg1b definition

-- Drop table


DROP TABLE IF EXISTS datahub_staging.dw_ips_stage_consump_stg1b;

CREATE TRANSIENT TABLE IF NOT EXISTS datahub_staging.dw_ips_stage_consump_stg1b
(
	seq INTEGER
	,compkey INTEGER  
	,readingkey INTEGER  
	,reading INTEGER  
	,maxread INTEGER  
	,consump INTEGER 
)
;
-- ALTER TABLE datahub_staging.dw_ips_stage_consump_stg1b owner to talend_readwrite;