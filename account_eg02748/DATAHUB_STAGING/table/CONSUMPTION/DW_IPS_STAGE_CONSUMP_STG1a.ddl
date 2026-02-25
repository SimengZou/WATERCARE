-- datahub_staging.dw_ips_stage_consump_stg1a definition

-- Drop table


DROP TABLE IF EXISTS datahub_staging.dw_ips_stage_consump_stg1a;

CREATE TRANSIENT TABLE IF NOT EXISTS datahub_staging.dw_ips_stage_consump_stg1a
(
	seq INTEGER
	,compkey INTEGER  
	,readingkey INTEGER  
	,reading INTEGER  
	,maxread INTEGER  
	,consump INTEGER 
)
;
-- ALTER TABLE datahub_staging.dw_ips_stage_consump_stg1a owner to talend_readwrite;