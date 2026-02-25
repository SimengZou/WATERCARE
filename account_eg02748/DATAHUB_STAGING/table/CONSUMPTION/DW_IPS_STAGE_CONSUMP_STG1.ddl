-- datahub_staging.dw_ips_stage_consump_stg1 definition

-- Drop table


DROP TABLE IF EXISTS datahub_staging.dw_ips_stage_consump_stg1;

CREATE TRANSIENT TABLE IF NOT EXISTS datahub_staging.dw_ips_stage_consump_stg1
(
	seq INTEGER
	,compkey INTEGER  
	,readingkey INTEGER  
	,reading INTEGER  
	,maxread INTEGER  
	,consump INTEGER 
)
;
-- ALTER TABLE datahub_staging.dw_ips_stage_consump_stg1 owner to talend_readwrite;