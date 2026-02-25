-- datahub_staging.dw_ips_stage_consump_stg2 definition

-- Drop table


DROP TABLE IF EXISTS datahub_staging.dw_ips_stage_consump_stg2;

CREATE TRANSIENT TABLE IF NOT EXISTS datahub_staging.dw_ips_stage_consump_stg2
(
    compkey INTEGER   
	,readingkey INTEGER 
	,readdttm TIMESTAMP 
	,reading INTEGER  
	,usage INTEGER  
	,readyflag VARCHAR(1) 
	,billed VARCHAR(1) 
)
;
-- ALTER TABLE datahub_staging.dw_ips_stage_consump_stg2 owner to talend_readwrite;