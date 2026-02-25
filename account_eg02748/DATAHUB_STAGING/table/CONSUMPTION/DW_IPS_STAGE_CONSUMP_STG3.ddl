-- datahub_staging.dw_ips_stage_consump_stg3 definition

-- Drop table


DROP TABLE IF EXISTS datahub_staging.dw_ips_stage_consump_stg3;

CREATE TRANSIENT TABLE IF NOT EXISTS datahub_staging.dw_ips_stage_consump_stg3
(

	comp_key INTEGER 
	,comp_reading_key INTEGER  
	,rownum INTEGER  
	,read_date TIMESTAMP 
	,reading INTEGER   
	,usage INTEGER 
)
;
-- ALTER TABLE datahub_staging.dw_ips_stage_consump_stg3 owner to talend_readwrite;