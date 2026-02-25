CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_RESOURCES_RATETABLE_CALCULATIONTABLE_ERROR AS SELECT src, 'IPS_RESOURCES_RATETABLE_CALCULATIONTABLE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CALCULATIONTABLEKEY), '0'), 38, 10) is null and 
                    src:CALCULATIONTABLEKEY is not null) THEN
                    'CALCULATIONTABLEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CALCULATIONTABLEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CYCLEDAYS), '0'), 38, 10) is null and 
                    src:CYCLEDAYS is not null) THEN
                    'CYCLEDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:CYCLEDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FORMULAINPUT), '0'), 38, 10) is null and 
                    src:FORMULAINPUT is not null) THEN
                    'FORMULAINPUT contains a non-numeric value : \'' || AS_VARCHAR(src:FORMULAINPUT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FORMULAOUTPUT), '0'), 38, 10) is null and 
                    src:FORMULAOUTPUT is not null) THEN
                    'FORMULAOUTPUT contains a non-numeric value : \'' || AS_VARCHAR(src:FORMULAOUTPUT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXIMUMCHARGE), '0'), 38, 10) is null and 
                    src:MAXIMUMCHARGE is not null) THEN
                    'MAXIMUMCHARGE contains a non-numeric value : \'' || AS_VARCHAR(src:MAXIMUMCHARGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINIMUMCHARGE), '0'), 38, 10) is null and 
                    src:MINIMUMCHARGE is not null) THEN
                    'MINIMUMCHARGE contains a non-numeric value : \'' || AS_VARCHAR(src:MINIMUMCHARGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATETABLESETUPKEY), '0'), 38, 10) is null and 
                    src:RATETABLESETUPKEY is not null) THEN
                    'RATETABLESETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RATETABLESETUPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USAGEDAYS), '0'), 38, 10) is null and 
                    src:USAGEDAYS is not null) THEN
                    'USAGEDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:USAGEDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USEFORPRORATING), '0'), 38, 10) is null and 
                    src:USEFORPRORATING is not null) THEN
                    'USEFORPRORATING contains a non-numeric value : \'' || AS_VARCHAR(src:USEFORPRORATING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null) THEN
                'VARIATION_ID contains a non-timestamp value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
                etl_load_datetime,
                etl_load_metadatafilename
                FROM 
                (
                select 
                    src,
                    etl_load_datetime,
                    etl_load_metadatafilename
                    from
                    (
                        SELECT
        
                            
            src,
            etl_load_datetime,
            etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
                                    
                src:CALCULATIONTABLEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_RATETABLE_CALCULATIONTABLE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CALCULATIONTABLEKEY), '0'), 38, 10) is null and 
                    src:CALCULATIONTABLEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CYCLEDAYS), '0'), 38, 10) is null and 
                    src:CYCLEDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FORMULAINPUT), '0'), 38, 10) is null and 
                    src:FORMULAINPUT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FORMULAOUTPUT), '0'), 38, 10) is null and 
                    src:FORMULAOUTPUT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXIMUMCHARGE), '0'), 38, 10) is null and 
                    src:MAXIMUMCHARGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINIMUMCHARGE), '0'), 38, 10) is null and 
                    src:MINIMUMCHARGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATETABLESETUPKEY), '0'), 38, 10) is null and 
                    src:RATETABLESETUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USAGEDAYS), '0'), 38, 10) is null and 
                    src:USAGEDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USEFORPRORATING), '0'), 38, 10) is null and 
                    src:USEFORPRORATING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)