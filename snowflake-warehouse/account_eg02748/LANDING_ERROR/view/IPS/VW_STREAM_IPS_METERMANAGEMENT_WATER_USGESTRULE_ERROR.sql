CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_METERMANAGEMENT_WATER_USGESTRULE_ERROR AS SELECT src, 'IPS_METERMANAGEMENT_WATER_USGESTRULE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CALCORDER), '0'), 38, 10) is null and 
                    src:CALCORDER is not null) THEN
                    'CALCORDER contains a non-numeric value : \'' || AS_VARCHAR(src:CALCORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONDITIONFORMULAKEY), '0'), 38, 10) is null and 
                    src:CONDITIONFORMULAKEY is not null) THEN
                    'CONDITIONFORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CONDITIONFORMULAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHCONDITIONFORMULAKEY), '0'), 38, 10) is null and 
                    src:HIGHCONDITIONFORMULAKEY is not null) THEN
                    'HIGHCONDITIONFORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:HIGHCONDITIONFORMULAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHUSGESTFORMULAKEY), '0'), 38, 10) is null and 
                    src:HIGHUSGESTFORMULAKEY is not null) THEN
                    'HIGHUSGESTFORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:HIGHUSGESTFORMULAKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USGESTFORMULAKEY), '0'), 38, 10) is null and 
                    src:USGESTFORMULAKEY is not null) THEN
                    'USGESTFORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:USGESTFORMULAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USGESTRULEGROUPKEY), '0'), 38, 10) is null and 
                    src:USGESTRULEGROUPKEY is not null) THEN
                    'USGESTRULEGROUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:USGESTRULEGROUPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USGESTRULEKEY), '0'), 38, 10) is null and 
                    src:USGESTRULEKEY is not null) THEN
                    'USGESTRULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:USGESTRULEKEY) || '\'' WHEN 
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
                                    
                src:USGESTRULEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_METERMANAGEMENT_WATER_USGESTRULE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CALCORDER), '0'), 38, 10) is null and 
                    src:CALCORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONDITIONFORMULAKEY), '0'), 38, 10) is null and 
                    src:CONDITIONFORMULAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHCONDITIONFORMULAKEY), '0'), 38, 10) is null and 
                    src:HIGHCONDITIONFORMULAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHUSGESTFORMULAKEY), '0'), 38, 10) is null and 
                    src:HIGHUSGESTFORMULAKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USGESTFORMULAKEY), '0'), 38, 10) is null and 
                    src:USGESTFORMULAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USGESTRULEGROUPKEY), '0'), 38, 10) is null and 
                    src:USGESTRULEGROUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USGESTRULEKEY), '0'), 38, 10) is null and 
                    src:USGESTRULEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)