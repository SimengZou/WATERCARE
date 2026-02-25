CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_MAPDEFAULTPREFERENCES_ERROR AS SELECT src, 'IPS_BILLING_MAPDEFAULTPREFERENCES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAPDEFAULTPREFERENCESKEY), '0'), 38, 10) is null and 
                    src:MAPDEFAULTPREFERENCESKEY is not null) THEN
                    'MAPDEFAULTPREFERENCESKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MAPDEFAULTPREFERENCESKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUE1), '0'), 38, 10) is null and 
                    src:VALUE1 is not null) THEN
                    'VALUE1 contains a non-numeric value : \'' || AS_VARCHAR(src:VALUE1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUE2), '0'), 38, 10) is null and 
                    src:VALUE2 is not null) THEN
                    'VALUE2 contains a non-numeric value : \'' || AS_VARCHAR(src:VALUE2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUE3), '0'), 38, 10) is null and 
                    src:VALUE3 is not null) THEN
                    'VALUE3 contains a non-numeric value : \'' || AS_VARCHAR(src:VALUE3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUE4), '0'), 38, 10) is null and 
                    src:VALUE4 is not null) THEN
                    'VALUE4 contains a non-numeric value : \'' || AS_VARCHAR(src:VALUE4) || '\'' WHEN 
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
                                    
                src:MAPDEFAULTPREFERENCESKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_MAPDEFAULTPREFERENCES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAPDEFAULTPREFERENCESKEY), '0'), 38, 10) is null and 
                    src:MAPDEFAULTPREFERENCESKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUE1), '0'), 38, 10) is null and 
                    src:VALUE1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUE2), '0'), 38, 10) is null and 
                    src:VALUE2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUE3), '0'), 38, 10) is null and 
                    src:VALUE3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUE4), '0'), 38, 10) is null and 
                    src:VALUE4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)