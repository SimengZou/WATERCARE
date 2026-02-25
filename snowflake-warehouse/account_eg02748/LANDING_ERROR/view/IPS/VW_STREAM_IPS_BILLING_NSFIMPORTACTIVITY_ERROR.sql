CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_NSFIMPORTACTIVITY_ERROR AS SELECT src, 'IPS_BILLING_NSFIMPORTACTIVITY' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACTIVITYDATE), '1900-01-01')) is null and 
                    src:ACTIVITYDATE is not null) THEN
                    'ACTIVITYDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ACTIVITYDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NSFIMPORTACTIVITYKEY), '0'), 38, 10) is null and 
                    src:NSFIMPORTACTIVITYKEY is not null) THEN
                    'NSFIMPORTACTIVITYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:NSFIMPORTACTIVITYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NSFTEMPLATE), '0'), 38, 10) is null and 
                    src:NSFTEMPLATE is not null) THEN
                    'NSFTEMPLATE contains a non-numeric value : \'' || AS_VARCHAR(src:NSFTEMPLATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTTYPE), '0'), 38, 10) is null and 
                    src:PAYMENTTYPE is not null) THEN
                    'PAYMENTTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RUNNUMBER), '0'), 38, 10) is null and 
                    src:RUNNUMBER is not null) THEN
                    'RUNNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:RUNNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STATUS), '0'), 38, 10) is null and 
                    src:STATUS is not null) THEN
                    'STATUS contains a non-numeric value : \'' || AS_VARCHAR(src:STATUS) || '\'' WHEN 
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
                                    
                src:NSFIMPORTACTIVITYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_NSFIMPORTACTIVITY as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACTIVITYDATE), '1900-01-01')) is null and 
                    src:ACTIVITYDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NSFIMPORTACTIVITYKEY), '0'), 38, 10) is null and 
                    src:NSFIMPORTACTIVITYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NSFTEMPLATE), '0'), 38, 10) is null and 
                    src:NSFTEMPLATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTTYPE), '0'), 38, 10) is null and 
                    src:PAYMENTTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RUNNUMBER), '0'), 38, 10) is null and 
                    src:RUNNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STATUS), '0'), 38, 10) is null and 
                    src:STATUS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)