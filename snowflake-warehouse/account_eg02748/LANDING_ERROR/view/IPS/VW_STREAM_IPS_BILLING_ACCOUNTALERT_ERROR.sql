CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ACCOUNTALERT_ERROR AS SELECT src, 'IPS_BILLING_ACCOUNTALERT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTALERTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTALERTKEY is not null) THEN
                    'ACCOUNTALERTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTALERTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTDELINQUENCYKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTDELINQUENCYKEY is not null) THEN
                    'ACCOUNTDELINQUENCYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTDELINQUENCYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALERTDEFNKEY), '0'), 38, 10) is null and 
                    src:ALERTDEFNKEY is not null) THEN
                    'ALERTDEFNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ALERTDEFNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BANKRUPTCYKEY), '0'), 38, 10) is null and 
                    src:BANKRUPTCYKEY is not null) THEN
                    'BANKRUPTCYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BANKRUPTCYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENCYMSALERTKEY), '0'), 38, 10) is null and 
                    src:DELINQUENCYMSALERTKEY is not null) THEN
                    'DELINQUENCYMSALERTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DELINQUENCYMSALERTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
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
                                    
                src:ACCOUNTALERTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ACCOUNTALERT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTALERTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTALERTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTDELINQUENCYKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTDELINQUENCYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALERTDEFNKEY), '0'), 38, 10) is null and 
                    src:ALERTDEFNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BANKRUPTCYKEY), '0'), 38, 10) is null and 
                    src:BANKRUPTCYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENCYMSALERTKEY), '0'), 38, 10) is null and 
                    src:DELINQUENCYMSALERTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)