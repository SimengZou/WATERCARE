CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_DEBITSCHEDULE_ERROR AS SELECT src, 'IPS_BILLING_DEBITSCHEDULE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTDIRECTDEBITKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTDIRECTDEBITKEY is not null) THEN
                    'ACCOUNTDIRECTDEBITKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTDIRECTDEBITKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEBITSCHEDULEKEY), '0'), 38, 10) is null and 
                    src:DEBITSCHEDULEKEY is not null) THEN
                    'DEBITSCHEDULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DEBITSCHEDULEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITRUN), '0'), 38, 10) is null and 
                    src:DIRECTDEBITRUN is not null) THEN
                    'DIRECTDEBITRUN contains a non-numeric value : \'' || AS_VARCHAR(src:DIRECTDEBITRUN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXTRACTDATE), '1900-01-01')) is null and 
                    src:EXTRACTDATE is not null) THEN
                    'EXTRACTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXTRACTDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTEXTRACTDATE), '1900-01-01')) is null and 
                    src:LASTEXTRACTDATE is not null) THEN
                    'LASTEXTRACTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:LASTEXTRACTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LATESTELIGIBLEBILL), '0'), 38, 10) is null and 
                    src:LATESTELIGIBLEBILL is not null) THEN
                    'LATESTELIGIBLEBILL contains a non-numeric value : \'' || AS_VARCHAR(src:LATESTELIGIBLEBILL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NEXTEXTRACTDATE), '1900-01-01')) is null and 
                    src:NEXTEXTRACTDATE is not null) THEN
                    'NEXTEXTRACTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:NEXTEXTRACTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYKEY), '0'), 38, 10) is null and 
                    src:PAYKEY is not null) THEN
                    'PAYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PAYKEY) || '\'' WHEN 
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
                                    
                src:DEBITSCHEDULEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_DEBITSCHEDULE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTDIRECTDEBITKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTDIRECTDEBITKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEBITSCHEDULEKEY), '0'), 38, 10) is null and 
                    src:DEBITSCHEDULEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITRUN), '0'), 38, 10) is null and 
                    src:DIRECTDEBITRUN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXTRACTDATE), '1900-01-01')) is null and 
                    src:EXTRACTDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTEXTRACTDATE), '1900-01-01')) is null and 
                    src:LASTEXTRACTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LATESTELIGIBLEBILL), '0'), 38, 10) is null and 
                    src:LATESTELIGIBLEBILL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NEXTEXTRACTDATE), '1900-01-01')) is null and 
                    src:NEXTEXTRACTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYKEY), '0'), 38, 10) is null and 
                    src:PAYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)