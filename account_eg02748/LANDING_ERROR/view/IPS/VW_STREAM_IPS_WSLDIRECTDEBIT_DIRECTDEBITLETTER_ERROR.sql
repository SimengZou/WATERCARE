CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLDIRECTDEBIT_DIRECTDEBITLETTER_ERROR AS SELECT src, 'IPS_WSLDIRECTDEBIT_DIRECTDEBITLETTER' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTDIRECTDEBITKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTDIRECTDEBITKEY is not null) THEN
                    'ACCOUNTDIRECTDEBITKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTDIRECTDEBITKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DATEINITIATE), '1900-01-01')) is null and 
                    src:DATEINITIATE is not null) THEN
                    'DATEINITIATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DATEINITIATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DATEOFCORRESPONDENCE), '1900-01-01')) is null and 
                    src:DATEOFCORRESPONDENCE is not null) THEN
                    'DATEOFCORRESPONDENCE contains a non-timestamp value : \'' || AS_VARCHAR(src:DATEOFCORRESPONDENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITLETTERKEY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITLETTERKEY is not null) THEN
                    'DIRECTDEBITLETTERKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DIRECTDEBITLETTERKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SDLDATE), '1900-01-01')) is null and 
                    src:SDLDATE is not null) THEN
                    'SDLDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:SDLDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEREQUESTNUMBER), '0'), 38, 10) is null and 
                    src:SERVICEREQUESTNUMBER is not null) THEN
                    'SERVICEREQUESTNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:SERVICEREQUESTNUMBER) || '\'' WHEN 
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
                                    
                src:DIRECTDEBITLETTERKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLDIRECTDEBIT_DIRECTDEBITLETTER as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTDIRECTDEBITKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTDIRECTDEBITKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DATEINITIATE), '1900-01-01')) is null and 
                    src:DATEINITIATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DATEOFCORRESPONDENCE), '1900-01-01')) is null and 
                    src:DATEOFCORRESPONDENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITLETTERKEY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITLETTERKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SDLDATE), '1900-01-01')) is null and 
                    src:SDLDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEREQUESTNUMBER), '0'), 38, 10) is null and 
                    src:SERVICEREQUESTNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)