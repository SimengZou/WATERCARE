CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLCRM_XRETAILAUDITGD_ERROR AS SELECT src, 'IPS_WSLCRM_XRETAILAUDITGD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSETKEY), '0'), 38, 10) is null and 
                    src:ASSETKEY is not null) THEN
                    'ASSETKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSETKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODIFIEDDATE), '1900-01-01')) is null and 
                    src:MODIFIEDDATE is not null) THEN
                    'MODIFIEDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:MODIFIEDDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READINGDATE), '1900-01-01')) is null and 
                    src:READINGDATE is not null) THEN
                    'READINGDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:READINGDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGKEY), '0'), 38, 10) is null and 
                    src:READINGKEY is not null) THEN
                    'READINGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:READINGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XRETAILAUDITDPKEY), '0'), 38, 10) is null and 
                    src:XRETAILAUDITDPKEY is not null) THEN
                    'XRETAILAUDITDPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XRETAILAUDITDPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XRETAILAUDITGDKEY), '0'), 38, 10) is null and 
                    src:XRETAILAUDITGDKEY is not null) THEN
                    'XRETAILAUDITGDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XRETAILAUDITGDKEY) || '\'' WHEN 
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
                                    
                src:XRETAILAUDITGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCRM_XRETAILAUDITGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSETKEY), '0'), 38, 10) is null and 
                    src:ASSETKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODIFIEDDATE), '1900-01-01')) is null and 
                    src:MODIFIEDDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READINGDATE), '1900-01-01')) is null and 
                    src:READINGDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGKEY), '0'), 38, 10) is null and 
                    src:READINGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XRETAILAUDITDPKEY), '0'), 38, 10) is null and 
                    src:XRETAILAUDITDPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XRETAILAUDITGDKEY), '0'), 38, 10) is null and 
                    src:XRETAILAUDITGDKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)