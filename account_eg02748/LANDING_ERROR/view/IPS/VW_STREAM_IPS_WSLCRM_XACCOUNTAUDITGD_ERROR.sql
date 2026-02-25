CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLCRM_XACCOUNTAUDITGD_ERROR AS SELECT src, 'IPS_WSLCRM_XACCOUNTAUDITGD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:"ACCOUNT"), '0'), 38, 10) is null and 
                    src:"ACCOUNT" is not null) THEN
                    '"ACCOUNT" contains a non-numeric value : \'' || AS_VARCHAR(src:"ACCOUNT") || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CHANGEDDATE), '1900-01-01')) is null and 
                    src:CHANGEDDATE is not null) THEN
                    'CHANGEDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:CHANGEDDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTITYKEY), '0'), 38, 10) is null and 
                    src:ENTITYKEY is not null) THEN
                    'ENTITYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ENTITYKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XACCOUNTAUDITDP), '0'), 38, 10) is null and 
                    src:XACCOUNTAUDITDP is not null) THEN
                    'XACCOUNTAUDITDP contains a non-numeric value : \'' || AS_VARCHAR(src:XACCOUNTAUDITDP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XACCOUNTAUDITGDKEY), '0'), 38, 10) is null and 
                    src:XACCOUNTAUDITGDKEY is not null) THEN
                    'XACCOUNTAUDITGDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XACCOUNTAUDITGDKEY) || '\'' WHEN 
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
                                    
                src:XACCOUNTAUDITGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCRM_XACCOUNTAUDITGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:"ACCOUNT"), '0'), 38, 10) is null and 
                    src:"ACCOUNT" is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CHANGEDDATE), '1900-01-01')) is null and 
                    src:CHANGEDDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTITYKEY), '0'), 38, 10) is null and 
                    src:ENTITYKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XACCOUNTAUDITDP), '0'), 38, 10) is null and 
                    src:XACCOUNTAUDITDP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XACCOUNTAUDITGDKEY), '0'), 38, 10) is null and 
                    src:XACCOUNTAUDITGDKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)