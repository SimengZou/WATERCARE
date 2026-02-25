CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLASSETWATER_XSRRETAILAUDITMETERGD_ERROR AS SELECT src, 'IPS_WSLASSETWATER_XSRRETAILAUDITMETERGD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) THEN
                    'ADDRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INSTALLEDDATE), '1900-01-01')) is null and 
                    src:INSTALLEDDATE is not null) THEN
                    'INSTALLEDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:INSTALLEDDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFDIALS), '0'), 38, 10) is null and 
                    src:NUMBEROFDIALS is not null) THEN
                    'NUMBEROFDIALS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFDIALS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:REMOVEDDATE), '1900-01-01')) is null and 
                    src:REMOVEDDATE is not null) THEN
                    'REMOVEDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:REMOVEDDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVNO), '0'), 38, 10) is null and 
                    src:SERVNO is not null) THEN
                    'SERVNO contains a non-numeric value : \'' || AS_VARCHAR(src:SERVNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XSRRETAILAUDITMETERDPKEY), '0'), 38, 10) is null and 
                    src:XSRRETAILAUDITMETERDPKEY is not null) THEN
                    'XSRRETAILAUDITMETERDPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XSRRETAILAUDITMETERDPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XSRRETAILAUDITMETERGDKEY), '0'), 38, 10) is null and 
                    src:XSRRETAILAUDITMETERGDKEY is not null) THEN
                    'XSRRETAILAUDITMETERGDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XSRRETAILAUDITMETERGDKEY) || '\'' WHEN 
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
                                    
                src:XSRRETAILAUDITMETERGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLASSETWATER_XSRRETAILAUDITMETERGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INSTALLEDDATE), '1900-01-01')) is null and 
                    src:INSTALLEDDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFDIALS), '0'), 38, 10) is null and 
                    src:NUMBEROFDIALS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:REMOVEDDATE), '1900-01-01')) is null and 
                    src:REMOVEDDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVNO), '0'), 38, 10) is null and 
                    src:SERVNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XSRRETAILAUDITMETERDPKEY), '0'), 38, 10) is null and 
                    src:XSRRETAILAUDITMETERDPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XSRRETAILAUDITMETERGDKEY), '0'), 38, 10) is null and 
                    src:XSRRETAILAUDITMETERGDKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)