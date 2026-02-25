CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_RESOURCES_CREDITHISTORY_ERROR AS SELECT src, 'IPS_RESOURCES_CREDITHISTORY' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APPLIEDDATE), '1900-01-01')) is null and 
                    src:APPLIEDDATE is not null) THEN
                    'APPLIEDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:APPLIEDDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CREDITHISTORYKEY), '0'), 38, 10) is null and 
                    src:CREDITHISTORYKEY is not null) THEN
                    'CREDITHISTORYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CREDITHISTORYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IDENTITYKEY), '0'), 38, 10) is null and 
                    src:IDENTITYKEY is not null) THEN
                    'IDENTITYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:IDENTITYKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POINTS), '0'), 38, 10) is null and 
                    src:POINTS is not null) THEN
                    'POINTS contains a non-numeric value : \'' || AS_VARCHAR(src:POINTS) || '\'' WHEN 
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
                                    
                src:CREDITHISTORYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_CREDITHISTORY as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APPLIEDDATE), '1900-01-01')) is null and 
                    src:APPLIEDDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CREDITHISTORYKEY), '0'), 38, 10) is null and 
                    src:CREDITHISTORYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IDENTITYKEY), '0'), 38, 10) is null and 
                    src:IDENTITYKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POINTS), '0'), 38, 10) is null and 
                    src:POINTS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)