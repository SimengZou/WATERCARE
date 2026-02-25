CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_COMPADDR_ERROR AS SELECT src, 'IPS_ASSETMANAGEMENT_COMPADDR' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) THEN
                    'ADDRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPADDKEY), '0'), 38, 10) is null and 
                    src:COMPADDKEY is not null) THEN
                    'COMPADDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPADDKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPTYPE), '0'), 38, 10) is null and 
                    src:COMPTYPE is not null) THEN
                    'COMPTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:COMPTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTFROMFT), '0'), 38, 10) is null and 
                    src:DISTFROMFT is not null) THEN
                    'DISTFROMFT contains a non-numeric value : \'' || AS_VARCHAR(src:DISTFROMFT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTFROMM), '0'), 38, 10) is null and 
                    src:DISTFROMM is not null) THEN
                    'DISTFROMM contains a non-numeric value : \'' || AS_VARCHAR(src:DISTFROMM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTTOFT), '0'), 38, 10) is null and 
                    src:DISTTOFT is not null) THEN
                    'DISTTOFT contains a non-numeric value : \'' || AS_VARCHAR(src:DISTTOFT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTTOM), '0'), 38, 10) is null and 
                    src:DISTTOM is not null) THEN
                    'DISTTOM contains a non-numeric value : \'' || AS_VARCHAR(src:DISTTOM) || '\'' WHEN 
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
                                    
                src:COMPADDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_COMPADDR as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPADDKEY), '0'), 38, 10) is null and 
                    src:COMPADDKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPTYPE), '0'), 38, 10) is null and 
                    src:COMPTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTFROMFT), '0'), 38, 10) is null and 
                    src:DISTFROMFT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTFROMM), '0'), 38, 10) is null and 
                    src:DISTFROMM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTTOFT), '0'), 38, 10) is null and 
                    src:DISTTOFT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTTOM), '0'), 38, 10) is null and 
                    src:DISTTOM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)