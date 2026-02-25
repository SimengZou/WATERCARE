CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_GRPCOMP_ERROR AS SELECT src, 'IPS_ASSETMANAGEMENT_GRPCOMP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GROUPITEMKEY), '0'), 38, 10) is null and 
                    src:GROUPITEMKEY is not null) THEN
                    'GROUPITEMKEY contains a non-numeric value : \'' || AS_VARCHAR(src:GROUPITEMKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GROUPKEY), '0'), 38, 10) is null and 
                    src:GROUPKEY is not null) THEN
                    'GROUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:GROUPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INDEXNO), '0'), 38, 10) is null and 
                    src:INDEXNO is not null) THEN
                    'INDEXNO contains a non-numeric value : \'' || AS_VARCHAR(src:INDEXNO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RWFEATUREKEY), '0'), 38, 10) is null and 
                    src:RWFEATUREKEY is not null) THEN
                    'RWFEATUREKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RWFEATUREKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RWFEATURETYPE), '0'), 38, 10) is null and 
                    src:RWFEATURETYPE is not null) THEN
                    'RWFEATURETYPE contains a non-numeric value : \'' || AS_VARCHAR(src:RWFEATURETYPE) || '\'' WHEN 
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
                                    
                src:GROUPITEMKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_GRPCOMP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTFROMFT), '0'), 38, 10) is null and 
                    src:DISTFROMFT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTFROMM), '0'), 38, 10) is null and 
                    src:DISTFROMM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTTOFT), '0'), 38, 10) is null and 
                    src:DISTTOFT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTTOM), '0'), 38, 10) is null and 
                    src:DISTTOM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GROUPITEMKEY), '0'), 38, 10) is null and 
                    src:GROUPITEMKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GROUPKEY), '0'), 38, 10) is null and 
                    src:GROUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INDEXNO), '0'), 38, 10) is null and 
                    src:INDEXNO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RWFEATUREKEY), '0'), 38, 10) is null and 
                    src:RWFEATUREKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RWFEATURETYPE), '0'), 38, 10) is null and 
                    src:RWFEATURETYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)