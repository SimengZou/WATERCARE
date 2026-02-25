CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_METERMANAGEMENT_WATER_READINGIMPORTMETERDETAIL_ERROR AS SELECT src, 'IPS_METERMANAGEMENT_WATER_READINGIMPORTMETERDETAIL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCEPTIONTYPE), '0'), 38, 10) is null and 
                    src:EXCEPTIONTYPE is not null) THEN
                    'EXCEPTIONTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:EXCEPTIONTYPE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION), '0'), 38, 10) is null and 
                    src:POSITION is not null) THEN
                    'POSITION contains a non-numeric value : \'' || AS_VARCHAR(src:POSITION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGIMPORTACTIVITYKEY), '0'), 38, 10) is null and 
                    src:READINGIMPORTACTIVITYKEY is not null) THEN
                    'READINGIMPORTACTIVITYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:READINGIMPORTACTIVITYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGIMPORTMETERDTLKEY), '0'), 38, 10) is null and 
                    src:READINGIMPORTMETERDTLKEY is not null) THEN
                    'READINGIMPORTMETERDTLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:READINGIMPORTMETERDTLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGIMPORTROUTEDTLKEY), '0'), 38, 10) is null and 
                    src:READINGIMPORTROUTEDTLKEY is not null) THEN
                    'READINGIMPORTROUTEDTLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:READINGIMPORTROUTEDTLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGKEY), '0'), 38, 10) is null and 
                    src:READINGKEY is not null) THEN
                    'READINGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:READINGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGREADING), '0'), 38, 10) is null and 
                    src:REGREADING is not null) THEN
                    'REGREADING contains a non-numeric value : \'' || AS_VARCHAR(src:REGREADING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGTYPE), '0'), 38, 10) is null and 
                    src:REGTYPE is not null) THEN
                    'REGTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:REGTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEQUENCENUMBER), '0'), 38, 10) is null and 
                    src:SEQUENCENUMBER is not null) THEN
                    'SEQUENCENUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:SEQUENCENUMBER) || '\'' WHEN 
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
                                    
                src:READINGIMPORTMETERDTLKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_METERMANAGEMENT_WATER_READINGIMPORTMETERDETAIL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCEPTIONTYPE), '0'), 38, 10) is null and 
                    src:EXCEPTIONTYPE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION), '0'), 38, 10) is null and 
                    src:POSITION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGIMPORTACTIVITYKEY), '0'), 38, 10) is null and 
                    src:READINGIMPORTACTIVITYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGIMPORTMETERDTLKEY), '0'), 38, 10) is null and 
                    src:READINGIMPORTMETERDTLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGIMPORTROUTEDTLKEY), '0'), 38, 10) is null and 
                    src:READINGIMPORTROUTEDTLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGKEY), '0'), 38, 10) is null and 
                    src:READINGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGREADING), '0'), 38, 10) is null and 
                    src:REGREADING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGTYPE), '0'), 38, 10) is null and 
                    src:REGTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEQUENCENUMBER), '0'), 38, 10) is null and 
                    src:SEQUENCENUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)