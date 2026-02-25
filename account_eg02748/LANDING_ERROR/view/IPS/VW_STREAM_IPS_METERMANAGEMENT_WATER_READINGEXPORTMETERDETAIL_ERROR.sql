CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_METERMANAGEMENT_WATER_READINGEXPORTMETERDETAIL_ERROR AS SELECT src, 'IPS_METERMANAGEMENT_WATER_READINGEXPORTMETERDETAIL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXCEPTIONEXPORTDATE), '1900-01-01')) is null and 
                    src:EXCEPTIONEXPORTDATE is not null) THEN
                    'EXCEPTIONEXPORTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXCEPTIONEXPORTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPORTTYPEEXCEPTION), '0'), 38, 10) is null and 
                    src:EXPORTTYPEEXCEPTION is not null) THEN
                    'EXPORTTYPEEXCEPTION contains a non-numeric value : \'' || AS_VARCHAR(src:EXPORTTYPEEXCEPTION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION), '0'), 38, 10) is null and 
                    src:POSITION is not null) THEN
                    'POSITION contains a non-numeric value : \'' || AS_VARCHAR(src:POSITION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGEXPORTMETERDTLKEY), '0'), 38, 10) is null and 
                    src:READINGEXPORTMETERDTLKEY is not null) THEN
                    'READINGEXPORTMETERDTLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:READINGEXPORTMETERDTLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGEXPORTROUTEDTLKEY), '0'), 38, 10) is null and 
                    src:READINGEXPORTROUTEDTLKEY is not null) THEN
                    'READINGEXPORTROUTEDTLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:READINGEXPORTROUTEDTLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROUTEKEY), '0'), 38, 10) is null and 
                    src:ROUTEKEY is not null) THEN
                    'ROUTEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ROUTEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEQUENCENUMBER), '0'), 38, 10) is null and 
                    src:SEQUENCENUMBER is not null) THEN
                    'SEQUENCENUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:SEQUENCENUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STATUS), '0'), 38, 10) is null and 
                    src:STATUS is not null) THEN
                    'STATUS contains a non-numeric value : \'' || AS_VARCHAR(src:STATUS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USEREXCEPTIONKEY), '0'), 38, 10) is null and 
                    src:USEREXCEPTIONKEY is not null) THEN
                    'USEREXCEPTIONKEY contains a non-numeric value : \'' || AS_VARCHAR(src:USEREXCEPTIONKEY) || '\'' WHEN 
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
                                    
                src:READINGEXPORTMETERDTLKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_METERMANAGEMENT_WATER_READINGEXPORTMETERDETAIL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXCEPTIONEXPORTDATE), '1900-01-01')) is null and 
                    src:EXCEPTIONEXPORTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPORTTYPEEXCEPTION), '0'), 38, 10) is null and 
                    src:EXPORTTYPEEXCEPTION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION), '0'), 38, 10) is null and 
                    src:POSITION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGEXPORTMETERDTLKEY), '0'), 38, 10) is null and 
                    src:READINGEXPORTMETERDTLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGEXPORTROUTEDTLKEY), '0'), 38, 10) is null and 
                    src:READINGEXPORTROUTEDTLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROUTEKEY), '0'), 38, 10) is null and 
                    src:ROUTEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEQUENCENUMBER), '0'), 38, 10) is null and 
                    src:SEQUENCENUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STATUS), '0'), 38, 10) is null and 
                    src:STATUS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USEREXCEPTIONKEY), '0'), 38, 10) is null and 
                    src:USEREXCEPTIONKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)