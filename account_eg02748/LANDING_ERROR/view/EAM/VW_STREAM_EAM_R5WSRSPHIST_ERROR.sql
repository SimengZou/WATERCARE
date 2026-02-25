CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5WSRSPHIST_ERROR AS SELECT src, 'EAM_R5WSRSPHIST' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSR_DATAAREA), '0'), 38, 10) is null and 
                    src:WSR_DATAAREA is not null) THEN
                    'WSR_DATAAREA contains a non-numeric value : \'' || AS_VARCHAR(src:WSR_DATAAREA) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WSR_LASTSAVED), '1900-01-01')) is null and 
                    src:WSR_LASTSAVED is not null) THEN
                    'WSR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:WSR_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WSR_TIME), '1900-01-01')) is null and 
                    src:WSR_TIME is not null) THEN
                    'WSR_TIME contains a non-timestamp value : \'' || AS_VARCHAR(src:WSR_TIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:WSR_UPDATECOUNT is not null) THEN
                    'WSR_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:WSR_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WSR_LASTSAVED), '1900-01-01')) is null and 
                src:WSR_LASTSAVED is not null) THEN
                'WSR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:WSR_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:WSR_MESSAGE,
                src:WSR_PROCESS  ORDER BY 
            src:WSR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5WSRSPHIST as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSR_DATAAREA), '0'), 38, 10) is null and 
                    src:WSR_DATAAREA is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WSR_LASTSAVED), '1900-01-01')) is null and 
                    src:WSR_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WSR_TIME), '1900-01-01')) is null and 
                    src:WSR_TIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:WSR_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WSR_LASTSAVED), '1900-01-01')) is null and 
                src:WSR_LASTSAVED is not null)