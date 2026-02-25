CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5SCHEDULEDJOBS_ERROR AS SELECT src, 'EAM_R5SCHEDULEDJOBS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCJ_LASTRUN), '1900-01-01')) is null and 
                    src:SCJ_LASTRUN is not null) THEN
                    'SCJ_LASTRUN contains a non-timestamp value : \'' || AS_VARCHAR(src:SCJ_LASTRUN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCJ_LASTSAVED), '1900-01-01')) is null and 
                    src:SCJ_LASTSAVED is not null) THEN
                    'SCJ_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:SCJ_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCJ_NEXTRUN), '1900-01-01')) is null and 
                    src:SCJ_NEXTRUN is not null) THEN
                    'SCJ_NEXTRUN contains a non-timestamp value : \'' || AS_VARCHAR(src:SCJ_NEXTRUN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCJ_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:SCJ_UPDATECOUNT is not null) THEN
                    'SCJ_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:SCJ_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCJ_LASTSAVED), '1900-01-01')) is null and 
                src:SCJ_LASTSAVED is not null) THEN
                'SCJ_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:SCJ_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:SCJ_CODE  ORDER BY 
            src:SCJ_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5SCHEDULEDJOBS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCJ_LASTRUN), '1900-01-01')) is null and 
                    src:SCJ_LASTRUN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCJ_LASTSAVED), '1900-01-01')) is null and 
                    src:SCJ_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCJ_NEXTRUN), '1900-01-01')) is null and 
                    src:SCJ_NEXTRUN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCJ_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:SCJ_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCJ_LASTSAVED), '1900-01-01')) is null and 
                src:SCJ_LASTSAVED is not null)