CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5EVENTOBJECTS_ERROR AS SELECT src, 'EAM_R5EVENTOBJECTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EOB_FROMPOINT), '0'), 38, 10) is null and 
                    src:EOB_FROMPOINT is not null) THEN
                    'EOB_FROMPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:EOB_FROMPOINT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EOB_LASTSAVED), '1900-01-01')) is null and 
                    src:EOB_LASTSAVED is not null) THEN
                    'EOB_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:EOB_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EOB_LEVEL), '0'), 38, 10) is null and 
                    src:EOB_LEVEL is not null) THEN
                    'EOB_LEVEL contains a non-numeric value : \'' || AS_VARCHAR(src:EOB_LEVEL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EOB_TOPOINT), '0'), 38, 10) is null and 
                    src:EOB_TOPOINT is not null) THEN
                    'EOB_TOPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:EOB_TOPOINT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EOB_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:EOB_UPDATECOUNT is not null) THEN
                    'EOB_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:EOB_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EOB_WEIGHTPERCENTAGE), '0'), 38, 10) is null and 
                    src:EOB_WEIGHTPERCENTAGE is not null) THEN
                    'EOB_WEIGHTPERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:EOB_WEIGHTPERCENTAGE) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EOB_LASTSAVED), '1900-01-01')) is null and 
                src:EOB_LASTSAVED is not null) THEN
                'EOB_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:EOB_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:EOB_EVENT,
                src:EOB_OBJECT,
                src:EOB_OBJECT_ORG  ORDER BY 
            src:EOB_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5EVENTOBJECTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EOB_FROMPOINT), '0'), 38, 10) is null and 
                    src:EOB_FROMPOINT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EOB_LASTSAVED), '1900-01-01')) is null and 
                    src:EOB_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EOB_LEVEL), '0'), 38, 10) is null and 
                    src:EOB_LEVEL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EOB_TOPOINT), '0'), 38, 10) is null and 
                    src:EOB_TOPOINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EOB_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:EOB_UPDATECOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EOB_WEIGHTPERCENTAGE), '0'), 38, 10) is null and 
                    src:EOB_WEIGHTPERCENTAGE is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EOB_LASTSAVED), '1900-01-01')) is null and 
                src:EOB_LASTSAVED is not null)