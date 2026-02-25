CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5WORKPACKAGES_ERROR AS SELECT src, 'EAM_R5WORKPACKAGES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WPK_DUEDATE), '1900-01-01')) is null and 
                    src:WPK_DUEDATE is not null) THEN
                    'WPK_DUEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:WPK_DUEDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WPK_DURATION), '0'), 38, 10) is null and 
                    src:WPK_DURATION is not null) THEN
                    'WPK_DURATION contains a non-numeric value : \'' || AS_VARCHAR(src:WPK_DURATION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WPK_ESTWORKLOAD), '0'), 38, 10) is null and 
                    src:WPK_ESTWORKLOAD is not null) THEN
                    'WPK_ESTWORKLOAD contains a non-numeric value : \'' || AS_VARCHAR(src:WPK_ESTWORKLOAD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WPK_FREQ), '0'), 38, 10) is null and 
                    src:WPK_FREQ is not null) THEN
                    'WPK_FREQ contains a non-numeric value : \'' || AS_VARCHAR(src:WPK_FREQ) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WPK_LASTSAVED), '1900-01-01')) is null and 
                    src:WPK_LASTSAVED is not null) THEN
                    'WPK_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:WPK_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WPK_PERFORMONDAY), '0'), 38, 10) is null and 
                    src:WPK_PERFORMONDAY is not null) THEN
                    'WPK_PERFORMONDAY contains a non-numeric value : \'' || AS_VARCHAR(src:WPK_PERFORMONDAY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WPK_PERSONS), '0'), 38, 10) is null and 
                    src:WPK_PERSONS is not null) THEN
                    'WPK_PERSONS contains a non-numeric value : \'' || AS_VARCHAR(src:WPK_PERSONS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WPK_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:WPK_UPDATECOUNT is not null) THEN
                    'WPK_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:WPK_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WPK_LASTSAVED), '1900-01-01')) is null and 
                src:WPK_LASTSAVED is not null) THEN
                'WPK_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:WPK_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:WPK_CODE  ORDER BY 
            src:WPK_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5WORKPACKAGES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WPK_DUEDATE), '1900-01-01')) is null and 
                    src:WPK_DUEDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WPK_DURATION), '0'), 38, 10) is null and 
                    src:WPK_DURATION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WPK_ESTWORKLOAD), '0'), 38, 10) is null and 
                    src:WPK_ESTWORKLOAD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WPK_FREQ), '0'), 38, 10) is null and 
                    src:WPK_FREQ is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WPK_LASTSAVED), '1900-01-01')) is null and 
                    src:WPK_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WPK_PERFORMONDAY), '0'), 38, 10) is null and 
                    src:WPK_PERFORMONDAY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WPK_PERSONS), '0'), 38, 10) is null and 
                    src:WPK_PERSONS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WPK_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:WPK_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WPK_LASTSAVED), '1900-01-01')) is null and 
                src:WPK_LASTSAVED is not null)