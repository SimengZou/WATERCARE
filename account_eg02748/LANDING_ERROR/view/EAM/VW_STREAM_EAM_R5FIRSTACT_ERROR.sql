CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5FIRSTACT_ERROR AS SELECT src, 'EAM_R5FIRSTACT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_ACT), '0'), 38, 10) is null and 
                    src:ACT_ACT is not null) THEN
                    'ACT_ACT contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_ACT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_DURATION), '0'), 38, 10) is null and 
                    src:ACT_DURATION is not null) THEN
                    'ACT_DURATION contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_DURATION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_EST), '0'), 38, 10) is null and 
                    src:ACT_EST is not null) THEN
                    'ACT_EST contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_EST) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_LASTSAVED), '1900-01-01')) is null and 
                    src:ACT_LASTSAVED is not null) THEN
                    'ACT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ACT_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_PERSONS), '0'), 38, 10) is null and 
                    src:ACT_PERSONS is not null) THEN
                    'ACT_PERSONS contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_PERSONS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_REM), '0'), 38, 10) is null and 
                    src:ACT_REM is not null) THEN
                    'ACT_REM contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_REM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_START), '1900-01-01')) is null and 
                    src:ACT_START is not null) THEN
                    'ACT_START contains a non-timestamp value : \'' || AS_VARCHAR(src:ACT_START) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_LASTSAVED), '1900-01-01')) is null and 
                src:ACT_LASTSAVED is not null) THEN
                'ACT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ACT_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ACT_EVENT  ORDER BY 
            src:ACT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5FIRSTACT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_ACT), '0'), 38, 10) is null and 
                    src:ACT_ACT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_DURATION), '0'), 38, 10) is null and 
                    src:ACT_DURATION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_EST), '0'), 38, 10) is null and 
                    src:ACT_EST is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_LASTSAVED), '1900-01-01')) is null and 
                    src:ACT_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_PERSONS), '0'), 38, 10) is null and 
                    src:ACT_PERSONS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_REM), '0'), 38, 10) is null and 
                    src:ACT_REM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_START), '1900-01-01')) is null and 
                    src:ACT_START is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_LASTSAVED), '1900-01-01')) is null and 
                src:ACT_LASTSAVED is not null)