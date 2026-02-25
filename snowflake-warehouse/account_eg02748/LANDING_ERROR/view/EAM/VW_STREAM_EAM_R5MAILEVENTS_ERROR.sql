CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5MAILEVENTS_ERROR AS SELECT src, 'EAM_R5MAILEVENTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MAE_DATE), '1900-01-01')) is null and 
                    src:MAE_DATE is not null) THEN
                    'MAE_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:MAE_DATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAE_EMAILERRCOUNT), '0'), 38, 10) is null and 
                    src:MAE_EMAILERRCOUNT is not null) THEN
                    'MAE_EMAILERRCOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:MAE_EMAILERRCOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MAE_LASTSAVED), '1900-01-01')) is null and 
                    src:MAE_LASTSAVED is not null) THEN
                    'MAE_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:MAE_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAE_PTFERRCOUNT), '0'), 38, 10) is null and 
                    src:MAE_PTFERRCOUNT is not null) THEN
                    'MAE_PTFERRCOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:MAE_PTFERRCOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAE_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:MAE_UPDATECOUNT is not null) THEN
                    'MAE_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:MAE_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MAE_LASTSAVED), '1900-01-01')) is null and 
                src:MAE_LASTSAVED is not null) THEN
                'MAE_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:MAE_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:MAE_CODE  ORDER BY 
            src:MAE_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MAILEVENTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MAE_DATE), '1900-01-01')) is null and 
                    src:MAE_DATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAE_EMAILERRCOUNT), '0'), 38, 10) is null and 
                    src:MAE_EMAILERRCOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MAE_LASTSAVED), '1900-01-01')) is null and 
                    src:MAE_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAE_PTFERRCOUNT), '0'), 38, 10) is null and 
                    src:MAE_PTFERRCOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAE_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:MAE_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MAE_LASTSAVED), '1900-01-01')) is null and 
                src:MAE_LASTSAVED is not null)