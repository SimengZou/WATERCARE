CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ELECSIGN_ERROR AS SELECT src, 'EAM_R5ELECSIGN' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ELS_DATE), '1900-01-01')) is null and 
                    src:ELS_DATE is not null) THEN
                    'ELS_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ELS_DATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ELS_EXTERNALDATE), '1900-01-01')) is null and 
                    src:ELS_EXTERNALDATE is not null) THEN
                    'ELS_EXTERNALDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ELS_EXTERNALDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ELS_LASTSAVED), '1900-01-01')) is null and 
                    src:ELS_LASTSAVED is not null) THEN
                    'ELS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ELS_LASTSAVED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ELS_LASTSAVED), '1900-01-01')) is null and 
                src:ELS_LASTSAVED is not null) THEN
                'ELS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ELS_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ELS_CODE  ORDER BY 
            src:ELS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ELECSIGN as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ELS_DATE), '1900-01-01')) is null and 
                    src:ELS_DATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ELS_EXTERNALDATE), '1900-01-01')) is null and 
                    src:ELS_EXTERNALDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ELS_LASTSAVED), '1900-01-01')) is null and 
                    src:ELS_LASTSAVED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ELS_LASTSAVED), '1900-01-01')) is null and 
                src:ELS_LASTSAVED is not null)