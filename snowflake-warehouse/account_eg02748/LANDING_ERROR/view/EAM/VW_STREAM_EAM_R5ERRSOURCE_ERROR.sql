CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ERRSOURCE_ERROR AS SELECT src, 'EAM_R5ERRSOURCE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERS_LASTSAVED), '1900-01-01')) is null and 
                    src:ERS_LASTSAVED is not null) THEN
                    'ERS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ERS_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERS_NUMBER), '0'), 38, 10) is null and 
                    src:ERS_NUMBER is not null) THEN
                    'ERS_NUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:ERS_NUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ERS_UPDATECOUNT is not null) THEN
                    'ERS_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ERS_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERS_LASTSAVED), '1900-01-01')) is null and 
                src:ERS_LASTSAVED is not null) THEN
                'ERS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ERS_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ERS_NUMBER,
                src:ERS_SOURCE,
                src:ERS_TYPE  ORDER BY 
            src:ERS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ERRSOURCE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERS_LASTSAVED), '1900-01-01')) is null and 
                    src:ERS_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERS_NUMBER), '0'), 38, 10) is null and 
                    src:ERS_NUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ERS_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERS_LASTSAVED), '1900-01-01')) is null and 
                src:ERS_LASTSAVED is not null)