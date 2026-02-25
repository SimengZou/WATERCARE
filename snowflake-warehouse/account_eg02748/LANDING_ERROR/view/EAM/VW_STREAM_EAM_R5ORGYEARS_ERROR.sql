CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ORGYEARS_ERROR AS SELECT src, 'EAM_R5ORGYEARS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OYE_END), '1900-01-01')) is null and 
                    src:OYE_END is not null) THEN
                    'OYE_END contains a non-timestamp value : \'' || AS_VARCHAR(src:OYE_END) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OYE_LASTSAVED), '1900-01-01')) is null and 
                    src:OYE_LASTSAVED is not null) THEN
                    'OYE_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:OYE_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OYE_PK), '0'), 38, 10) is null and 
                    src:OYE_PK is not null) THEN
                    'OYE_PK contains a non-numeric value : \'' || AS_VARCHAR(src:OYE_PK) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OYE_START), '1900-01-01')) is null and 
                    src:OYE_START is not null) THEN
                    'OYE_START contains a non-timestamp value : \'' || AS_VARCHAR(src:OYE_START) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OYE_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:OYE_UPDATECOUNT is not null) THEN
                    'OYE_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:OYE_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OYE_LASTSAVED), '1900-01-01')) is null and 
                src:OYE_LASTSAVED is not null) THEN
                'OYE_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:OYE_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:OYE_PK  ORDER BY 
            src:OYE_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ORGYEARS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OYE_END), '1900-01-01')) is null and 
                    src:OYE_END is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OYE_LASTSAVED), '1900-01-01')) is null and 
                    src:OYE_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OYE_PK), '0'), 38, 10) is null and 
                    src:OYE_PK is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OYE_START), '1900-01-01')) is null and 
                    src:OYE_START is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OYE_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:OYE_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OYE_LASTSAVED), '1900-01-01')) is null and 
                src:OYE_LASTSAVED is not null)