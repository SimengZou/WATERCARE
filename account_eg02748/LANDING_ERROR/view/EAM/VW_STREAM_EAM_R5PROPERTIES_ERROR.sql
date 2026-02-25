CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5PROPERTIES_ERROR AS SELECT src, 'EAM_R5PROPERTIES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRO_CREATED), '1900-01-01')) is null and 
                    src:PRO_CREATED is not null) THEN
                    'PRO_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:PRO_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRO_LASTSAVED), '1900-01-01')) is null and 
                    src:PRO_LASTSAVED is not null) THEN
                    'PRO_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PRO_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRO_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PRO_UPDATECOUNT is not null) THEN
                    'PRO_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PRO_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRO_UPDATED), '1900-01-01')) is null and 
                    src:PRO_UPDATED is not null) THEN
                    'PRO_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:PRO_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRO_LASTSAVED), '1900-01-01')) is null and 
                src:PRO_LASTSAVED is not null) THEN
                'PRO_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PRO_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:PRO_CODE  ORDER BY 
            src:PRO_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PROPERTIES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRO_CREATED), '1900-01-01')) is null and 
                    src:PRO_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRO_LASTSAVED), '1900-01-01')) is null and 
                    src:PRO_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRO_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PRO_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRO_UPDATED), '1900-01-01')) is null and 
                    src:PRO_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRO_LASTSAVED), '1900-01-01')) is null and 
                src:PRO_LASTSAVED is not null)