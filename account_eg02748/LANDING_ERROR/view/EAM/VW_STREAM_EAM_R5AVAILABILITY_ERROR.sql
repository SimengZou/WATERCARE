CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5AVAILABILITY_ERROR AS SELECT src, 'EAM_R5AVAILABILITY' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AVL_DATE), '1900-01-01')) is null and 
                    src:AVL_DATE is not null) THEN
                    'AVL_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:AVL_DATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVL_HIRHOURS), '0'), 38, 10) is null and 
                    src:AVL_HIRHOURS is not null) THEN
                    'AVL_HIRHOURS contains a non-numeric value : \'' || AS_VARCHAR(src:AVL_HIRHOURS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AVL_LASTSAVED), '1900-01-01')) is null and 
                    src:AVL_LASTSAVED is not null) THEN
                    'AVL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:AVL_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVL_NETHIRED), '0'), 38, 10) is null and 
                    src:AVL_NETHIRED is not null) THEN
                    'AVL_NETHIRED contains a non-numeric value : \'' || AS_VARCHAR(src:AVL_NETHIRED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVL_NETHOURS), '0'), 38, 10) is null and 
                    src:AVL_NETHOURS is not null) THEN
                    'AVL_NETHOURS contains a non-numeric value : \'' || AS_VARCHAR(src:AVL_NETHOURS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVL_OWNHOURS), '0'), 38, 10) is null and 
                    src:AVL_OWNHOURS is not null) THEN
                    'AVL_OWNHOURS contains a non-numeric value : \'' || AS_VARCHAR(src:AVL_OWNHOURS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:AVL_UPDATECOUNT is not null) THEN
                    'AVL_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:AVL_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AVL_LASTSAVED), '1900-01-01')) is null and 
                src:AVL_LASTSAVED is not null) THEN
                'AVL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:AVL_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:AVL_DATE,
                src:AVL_MRC,
                src:AVL_TRADE  ORDER BY 
            src:AVL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5AVAILABILITY as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AVL_DATE), '1900-01-01')) is null and 
                    src:AVL_DATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVL_HIRHOURS), '0'), 38, 10) is null and 
                    src:AVL_HIRHOURS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AVL_LASTSAVED), '1900-01-01')) is null and 
                    src:AVL_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVL_NETHIRED), '0'), 38, 10) is null and 
                    src:AVL_NETHIRED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVL_NETHOURS), '0'), 38, 10) is null and 
                    src:AVL_NETHOURS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVL_OWNHOURS), '0'), 38, 10) is null and 
                    src:AVL_OWNHOURS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:AVL_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AVL_LASTSAVED), '1900-01-01')) is null and 
                src:AVL_LASTSAVED is not null)