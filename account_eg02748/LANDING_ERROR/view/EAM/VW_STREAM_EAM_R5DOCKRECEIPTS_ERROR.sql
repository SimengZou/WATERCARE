CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5DOCKRECEIPTS_ERROR AS SELECT src, 'EAM_R5DOCKRECEIPTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DCK_CREATED), '1900-01-01')) is null and 
                    src:DCK_CREATED is not null) THEN
                    'DCK_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:DCK_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DCK_LASTSAVED), '1900-01-01')) is null and 
                    src:DCK_LASTSAVED is not null) THEN
                    'DCK_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DCK_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DCK_RECVDATE), '1900-01-01')) is null and 
                    src:DCK_RECVDATE is not null) THEN
                    'DCK_RECVDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DCK_RECVDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DCK_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DCK_UPDATECOUNT is not null) THEN
                    'DCK_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DCK_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DCK_UPDATED), '1900-01-01')) is null and 
                    src:DCK_UPDATED is not null) THEN
                    'DCK_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:DCK_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DCK_LASTSAVED), '1900-01-01')) is null and 
                src:DCK_LASTSAVED is not null) THEN
                'DCK_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DCK_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:DCK_CODE  ORDER BY 
            src:DCK_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DOCKRECEIPTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DCK_CREATED), '1900-01-01')) is null and 
                    src:DCK_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DCK_LASTSAVED), '1900-01-01')) is null and 
                    src:DCK_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DCK_RECVDATE), '1900-01-01')) is null and 
                    src:DCK_RECVDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DCK_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DCK_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DCK_UPDATED), '1900-01-01')) is null and 
                    src:DCK_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DCK_LASTSAVED), '1900-01-01')) is null and 
                src:DCK_LASTSAVED is not null)