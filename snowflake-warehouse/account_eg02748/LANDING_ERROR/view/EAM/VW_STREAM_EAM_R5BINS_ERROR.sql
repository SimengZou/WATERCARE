CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5BINS_ERROR AS SELECT src, 'EAM_R5BINS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BIN_CREATED), '1900-01-01')) is null and 
                    src:BIN_CREATED is not null) THEN
                    'BIN_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:BIN_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BIN_LASTSAVED), '1900-01-01')) is null and 
                    src:BIN_LASTSAVED is not null) THEN
                    'BIN_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:BIN_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BIN_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:BIN_UPDATECOUNT is not null) THEN
                    'BIN_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:BIN_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BIN_UPDATED), '1900-01-01')) is null and 
                    src:BIN_UPDATED is not null) THEN
                    'BIN_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:BIN_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BIN_LASTSAVED), '1900-01-01')) is null and 
                src:BIN_LASTSAVED is not null) THEN
                'BIN_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:BIN_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:BIN_CODE,
                src:BIN_STORE  ORDER BY 
            src:BIN_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5BINS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BIN_CREATED), '1900-01-01')) is null and 
                    src:BIN_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BIN_LASTSAVED), '1900-01-01')) is null and 
                    src:BIN_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BIN_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:BIN_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BIN_UPDATED), '1900-01-01')) is null and 
                    src:BIN_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BIN_LASTSAVED), '1900-01-01')) is null and 
                src:BIN_LASTSAVED is not null)