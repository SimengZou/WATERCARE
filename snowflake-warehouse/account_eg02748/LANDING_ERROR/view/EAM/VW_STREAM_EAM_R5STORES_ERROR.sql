CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5STORES_ERROR AS SELECT src, 'EAM_R5STORES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_CREATED), '1900-01-01')) is null and 
                    src:STR_CREATED is not null) THEN
                    'STR_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:STR_CREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_INTERNALLEADTIME), '0'), 38, 10) is null and 
                    src:STR_INTERNALLEADTIME is not null) THEN
                    'STR_INTERNALLEADTIME contains a non-numeric value : \'' || AS_VARCHAR(src:STR_INTERNALLEADTIME) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_LASTSAVED), '1900-01-01')) is null and 
                    src:STR_LASTSAVED is not null) THEN
                    'STR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:STR_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:STR_LASTSTATUSUPDATE is not null) THEN
                    'STR_LASTSTATUSUPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:STR_LASTSTATUSUPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_LEADTIME), '0'), 38, 10) is null and 
                    src:STR_LEADTIME is not null) THEN
                    'STR_LEADTIME contains a non-numeric value : \'' || AS_VARCHAR(src:STR_LEADTIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_RESERVEDPARTBUFFER), '0'), 38, 10) is null and 
                    src:STR_RESERVEDPARTBUFFER is not null) THEN
                    'STR_RESERVEDPARTBUFFER contains a non-numeric value : \'' || AS_VARCHAR(src:STR_RESERVEDPARTBUFFER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_UDFDATE01), '1900-01-01')) is null and 
                    src:STR_UDFDATE01 is not null) THEN
                    'STR_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:STR_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_UDFDATE02), '1900-01-01')) is null and 
                    src:STR_UDFDATE02 is not null) THEN
                    'STR_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:STR_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_UDFDATE03), '1900-01-01')) is null and 
                    src:STR_UDFDATE03 is not null) THEN
                    'STR_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:STR_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_UDFDATE04), '1900-01-01')) is null and 
                    src:STR_UDFDATE04 is not null) THEN
                    'STR_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:STR_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_UDFDATE05), '1900-01-01')) is null and 
                    src:STR_UDFDATE05 is not null) THEN
                    'STR_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:STR_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_UDFNUM01), '0'), 38, 10) is null and 
                    src:STR_UDFNUM01 is not null) THEN
                    'STR_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:STR_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_UDFNUM02), '0'), 38, 10) is null and 
                    src:STR_UDFNUM02 is not null) THEN
                    'STR_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:STR_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_UDFNUM03), '0'), 38, 10) is null and 
                    src:STR_UDFNUM03 is not null) THEN
                    'STR_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:STR_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_UDFNUM04), '0'), 38, 10) is null and 
                    src:STR_UDFNUM04 is not null) THEN
                    'STR_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:STR_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_UDFNUM05), '0'), 38, 10) is null and 
                    src:STR_UDFNUM05 is not null) THEN
                    'STR_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:STR_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:STR_UPDATECOUNT is not null) THEN
                    'STR_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:STR_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_UPDATED), '1900-01-01')) is null and 
                    src:STR_UPDATED is not null) THEN
                    'STR_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:STR_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_LASTSAVED), '1900-01-01')) is null and 
                src:STR_LASTSAVED is not null) THEN
                'STR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:STR_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:STR_CODE  ORDER BY 
            src:STR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5STORES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_CREATED), '1900-01-01')) is null and 
                    src:STR_CREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_INTERNALLEADTIME), '0'), 38, 10) is null and 
                    src:STR_INTERNALLEADTIME is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_LASTSAVED), '1900-01-01')) is null and 
                    src:STR_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:STR_LASTSTATUSUPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_LEADTIME), '0'), 38, 10) is null and 
                    src:STR_LEADTIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_RESERVEDPARTBUFFER), '0'), 38, 10) is null and 
                    src:STR_RESERVEDPARTBUFFER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_UDFDATE01), '1900-01-01')) is null and 
                    src:STR_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_UDFDATE02), '1900-01-01')) is null and 
                    src:STR_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_UDFDATE03), '1900-01-01')) is null and 
                    src:STR_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_UDFDATE04), '1900-01-01')) is null and 
                    src:STR_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_UDFDATE05), '1900-01-01')) is null and 
                    src:STR_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_UDFNUM01), '0'), 38, 10) is null and 
                    src:STR_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_UDFNUM02), '0'), 38, 10) is null and 
                    src:STR_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_UDFNUM03), '0'), 38, 10) is null and 
                    src:STR_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_UDFNUM04), '0'), 38, 10) is null and 
                    src:STR_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_UDFNUM05), '0'), 38, 10) is null and 
                    src:STR_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:STR_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_UPDATED), '1900-01-01')) is null and 
                    src:STR_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STR_LASTSAVED), '1900-01-01')) is null and 
                src:STR_LASTSAVED is not null)