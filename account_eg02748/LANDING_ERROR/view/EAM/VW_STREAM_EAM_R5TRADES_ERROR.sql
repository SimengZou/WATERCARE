CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5TRADES_ERROR AS SELECT src, 'EAM_R5TRADES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_CREATED), '1900-01-01')) is null and 
                    src:TRD_CREATED is not null) THEN
                    'TRD_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:TRD_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_LASTSAVED), '1900-01-01')) is null and 
                    src:TRD_LASTSAVED is not null) THEN
                    'TRD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:TRD_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:TRD_LASTSTATUSUPDATE is not null) THEN
                    'TRD_LASTSTATUSUPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:TRD_LASTSTATUSUPDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_UDFDATE01), '1900-01-01')) is null and 
                    src:TRD_UDFDATE01 is not null) THEN
                    'TRD_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRD_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_UDFDATE02), '1900-01-01')) is null and 
                    src:TRD_UDFDATE02 is not null) THEN
                    'TRD_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRD_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_UDFDATE03), '1900-01-01')) is null and 
                    src:TRD_UDFDATE03 is not null) THEN
                    'TRD_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRD_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_UDFDATE04), '1900-01-01')) is null and 
                    src:TRD_UDFDATE04 is not null) THEN
                    'TRD_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRD_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_UDFDATE05), '1900-01-01')) is null and 
                    src:TRD_UDFDATE05 is not null) THEN
                    'TRD_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRD_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRD_UDFNUM01), '0'), 38, 10) is null and 
                    src:TRD_UDFNUM01 is not null) THEN
                    'TRD_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:TRD_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRD_UDFNUM02), '0'), 38, 10) is null and 
                    src:TRD_UDFNUM02 is not null) THEN
                    'TRD_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:TRD_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRD_UDFNUM03), '0'), 38, 10) is null and 
                    src:TRD_UDFNUM03 is not null) THEN
                    'TRD_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:TRD_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRD_UDFNUM04), '0'), 38, 10) is null and 
                    src:TRD_UDFNUM04 is not null) THEN
                    'TRD_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:TRD_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRD_UDFNUM05), '0'), 38, 10) is null and 
                    src:TRD_UDFNUM05 is not null) THEN
                    'TRD_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:TRD_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRD_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:TRD_UPDATECOUNT is not null) THEN
                    'TRD_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:TRD_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_UPDATED), '1900-01-01')) is null and 
                    src:TRD_UPDATED is not null) THEN
                    'TRD_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:TRD_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_LASTSAVED), '1900-01-01')) is null and 
                src:TRD_LASTSAVED is not null) THEN
                'TRD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:TRD_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:TRD_CODE  ORDER BY 
            src:TRD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5TRADES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_CREATED), '1900-01-01')) is null and 
                    src:TRD_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_LASTSAVED), '1900-01-01')) is null and 
                    src:TRD_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:TRD_LASTSTATUSUPDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_UDFDATE01), '1900-01-01')) is null and 
                    src:TRD_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_UDFDATE02), '1900-01-01')) is null and 
                    src:TRD_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_UDFDATE03), '1900-01-01')) is null and 
                    src:TRD_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_UDFDATE04), '1900-01-01')) is null and 
                    src:TRD_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_UDFDATE05), '1900-01-01')) is null and 
                    src:TRD_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRD_UDFNUM01), '0'), 38, 10) is null and 
                    src:TRD_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRD_UDFNUM02), '0'), 38, 10) is null and 
                    src:TRD_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRD_UDFNUM03), '0'), 38, 10) is null and 
                    src:TRD_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRD_UDFNUM04), '0'), 38, 10) is null and 
                    src:TRD_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRD_UDFNUM05), '0'), 38, 10) is null and 
                    src:TRD_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRD_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:TRD_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_UPDATED), '1900-01-01')) is null and 
                    src:TRD_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRD_LASTSAVED), '1900-01-01')) is null and 
                src:TRD_LASTSAVED is not null)