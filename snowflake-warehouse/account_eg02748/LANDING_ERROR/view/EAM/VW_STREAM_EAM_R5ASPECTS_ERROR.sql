CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ASPECTS_ERROR AS SELECT src, 'EAM_R5ASPECTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_CREATED), '1900-01-01')) is null and 
                    src:ASP_CREATED is not null) THEN
                    'ASP_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:ASP_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_LASTSAVED), '1900-01-01')) is null and 
                    src:ASP_LASTSAVED is not null) THEN
                    'ASP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ASP_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_UDFDATE01), '1900-01-01')) is null and 
                    src:ASP_UDFDATE01 is not null) THEN
                    'ASP_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:ASP_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_UDFDATE02), '1900-01-01')) is null and 
                    src:ASP_UDFDATE02 is not null) THEN
                    'ASP_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:ASP_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_UDFDATE03), '1900-01-01')) is null and 
                    src:ASP_UDFDATE03 is not null) THEN
                    'ASP_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:ASP_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_UDFDATE04), '1900-01-01')) is null and 
                    src:ASP_UDFDATE04 is not null) THEN
                    'ASP_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:ASP_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_UDFDATE05), '1900-01-01')) is null and 
                    src:ASP_UDFDATE05 is not null) THEN
                    'ASP_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:ASP_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASP_UDFNUM01), '0'), 38, 10) is null and 
                    src:ASP_UDFNUM01 is not null) THEN
                    'ASP_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:ASP_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASP_UDFNUM02), '0'), 38, 10) is null and 
                    src:ASP_UDFNUM02 is not null) THEN
                    'ASP_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:ASP_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASP_UDFNUM03), '0'), 38, 10) is null and 
                    src:ASP_UDFNUM03 is not null) THEN
                    'ASP_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:ASP_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASP_UDFNUM04), '0'), 38, 10) is null and 
                    src:ASP_UDFNUM04 is not null) THEN
                    'ASP_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:ASP_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASP_UDFNUM05), '0'), 38, 10) is null and 
                    src:ASP_UDFNUM05 is not null) THEN
                    'ASP_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:ASP_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ASP_UPDATECOUNT is not null) THEN
                    'ASP_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ASP_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_UPDATED), '1900-01-01')) is null and 
                    src:ASP_UPDATED is not null) THEN
                    'ASP_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:ASP_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_LASTSAVED), '1900-01-01')) is null and 
                src:ASP_LASTSAVED is not null) THEN
                'ASP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ASP_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ASP_CODE  ORDER BY 
            src:ASP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ASPECTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_CREATED), '1900-01-01')) is null and 
                    src:ASP_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_LASTSAVED), '1900-01-01')) is null and 
                    src:ASP_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_UDFDATE01), '1900-01-01')) is null and 
                    src:ASP_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_UDFDATE02), '1900-01-01')) is null and 
                    src:ASP_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_UDFDATE03), '1900-01-01')) is null and 
                    src:ASP_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_UDFDATE04), '1900-01-01')) is null and 
                    src:ASP_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_UDFDATE05), '1900-01-01')) is null and 
                    src:ASP_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASP_UDFNUM01), '0'), 38, 10) is null and 
                    src:ASP_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASP_UDFNUM02), '0'), 38, 10) is null and 
                    src:ASP_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASP_UDFNUM03), '0'), 38, 10) is null and 
                    src:ASP_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASP_UDFNUM04), '0'), 38, 10) is null and 
                    src:ASP_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASP_UDFNUM05), '0'), 38, 10) is null and 
                    src:ASP_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ASP_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_UPDATED), '1900-01-01')) is null and 
                    src:ASP_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASP_LASTSAVED), '1900-01-01')) is null and 
                src:ASP_LASTSAVED is not null)