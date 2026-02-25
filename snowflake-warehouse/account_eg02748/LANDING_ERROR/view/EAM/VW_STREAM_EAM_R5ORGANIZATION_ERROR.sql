CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ORGANIZATION_ERROR AS SELECT src, 'EAM_R5ORGANIZATION' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_CREATED), '1900-01-01')) is null and 
                    src:ORG_CREATED is not null) THEN
                    'ORG_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORG_CREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_INVQTYTOL), '0'), 38, 10) is null and 
                    src:ORG_INVQTYTOL is not null) THEN
                    'ORG_INVQTYTOL contains a non-numeric value : \'' || AS_VARCHAR(src:ORG_INVQTYTOL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_LASTSAVED), '1900-01-01')) is null and 
                    src:ORG_LASTSAVED is not null) THEN
                    'ORG_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORG_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_LATITUDE), '0'), 38, 10) is null and 
                    src:ORG_LATITUDE is not null) THEN
                    'ORG_LATITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:ORG_LATITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_LONGITUDE), '0'), 38, 10) is null and 
                    src:ORG_LONGITUDE is not null) THEN
                    'ORG_LONGITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:ORG_LONGITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_MATCHLTA), '0'), 38, 10) is null and 
                    src:ORG_MATCHLTA is not null) THEN
                    'ORG_MATCHLTA contains a non-numeric value : \'' || AS_VARCHAR(src:ORG_MATCHLTA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_MATCHLTP), '0'), 38, 10) is null and 
                    src:ORG_MATCHLTP is not null) THEN
                    'ORG_MATCHLTP contains a non-numeric value : \'' || AS_VARCHAR(src:ORG_MATCHLTP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_TIMEZONE), '0'), 38, 10) is null and 
                    src:ORG_TIMEZONE is not null) THEN
                    'ORG_TIMEZONE contains a non-numeric value : \'' || AS_VARCHAR(src:ORG_TIMEZONE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_UDFDATE01), '1900-01-01')) is null and 
                    src:ORG_UDFDATE01 is not null) THEN
                    'ORG_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORG_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_UDFDATE02), '1900-01-01')) is null and 
                    src:ORG_UDFDATE02 is not null) THEN
                    'ORG_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORG_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_UDFDATE03), '1900-01-01')) is null and 
                    src:ORG_UDFDATE03 is not null) THEN
                    'ORG_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORG_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_UDFDATE04), '1900-01-01')) is null and 
                    src:ORG_UDFDATE04 is not null) THEN
                    'ORG_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORG_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_UDFDATE05), '1900-01-01')) is null and 
                    src:ORG_UDFDATE05 is not null) THEN
                    'ORG_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORG_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_UDFNUM01), '0'), 38, 10) is null and 
                    src:ORG_UDFNUM01 is not null) THEN
                    'ORG_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:ORG_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_UDFNUM02), '0'), 38, 10) is null and 
                    src:ORG_UDFNUM02 is not null) THEN
                    'ORG_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:ORG_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_UDFNUM03), '0'), 38, 10) is null and 
                    src:ORG_UDFNUM03 is not null) THEN
                    'ORG_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:ORG_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_UDFNUM04), '0'), 38, 10) is null and 
                    src:ORG_UDFNUM04 is not null) THEN
                    'ORG_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:ORG_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_UDFNUM05), '0'), 38, 10) is null and 
                    src:ORG_UDFNUM05 is not null) THEN
                    'ORG_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:ORG_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ORG_UPDATECOUNT is not null) THEN
                    'ORG_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ORG_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_UPDATED), '1900-01-01')) is null and 
                    src:ORG_UPDATED is not null) THEN
                    'ORG_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORG_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_LASTSAVED), '1900-01-01')) is null and 
                src:ORG_LASTSAVED is not null) THEN
                'ORG_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORG_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ORG_CODE  ORDER BY 
            src:ORG_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ORGANIZATION as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_CREATED), '1900-01-01')) is null and 
                    src:ORG_CREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_INVQTYTOL), '0'), 38, 10) is null and 
                    src:ORG_INVQTYTOL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_LASTSAVED), '1900-01-01')) is null and 
                    src:ORG_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_LATITUDE), '0'), 38, 10) is null and 
                    src:ORG_LATITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_LONGITUDE), '0'), 38, 10) is null and 
                    src:ORG_LONGITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_MATCHLTA), '0'), 38, 10) is null and 
                    src:ORG_MATCHLTA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_MATCHLTP), '0'), 38, 10) is null and 
                    src:ORG_MATCHLTP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_TIMEZONE), '0'), 38, 10) is null and 
                    src:ORG_TIMEZONE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_UDFDATE01), '1900-01-01')) is null and 
                    src:ORG_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_UDFDATE02), '1900-01-01')) is null and 
                    src:ORG_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_UDFDATE03), '1900-01-01')) is null and 
                    src:ORG_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_UDFDATE04), '1900-01-01')) is null and 
                    src:ORG_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_UDFDATE05), '1900-01-01')) is null and 
                    src:ORG_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_UDFNUM01), '0'), 38, 10) is null and 
                    src:ORG_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_UDFNUM02), '0'), 38, 10) is null and 
                    src:ORG_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_UDFNUM03), '0'), 38, 10) is null and 
                    src:ORG_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_UDFNUM04), '0'), 38, 10) is null and 
                    src:ORG_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_UDFNUM05), '0'), 38, 10) is null and 
                    src:ORG_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORG_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ORG_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_UPDATED), '1900-01-01')) is null and 
                    src:ORG_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORG_LASTSAVED), '1900-01-01')) is null and 
                src:ORG_LASTSAVED is not null)