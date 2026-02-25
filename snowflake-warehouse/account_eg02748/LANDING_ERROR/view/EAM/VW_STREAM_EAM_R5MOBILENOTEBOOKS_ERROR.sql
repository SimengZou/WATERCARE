CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5MOBILENOTEBOOKS_ERROR AS SELECT src, 'EAM_R5MOBILENOTEBOOKS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_CREATED), '1900-01-01')) is null and 
                    src:MNB_CREATED is not null) THEN
                    'MNB_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:MNB_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_LASTSAVED), '1900-01-01')) is null and 
                    src:MNB_LASTSAVED is not null) THEN
                    'MNB_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:MNB_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_MOBILECREATED), '1900-01-01')) is null and 
                    src:MNB_MOBILECREATED is not null) THEN
                    'MNB_MOBILECREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:MNB_MOBILECREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MNB_STATIONING_MAJOR), '0'), 38, 10) is null and 
                    src:MNB_STATIONING_MAJOR is not null) THEN
                    'MNB_STATIONING_MAJOR contains a non-numeric value : \'' || AS_VARCHAR(src:MNB_STATIONING_MAJOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MNB_STATIONING_MINOR), '0'), 38, 10) is null and 
                    src:MNB_STATIONING_MINOR is not null) THEN
                    'MNB_STATIONING_MINOR contains a non-numeric value : \'' || AS_VARCHAR(src:MNB_STATIONING_MINOR) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_UDFDATE01), '1900-01-01')) is null and 
                    src:MNB_UDFDATE01 is not null) THEN
                    'MNB_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:MNB_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_UDFDATE02), '1900-01-01')) is null and 
                    src:MNB_UDFDATE02 is not null) THEN
                    'MNB_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:MNB_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_UDFDATE03), '1900-01-01')) is null and 
                    src:MNB_UDFDATE03 is not null) THEN
                    'MNB_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:MNB_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_UDFDATE04), '1900-01-01')) is null and 
                    src:MNB_UDFDATE04 is not null) THEN
                    'MNB_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:MNB_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_UDFDATE05), '1900-01-01')) is null and 
                    src:MNB_UDFDATE05 is not null) THEN
                    'MNB_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:MNB_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MNB_UDFNUM01), '0'), 38, 10) is null and 
                    src:MNB_UDFNUM01 is not null) THEN
                    'MNB_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:MNB_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MNB_UDFNUM02), '0'), 38, 10) is null and 
                    src:MNB_UDFNUM02 is not null) THEN
                    'MNB_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:MNB_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MNB_UDFNUM03), '0'), 38, 10) is null and 
                    src:MNB_UDFNUM03 is not null) THEN
                    'MNB_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:MNB_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MNB_UDFNUM04), '0'), 38, 10) is null and 
                    src:MNB_UDFNUM04 is not null) THEN
                    'MNB_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:MNB_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MNB_UDFNUM05), '0'), 38, 10) is null and 
                    src:MNB_UDFNUM05 is not null) THEN
                    'MNB_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:MNB_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MNB_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:MNB_UPDATECOUNT is not null) THEN
                    'MNB_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:MNB_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_UPDATED), '1900-01-01')) is null and 
                    src:MNB_UPDATED is not null) THEN
                    'MNB_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:MNB_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_LASTSAVED), '1900-01-01')) is null and 
                src:MNB_LASTSAVED is not null) THEN
                'MNB_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:MNB_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:MNB_CODE  ORDER BY 
            src:MNB_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MOBILENOTEBOOKS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_CREATED), '1900-01-01')) is null and 
                    src:MNB_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_LASTSAVED), '1900-01-01')) is null and 
                    src:MNB_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_MOBILECREATED), '1900-01-01')) is null and 
                    src:MNB_MOBILECREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MNB_STATIONING_MAJOR), '0'), 38, 10) is null and 
                    src:MNB_STATIONING_MAJOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MNB_STATIONING_MINOR), '0'), 38, 10) is null and 
                    src:MNB_STATIONING_MINOR is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_UDFDATE01), '1900-01-01')) is null and 
                    src:MNB_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_UDFDATE02), '1900-01-01')) is null and 
                    src:MNB_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_UDFDATE03), '1900-01-01')) is null and 
                    src:MNB_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_UDFDATE04), '1900-01-01')) is null and 
                    src:MNB_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_UDFDATE05), '1900-01-01')) is null and 
                    src:MNB_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MNB_UDFNUM01), '0'), 38, 10) is null and 
                    src:MNB_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MNB_UDFNUM02), '0'), 38, 10) is null and 
                    src:MNB_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MNB_UDFNUM03), '0'), 38, 10) is null and 
                    src:MNB_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MNB_UDFNUM04), '0'), 38, 10) is null and 
                    src:MNB_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MNB_UDFNUM05), '0'), 38, 10) is null and 
                    src:MNB_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MNB_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:MNB_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_UPDATED), '1900-01-01')) is null and 
                    src:MNB_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MNB_LASTSAVED), '1900-01-01')) is null and 
                src:MNB_LASTSAVED is not null)