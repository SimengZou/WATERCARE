CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5WORKSAFETY_ERROR AS SELECT src, 'EAM_R5WORKSAFETY' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_CREATED), '1900-01-01')) is null and 
                    src:KSF_CREATED is not null) THEN
                    'KSF_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:KSF_CREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_HAZARDREV), '0'), 38, 10) is null and 
                    src:KSF_HAZARDREV is not null) THEN
                    'KSF_HAZARDREV contains a non-numeric value : \'' || AS_VARCHAR(src:KSF_HAZARDREV) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_LASTSAVED), '1900-01-01')) is null and 
                    src:KSF_LASTSAVED is not null) THEN
                    'KSF_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:KSF_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_PRECAUTIONREV), '0'), 38, 10) is null and 
                    src:KSF_PRECAUTIONREV is not null) THEN
                    'KSF_PRECAUTIONREV contains a non-numeric value : \'' || AS_VARCHAR(src:KSF_PRECAUTIONREV) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_REVIEWED), '1900-01-01')) is null and 
                    src:KSF_REVIEWED is not null) THEN
                    'KSF_REVIEWED contains a non-timestamp value : \'' || AS_VARCHAR(src:KSF_REVIEWED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_SEQUENCE), '0'), 38, 10) is null and 
                    src:KSF_SEQUENCE is not null) THEN
                    'KSF_SEQUENCE contains a non-numeric value : \'' || AS_VARCHAR(src:KSF_SEQUENCE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_SOURCEUPDATED), '1900-01-01')) is null and 
                    src:KSF_SOURCEUPDATED is not null) THEN
                    'KSF_SOURCEUPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:KSF_SOURCEUPDATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_UDFDATE01), '1900-01-01')) is null and 
                    src:KSF_UDFDATE01 is not null) THEN
                    'KSF_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:KSF_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_UDFDATE02), '1900-01-01')) is null and 
                    src:KSF_UDFDATE02 is not null) THEN
                    'KSF_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:KSF_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_UDFDATE03), '1900-01-01')) is null and 
                    src:KSF_UDFDATE03 is not null) THEN
                    'KSF_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:KSF_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_UDFDATE04), '1900-01-01')) is null and 
                    src:KSF_UDFDATE04 is not null) THEN
                    'KSF_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:KSF_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_UDFDATE05), '1900-01-01')) is null and 
                    src:KSF_UDFDATE05 is not null) THEN
                    'KSF_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:KSF_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_UDFNUM01), '0'), 38, 10) is null and 
                    src:KSF_UDFNUM01 is not null) THEN
                    'KSF_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:KSF_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_UDFNUM02), '0'), 38, 10) is null and 
                    src:KSF_UDFNUM02 is not null) THEN
                    'KSF_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:KSF_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_UDFNUM03), '0'), 38, 10) is null and 
                    src:KSF_UDFNUM03 is not null) THEN
                    'KSF_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:KSF_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_UDFNUM04), '0'), 38, 10) is null and 
                    src:KSF_UDFNUM04 is not null) THEN
                    'KSF_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:KSF_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_UDFNUM05), '0'), 38, 10) is null and 
                    src:KSF_UDFNUM05 is not null) THEN
                    'KSF_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:KSF_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:KSF_UPDATECOUNT is not null) THEN
                    'KSF_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:KSF_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_UPDATED), '1900-01-01')) is null and 
                    src:KSF_UPDATED is not null) THEN
                    'KSF_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:KSF_UPDATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_VALIDUNTIL), '1900-01-01')) is null and 
                    src:KSF_VALIDUNTIL is not null) THEN
                    'KSF_VALIDUNTIL contains a non-timestamp value : \'' || AS_VARCHAR(src:KSF_VALIDUNTIL) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_LASTSAVED), '1900-01-01')) is null and 
                src:KSF_LASTSAVED is not null) THEN
                'KSF_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:KSF_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:KSF_PK  ORDER BY 
            src:KSF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5WORKSAFETY as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_CREATED), '1900-01-01')) is null and 
                    src:KSF_CREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_HAZARDREV), '0'), 38, 10) is null and 
                    src:KSF_HAZARDREV is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_LASTSAVED), '1900-01-01')) is null and 
                    src:KSF_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_PRECAUTIONREV), '0'), 38, 10) is null and 
                    src:KSF_PRECAUTIONREV is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_REVIEWED), '1900-01-01')) is null and 
                    src:KSF_REVIEWED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_SEQUENCE), '0'), 38, 10) is null and 
                    src:KSF_SEQUENCE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_SOURCEUPDATED), '1900-01-01')) is null and 
                    src:KSF_SOURCEUPDATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_UDFDATE01), '1900-01-01')) is null and 
                    src:KSF_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_UDFDATE02), '1900-01-01')) is null and 
                    src:KSF_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_UDFDATE03), '1900-01-01')) is null and 
                    src:KSF_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_UDFDATE04), '1900-01-01')) is null and 
                    src:KSF_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_UDFDATE05), '1900-01-01')) is null and 
                    src:KSF_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_UDFNUM01), '0'), 38, 10) is null and 
                    src:KSF_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_UDFNUM02), '0'), 38, 10) is null and 
                    src:KSF_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_UDFNUM03), '0'), 38, 10) is null and 
                    src:KSF_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_UDFNUM04), '0'), 38, 10) is null and 
                    src:KSF_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_UDFNUM05), '0'), 38, 10) is null and 
                    src:KSF_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KSF_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:KSF_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_UPDATED), '1900-01-01')) is null and 
                    src:KSF_UPDATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_VALIDUNTIL), '1900-01-01')) is null and 
                    src:KSF_VALIDUNTIL is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:KSF_LASTSAVED), '1900-01-01')) is null and 
                src:KSF_LASTSAVED is not null)