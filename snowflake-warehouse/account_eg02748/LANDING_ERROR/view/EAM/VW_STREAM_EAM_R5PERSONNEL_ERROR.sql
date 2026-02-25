CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5PERSONNEL_ERROR AS SELECT src, 'EAM_R5PERSONNEL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_BIRTHDATE), '1900-01-01')) is null and 
                    src:PER_BIRTHDATE is not null) THEN
                    'PER_BIRTHDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:PER_BIRTHDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PER_CAPACITY), '0'), 38, 10) is null and 
                    src:PER_CAPACITY is not null) THEN
                    'PER_CAPACITY contains a non-numeric value : \'' || AS_VARCHAR(src:PER_CAPACITY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_CREATED), '1900-01-01')) is null and 
                    src:PER_CREATED is not null) THEN
                    'PER_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:PER_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_HIREDATE), '1900-01-01')) is null and 
                    src:PER_HIREDATE is not null) THEN
                    'PER_HIREDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:PER_HIREDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_LASTSAVED), '1900-01-01')) is null and 
                    src:PER_LASTSAVED is not null) THEN
                    'PER_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PER_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_TERMINATEDDATE), '1900-01-01')) is null and 
                    src:PER_TERMINATEDDATE is not null) THEN
                    'PER_TERMINATEDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:PER_TERMINATEDDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_UDFDATE01), '1900-01-01')) is null and 
                    src:PER_UDFDATE01 is not null) THEN
                    'PER_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:PER_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_UDFDATE02), '1900-01-01')) is null and 
                    src:PER_UDFDATE02 is not null) THEN
                    'PER_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:PER_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_UDFDATE03), '1900-01-01')) is null and 
                    src:PER_UDFDATE03 is not null) THEN
                    'PER_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:PER_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_UDFDATE04), '1900-01-01')) is null and 
                    src:PER_UDFDATE04 is not null) THEN
                    'PER_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:PER_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_UDFDATE05), '1900-01-01')) is null and 
                    src:PER_UDFDATE05 is not null) THEN
                    'PER_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:PER_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PER_UDFNUM01), '0'), 38, 10) is null and 
                    src:PER_UDFNUM01 is not null) THEN
                    'PER_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:PER_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PER_UDFNUM02), '0'), 38, 10) is null and 
                    src:PER_UDFNUM02 is not null) THEN
                    'PER_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:PER_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PER_UDFNUM03), '0'), 38, 10) is null and 
                    src:PER_UDFNUM03 is not null) THEN
                    'PER_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:PER_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PER_UDFNUM04), '0'), 38, 10) is null and 
                    src:PER_UDFNUM04 is not null) THEN
                    'PER_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:PER_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PER_UDFNUM05), '0'), 38, 10) is null and 
                    src:PER_UDFNUM05 is not null) THEN
                    'PER_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:PER_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PER_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PER_UPDATECOUNT is not null) THEN
                    'PER_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PER_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_UPDATED), '1900-01-01')) is null and 
                    src:PER_UPDATED is not null) THEN
                    'PER_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:PER_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_LASTSAVED), '1900-01-01')) is null and 
                src:PER_LASTSAVED is not null) THEN
                'PER_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PER_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:PER_CODE  ORDER BY 
            src:PER_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PERSONNEL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_BIRTHDATE), '1900-01-01')) is null and 
                    src:PER_BIRTHDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PER_CAPACITY), '0'), 38, 10) is null and 
                    src:PER_CAPACITY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_CREATED), '1900-01-01')) is null and 
                    src:PER_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_HIREDATE), '1900-01-01')) is null and 
                    src:PER_HIREDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_LASTSAVED), '1900-01-01')) is null and 
                    src:PER_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_TERMINATEDDATE), '1900-01-01')) is null and 
                    src:PER_TERMINATEDDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_UDFDATE01), '1900-01-01')) is null and 
                    src:PER_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_UDFDATE02), '1900-01-01')) is null and 
                    src:PER_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_UDFDATE03), '1900-01-01')) is null and 
                    src:PER_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_UDFDATE04), '1900-01-01')) is null and 
                    src:PER_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_UDFDATE05), '1900-01-01')) is null and 
                    src:PER_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PER_UDFNUM01), '0'), 38, 10) is null and 
                    src:PER_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PER_UDFNUM02), '0'), 38, 10) is null and 
                    src:PER_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PER_UDFNUM03), '0'), 38, 10) is null and 
                    src:PER_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PER_UDFNUM04), '0'), 38, 10) is null and 
                    src:PER_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PER_UDFNUM05), '0'), 38, 10) is null and 
                    src:PER_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PER_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PER_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_UPDATED), '1900-01-01')) is null and 
                    src:PER_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PER_LASTSAVED), '1900-01-01')) is null and 
                src:PER_LASTSAVED is not null)