CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5USERS_ERROR AS SELECT src, 'EAM_R5USERS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_CREATED), '1900-01-01')) is null and 
                    src:USR_CREATED is not null) THEN
                    'USR_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:USR_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_DATELOCKED), '1900-01-01')) is null and 
                    src:USR_DATELOCKED is not null) THEN
                    'USR_DATELOCKED contains a non-timestamp value : \'' || AS_VARCHAR(src:USR_DATELOCKED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_DEFAULT_KPI_SIZE), '0'), 38, 10) is null and 
                    src:USR_DEFAULT_KPI_SIZE is not null) THEN
                    'USR_DEFAULT_KPI_SIZE contains a non-numeric value : \'' || AS_VARCHAR(src:USR_DEFAULT_KPI_SIZE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_EXPPASS), '1900-01-01')) is null and 
                    src:USR_EXPPASS is not null) THEN
                    'USR_EXPPASS contains a non-timestamp value : \'' || AS_VARCHAR(src:USR_EXPPASS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_EXPUSER), '1900-01-01')) is null and 
                    src:USR_EXPUSER is not null) THEN
                    'USR_EXPUSER contains a non-timestamp value : \'' || AS_VARCHAR(src:USR_EXPUSER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_INVAPPVLIMIT), '0'), 38, 10) is null and 
                    src:USR_INVAPPVLIMIT is not null) THEN
                    'USR_INVAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:USR_INVAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_INVAPPVLIMITNONPO), '0'), 38, 10) is null and 
                    src:USR_INVAPPVLIMITNONPO is not null) THEN
                    'USR_INVAPPVLIMITNONPO contains a non-numeric value : \'' || AS_VARCHAR(src:USR_INVAPPVLIMITNONPO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_LASTSAVED), '1900-01-01')) is null and 
                    src:USR_LASTSAVED is not null) THEN
                    'USR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:USR_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_PICAPPVLIMIT), '0'), 38, 10) is null and 
                    src:USR_PICAPPVLIMIT is not null) THEN
                    'USR_PICAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:USR_PICAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_PORDAPPVLIMIT), '0'), 38, 10) is null and 
                    src:USR_PORDAPPVLIMIT is not null) THEN
                    'USR_PORDAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:USR_PORDAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_PORDAUTHAPPVLIMIT), '0'), 38, 10) is null and 
                    src:USR_PORDAUTHAPPVLIMIT is not null) THEN
                    'USR_PORDAUTHAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:USR_PORDAUTHAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_REQAPPVLIMIT), '0'), 38, 10) is null and 
                    src:USR_REQAPPVLIMIT is not null) THEN
                    'USR_REQAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:USR_REQAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_REQAUTHAPPVLIMIT), '0'), 38, 10) is null and 
                    src:USR_REQAUTHAPPVLIMIT is not null) THEN
                    'USR_REQAUTHAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:USR_REQAUTHAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_SESSIONTIMEOUT), '0'), 38, 10) is null and 
                    src:USR_SESSIONTIMEOUT is not null) THEN
                    'USR_SESSIONTIMEOUT contains a non-numeric value : \'' || AS_VARCHAR(src:USR_SESSIONTIMEOUT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_SUCCESSMSGTIMEOUT), '0'), 38, 10) is null and 
                    src:USR_SUCCESSMSGTIMEOUT is not null) THEN
                    'USR_SUCCESSMSGTIMEOUT contains a non-numeric value : \'' || AS_VARCHAR(src:USR_SUCCESSMSGTIMEOUT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_UDFDATE01), '1900-01-01')) is null and 
                    src:USR_UDFDATE01 is not null) THEN
                    'USR_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:USR_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_UDFDATE02), '1900-01-01')) is null and 
                    src:USR_UDFDATE02 is not null) THEN
                    'USR_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:USR_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_UDFDATE03), '1900-01-01')) is null and 
                    src:USR_UDFDATE03 is not null) THEN
                    'USR_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:USR_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_UDFDATE04), '1900-01-01')) is null and 
                    src:USR_UDFDATE04 is not null) THEN
                    'USR_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:USR_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_UDFDATE05), '1900-01-01')) is null and 
                    src:USR_UDFDATE05 is not null) THEN
                    'USR_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:USR_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_UDFNUM01), '0'), 38, 10) is null and 
                    src:USR_UDFNUM01 is not null) THEN
                    'USR_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:USR_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_UDFNUM02), '0'), 38, 10) is null and 
                    src:USR_UDFNUM02 is not null) THEN
                    'USR_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:USR_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_UDFNUM03), '0'), 38, 10) is null and 
                    src:USR_UDFNUM03 is not null) THEN
                    'USR_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:USR_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_UDFNUM04), '0'), 38, 10) is null and 
                    src:USR_UDFNUM04 is not null) THEN
                    'USR_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:USR_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_UDFNUM05), '0'), 38, 10) is null and 
                    src:USR_UDFNUM05 is not null) THEN
                    'USR_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:USR_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:USR_UPDATECOUNT is not null) THEN
                    'USR_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:USR_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_UPDATED), '1900-01-01')) is null and 
                    src:USR_UPDATED is not null) THEN
                    'USR_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:USR_UPDATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_VIOLATIONS), '0'), 38, 10) is null and 
                    src:USR_VIOLATIONS is not null) THEN
                    'USR_VIOLATIONS contains a non-numeric value : \'' || AS_VARCHAR(src:USR_VIOLATIONS) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_LASTSAVED), '1900-01-01')) is null and 
                src:USR_LASTSAVED is not null) THEN
                'USR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:USR_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:USR_CODE  ORDER BY 
            src:USR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5USERS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_CREATED), '1900-01-01')) is null and 
                    src:USR_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_DATELOCKED), '1900-01-01')) is null and 
                    src:USR_DATELOCKED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_DEFAULT_KPI_SIZE), '0'), 38, 10) is null and 
                    src:USR_DEFAULT_KPI_SIZE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_EXPPASS), '1900-01-01')) is null and 
                    src:USR_EXPPASS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_EXPUSER), '1900-01-01')) is null and 
                    src:USR_EXPUSER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_INVAPPVLIMIT), '0'), 38, 10) is null and 
                    src:USR_INVAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_INVAPPVLIMITNONPO), '0'), 38, 10) is null and 
                    src:USR_INVAPPVLIMITNONPO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_LASTSAVED), '1900-01-01')) is null and 
                    src:USR_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_PICAPPVLIMIT), '0'), 38, 10) is null and 
                    src:USR_PICAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_PORDAPPVLIMIT), '0'), 38, 10) is null and 
                    src:USR_PORDAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_PORDAUTHAPPVLIMIT), '0'), 38, 10) is null and 
                    src:USR_PORDAUTHAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_REQAPPVLIMIT), '0'), 38, 10) is null and 
                    src:USR_REQAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_REQAUTHAPPVLIMIT), '0'), 38, 10) is null and 
                    src:USR_REQAUTHAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_SESSIONTIMEOUT), '0'), 38, 10) is null and 
                    src:USR_SESSIONTIMEOUT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_SUCCESSMSGTIMEOUT), '0'), 38, 10) is null and 
                    src:USR_SUCCESSMSGTIMEOUT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_UDFDATE01), '1900-01-01')) is null and 
                    src:USR_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_UDFDATE02), '1900-01-01')) is null and 
                    src:USR_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_UDFDATE03), '1900-01-01')) is null and 
                    src:USR_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_UDFDATE04), '1900-01-01')) is null and 
                    src:USR_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_UDFDATE05), '1900-01-01')) is null and 
                    src:USR_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_UDFNUM01), '0'), 38, 10) is null and 
                    src:USR_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_UDFNUM02), '0'), 38, 10) is null and 
                    src:USR_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_UDFNUM03), '0'), 38, 10) is null and 
                    src:USR_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_UDFNUM04), '0'), 38, 10) is null and 
                    src:USR_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_UDFNUM05), '0'), 38, 10) is null and 
                    src:USR_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:USR_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_UPDATED), '1900-01-01')) is null and 
                    src:USR_UPDATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USR_VIOLATIONS), '0'), 38, 10) is null and 
                    src:USR_VIOLATIONS is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USR_LASTSAVED), '1900-01-01')) is null and 
                src:USR_LASTSAVED is not null)