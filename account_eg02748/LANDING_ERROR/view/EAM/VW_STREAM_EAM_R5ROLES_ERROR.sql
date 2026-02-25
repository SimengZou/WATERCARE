CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ROLES_ERROR AS SELECT src, 'EAM_R5ROLES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_INVAPPVLIMIT), '0'), 38, 10) is null and 
                    src:ROL_INVAPPVLIMIT is not null) THEN
                    'ROL_INVAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:ROL_INVAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_INVAPPVLIMITNONPO), '0'), 38, 10) is null and 
                    src:ROL_INVAPPVLIMITNONPO is not null) THEN
                    'ROL_INVAPPVLIMITNONPO contains a non-numeric value : \'' || AS_VARCHAR(src:ROL_INVAPPVLIMITNONPO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ROL_LASTSAVED), '1900-01-01')) is null and 
                    src:ROL_LASTSAVED is not null) THEN
                    'ROL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ROL_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_PICAPPVLIMIT), '0'), 38, 10) is null and 
                    src:ROL_PICAPPVLIMIT is not null) THEN
                    'ROL_PICAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:ROL_PICAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_PORDAPPVLIMIT), '0'), 38, 10) is null and 
                    src:ROL_PORDAPPVLIMIT is not null) THEN
                    'ROL_PORDAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:ROL_PORDAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_PORDAUTHAPPVLIMIT), '0'), 38, 10) is null and 
                    src:ROL_PORDAUTHAPPVLIMIT is not null) THEN
                    'ROL_PORDAUTHAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:ROL_PORDAUTHAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_REQAPPVLIMIT), '0'), 38, 10) is null and 
                    src:ROL_REQAPPVLIMIT is not null) THEN
                    'ROL_REQAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:ROL_REQAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_REQAUTHAPPVLIMIT), '0'), 38, 10) is null and 
                    src:ROL_REQAUTHAPPVLIMIT is not null) THEN
                    'ROL_REQAUTHAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:ROL_REQAUTHAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_SUCCESSMSGTIMEOUT), '0'), 38, 10) is null and 
                    src:ROL_SUCCESSMSGTIMEOUT is not null) THEN
                    'ROL_SUCCESSMSGTIMEOUT contains a non-numeric value : \'' || AS_VARCHAR(src:ROL_SUCCESSMSGTIMEOUT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ROL_UPDATECOUNT is not null) THEN
                    'ROL_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ROL_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ROL_LASTSAVED), '1900-01-01')) is null and 
                src:ROL_LASTSAVED is not null) THEN
                'ROL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ROL_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ROL_CODE  ORDER BY 
            src:ROL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ROLES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_INVAPPVLIMIT), '0'), 38, 10) is null and 
                    src:ROL_INVAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_INVAPPVLIMITNONPO), '0'), 38, 10) is null and 
                    src:ROL_INVAPPVLIMITNONPO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ROL_LASTSAVED), '1900-01-01')) is null and 
                    src:ROL_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_PICAPPVLIMIT), '0'), 38, 10) is null and 
                    src:ROL_PICAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_PORDAPPVLIMIT), '0'), 38, 10) is null and 
                    src:ROL_PORDAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_PORDAUTHAPPVLIMIT), '0'), 38, 10) is null and 
                    src:ROL_PORDAUTHAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_REQAPPVLIMIT), '0'), 38, 10) is null and 
                    src:ROL_REQAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_REQAUTHAPPVLIMIT), '0'), 38, 10) is null and 
                    src:ROL_REQAUTHAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_SUCCESSMSGTIMEOUT), '0'), 38, 10) is null and 
                    src:ROL_SUCCESSMSGTIMEOUT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ROL_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ROL_LASTSAVED), '1900-01-01')) is null and 
                src:ROL_LASTSAVED is not null)