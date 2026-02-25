CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5USERORGANIZATION_ERROR AS SELECT src, 'EAM_R5USERORGANIZATION' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UOG_CREATED), '1900-01-01')) is null and 
                    src:UOG_CREATED is not null) THEN
                    'UOG_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:UOG_CREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOG_INVAPPVLIMIT), '0'), 38, 10) is null and 
                    src:UOG_INVAPPVLIMIT is not null) THEN
                    'UOG_INVAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:UOG_INVAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOG_INVAPPVLIMITNONPO), '0'), 38, 10) is null and 
                    src:UOG_INVAPPVLIMITNONPO is not null) THEN
                    'UOG_INVAPPVLIMITNONPO contains a non-numeric value : \'' || AS_VARCHAR(src:UOG_INVAPPVLIMITNONPO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UOG_LASTSAVED), '1900-01-01')) is null and 
                    src:UOG_LASTSAVED is not null) THEN
                    'UOG_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:UOG_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOG_PICAPPVLIMIT), '0'), 38, 10) is null and 
                    src:UOG_PICAPPVLIMIT is not null) THEN
                    'UOG_PICAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:UOG_PICAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOG_PORDAPPVLIMIT), '0'), 38, 10) is null and 
                    src:UOG_PORDAPPVLIMIT is not null) THEN
                    'UOG_PORDAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:UOG_PORDAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOG_PORDAUTHAPPVLIMIT), '0'), 38, 10) is null and 
                    src:UOG_PORDAUTHAPPVLIMIT is not null) THEN
                    'UOG_PORDAUTHAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:UOG_PORDAUTHAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOG_REQAPPVLIMIT), '0'), 38, 10) is null and 
                    src:UOG_REQAPPVLIMIT is not null) THEN
                    'UOG_REQAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:UOG_REQAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOG_REQAUTHAPPVLIMIT), '0'), 38, 10) is null and 
                    src:UOG_REQAUTHAPPVLIMIT is not null) THEN
                    'UOG_REQAUTHAPPVLIMIT contains a non-numeric value : \'' || AS_VARCHAR(src:UOG_REQAUTHAPPVLIMIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOG_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:UOG_UPDATECOUNT is not null) THEN
                    'UOG_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:UOG_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UOG_UPDATED), '1900-01-01')) is null and 
                    src:UOG_UPDATED is not null) THEN
                    'UOG_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:UOG_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UOG_LASTSAVED), '1900-01-01')) is null and 
                src:UOG_LASTSAVED is not null) THEN
                'UOG_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:UOG_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:UOG_ORG,
                src:UOG_ROLE,
                src:UOG_USER  ORDER BY 
            src:UOG_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5USERORGANIZATION as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UOG_CREATED), '1900-01-01')) is null and 
                    src:UOG_CREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOG_INVAPPVLIMIT), '0'), 38, 10) is null and 
                    src:UOG_INVAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOG_INVAPPVLIMITNONPO), '0'), 38, 10) is null and 
                    src:UOG_INVAPPVLIMITNONPO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UOG_LASTSAVED), '1900-01-01')) is null and 
                    src:UOG_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOG_PICAPPVLIMIT), '0'), 38, 10) is null and 
                    src:UOG_PICAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOG_PORDAPPVLIMIT), '0'), 38, 10) is null and 
                    src:UOG_PORDAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOG_PORDAUTHAPPVLIMIT), '0'), 38, 10) is null and 
                    src:UOG_PORDAUTHAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOG_REQAPPVLIMIT), '0'), 38, 10) is null and 
                    src:UOG_REQAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOG_REQAUTHAPPVLIMIT), '0'), 38, 10) is null and 
                    src:UOG_REQAUTHAPPVLIMIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOG_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:UOG_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UOG_UPDATED), '1900-01-01')) is null and 
                    src:UOG_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UOG_LASTSAVED), '1900-01-01')) is null and 
                src:UOG_LASTSAVED is not null)