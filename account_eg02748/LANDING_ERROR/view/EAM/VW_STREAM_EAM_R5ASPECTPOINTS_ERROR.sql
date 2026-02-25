CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ASPECTPOINTS_ERROR AS SELECT src, 'EAM_R5ASPECTPOINTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_FACTOR), '0'), 38, 10) is null and 
                    src:APO_FACTOR is not null) THEN
                    'APO_FACTOR contains a non-numeric value : \'' || AS_VARCHAR(src:APO_FACTOR) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APO_LASTSAVED), '1900-01-01')) is null and 
                    src:APO_LASTSAVED is not null) THEN
                    'APO_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:APO_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_MAX), '0'), 38, 10) is null and 
                    src:APO_MAX is not null) THEN
                    'APO_MAX contains a non-numeric value : \'' || AS_VARCHAR(src:APO_MAX) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_MAXEXTR), '0'), 38, 10) is null and 
                    src:APO_MAXEXTR is not null) THEN
                    'APO_MAXEXTR contains a non-numeric value : \'' || AS_VARCHAR(src:APO_MAXEXTR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_MAXTOL), '0'), 38, 10) is null and 
                    src:APO_MAXTOL is not null) THEN
                    'APO_MAXTOL contains a non-numeric value : \'' || AS_VARCHAR(src:APO_MAXTOL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_MIN), '0'), 38, 10) is null and 
                    src:APO_MIN is not null) THEN
                    'APO_MIN contains a non-numeric value : \'' || AS_VARCHAR(src:APO_MIN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_MINEXTR), '0'), 38, 10) is null and 
                    src:APO_MINEXTR is not null) THEN
                    'APO_MINEXTR contains a non-numeric value : \'' || AS_VARCHAR(src:APO_MINEXTR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_MINTOL), '0'), 38, 10) is null and 
                    src:APO_MINTOL is not null) THEN
                    'APO_MINTOL contains a non-numeric value : \'' || AS_VARCHAR(src:APO_MINTOL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_NOMINAL), '0'), 38, 10) is null and 
                    src:APO_NOMINAL is not null) THEN
                    'APO_NOMINAL contains a non-numeric value : \'' || AS_VARCHAR(src:APO_NOMINAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_REPLACEFREQ), '0'), 38, 10) is null and 
                    src:APO_REPLACEFREQ is not null) THEN
                    'APO_REPLACEFREQ contains a non-numeric value : \'' || AS_VARCHAR(src:APO_REPLACEFREQ) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APO_UDFDATE01), '1900-01-01')) is null and 
                    src:APO_UDFDATE01 is not null) THEN
                    'APO_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:APO_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APO_UDFDATE02), '1900-01-01')) is null and 
                    src:APO_UDFDATE02 is not null) THEN
                    'APO_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:APO_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APO_UDFDATE03), '1900-01-01')) is null and 
                    src:APO_UDFDATE03 is not null) THEN
                    'APO_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:APO_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APO_UDFDATE04), '1900-01-01')) is null and 
                    src:APO_UDFDATE04 is not null) THEN
                    'APO_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:APO_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APO_UDFDATE05), '1900-01-01')) is null and 
                    src:APO_UDFDATE05 is not null) THEN
                    'APO_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:APO_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_UDFNUM01), '0'), 38, 10) is null and 
                    src:APO_UDFNUM01 is not null) THEN
                    'APO_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:APO_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_UDFNUM02), '0'), 38, 10) is null and 
                    src:APO_UDFNUM02 is not null) THEN
                    'APO_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:APO_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_UDFNUM03), '0'), 38, 10) is null and 
                    src:APO_UDFNUM03 is not null) THEN
                    'APO_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:APO_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_UDFNUM04), '0'), 38, 10) is null and 
                    src:APO_UDFNUM04 is not null) THEN
                    'APO_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:APO_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_UDFNUM05), '0'), 38, 10) is null and 
                    src:APO_UDFNUM05 is not null) THEN
                    'APO_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:APO_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:APO_UPDATECOUNT is not null) THEN
                    'APO_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:APO_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APO_UPDATED), '1900-01-01')) is null and 
                    src:APO_UPDATED is not null) THEN
                    'APO_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:APO_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APO_LASTSAVED), '1900-01-01')) is null and 
                src:APO_LASTSAVED is not null) THEN
                'APO_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:APO_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:APO_ASPECT,
                src:APO_OBJECT,
                src:APO_OBJECT_ORG,
                src:APO_OBTYPE,
                src:APO_POINT,
                src:APO_POINTTYPE  ORDER BY 
            src:APO_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ASPECTPOINTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_FACTOR), '0'), 38, 10) is null and 
                    src:APO_FACTOR is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APO_LASTSAVED), '1900-01-01')) is null and 
                    src:APO_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_MAX), '0'), 38, 10) is null and 
                    src:APO_MAX is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_MAXEXTR), '0'), 38, 10) is null and 
                    src:APO_MAXEXTR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_MAXTOL), '0'), 38, 10) is null and 
                    src:APO_MAXTOL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_MIN), '0'), 38, 10) is null and 
                    src:APO_MIN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_MINEXTR), '0'), 38, 10) is null and 
                    src:APO_MINEXTR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_MINTOL), '0'), 38, 10) is null and 
                    src:APO_MINTOL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_NOMINAL), '0'), 38, 10) is null and 
                    src:APO_NOMINAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_REPLACEFREQ), '0'), 38, 10) is null and 
                    src:APO_REPLACEFREQ is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APO_UDFDATE01), '1900-01-01')) is null and 
                    src:APO_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APO_UDFDATE02), '1900-01-01')) is null and 
                    src:APO_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APO_UDFDATE03), '1900-01-01')) is null and 
                    src:APO_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APO_UDFDATE04), '1900-01-01')) is null and 
                    src:APO_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APO_UDFDATE05), '1900-01-01')) is null and 
                    src:APO_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_UDFNUM01), '0'), 38, 10) is null and 
                    src:APO_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_UDFNUM02), '0'), 38, 10) is null and 
                    src:APO_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_UDFNUM03), '0'), 38, 10) is null and 
                    src:APO_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_UDFNUM04), '0'), 38, 10) is null and 
                    src:APO_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_UDFNUM05), '0'), 38, 10) is null and 
                    src:APO_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APO_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:APO_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APO_UPDATED), '1900-01-01')) is null and 
                    src:APO_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APO_LASTSAVED), '1900-01-01')) is null and 
                src:APO_LASTSAVED is not null)