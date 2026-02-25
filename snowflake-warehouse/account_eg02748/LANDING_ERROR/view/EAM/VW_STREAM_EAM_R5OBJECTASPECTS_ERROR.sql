CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5OBJECTASPECTS_ERROR AS SELECT src, 'EAM_R5OBJECTASPECTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_FACTOR), '0'), 38, 10) is null and 
                    src:OBA_FACTOR is not null) THEN
                    'OBA_FACTOR contains a non-numeric value : \'' || AS_VARCHAR(src:OBA_FACTOR) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBA_LASTSAVED), '1900-01-01')) is null and 
                    src:OBA_LASTSAVED is not null) THEN
                    'OBA_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBA_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_MAX), '0'), 38, 10) is null and 
                    src:OBA_MAX is not null) THEN
                    'OBA_MAX contains a non-numeric value : \'' || AS_VARCHAR(src:OBA_MAX) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_MAXEXTR), '0'), 38, 10) is null and 
                    src:OBA_MAXEXTR is not null) THEN
                    'OBA_MAXEXTR contains a non-numeric value : \'' || AS_VARCHAR(src:OBA_MAXEXTR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_MAXTOL), '0'), 38, 10) is null and 
                    src:OBA_MAXTOL is not null) THEN
                    'OBA_MAXTOL contains a non-numeric value : \'' || AS_VARCHAR(src:OBA_MAXTOL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_MIN), '0'), 38, 10) is null and 
                    src:OBA_MIN is not null) THEN
                    'OBA_MIN contains a non-numeric value : \'' || AS_VARCHAR(src:OBA_MIN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_MINEXTR), '0'), 38, 10) is null and 
                    src:OBA_MINEXTR is not null) THEN
                    'OBA_MINEXTR contains a non-numeric value : \'' || AS_VARCHAR(src:OBA_MINEXTR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_MINTOL), '0'), 38, 10) is null and 
                    src:OBA_MINTOL is not null) THEN
                    'OBA_MINTOL contains a non-numeric value : \'' || AS_VARCHAR(src:OBA_MINTOL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_NOMINAL), '0'), 38, 10) is null and 
                    src:OBA_NOMINAL is not null) THEN
                    'OBA_NOMINAL contains a non-numeric value : \'' || AS_VARCHAR(src:OBA_NOMINAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_REPLACEFREQ), '0'), 38, 10) is null and 
                    src:OBA_REPLACEFREQ is not null) THEN
                    'OBA_REPLACEFREQ contains a non-numeric value : \'' || AS_VARCHAR(src:OBA_REPLACEFREQ) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBA_UDFDATE01), '1900-01-01')) is null and 
                    src:OBA_UDFDATE01 is not null) THEN
                    'OBA_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:OBA_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBA_UDFDATE02), '1900-01-01')) is null and 
                    src:OBA_UDFDATE02 is not null) THEN
                    'OBA_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:OBA_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBA_UDFDATE03), '1900-01-01')) is null and 
                    src:OBA_UDFDATE03 is not null) THEN
                    'OBA_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:OBA_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBA_UDFDATE04), '1900-01-01')) is null and 
                    src:OBA_UDFDATE04 is not null) THEN
                    'OBA_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:OBA_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBA_UDFDATE05), '1900-01-01')) is null and 
                    src:OBA_UDFDATE05 is not null) THEN
                    'OBA_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:OBA_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_UDFNUM01), '0'), 38, 10) is null and 
                    src:OBA_UDFNUM01 is not null) THEN
                    'OBA_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:OBA_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_UDFNUM02), '0'), 38, 10) is null and 
                    src:OBA_UDFNUM02 is not null) THEN
                    'OBA_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:OBA_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_UDFNUM03), '0'), 38, 10) is null and 
                    src:OBA_UDFNUM03 is not null) THEN
                    'OBA_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:OBA_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_UDFNUM04), '0'), 38, 10) is null and 
                    src:OBA_UDFNUM04 is not null) THEN
                    'OBA_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:OBA_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_UDFNUM05), '0'), 38, 10) is null and 
                    src:OBA_UDFNUM05 is not null) THEN
                    'OBA_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:OBA_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:OBA_UPDATECOUNT is not null) THEN
                    'OBA_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:OBA_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBA_LASTSAVED), '1900-01-01')) is null and 
                src:OBA_LASTSAVED is not null) THEN
                'OBA_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBA_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:OBA_ASPECT,
                src:OBA_OBJECT,
                src:OBA_OBJECT_ORG,
                src:OBA_OBTYPE  ORDER BY 
            src:OBA_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5OBJECTASPECTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_FACTOR), '0'), 38, 10) is null and 
                    src:OBA_FACTOR is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBA_LASTSAVED), '1900-01-01')) is null and 
                    src:OBA_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_MAX), '0'), 38, 10) is null and 
                    src:OBA_MAX is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_MAXEXTR), '0'), 38, 10) is null and 
                    src:OBA_MAXEXTR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_MAXTOL), '0'), 38, 10) is null and 
                    src:OBA_MAXTOL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_MIN), '0'), 38, 10) is null and 
                    src:OBA_MIN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_MINEXTR), '0'), 38, 10) is null and 
                    src:OBA_MINEXTR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_MINTOL), '0'), 38, 10) is null and 
                    src:OBA_MINTOL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_NOMINAL), '0'), 38, 10) is null and 
                    src:OBA_NOMINAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_REPLACEFREQ), '0'), 38, 10) is null and 
                    src:OBA_REPLACEFREQ is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBA_UDFDATE01), '1900-01-01')) is null and 
                    src:OBA_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBA_UDFDATE02), '1900-01-01')) is null and 
                    src:OBA_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBA_UDFDATE03), '1900-01-01')) is null and 
                    src:OBA_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBA_UDFDATE04), '1900-01-01')) is null and 
                    src:OBA_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBA_UDFDATE05), '1900-01-01')) is null and 
                    src:OBA_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_UDFNUM01), '0'), 38, 10) is null and 
                    src:OBA_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_UDFNUM02), '0'), 38, 10) is null and 
                    src:OBA_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_UDFNUM03), '0'), 38, 10) is null and 
                    src:OBA_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_UDFNUM04), '0'), 38, 10) is null and 
                    src:OBA_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_UDFNUM05), '0'), 38, 10) is null and 
                    src:OBA_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBA_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:OBA_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBA_LASTSAVED), '1900-01-01')) is null and 
                src:OBA_LASTSAVED is not null)