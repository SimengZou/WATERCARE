CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5RELIABILITYRANKINGS_ERROR AS SELECT src, 'EAM_R5RELIABILITYRANKINGS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_CONDSCOREEND), '0'), 38, 10) is null and 
                    src:RRK_CONDSCOREEND is not null) THEN
                    'RRK_CONDSCOREEND contains a non-numeric value : \'' || AS_VARCHAR(src:RRK_CONDSCOREEND) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_CONDSCORESTART), '0'), 38, 10) is null and 
                    src:RRK_CONDSCORESTART is not null) THEN
                    'RRK_CONDSCORESTART contains a non-numeric value : \'' || AS_VARCHAR(src:RRK_CONDSCORESTART) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_CONDSCORETHRESHOLD), '0'), 38, 10) is null and 
                    src:RRK_CONDSCORETHRESHOLD is not null) THEN
                    'RRK_CONDSCORETHRESHOLD contains a non-numeric value : \'' || AS_VARCHAR(src:RRK_CONDSCORETHRESHOLD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_CORRSCORERANKINGREV), '0'), 38, 10) is null and 
                    src:RRK_CORRSCORERANKINGREV is not null) THEN
                    'RRK_CORRSCORERANKINGREV contains a non-numeric value : \'' || AS_VARCHAR(src:RRK_CORRSCORERANKINGREV) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_CREATED), '1900-01-01')) is null and 
                    src:RRK_CREATED is not null) THEN
                    'RRK_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:RRK_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_LASTSAVED), '1900-01-01')) is null and 
                    src:RRK_LASTSAVED is not null) THEN
                    'RRK_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:RRK_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_PRECISION), '0'), 38, 10) is null and 
                    src:RRK_PRECISION is not null) THEN
                    'RRK_PRECISION contains a non-numeric value : \'' || AS_VARCHAR(src:RRK_PRECISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_REVISION), '0'), 38, 10) is null and 
                    src:RRK_REVISION is not null) THEN
                    'RRK_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:RRK_REVISION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_SETUPLASTUPDATED), '1900-01-01')) is null and 
                    src:RRK_SETUPLASTUPDATED is not null) THEN
                    'RRK_SETUPLASTUPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:RRK_SETUPLASTUPDATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_UDFDATE01), '1900-01-01')) is null and 
                    src:RRK_UDFDATE01 is not null) THEN
                    'RRK_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:RRK_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_UDFDATE02), '1900-01-01')) is null and 
                    src:RRK_UDFDATE02 is not null) THEN
                    'RRK_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:RRK_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_UDFDATE03), '1900-01-01')) is null and 
                    src:RRK_UDFDATE03 is not null) THEN
                    'RRK_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:RRK_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_UDFDATE04), '1900-01-01')) is null and 
                    src:RRK_UDFDATE04 is not null) THEN
                    'RRK_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:RRK_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_UDFDATE05), '1900-01-01')) is null and 
                    src:RRK_UDFDATE05 is not null) THEN
                    'RRK_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:RRK_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_UDFNUM01), '0'), 38, 10) is null and 
                    src:RRK_UDFNUM01 is not null) THEN
                    'RRK_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:RRK_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_UDFNUM02), '0'), 38, 10) is null and 
                    src:RRK_UDFNUM02 is not null) THEN
                    'RRK_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:RRK_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_UDFNUM03), '0'), 38, 10) is null and 
                    src:RRK_UDFNUM03 is not null) THEN
                    'RRK_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:RRK_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_UDFNUM04), '0'), 38, 10) is null and 
                    src:RRK_UDFNUM04 is not null) THEN
                    'RRK_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:RRK_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_UDFNUM05), '0'), 38, 10) is null and 
                    src:RRK_UDFNUM05 is not null) THEN
                    'RRK_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:RRK_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:RRK_UPDATECOUNT is not null) THEN
                    'RRK_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:RRK_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_LASTSAVED), '1900-01-01')) is null and 
                src:RRK_LASTSAVED is not null) THEN
                'RRK_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:RRK_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:RRK_CODE,
                src:RRK_ORG,
                src:RRK_REVISION  ORDER BY 
            src:RRK_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5RELIABILITYRANKINGS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_CONDSCOREEND), '0'), 38, 10) is null and 
                    src:RRK_CONDSCOREEND is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_CONDSCORESTART), '0'), 38, 10) is null and 
                    src:RRK_CONDSCORESTART is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_CONDSCORETHRESHOLD), '0'), 38, 10) is null and 
                    src:RRK_CONDSCORETHRESHOLD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_CORRSCORERANKINGREV), '0'), 38, 10) is null and 
                    src:RRK_CORRSCORERANKINGREV is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_CREATED), '1900-01-01')) is null and 
                    src:RRK_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_LASTSAVED), '1900-01-01')) is null and 
                    src:RRK_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_PRECISION), '0'), 38, 10) is null and 
                    src:RRK_PRECISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_REVISION), '0'), 38, 10) is null and 
                    src:RRK_REVISION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_SETUPLASTUPDATED), '1900-01-01')) is null and 
                    src:RRK_SETUPLASTUPDATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_UDFDATE01), '1900-01-01')) is null and 
                    src:RRK_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_UDFDATE02), '1900-01-01')) is null and 
                    src:RRK_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_UDFDATE03), '1900-01-01')) is null and 
                    src:RRK_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_UDFDATE04), '1900-01-01')) is null and 
                    src:RRK_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_UDFDATE05), '1900-01-01')) is null and 
                    src:RRK_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_UDFNUM01), '0'), 38, 10) is null and 
                    src:RRK_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_UDFNUM02), '0'), 38, 10) is null and 
                    src:RRK_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_UDFNUM03), '0'), 38, 10) is null and 
                    src:RRK_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_UDFNUM04), '0'), 38, 10) is null and 
                    src:RRK_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_UDFNUM05), '0'), 38, 10) is null and 
                    src:RRK_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRK_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:RRK_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRK_LASTSAVED), '1900-01-01')) is null and 
                src:RRK_LASTSAVED is not null)