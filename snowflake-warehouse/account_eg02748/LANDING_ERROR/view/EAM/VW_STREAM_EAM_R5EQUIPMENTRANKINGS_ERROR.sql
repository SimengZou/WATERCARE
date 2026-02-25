CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5EQUIPMENTRANKINGS_ERROR AS SELECT src, 'EAM_R5EQUIPMENTRANKINGS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_CORRSCOREDATE), '1900-01-01')) is null and 
                    src:EQR_CORRSCOREDATE is not null) THEN
                    'EQR_CORRSCOREDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EQR_CORRSCOREDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_CORRSCORERANKINGREV), '0'), 38, 10) is null and 
                    src:EQR_CORRSCORERANKINGREV is not null) THEN
                    'EQR_CORRSCORERANKINGREV contains a non-numeric value : \'' || AS_VARCHAR(src:EQR_CORRSCORERANKINGREV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_CORRSCORESCORE), '0'), 38, 10) is null and 
                    src:EQR_CORRSCORESCORE is not null) THEN
                    'EQR_CORRSCORESCORE contains a non-numeric value : \'' || AS_VARCHAR(src:EQR_CORRSCORESCORE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_CREATED), '1900-01-01')) is null and 
                    src:EQR_CREATED is not null) THEN
                    'EQR_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:EQR_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_LASTSAVED), '1900-01-01')) is null and 
                    src:EQR_LASTSAVED is not null) THEN
                    'EQR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:EQR_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_NEXTREFRESH), '1900-01-01')) is null and 
                    src:EQR_NEXTREFRESH is not null) THEN
                    'EQR_NEXTREFRESH contains a non-timestamp value : \'' || AS_VARCHAR(src:EQR_NEXTREFRESH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_RANKINGREVISION), '0'), 38, 10) is null and 
                    src:EQR_RANKINGREVISION is not null) THEN
                    'EQR_RANKINGREVISION contains a non-numeric value : \'' || AS_VARCHAR(src:EQR_RANKINGREVISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_RANKINGSCORE), '0'), 38, 10) is null and 
                    src:EQR_RANKINGSCORE is not null) THEN
                    'EQR_RANKINGSCORE contains a non-numeric value : \'' || AS_VARCHAR(src:EQR_RANKINGSCORE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_REFRESHEVERY), '0'), 38, 10) is null and 
                    src:EQR_REFRESHEVERY is not null) THEN
                    'EQR_REFRESHEVERY contains a non-numeric value : \'' || AS_VARCHAR(src:EQR_REFRESHEVERY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_REFRESHSEQUENCE), '0'), 38, 10) is null and 
                    src:EQR_REFRESHSEQUENCE is not null) THEN
                    'EQR_REFRESHSEQUENCE contains a non-numeric value : \'' || AS_VARCHAR(src:EQR_REFRESHSEQUENCE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_SURVEYLASTUPDATED), '1900-01-01')) is null and 
                    src:EQR_SURVEYLASTUPDATED is not null) THEN
                    'EQR_SURVEYLASTUPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:EQR_SURVEYLASTUPDATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_UDFDATE01), '1900-01-01')) is null and 
                    src:EQR_UDFDATE01 is not null) THEN
                    'EQR_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:EQR_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_UDFDATE02), '1900-01-01')) is null and 
                    src:EQR_UDFDATE02 is not null) THEN
                    'EQR_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:EQR_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_UDFDATE03), '1900-01-01')) is null and 
                    src:EQR_UDFDATE03 is not null) THEN
                    'EQR_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:EQR_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_UDFDATE04), '1900-01-01')) is null and 
                    src:EQR_UDFDATE04 is not null) THEN
                    'EQR_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:EQR_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_UDFDATE05), '1900-01-01')) is null and 
                    src:EQR_UDFDATE05 is not null) THEN
                    'EQR_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:EQR_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_UDFNUM01), '0'), 38, 10) is null and 
                    src:EQR_UDFNUM01 is not null) THEN
                    'EQR_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:EQR_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_UDFNUM02), '0'), 38, 10) is null and 
                    src:EQR_UDFNUM02 is not null) THEN
                    'EQR_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:EQR_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_UDFNUM03), '0'), 38, 10) is null and 
                    src:EQR_UDFNUM03 is not null) THEN
                    'EQR_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:EQR_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_UDFNUM04), '0'), 38, 10) is null and 
                    src:EQR_UDFNUM04 is not null) THEN
                    'EQR_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:EQR_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_UDFNUM05), '0'), 38, 10) is null and 
                    src:EQR_UDFNUM05 is not null) THEN
                    'EQR_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:EQR_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:EQR_UPDATECOUNT is not null) THEN
                    'EQR_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:EQR_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_UPDATED), '1900-01-01')) is null and 
                    src:EQR_UPDATED is not null) THEN
                    'EQR_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:EQR_UPDATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_VALUESLASTCALCULATED), '1900-01-01')) is null and 
                    src:EQR_VALUESLASTCALCULATED is not null) THEN
                    'EQR_VALUESLASTCALCULATED contains a non-timestamp value : \'' || AS_VARCHAR(src:EQR_VALUESLASTCALCULATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_LASTSAVED), '1900-01-01')) is null and 
                src:EQR_LASTSAVED is not null) THEN
                'EQR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:EQR_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:EQR_OBJCODE,
                src:EQR_OBJORG,
                src:EQR_RANKINGCODE,
                src:EQR_RANKINGORG,
                src:EQR_RANKINGREVISION  ORDER BY 
            src:EQR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5EQUIPMENTRANKINGS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_CORRSCOREDATE), '1900-01-01')) is null and 
                    src:EQR_CORRSCOREDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_CORRSCORERANKINGREV), '0'), 38, 10) is null and 
                    src:EQR_CORRSCORERANKINGREV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_CORRSCORESCORE), '0'), 38, 10) is null and 
                    src:EQR_CORRSCORESCORE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_CREATED), '1900-01-01')) is null and 
                    src:EQR_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_LASTSAVED), '1900-01-01')) is null and 
                    src:EQR_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_NEXTREFRESH), '1900-01-01')) is null and 
                    src:EQR_NEXTREFRESH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_RANKINGREVISION), '0'), 38, 10) is null and 
                    src:EQR_RANKINGREVISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_RANKINGSCORE), '0'), 38, 10) is null and 
                    src:EQR_RANKINGSCORE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_REFRESHEVERY), '0'), 38, 10) is null and 
                    src:EQR_REFRESHEVERY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_REFRESHSEQUENCE), '0'), 38, 10) is null and 
                    src:EQR_REFRESHSEQUENCE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_SURVEYLASTUPDATED), '1900-01-01')) is null and 
                    src:EQR_SURVEYLASTUPDATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_UDFDATE01), '1900-01-01')) is null and 
                    src:EQR_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_UDFDATE02), '1900-01-01')) is null and 
                    src:EQR_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_UDFDATE03), '1900-01-01')) is null and 
                    src:EQR_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_UDFDATE04), '1900-01-01')) is null and 
                    src:EQR_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_UDFDATE05), '1900-01-01')) is null and 
                    src:EQR_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_UDFNUM01), '0'), 38, 10) is null and 
                    src:EQR_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_UDFNUM02), '0'), 38, 10) is null and 
                    src:EQR_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_UDFNUM03), '0'), 38, 10) is null and 
                    src:EQR_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_UDFNUM04), '0'), 38, 10) is null and 
                    src:EQR_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_UDFNUM05), '0'), 38, 10) is null and 
                    src:EQR_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EQR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:EQR_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_UPDATED), '1900-01-01')) is null and 
                    src:EQR_UPDATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_VALUESLASTCALCULATED), '1900-01-01')) is null and 
                    src:EQR_VALUESLASTCALCULATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EQR_LASTSAVED), '1900-01-01')) is null and 
                src:EQR_LASTSAVED is not null)