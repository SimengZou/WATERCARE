CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ORGANIZATION AS SELECT
                        src:ORG_ACCOUNTINGENTITY::varchar AS ORG_ACCOUNTINGENTITY, 
                        src:ORG_BOOKID::varchar AS ORG_BOOKID, 
                        src:ORG_CALGROUPCODE::varchar AS ORG_CALGROUPCODE, 
                        src:ORG_CALGROUPORG::varchar AS ORG_CALGROUPORG, 
                        src:ORG_CODE::varchar AS ORG_CODE, 
                        src:ORG_CODEREF::varchar AS ORG_CODEREF, 
                        src:ORG_COMMON::varchar AS ORG_COMMON, 
                        src:ORG_CREATED::datetime AS ORG_CREATED, 
                        src:ORG_CURR::varchar AS ORG_CURR, 
                        src:ORG_DEPMETHOD::varchar AS ORG_DEPMETHOD, 
                        src:ORG_DEPRMETHOD::varchar AS ORG_DEPRMETHOD, 
                        src:ORG_DESC::varchar AS ORG_DESC, 
                        src:ORG_DUNSNUM::varchar AS ORG_DUNSNUM, 
                        src:ORG_EXTERNALORG::varchar AS ORG_EXTERNALORG, 
                        src:ORG_INVQTYTOL::numeric(38, 10) AS ORG_INVQTYTOL, 
                        src:ORG_LASTSAVED::datetime AS ORG_LASTSAVED, 
                        src:ORG_LATITUDE::numeric(38, 10) AS ORG_LATITUDE, 
                        src:ORG_LOCALE::varchar AS ORG_LOCALE, 
                        src:ORG_LONGITUDE::numeric(38, 10) AS ORG_LONGITUDE, 
                        src:ORG_MATCHLTA::numeric(38, 10) AS ORG_MATCHLTA, 
                        src:ORG_MATCHLTP::numeric(38, 10) AS ORG_MATCHLTP, 
                        src:ORG_REQUESTRECALCDEP::varchar AS ORG_REQUESTRECALCDEP, 
                        src:ORG_SEGMENTVALUE::varchar AS ORG_SEGMENTVALUE, 
                        src:ORG_STREAMPLUSPROJECT::varchar AS ORG_STREAMPLUSPROJECT, 
                        src:ORG_TIMEZONE::numeric(38, 10) AS ORG_TIMEZONE, 
                        src:ORG_UDFCHAR01::varchar AS ORG_UDFCHAR01, 
                        src:ORG_UDFCHAR02::varchar AS ORG_UDFCHAR02, 
                        src:ORG_UDFCHAR03::varchar AS ORG_UDFCHAR03, 
                        src:ORG_UDFCHAR04::varchar AS ORG_UDFCHAR04, 
                        src:ORG_UDFCHAR05::varchar AS ORG_UDFCHAR05, 
                        src:ORG_UDFCHAR06::varchar AS ORG_UDFCHAR06, 
                        src:ORG_UDFCHAR07::varchar AS ORG_UDFCHAR07, 
                        src:ORG_UDFCHAR08::varchar AS ORG_UDFCHAR08, 
                        src:ORG_UDFCHAR09::varchar AS ORG_UDFCHAR09, 
                        src:ORG_UDFCHAR10::varchar AS ORG_UDFCHAR10, 
                        src:ORG_UDFCHKBOX01::varchar AS ORG_UDFCHKBOX01, 
                        src:ORG_UDFCHKBOX02::varchar AS ORG_UDFCHKBOX02, 
                        src:ORG_UDFCHKBOX03::varchar AS ORG_UDFCHKBOX03, 
                        src:ORG_UDFCHKBOX04::varchar AS ORG_UDFCHKBOX04, 
                        src:ORG_UDFCHKBOX05::varchar AS ORG_UDFCHKBOX05, 
                        src:ORG_UDFDATE01::datetime AS ORG_UDFDATE01, 
                        src:ORG_UDFDATE02::datetime AS ORG_UDFDATE02, 
                        src:ORG_UDFDATE03::datetime AS ORG_UDFDATE03, 
                        src:ORG_UDFDATE04::datetime AS ORG_UDFDATE04, 
                        src:ORG_UDFDATE05::datetime AS ORG_UDFDATE05, 
                        src:ORG_UDFNUM01::numeric(38, 10) AS ORG_UDFNUM01, 
                        src:ORG_UDFNUM02::numeric(38, 10) AS ORG_UDFNUM02, 
                        src:ORG_UDFNUM03::numeric(38, 10) AS ORG_UDFNUM03, 
                        src:ORG_UDFNUM04::numeric(38, 10) AS ORG_UDFNUM04, 
                        src:ORG_UDFNUM05::numeric(38, 10) AS ORG_UDFNUM05, 
                        src:ORG_UPDATECOUNT::numeric(38, 10) AS ORG_UPDATECOUNT, 
                        src:ORG_UPDATED::datetime AS ORG_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:ORG_CODE,
            src:ORG_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ORG_CODE  ORDER BY 
            src:ORG_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ORGANIZATION as strm))