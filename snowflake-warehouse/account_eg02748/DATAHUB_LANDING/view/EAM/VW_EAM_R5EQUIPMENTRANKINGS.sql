CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5EQUIPMENTRANKINGS AS SELECT
                        src:EQR_CALCULATIONERROR::varchar AS EQR_CALCULATIONERROR, 
                        src:EQR_CORRSCOREDATE::datetime AS EQR_CORRSCOREDATE, 
                        src:EQR_CORRSCOREINDEX::varchar AS EQR_CORRSCOREINDEX, 
                        src:EQR_CORRSCORERANKING::varchar AS EQR_CORRSCORERANKING, 
                        src:EQR_CORRSCORERANKINGREV::numeric(38, 10) AS EQR_CORRSCORERANKINGREV, 
                        src:EQR_CORRSCORERANKING_ORG::varchar AS EQR_CORRSCORERANKING_ORG, 
                        src:EQR_CORRSCORESCORE::numeric(38, 10) AS EQR_CORRSCORESCORE, 
                        src:EQR_CORRSCORESTATUS::varchar AS EQR_CORRSCORESTATUS, 
                        src:EQR_CREATED::datetime AS EQR_CREATED, 
                        src:EQR_CREATEDBY::varchar AS EQR_CREATEDBY, 
                        src:EQR_DEFAULT::varchar AS EQR_DEFAULT, 
                        src:EQR_LASTSAVED::datetime AS EQR_LASTSAVED, 
                        src:EQR_LOCKRANKINGVALUES::varchar AS EQR_LOCKRANKINGVALUES, 
                        src:EQR_NEXTREFRESH::datetime AS EQR_NEXTREFRESH, 
                        src:EQR_NOTE::varchar AS EQR_NOTE, 
                        src:EQR_OBJCODE::varchar AS EQR_OBJCODE, 
                        src:EQR_OBJORG::varchar AS EQR_OBJORG, 
                        src:EQR_RANKINGCODE::varchar AS EQR_RANKINGCODE, 
                        src:EQR_RANKINGINDEX::varchar AS EQR_RANKINGINDEX, 
                        src:EQR_RANKINGORG::varchar AS EQR_RANKINGORG, 
                        src:EQR_RANKINGREVISION::numeric(38, 10) AS EQR_RANKINGREVISION, 
                        src:EQR_RANKINGSCORE::numeric(38, 10) AS EQR_RANKINGSCORE, 
                        src:EQR_REFRESHEVERY::numeric(38, 10) AS EQR_REFRESHEVERY, 
                        src:EQR_REFRESHEVERYUNIT::varchar AS EQR_REFRESHEVERYUNIT, 
                        src:EQR_REFRESHSEQUENCE::numeric(38, 10) AS EQR_REFRESHSEQUENCE, 
                        src:EQR_SURVEYLASTUPDATED::datetime AS EQR_SURVEYLASTUPDATED, 
                        src:EQR_UDFCHAR01::varchar AS EQR_UDFCHAR01, 
                        src:EQR_UDFCHAR02::varchar AS EQR_UDFCHAR02, 
                        src:EQR_UDFCHAR03::varchar AS EQR_UDFCHAR03, 
                        src:EQR_UDFCHAR04::varchar AS EQR_UDFCHAR04, 
                        src:EQR_UDFCHAR05::varchar AS EQR_UDFCHAR05, 
                        src:EQR_UDFCHAR06::varchar AS EQR_UDFCHAR06, 
                        src:EQR_UDFCHAR07::varchar AS EQR_UDFCHAR07, 
                        src:EQR_UDFCHAR08::varchar AS EQR_UDFCHAR08, 
                        src:EQR_UDFCHAR09::varchar AS EQR_UDFCHAR09, 
                        src:EQR_UDFCHAR10::varchar AS EQR_UDFCHAR10, 
                        src:EQR_UDFCHAR11::varchar AS EQR_UDFCHAR11, 
                        src:EQR_UDFCHAR12::varchar AS EQR_UDFCHAR12, 
                        src:EQR_UDFCHAR13::varchar AS EQR_UDFCHAR13, 
                        src:EQR_UDFCHAR14::varchar AS EQR_UDFCHAR14, 
                        src:EQR_UDFCHAR15::varchar AS EQR_UDFCHAR15, 
                        src:EQR_UDFCHAR16::varchar AS EQR_UDFCHAR16, 
                        src:EQR_UDFCHAR17::varchar AS EQR_UDFCHAR17, 
                        src:EQR_UDFCHAR18::varchar AS EQR_UDFCHAR18, 
                        src:EQR_UDFCHAR19::varchar AS EQR_UDFCHAR19, 
                        src:EQR_UDFCHAR20::varchar AS EQR_UDFCHAR20, 
                        src:EQR_UDFCHAR21::varchar AS EQR_UDFCHAR21, 
                        src:EQR_UDFCHAR22::varchar AS EQR_UDFCHAR22, 
                        src:EQR_UDFCHAR23::varchar AS EQR_UDFCHAR23, 
                        src:EQR_UDFCHAR24::varchar AS EQR_UDFCHAR24, 
                        src:EQR_UDFCHAR25::varchar AS EQR_UDFCHAR25, 
                        src:EQR_UDFCHAR26::varchar AS EQR_UDFCHAR26, 
                        src:EQR_UDFCHAR27::varchar AS EQR_UDFCHAR27, 
                        src:EQR_UDFCHAR28::varchar AS EQR_UDFCHAR28, 
                        src:EQR_UDFCHAR29::varchar AS EQR_UDFCHAR29, 
                        src:EQR_UDFCHAR30::varchar AS EQR_UDFCHAR30, 
                        src:EQR_UDFCHKBOX01::varchar AS EQR_UDFCHKBOX01, 
                        src:EQR_UDFCHKBOX02::varchar AS EQR_UDFCHKBOX02, 
                        src:EQR_UDFCHKBOX03::varchar AS EQR_UDFCHKBOX03, 
                        src:EQR_UDFCHKBOX04::varchar AS EQR_UDFCHKBOX04, 
                        src:EQR_UDFCHKBOX05::varchar AS EQR_UDFCHKBOX05, 
                        src:EQR_UDFDATE01::datetime AS EQR_UDFDATE01, 
                        src:EQR_UDFDATE02::datetime AS EQR_UDFDATE02, 
                        src:EQR_UDFDATE03::datetime AS EQR_UDFDATE03, 
                        src:EQR_UDFDATE04::datetime AS EQR_UDFDATE04, 
                        src:EQR_UDFDATE05::datetime AS EQR_UDFDATE05, 
                        src:EQR_UDFNUM01::numeric(38, 10) AS EQR_UDFNUM01, 
                        src:EQR_UDFNUM02::numeric(38, 10) AS EQR_UDFNUM02, 
                        src:EQR_UDFNUM03::numeric(38, 10) AS EQR_UDFNUM03, 
                        src:EQR_UDFNUM04::numeric(38, 10) AS EQR_UDFNUM04, 
                        src:EQR_UDFNUM05::numeric(38, 10) AS EQR_UDFNUM05, 
                        src:EQR_UPDATECOUNT::numeric(38, 10) AS EQR_UPDATECOUNT, 
                        src:EQR_UPDATED::datetime AS EQR_UPDATED, 
                        src:EQR_UPDATEDBY::varchar AS EQR_UPDATEDBY, 
                        src:EQR_VALUESLASTCALCULATED::datetime AS EQR_VALUESLASTCALCULATED, 
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
    
                        
                src:EQR_OBJCODE,
                src:EQR_OBJORG,
                src:EQR_RANKINGCODE,
                src:EQR_RANKINGORG,
                src:EQR_RANKINGREVISION,
            src:EQR_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:EQR_OBJCODE,
                src:EQR_OBJORG,
                src:EQR_RANKINGCODE,
                src:EQR_RANKINGORG,
                src:EQR_RANKINGREVISION  ORDER BY 
            src:EQR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5EQUIPMENTRANKINGS as strm))