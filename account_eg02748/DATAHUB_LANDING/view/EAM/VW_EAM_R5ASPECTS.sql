CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ASPECTS AS SELECT
                        src:ASP_CLASS::varchar AS ASP_CLASS, 
                        src:ASP_CLASS_ORG::varchar AS ASP_CLASS_ORG, 
                        src:ASP_CODE::varchar AS ASP_CODE, 
                        src:ASP_CREATED::datetime AS ASP_CREATED, 
                        src:ASP_DESC::varchar AS ASP_DESC, 
                        src:ASP_LASTSAVED::datetime AS ASP_LASTSAVED, 
                        src:ASP_NOTUSED::varchar AS ASP_NOTUSED, 
                        src:ASP_PARENT::varchar AS ASP_PARENT, 
                        src:ASP_RANDOM::varchar AS ASP_RANDOM, 
                        src:ASP_TIMEDEP::varchar AS ASP_TIMEDEP, 
                        src:ASP_UDFCHAR01::varchar AS ASP_UDFCHAR01, 
                        src:ASP_UDFCHAR02::varchar AS ASP_UDFCHAR02, 
                        src:ASP_UDFCHAR03::varchar AS ASP_UDFCHAR03, 
                        src:ASP_UDFCHAR04::varchar AS ASP_UDFCHAR04, 
                        src:ASP_UDFCHAR05::varchar AS ASP_UDFCHAR05, 
                        src:ASP_UDFCHAR06::varchar AS ASP_UDFCHAR06, 
                        src:ASP_UDFCHAR07::varchar AS ASP_UDFCHAR07, 
                        src:ASP_UDFCHAR08::varchar AS ASP_UDFCHAR08, 
                        src:ASP_UDFCHAR09::varchar AS ASP_UDFCHAR09, 
                        src:ASP_UDFCHAR10::varchar AS ASP_UDFCHAR10, 
                        src:ASP_UDFCHAR11::varchar AS ASP_UDFCHAR11, 
                        src:ASP_UDFCHAR12::varchar AS ASP_UDFCHAR12, 
                        src:ASP_UDFCHAR13::varchar AS ASP_UDFCHAR13, 
                        src:ASP_UDFCHAR14::varchar AS ASP_UDFCHAR14, 
                        src:ASP_UDFCHAR15::varchar AS ASP_UDFCHAR15, 
                        src:ASP_UDFCHAR16::varchar AS ASP_UDFCHAR16, 
                        src:ASP_UDFCHAR17::varchar AS ASP_UDFCHAR17, 
                        src:ASP_UDFCHAR18::varchar AS ASP_UDFCHAR18, 
                        src:ASP_UDFCHAR19::varchar AS ASP_UDFCHAR19, 
                        src:ASP_UDFCHAR20::varchar AS ASP_UDFCHAR20, 
                        src:ASP_UDFCHAR21::varchar AS ASP_UDFCHAR21, 
                        src:ASP_UDFCHAR22::varchar AS ASP_UDFCHAR22, 
                        src:ASP_UDFCHAR23::varchar AS ASP_UDFCHAR23, 
                        src:ASP_UDFCHAR24::varchar AS ASP_UDFCHAR24, 
                        src:ASP_UDFCHAR25::varchar AS ASP_UDFCHAR25, 
                        src:ASP_UDFCHAR26::varchar AS ASP_UDFCHAR26, 
                        src:ASP_UDFCHAR27::varchar AS ASP_UDFCHAR27, 
                        src:ASP_UDFCHAR28::varchar AS ASP_UDFCHAR28, 
                        src:ASP_UDFCHAR29::varchar AS ASP_UDFCHAR29, 
                        src:ASP_UDFCHAR30::varchar AS ASP_UDFCHAR30, 
                        src:ASP_UDFCHKBOX01::varchar AS ASP_UDFCHKBOX01, 
                        src:ASP_UDFCHKBOX02::varchar AS ASP_UDFCHKBOX02, 
                        src:ASP_UDFCHKBOX03::varchar AS ASP_UDFCHKBOX03, 
                        src:ASP_UDFCHKBOX04::varchar AS ASP_UDFCHKBOX04, 
                        src:ASP_UDFCHKBOX05::varchar AS ASP_UDFCHKBOX05, 
                        src:ASP_UDFDATE01::datetime AS ASP_UDFDATE01, 
                        src:ASP_UDFDATE02::datetime AS ASP_UDFDATE02, 
                        src:ASP_UDFDATE03::datetime AS ASP_UDFDATE03, 
                        src:ASP_UDFDATE04::datetime AS ASP_UDFDATE04, 
                        src:ASP_UDFDATE05::datetime AS ASP_UDFDATE05, 
                        src:ASP_UDFNUM01::numeric(38, 10) AS ASP_UDFNUM01, 
                        src:ASP_UDFNUM02::numeric(38, 10) AS ASP_UDFNUM02, 
                        src:ASP_UDFNUM03::numeric(38, 10) AS ASP_UDFNUM03, 
                        src:ASP_UDFNUM04::numeric(38, 10) AS ASP_UDFNUM04, 
                        src:ASP_UDFNUM05::numeric(38, 10) AS ASP_UDFNUM05, 
                        src:ASP_UPDATECOUNT::numeric(38, 10) AS ASP_UPDATECOUNT, 
                        src:ASP_UPDATED::datetime AS ASP_UPDATED, 
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
    
                        
                src:ASP_CODE,
            src:ASP_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ASP_CODE  ORDER BY 
            src:ASP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ASPECTS as strm))