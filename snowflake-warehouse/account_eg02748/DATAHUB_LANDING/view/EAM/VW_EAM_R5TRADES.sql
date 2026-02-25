CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5TRADES AS SELECT
                        src:TRD_ABBREVIATION::varchar AS TRD_ABBREVIATION, 
                        src:TRD_CLASS::varchar AS TRD_CLASS, 
                        src:TRD_CLASS_ORG::varchar AS TRD_CLASS_ORG, 
                        src:TRD_CODE::varchar AS TRD_CODE, 
                        src:TRD_CREATED::datetime AS TRD_CREATED, 
                        src:TRD_DESC::varchar AS TRD_DESC, 
                        src:TRD_LASTSAVED::datetime AS TRD_LASTSAVED, 
                        src:TRD_LASTSTATUSUPDATE::datetime AS TRD_LASTSTATUSUPDATE, 
                        src:TRD_NOTUSED::varchar AS TRD_NOTUSED, 
                        src:TRD_ORG::varchar AS TRD_ORG, 
                        src:TRD_UDFCHAR01::varchar AS TRD_UDFCHAR01, 
                        src:TRD_UDFCHAR02::varchar AS TRD_UDFCHAR02, 
                        src:TRD_UDFCHAR03::varchar AS TRD_UDFCHAR03, 
                        src:TRD_UDFCHAR04::varchar AS TRD_UDFCHAR04, 
                        src:TRD_UDFCHAR05::varchar AS TRD_UDFCHAR05, 
                        src:TRD_UDFCHAR06::varchar AS TRD_UDFCHAR06, 
                        src:TRD_UDFCHAR07::varchar AS TRD_UDFCHAR07, 
                        src:TRD_UDFCHAR08::varchar AS TRD_UDFCHAR08, 
                        src:TRD_UDFCHAR09::varchar AS TRD_UDFCHAR09, 
                        src:TRD_UDFCHAR10::varchar AS TRD_UDFCHAR10, 
                        src:TRD_UDFCHAR11::varchar AS TRD_UDFCHAR11, 
                        src:TRD_UDFCHAR12::varchar AS TRD_UDFCHAR12, 
                        src:TRD_UDFCHAR13::varchar AS TRD_UDFCHAR13, 
                        src:TRD_UDFCHAR14::varchar AS TRD_UDFCHAR14, 
                        src:TRD_UDFCHAR15::varchar AS TRD_UDFCHAR15, 
                        src:TRD_UDFCHAR16::varchar AS TRD_UDFCHAR16, 
                        src:TRD_UDFCHAR17::varchar AS TRD_UDFCHAR17, 
                        src:TRD_UDFCHAR18::varchar AS TRD_UDFCHAR18, 
                        src:TRD_UDFCHAR19::varchar AS TRD_UDFCHAR19, 
                        src:TRD_UDFCHAR20::varchar AS TRD_UDFCHAR20, 
                        src:TRD_UDFCHAR21::varchar AS TRD_UDFCHAR21, 
                        src:TRD_UDFCHAR22::varchar AS TRD_UDFCHAR22, 
                        src:TRD_UDFCHAR23::varchar AS TRD_UDFCHAR23, 
                        src:TRD_UDFCHAR24::varchar AS TRD_UDFCHAR24, 
                        src:TRD_UDFCHAR25::varchar AS TRD_UDFCHAR25, 
                        src:TRD_UDFCHAR26::varchar AS TRD_UDFCHAR26, 
                        src:TRD_UDFCHAR27::varchar AS TRD_UDFCHAR27, 
                        src:TRD_UDFCHAR28::varchar AS TRD_UDFCHAR28, 
                        src:TRD_UDFCHAR29::varchar AS TRD_UDFCHAR29, 
                        src:TRD_UDFCHAR30::varchar AS TRD_UDFCHAR30, 
                        src:TRD_UDFCHKBOX01::varchar AS TRD_UDFCHKBOX01, 
                        src:TRD_UDFCHKBOX02::varchar AS TRD_UDFCHKBOX02, 
                        src:TRD_UDFCHKBOX03::varchar AS TRD_UDFCHKBOX03, 
                        src:TRD_UDFCHKBOX04::varchar AS TRD_UDFCHKBOX04, 
                        src:TRD_UDFCHKBOX05::varchar AS TRD_UDFCHKBOX05, 
                        src:TRD_UDFDATE01::datetime AS TRD_UDFDATE01, 
                        src:TRD_UDFDATE02::datetime AS TRD_UDFDATE02, 
                        src:TRD_UDFDATE03::datetime AS TRD_UDFDATE03, 
                        src:TRD_UDFDATE04::datetime AS TRD_UDFDATE04, 
                        src:TRD_UDFDATE05::datetime AS TRD_UDFDATE05, 
                        src:TRD_UDFNUM01::numeric(38, 10) AS TRD_UDFNUM01, 
                        src:TRD_UDFNUM02::numeric(38, 10) AS TRD_UDFNUM02, 
                        src:TRD_UDFNUM03::numeric(38, 10) AS TRD_UDFNUM03, 
                        src:TRD_UDFNUM04::numeric(38, 10) AS TRD_UDFNUM04, 
                        src:TRD_UDFNUM05::numeric(38, 10) AS TRD_UDFNUM05, 
                        src:TRD_UPDATECOUNT::numeric(38, 10) AS TRD_UPDATECOUNT, 
                        src:TRD_UPDATED::datetime AS TRD_UPDATED, 
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
    
                        
                src:TRD_CODE,
            src:TRD_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:TRD_CODE  ORDER BY 
            src:TRD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5TRADES as strm))