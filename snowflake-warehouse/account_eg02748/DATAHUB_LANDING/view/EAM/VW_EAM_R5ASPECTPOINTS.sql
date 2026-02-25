CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ASPECTPOINTS AS SELECT
                        src:APO_ASPECT::varchar AS APO_ASPECT, 
                        src:APO_CONFRATING::varchar AS APO_CONFRATING, 
                        src:APO_FACTOR::numeric(38, 10) AS APO_FACTOR, 
                        src:APO_LASTSAVED::datetime AS APO_LASTSAVED, 
                        src:APO_MAX::numeric(38, 10) AS APO_MAX, 
                        src:APO_MAXEXTR::numeric(38, 10) AS APO_MAXEXTR, 
                        src:APO_MAXFORMULA::varchar AS APO_MAXFORMULA, 
                        src:APO_MAXPPM::varchar AS APO_MAXPPM, 
                        src:APO_MAXSTWO::varchar AS APO_MAXSTWO, 
                        src:APO_MAXTOL::numeric(38, 10) AS APO_MAXTOL, 
                        src:APO_METHOD::varchar AS APO_METHOD, 
                        src:APO_MIN::numeric(38, 10) AS APO_MIN, 
                        src:APO_MINEXTR::numeric(38, 10) AS APO_MINEXTR, 
                        src:APO_MINFORMULA::varchar AS APO_MINFORMULA, 
                        src:APO_MINPPM::varchar AS APO_MINPPM, 
                        src:APO_MINSTWO::varchar AS APO_MINSTWO, 
                        src:APO_MINTOL::numeric(38, 10) AS APO_MINTOL, 
                        src:APO_NOMINAL::numeric(38, 10) AS APO_NOMINAL, 
                        src:APO_OBJECT::varchar AS APO_OBJECT, 
                        src:APO_OBJECT_ORG::varchar AS APO_OBJECT_ORG, 
                        src:APO_OBRTYPE::varchar AS APO_OBRTYPE, 
                        src:APO_OBTYPE::varchar AS APO_OBTYPE, 
                        src:APO_POINT::varchar AS APO_POINT, 
                        src:APO_POINTTYPE::varchar AS APO_POINTTYPE, 
                        src:APO_REPLACEFREQ::numeric(38, 10) AS APO_REPLACEFREQ, 
                        src:APO_RISK::varchar AS APO_RISK, 
                        src:APO_UDFCHAR01::varchar AS APO_UDFCHAR01, 
                        src:APO_UDFCHAR02::varchar AS APO_UDFCHAR02, 
                        src:APO_UDFCHAR03::varchar AS APO_UDFCHAR03, 
                        src:APO_UDFCHAR04::varchar AS APO_UDFCHAR04, 
                        src:APO_UDFCHAR05::varchar AS APO_UDFCHAR05, 
                        src:APO_UDFCHAR06::varchar AS APO_UDFCHAR06, 
                        src:APO_UDFCHAR07::varchar AS APO_UDFCHAR07, 
                        src:APO_UDFCHAR08::varchar AS APO_UDFCHAR08, 
                        src:APO_UDFCHAR09::varchar AS APO_UDFCHAR09, 
                        src:APO_UDFCHAR10::varchar AS APO_UDFCHAR10, 
                        src:APO_UDFCHAR11::varchar AS APO_UDFCHAR11, 
                        src:APO_UDFCHAR12::varchar AS APO_UDFCHAR12, 
                        src:APO_UDFCHAR13::varchar AS APO_UDFCHAR13, 
                        src:APO_UDFCHAR14::varchar AS APO_UDFCHAR14, 
                        src:APO_UDFCHAR15::varchar AS APO_UDFCHAR15, 
                        src:APO_UDFCHAR16::varchar AS APO_UDFCHAR16, 
                        src:APO_UDFCHAR17::varchar AS APO_UDFCHAR17, 
                        src:APO_UDFCHAR18::varchar AS APO_UDFCHAR18, 
                        src:APO_UDFCHAR19::varchar AS APO_UDFCHAR19, 
                        src:APO_UDFCHAR20::varchar AS APO_UDFCHAR20, 
                        src:APO_UDFCHAR21::varchar AS APO_UDFCHAR21, 
                        src:APO_UDFCHAR22::varchar AS APO_UDFCHAR22, 
                        src:APO_UDFCHAR23::varchar AS APO_UDFCHAR23, 
                        src:APO_UDFCHAR24::varchar AS APO_UDFCHAR24, 
                        src:APO_UDFCHAR25::varchar AS APO_UDFCHAR25, 
                        src:APO_UDFCHAR26::varchar AS APO_UDFCHAR26, 
                        src:APO_UDFCHAR27::varchar AS APO_UDFCHAR27, 
                        src:APO_UDFCHAR28::varchar AS APO_UDFCHAR28, 
                        src:APO_UDFCHAR29::varchar AS APO_UDFCHAR29, 
                        src:APO_UDFCHAR30::varchar AS APO_UDFCHAR30, 
                        src:APO_UDFCHKBOX01::varchar AS APO_UDFCHKBOX01, 
                        src:APO_UDFCHKBOX02::varchar AS APO_UDFCHKBOX02, 
                        src:APO_UDFCHKBOX03::varchar AS APO_UDFCHKBOX03, 
                        src:APO_UDFCHKBOX04::varchar AS APO_UDFCHKBOX04, 
                        src:APO_UDFCHKBOX05::varchar AS APO_UDFCHKBOX05, 
                        src:APO_UDFDATE01::datetime AS APO_UDFDATE01, 
                        src:APO_UDFDATE02::datetime AS APO_UDFDATE02, 
                        src:APO_UDFDATE03::datetime AS APO_UDFDATE03, 
                        src:APO_UDFDATE04::datetime AS APO_UDFDATE04, 
                        src:APO_UDFDATE05::datetime AS APO_UDFDATE05, 
                        src:APO_UDFNUM01::numeric(38, 10) AS APO_UDFNUM01, 
                        src:APO_UDFNUM02::numeric(38, 10) AS APO_UDFNUM02, 
                        src:APO_UDFNUM03::numeric(38, 10) AS APO_UDFNUM03, 
                        src:APO_UDFNUM04::numeric(38, 10) AS APO_UDFNUM04, 
                        src:APO_UDFNUM05::numeric(38, 10) AS APO_UDFNUM05, 
                        src:APO_UOM::varchar AS APO_UOM, 
                        src:APO_UPDATECOUNT::numeric(38, 10) AS APO_UPDATECOUNT, 
                        src:APO_UPDATED::datetime AS APO_UPDATED, 
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
    
                        
                src:APO_ASPECT,
                src:APO_OBJECT,
                src:APO_OBJECT_ORG,
                src:APO_OBTYPE,
                src:APO_POINT,
                src:APO_POINTTYPE,
            src:APO_LASTSAVED
                ,src,
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
                FROM DATAHUB_LANDING.EAM_R5ASPECTPOINTS as strm))