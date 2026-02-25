CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5OBJECTASPECTS AS SELECT
                        src:OBA_ASPECT::varchar AS OBA_ASPECT, 
                        src:OBA_CONFRATING::varchar AS OBA_CONFRATING, 
                        src:OBA_FACTOR::numeric(38, 10) AS OBA_FACTOR, 
                        src:OBA_LASTSAVED::datetime AS OBA_LASTSAVED, 
                        src:OBA_MAX::numeric(38, 10) AS OBA_MAX, 
                        src:OBA_MAXEXTR::numeric(38, 10) AS OBA_MAXEXTR, 
                        src:OBA_MAXFORMULA::varchar AS OBA_MAXFORMULA, 
                        src:OBA_MAXPPM::varchar AS OBA_MAXPPM, 
                        src:OBA_MAXSTWO::varchar AS OBA_MAXSTWO, 
                        src:OBA_MAXTOL::numeric(38, 10) AS OBA_MAXTOL, 
                        src:OBA_METHOD::varchar AS OBA_METHOD, 
                        src:OBA_MIN::numeric(38, 10) AS OBA_MIN, 
                        src:OBA_MINEXTR::numeric(38, 10) AS OBA_MINEXTR, 
                        src:OBA_MINFORMULA::varchar AS OBA_MINFORMULA, 
                        src:OBA_MINPPM::varchar AS OBA_MINPPM, 
                        src:OBA_MINSTWO::varchar AS OBA_MINSTWO, 
                        src:OBA_MINTOL::numeric(38, 10) AS OBA_MINTOL, 
                        src:OBA_NOMINAL::numeric(38, 10) AS OBA_NOMINAL, 
                        src:OBA_OBJECT::varchar AS OBA_OBJECT, 
                        src:OBA_OBJECT_ORG::varchar AS OBA_OBJECT_ORG, 
                        src:OBA_OBRTYPE::varchar AS OBA_OBRTYPE, 
                        src:OBA_OBTYPE::varchar AS OBA_OBTYPE, 
                        src:OBA_REPLACEFREQ::numeric(38, 10) AS OBA_REPLACEFREQ, 
                        src:OBA_RISK::varchar AS OBA_RISK, 
                        src:OBA_UDFCHAR01::varchar AS OBA_UDFCHAR01, 
                        src:OBA_UDFCHAR02::varchar AS OBA_UDFCHAR02, 
                        src:OBA_UDFCHAR03::varchar AS OBA_UDFCHAR03, 
                        src:OBA_UDFCHAR04::varchar AS OBA_UDFCHAR04, 
                        src:OBA_UDFCHAR05::varchar AS OBA_UDFCHAR05, 
                        src:OBA_UDFCHAR06::varchar AS OBA_UDFCHAR06, 
                        src:OBA_UDFCHAR07::varchar AS OBA_UDFCHAR07, 
                        src:OBA_UDFCHAR08::varchar AS OBA_UDFCHAR08, 
                        src:OBA_UDFCHAR09::varchar AS OBA_UDFCHAR09, 
                        src:OBA_UDFCHAR10::varchar AS OBA_UDFCHAR10, 
                        src:OBA_UDFCHAR11::varchar AS OBA_UDFCHAR11, 
                        src:OBA_UDFCHAR12::varchar AS OBA_UDFCHAR12, 
                        src:OBA_UDFCHAR13::varchar AS OBA_UDFCHAR13, 
                        src:OBA_UDFCHAR14::varchar AS OBA_UDFCHAR14, 
                        src:OBA_UDFCHAR15::varchar AS OBA_UDFCHAR15, 
                        src:OBA_UDFCHAR16::varchar AS OBA_UDFCHAR16, 
                        src:OBA_UDFCHAR17::varchar AS OBA_UDFCHAR17, 
                        src:OBA_UDFCHAR18::varchar AS OBA_UDFCHAR18, 
                        src:OBA_UDFCHAR19::varchar AS OBA_UDFCHAR19, 
                        src:OBA_UDFCHAR20::varchar AS OBA_UDFCHAR20, 
                        src:OBA_UDFCHAR21::varchar AS OBA_UDFCHAR21, 
                        src:OBA_UDFCHAR22::varchar AS OBA_UDFCHAR22, 
                        src:OBA_UDFCHAR23::varchar AS OBA_UDFCHAR23, 
                        src:OBA_UDFCHAR24::varchar AS OBA_UDFCHAR24, 
                        src:OBA_UDFCHAR25::varchar AS OBA_UDFCHAR25, 
                        src:OBA_UDFCHAR26::varchar AS OBA_UDFCHAR26, 
                        src:OBA_UDFCHAR27::varchar AS OBA_UDFCHAR27, 
                        src:OBA_UDFCHAR28::varchar AS OBA_UDFCHAR28, 
                        src:OBA_UDFCHAR29::varchar AS OBA_UDFCHAR29, 
                        src:OBA_UDFCHAR30::varchar AS OBA_UDFCHAR30, 
                        src:OBA_UDFCHKBOX01::varchar AS OBA_UDFCHKBOX01, 
                        src:OBA_UDFCHKBOX02::varchar AS OBA_UDFCHKBOX02, 
                        src:OBA_UDFCHKBOX03::varchar AS OBA_UDFCHKBOX03, 
                        src:OBA_UDFCHKBOX04::varchar AS OBA_UDFCHKBOX04, 
                        src:OBA_UDFCHKBOX05::varchar AS OBA_UDFCHKBOX05, 
                        src:OBA_UDFDATE01::datetime AS OBA_UDFDATE01, 
                        src:OBA_UDFDATE02::datetime AS OBA_UDFDATE02, 
                        src:OBA_UDFDATE03::datetime AS OBA_UDFDATE03, 
                        src:OBA_UDFDATE04::datetime AS OBA_UDFDATE04, 
                        src:OBA_UDFDATE05::datetime AS OBA_UDFDATE05, 
                        src:OBA_UDFNUM01::numeric(38, 10) AS OBA_UDFNUM01, 
                        src:OBA_UDFNUM02::numeric(38, 10) AS OBA_UDFNUM02, 
                        src:OBA_UDFNUM03::numeric(38, 10) AS OBA_UDFNUM03, 
                        src:OBA_UDFNUM04::numeric(38, 10) AS OBA_UDFNUM04, 
                        src:OBA_UDFNUM05::numeric(38, 10) AS OBA_UDFNUM05, 
                        src:OBA_UOM::varchar AS OBA_UOM, 
                        src:OBA_UPDATECOUNT::numeric(38, 10) AS OBA_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:OBA_LASTSAVED::datetime as ETL_LASTSAVED,
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
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
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBA_FACTOR), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OBA_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBA_MAX), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBA_MAXEXTR), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBA_MAXTOL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBA_MIN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBA_MINEXTR), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBA_MINTOL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBA_NOMINAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBA_REPLACEFREQ), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OBA_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OBA_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OBA_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OBA_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OBA_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBA_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBA_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBA_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBA_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBA_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBA_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OBA_LASTSAVED), '1900-01-01')) is not null