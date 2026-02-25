CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5STANDWACTS AS SELECT
                        src:WAC_ACT::numeric(38, 10) AS WAC_ACT, 
                        src:WAC_ASMLEVEL::varchar AS WAC_ASMLEVEL, 
                        src:WAC_COMPLEVEL::varchar AS WAC_COMPLEVEL, 
                        src:WAC_CREATED::datetime AS WAC_CREATED, 
                        src:WAC_DURATION::numeric(38, 10) AS WAC_DURATION, 
                        src:WAC_EST::numeric(38, 10) AS WAC_EST, 
                        src:WAC_HIRE::varchar AS WAC_HIRE, 
                        src:WAC_LABORRTYPE::varchar AS WAC_LABORRTYPE, 
                        src:WAC_LABORTYPE::varchar AS WAC_LABORTYPE, 
                        src:WAC_LASTSAVED::datetime AS WAC_LASTSAVED, 
                        src:WAC_MANUFACT::varchar AS WAC_MANUFACT, 
                        src:WAC_MATLIST::varchar AS WAC_MATLIST, 
                        src:WAC_NOTE::varchar AS WAC_NOTE, 
                        src:WAC_PERSONS::numeric(38, 10) AS WAC_PERSONS, 
                        src:WAC_QTY::numeric(38, 10) AS WAC_QTY, 
                        src:WAC_RPC::varchar AS WAC_RPC, 
                        src:WAC_SPECIAL::varchar AS WAC_SPECIAL, 
                        src:WAC_STANDWORK::varchar AS WAC_STANDWORK, 
                        src:WAC_START::numeric(38, 10) AS WAC_START, 
                        src:WAC_SUPPLIER::varchar AS WAC_SUPPLIER, 
                        src:WAC_SUPPLIER_ORG::varchar AS WAC_SUPPLIER_ORG, 
                        src:WAC_SYSLEVEL::varchar AS WAC_SYSLEVEL, 
                        src:WAC_TASK::varchar AS WAC_TASK, 
                        src:WAC_TPF::varchar AS WAC_TPF, 
                        src:WAC_TRADE::varchar AS WAC_TRADE, 
                        src:WAC_UDFCHAR01::varchar AS WAC_UDFCHAR01, 
                        src:WAC_UDFCHAR02::varchar AS WAC_UDFCHAR02, 
                        src:WAC_UDFCHAR03::varchar AS WAC_UDFCHAR03, 
                        src:WAC_UDFCHAR04::varchar AS WAC_UDFCHAR04, 
                        src:WAC_UDFCHAR05::varchar AS WAC_UDFCHAR05, 
                        src:WAC_UDFCHAR06::varchar AS WAC_UDFCHAR06, 
                        src:WAC_UDFCHAR07::varchar AS WAC_UDFCHAR07, 
                        src:WAC_UDFCHAR08::varchar AS WAC_UDFCHAR08, 
                        src:WAC_UDFCHAR09::varchar AS WAC_UDFCHAR09, 
                        src:WAC_UDFCHAR10::varchar AS WAC_UDFCHAR10, 
                        src:WAC_UDFCHAR11::varchar AS WAC_UDFCHAR11, 
                        src:WAC_UDFCHAR12::varchar AS WAC_UDFCHAR12, 
                        src:WAC_UDFCHAR13::varchar AS WAC_UDFCHAR13, 
                        src:WAC_UDFCHAR14::varchar AS WAC_UDFCHAR14, 
                        src:WAC_UDFCHAR15::varchar AS WAC_UDFCHAR15, 
                        src:WAC_UDFCHAR16::varchar AS WAC_UDFCHAR16, 
                        src:WAC_UDFCHAR17::varchar AS WAC_UDFCHAR17, 
                        src:WAC_UDFCHAR18::varchar AS WAC_UDFCHAR18, 
                        src:WAC_UDFCHAR19::varchar AS WAC_UDFCHAR19, 
                        src:WAC_UDFCHAR20::varchar AS WAC_UDFCHAR20, 
                        src:WAC_UDFCHAR21::varchar AS WAC_UDFCHAR21, 
                        src:WAC_UDFCHAR22::varchar AS WAC_UDFCHAR22, 
                        src:WAC_UDFCHAR23::varchar AS WAC_UDFCHAR23, 
                        src:WAC_UDFCHAR24::varchar AS WAC_UDFCHAR24, 
                        src:WAC_UDFCHAR25::varchar AS WAC_UDFCHAR25, 
                        src:WAC_UDFCHAR26::varchar AS WAC_UDFCHAR26, 
                        src:WAC_UDFCHAR27::varchar AS WAC_UDFCHAR27, 
                        src:WAC_UDFCHAR28::varchar AS WAC_UDFCHAR28, 
                        src:WAC_UDFCHAR29::varchar AS WAC_UDFCHAR29, 
                        src:WAC_UDFCHAR30::varchar AS WAC_UDFCHAR30, 
                        src:WAC_UDFCHKBOX01::varchar AS WAC_UDFCHKBOX01, 
                        src:WAC_UDFCHKBOX02::varchar AS WAC_UDFCHKBOX02, 
                        src:WAC_UDFCHKBOX03::varchar AS WAC_UDFCHKBOX03, 
                        src:WAC_UDFCHKBOX04::varchar AS WAC_UDFCHKBOX04, 
                        src:WAC_UDFCHKBOX05::varchar AS WAC_UDFCHKBOX05, 
                        src:WAC_UDFDATE01::datetime AS WAC_UDFDATE01, 
                        src:WAC_UDFDATE02::datetime AS WAC_UDFDATE02, 
                        src:WAC_UDFDATE03::datetime AS WAC_UDFDATE03, 
                        src:WAC_UDFDATE04::datetime AS WAC_UDFDATE04, 
                        src:WAC_UDFDATE05::datetime AS WAC_UDFDATE05, 
                        src:WAC_UDFNOTE01::varchar AS WAC_UDFNOTE01, 
                        src:WAC_UDFNOTE02::varchar AS WAC_UDFNOTE02, 
                        src:WAC_UDFNUM01::numeric(38, 10) AS WAC_UDFNUM01, 
                        src:WAC_UDFNUM02::numeric(38, 10) AS WAC_UDFNUM02, 
                        src:WAC_UDFNUM03::numeric(38, 10) AS WAC_UDFNUM03, 
                        src:WAC_UDFNUM04::numeric(38, 10) AS WAC_UDFNUM04, 
                        src:WAC_UDFNUM05::numeric(38, 10) AS WAC_UDFNUM05, 
                        src:WAC_UOM::varchar AS WAC_UOM, 
                        src:WAC_UPDATECOUNT::numeric(38, 10) AS WAC_UPDATECOUNT, 
                        src:WAC_UPDATED::datetime AS WAC_UPDATED, 
                        src:WAC_WAP::varchar AS WAC_WAP, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:WAC_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:WAC_ACT,
                src:WAC_STANDWORK  ORDER BY 
            src:WAC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5STANDWACTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WAC_ACT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WAC_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WAC_DURATION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WAC_EST), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WAC_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WAC_PERSONS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WAC_QTY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WAC_START), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WAC_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WAC_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WAC_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WAC_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WAC_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WAC_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WAC_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WAC_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WAC_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WAC_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WAC_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WAC_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WAC_LASTSAVED), '1900-01-01')) is not null