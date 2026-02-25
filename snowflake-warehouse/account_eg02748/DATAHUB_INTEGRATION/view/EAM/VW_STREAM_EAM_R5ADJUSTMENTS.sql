CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ADJUSTMENTS AS SELECT
                        src:ADJ_CODE::varchar AS ADJ_CODE, 
                        src:ADJ_DESC::varchar AS ADJ_DESC, 
                        src:ADJ_LASTSAVED::datetime AS ADJ_LASTSAVED, 
                        src:ADJ_NOTUSED::varchar AS ADJ_NOTUSED, 
                        src:ADJ_ORG::varchar AS ADJ_ORG, 
                        src:ADJ_RATE::numeric(38, 10) AS ADJ_RATE, 
                        src:ADJ_STWO::varchar AS ADJ_STWO, 
                        src:ADJ_UDFCHAR01::varchar AS ADJ_UDFCHAR01, 
                        src:ADJ_UDFCHAR02::varchar AS ADJ_UDFCHAR02, 
                        src:ADJ_UDFCHAR03::varchar AS ADJ_UDFCHAR03, 
                        src:ADJ_UDFCHAR04::varchar AS ADJ_UDFCHAR04, 
                        src:ADJ_UDFCHAR05::varchar AS ADJ_UDFCHAR05, 
                        src:ADJ_UDFCHAR06::varchar AS ADJ_UDFCHAR06, 
                        src:ADJ_UDFCHAR07::varchar AS ADJ_UDFCHAR07, 
                        src:ADJ_UDFCHAR08::varchar AS ADJ_UDFCHAR08, 
                        src:ADJ_UDFCHAR09::varchar AS ADJ_UDFCHAR09, 
                        src:ADJ_UDFCHAR10::varchar AS ADJ_UDFCHAR10, 
                        src:ADJ_UDFCHAR11::varchar AS ADJ_UDFCHAR11, 
                        src:ADJ_UDFCHAR12::varchar AS ADJ_UDFCHAR12, 
                        src:ADJ_UDFCHAR13::varchar AS ADJ_UDFCHAR13, 
                        src:ADJ_UDFCHAR14::varchar AS ADJ_UDFCHAR14, 
                        src:ADJ_UDFCHAR15::varchar AS ADJ_UDFCHAR15, 
                        src:ADJ_UDFCHAR16::varchar AS ADJ_UDFCHAR16, 
                        src:ADJ_UDFCHAR17::varchar AS ADJ_UDFCHAR17, 
                        src:ADJ_UDFCHAR18::varchar AS ADJ_UDFCHAR18, 
                        src:ADJ_UDFCHAR19::varchar AS ADJ_UDFCHAR19, 
                        src:ADJ_UDFCHAR20::varchar AS ADJ_UDFCHAR20, 
                        src:ADJ_UDFCHAR21::varchar AS ADJ_UDFCHAR21, 
                        src:ADJ_UDFCHAR22::varchar AS ADJ_UDFCHAR22, 
                        src:ADJ_UDFCHAR23::varchar AS ADJ_UDFCHAR23, 
                        src:ADJ_UDFCHAR24::varchar AS ADJ_UDFCHAR24, 
                        src:ADJ_UDFCHAR25::varchar AS ADJ_UDFCHAR25, 
                        src:ADJ_UDFCHAR26::varchar AS ADJ_UDFCHAR26, 
                        src:ADJ_UDFCHAR27::varchar AS ADJ_UDFCHAR27, 
                        src:ADJ_UDFCHAR28::varchar AS ADJ_UDFCHAR28, 
                        src:ADJ_UDFCHAR29::varchar AS ADJ_UDFCHAR29, 
                        src:ADJ_UDFCHAR30::varchar AS ADJ_UDFCHAR30, 
                        src:ADJ_UDFCHKBOX01::varchar AS ADJ_UDFCHKBOX01, 
                        src:ADJ_UDFCHKBOX02::varchar AS ADJ_UDFCHKBOX02, 
                        src:ADJ_UDFCHKBOX03::varchar AS ADJ_UDFCHKBOX03, 
                        src:ADJ_UDFCHKBOX04::varchar AS ADJ_UDFCHKBOX04, 
                        src:ADJ_UDFCHKBOX05::varchar AS ADJ_UDFCHKBOX05, 
                        src:ADJ_UDFDATE01::datetime AS ADJ_UDFDATE01, 
                        src:ADJ_UDFDATE02::datetime AS ADJ_UDFDATE02, 
                        src:ADJ_UDFDATE03::datetime AS ADJ_UDFDATE03, 
                        src:ADJ_UDFDATE04::datetime AS ADJ_UDFDATE04, 
                        src:ADJ_UDFDATE05::datetime AS ADJ_UDFDATE05, 
                        src:ADJ_UDFNUM01::numeric(38, 10) AS ADJ_UDFNUM01, 
                        src:ADJ_UDFNUM02::numeric(38, 10) AS ADJ_UDFNUM02, 
                        src:ADJ_UDFNUM03::numeric(38, 10) AS ADJ_UDFNUM03, 
                        src:ADJ_UDFNUM04::numeric(38, 10) AS ADJ_UDFNUM04, 
                        src:ADJ_UDFNUM05::numeric(38, 10) AS ADJ_UDFNUM05, 
                        src:ADJ_UPDATECOUNT::numeric(38, 10) AS ADJ_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ADJ_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:ADJ_CODE,
                src:ADJ_ORG  ORDER BY 
            src:ADJ_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ADJUSTMENTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADJ_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADJ_RATE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADJ_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADJ_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADJ_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADJ_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADJ_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADJ_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADJ_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADJ_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADJ_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADJ_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADJ_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADJ_LASTSAVED), '1900-01-01')) is not null