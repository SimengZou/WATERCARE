CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5OBJECTSERVICECODES AS SELECT
                        src:OSC_APPLYTOCHILDREN::varchar AS OSC_APPLYTOCHILDREN, 
                        src:OSC_LASTSAVED::datetime AS OSC_LASTSAVED, 
                        src:OSC_OBJECT::varchar AS OSC_OBJECT, 
                        src:OSC_OBJECT_ORG::varchar AS OSC_OBJECT_ORG, 
                        src:OSC_PARENT::varchar AS OSC_PARENT, 
                        src:OSC_PK::varchar AS OSC_PK, 
                        src:OSC_SERVICECODE::varchar AS OSC_SERVICECODE, 
                        src:OSC_SERVICECODE_ORG::varchar AS OSC_SERVICECODE_ORG, 
                        src:OSC_SKIPTHISLEVEL::varchar AS OSC_SKIPTHISLEVEL, 
                        src:OSC_UDFCHAR01::varchar AS OSC_UDFCHAR01, 
                        src:OSC_UDFCHAR02::varchar AS OSC_UDFCHAR02, 
                        src:OSC_UDFCHAR03::varchar AS OSC_UDFCHAR03, 
                        src:OSC_UDFCHAR04::varchar AS OSC_UDFCHAR04, 
                        src:OSC_UDFCHAR05::varchar AS OSC_UDFCHAR05, 
                        src:OSC_UDFCHAR06::varchar AS OSC_UDFCHAR06, 
                        src:OSC_UDFCHAR07::varchar AS OSC_UDFCHAR07, 
                        src:OSC_UDFCHAR08::varchar AS OSC_UDFCHAR08, 
                        src:OSC_UDFCHAR09::varchar AS OSC_UDFCHAR09, 
                        src:OSC_UDFCHAR10::varchar AS OSC_UDFCHAR10, 
                        src:OSC_UDFCHAR11::varchar AS OSC_UDFCHAR11, 
                        src:OSC_UDFCHAR12::varchar AS OSC_UDFCHAR12, 
                        src:OSC_UDFCHAR13::varchar AS OSC_UDFCHAR13, 
                        src:OSC_UDFCHAR14::varchar AS OSC_UDFCHAR14, 
                        src:OSC_UDFCHAR15::varchar AS OSC_UDFCHAR15, 
                        src:OSC_UDFCHAR16::varchar AS OSC_UDFCHAR16, 
                        src:OSC_UDFCHAR17::varchar AS OSC_UDFCHAR17, 
                        src:OSC_UDFCHAR18::varchar AS OSC_UDFCHAR18, 
                        src:OSC_UDFCHAR19::varchar AS OSC_UDFCHAR19, 
                        src:OSC_UDFCHAR20::varchar AS OSC_UDFCHAR20, 
                        src:OSC_UDFCHAR21::varchar AS OSC_UDFCHAR21, 
                        src:OSC_UDFCHAR22::varchar AS OSC_UDFCHAR22, 
                        src:OSC_UDFCHAR23::varchar AS OSC_UDFCHAR23, 
                        src:OSC_UDFCHAR24::varchar AS OSC_UDFCHAR24, 
                        src:OSC_UDFCHAR25::varchar AS OSC_UDFCHAR25, 
                        src:OSC_UDFCHAR26::varchar AS OSC_UDFCHAR26, 
                        src:OSC_UDFCHAR27::varchar AS OSC_UDFCHAR27, 
                        src:OSC_UDFCHAR28::varchar AS OSC_UDFCHAR28, 
                        src:OSC_UDFCHAR29::varchar AS OSC_UDFCHAR29, 
                        src:OSC_UDFCHAR30::varchar AS OSC_UDFCHAR30, 
                        src:OSC_UDFCHKBOX01::varchar AS OSC_UDFCHKBOX01, 
                        src:OSC_UDFCHKBOX02::varchar AS OSC_UDFCHKBOX02, 
                        src:OSC_UDFCHKBOX03::varchar AS OSC_UDFCHKBOX03, 
                        src:OSC_UDFCHKBOX04::varchar AS OSC_UDFCHKBOX04, 
                        src:OSC_UDFCHKBOX05::varchar AS OSC_UDFCHKBOX05, 
                        src:OSC_UDFDATE01::datetime AS OSC_UDFDATE01, 
                        src:OSC_UDFDATE02::datetime AS OSC_UDFDATE02, 
                        src:OSC_UDFDATE03::datetime AS OSC_UDFDATE03, 
                        src:OSC_UDFDATE04::datetime AS OSC_UDFDATE04, 
                        src:OSC_UDFDATE05::datetime AS OSC_UDFDATE05, 
                        src:OSC_UDFNUM01::numeric(38, 10) AS OSC_UDFNUM01, 
                        src:OSC_UDFNUM02::numeric(38, 10) AS OSC_UDFNUM02, 
                        src:OSC_UDFNUM03::numeric(38, 10) AS OSC_UDFNUM03, 
                        src:OSC_UDFNUM04::numeric(38, 10) AS OSC_UDFNUM04, 
                        src:OSC_UDFNUM05::numeric(38, 10) AS OSC_UDFNUM05, 
                        src:OSC_UPDATECOUNT::numeric(38, 10) AS OSC_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:OSC_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:OSC_PK  ORDER BY 
            src:OSC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5OBJECTSERVICECODES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OSC_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OSC_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OSC_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OSC_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OSC_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OSC_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OSC_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OSC_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OSC_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OSC_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OSC_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OSC_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OSC_LASTSAVED), '1900-01-01')) is not null