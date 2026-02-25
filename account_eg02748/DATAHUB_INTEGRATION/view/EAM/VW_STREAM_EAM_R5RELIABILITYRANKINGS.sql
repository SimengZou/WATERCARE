CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5RELIABILITYRANKINGS AS SELECT
                        src:RRK_CLASS::varchar AS RRK_CLASS, 
                        src:RRK_CLASS_ORG::varchar AS RRK_CLASS_ORG, 
                        src:RRK_CODE::varchar AS RRK_CODE, 
                        src:RRK_CONDITIONPROTOCOL::varchar AS RRK_CONDITIONPROTOCOL, 
                        src:RRK_CONDSCOREEND::numeric(38, 10) AS RRK_CONDSCOREEND, 
                        src:RRK_CONDSCORESTART::numeric(38, 10) AS RRK_CONDSCORESTART, 
                        src:RRK_CONDSCORETHRESHOLD::numeric(38, 10) AS RRK_CONDSCORETHRESHOLD, 
                        src:RRK_CORRSCORERANKING::varchar AS RRK_CORRSCORERANKING, 
                        src:RRK_CORRSCORERANKINGREV::numeric(38, 10) AS RRK_CORRSCORERANKINGREV, 
                        src:RRK_CORRSCORERANKING_ORG::varchar AS RRK_CORRSCORERANKING_ORG, 
                        src:RRK_CREATED::datetime AS RRK_CREATED, 
                        src:RRK_CREATEDBY::varchar AS RRK_CREATEDBY, 
                        src:RRK_DECAYCURVE::varchar AS RRK_DECAYCURVE, 
                        src:RRK_DECAYCURVE_ORG::varchar AS RRK_DECAYCURVE_ORG, 
                        src:RRK_DESC::varchar AS RRK_DESC, 
                        src:RRK_LASTSAVED::datetime AS RRK_LASTSAVED, 
                        src:RRK_NOTUSED::varchar AS RRK_NOTUSED, 
                        src:RRK_ORG::varchar AS RRK_ORG, 
                        src:RRK_PERFORMANCEFORMULA::varchar AS RRK_PERFORMANCEFORMULA, 
                        src:RRK_PERFORMANCEFORMULA_ORG::varchar AS RRK_PERFORMANCEFORMULA_ORG, 
                        src:RRK_PRECISION::numeric(38, 10) AS RRK_PRECISION, 
                        src:RRK_REVISION::numeric(38, 10) AS RRK_REVISION, 
                        src:RRK_REVRSTATUS::varchar AS RRK_REVRSTATUS, 
                        src:RRK_REVSTATUS::varchar AS RRK_REVSTATUS, 
                        src:RRK_RTYPE::varchar AS RRK_RTYPE, 
                        src:RRK_SETUPLASTUPDATED::datetime AS RRK_SETUPLASTUPDATED, 
                        src:RRK_TRACKHISTORY::varchar AS RRK_TRACKHISTORY, 
                        src:RRK_TYPE::varchar AS RRK_TYPE, 
                        src:RRK_UDFCHAR01::varchar AS RRK_UDFCHAR01, 
                        src:RRK_UDFCHAR02::varchar AS RRK_UDFCHAR02, 
                        src:RRK_UDFCHAR03::varchar AS RRK_UDFCHAR03, 
                        src:RRK_UDFCHAR04::varchar AS RRK_UDFCHAR04, 
                        src:RRK_UDFCHAR05::varchar AS RRK_UDFCHAR05, 
                        src:RRK_UDFCHAR06::varchar AS RRK_UDFCHAR06, 
                        src:RRK_UDFCHAR07::varchar AS RRK_UDFCHAR07, 
                        src:RRK_UDFCHAR08::varchar AS RRK_UDFCHAR08, 
                        src:RRK_UDFCHAR09::varchar AS RRK_UDFCHAR09, 
                        src:RRK_UDFCHAR10::varchar AS RRK_UDFCHAR10, 
                        src:RRK_UDFCHAR11::varchar AS RRK_UDFCHAR11, 
                        src:RRK_UDFCHAR12::varchar AS RRK_UDFCHAR12, 
                        src:RRK_UDFCHAR13::varchar AS RRK_UDFCHAR13, 
                        src:RRK_UDFCHAR14::varchar AS RRK_UDFCHAR14, 
                        src:RRK_UDFCHAR15::varchar AS RRK_UDFCHAR15, 
                        src:RRK_UDFCHAR16::varchar AS RRK_UDFCHAR16, 
                        src:RRK_UDFCHAR17::varchar AS RRK_UDFCHAR17, 
                        src:RRK_UDFCHAR18::varchar AS RRK_UDFCHAR18, 
                        src:RRK_UDFCHAR19::varchar AS RRK_UDFCHAR19, 
                        src:RRK_UDFCHAR20::varchar AS RRK_UDFCHAR20, 
                        src:RRK_UDFCHAR21::varchar AS RRK_UDFCHAR21, 
                        src:RRK_UDFCHAR22::varchar AS RRK_UDFCHAR22, 
                        src:RRK_UDFCHAR23::varchar AS RRK_UDFCHAR23, 
                        src:RRK_UDFCHAR24::varchar AS RRK_UDFCHAR24, 
                        src:RRK_UDFCHAR25::varchar AS RRK_UDFCHAR25, 
                        src:RRK_UDFCHAR26::varchar AS RRK_UDFCHAR26, 
                        src:RRK_UDFCHAR27::varchar AS RRK_UDFCHAR27, 
                        src:RRK_UDFCHAR28::varchar AS RRK_UDFCHAR28, 
                        src:RRK_UDFCHAR29::varchar AS RRK_UDFCHAR29, 
                        src:RRK_UDFCHAR30::varchar AS RRK_UDFCHAR30, 
                        src:RRK_UDFCHKBOX01::varchar AS RRK_UDFCHKBOX01, 
                        src:RRK_UDFCHKBOX02::varchar AS RRK_UDFCHKBOX02, 
                        src:RRK_UDFCHKBOX03::varchar AS RRK_UDFCHKBOX03, 
                        src:RRK_UDFCHKBOX04::varchar AS RRK_UDFCHKBOX04, 
                        src:RRK_UDFCHKBOX05::varchar AS RRK_UDFCHKBOX05, 
                        src:RRK_UDFDATE01::datetime AS RRK_UDFDATE01, 
                        src:RRK_UDFDATE02::datetime AS RRK_UDFDATE02, 
                        src:RRK_UDFDATE03::datetime AS RRK_UDFDATE03, 
                        src:RRK_UDFDATE04::datetime AS RRK_UDFDATE04, 
                        src:RRK_UDFDATE05::datetime AS RRK_UDFDATE05, 
                        src:RRK_UDFNUM01::numeric(38, 10) AS RRK_UDFNUM01, 
                        src:RRK_UDFNUM02::numeric(38, 10) AS RRK_UDFNUM02, 
                        src:RRK_UDFNUM03::numeric(38, 10) AS RRK_UDFNUM03, 
                        src:RRK_UDFNUM04::numeric(38, 10) AS RRK_UDFNUM04, 
                        src:RRK_UDFNUM05::numeric(38, 10) AS RRK_UDFNUM05, 
                        src:RRK_UPDATECOUNT::numeric(38, 10) AS RRK_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:RRK_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:RRK_CODE,
                src:RRK_ORG,
                src:RRK_REVISION  ORDER BY 
            src:RRK_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5RELIABILITYRANKINGS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRK_CONDSCOREEND), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRK_CONDSCORESTART), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRK_CONDSCORETHRESHOLD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRK_CORRSCORERANKINGREV), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RRK_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RRK_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRK_PRECISION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRK_REVISION), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RRK_SETUPLASTUPDATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RRK_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RRK_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RRK_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RRK_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RRK_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRK_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRK_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRK_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRK_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRK_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRK_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RRK_LASTSAVED), '1900-01-01')) is not null