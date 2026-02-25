CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5TRANSACTIONS AS SELECT
                        src:TRA_ACD::numeric(38, 10) AS TRA_ACD, 
                        src:TRA_ADVICE::varchar AS TRA_ADVICE, 
                        src:TRA_AUTH::varchar AS TRA_AUTH, 
                        src:TRA_CLASS::varchar AS TRA_CLASS, 
                        src:TRA_CLASS_ORG::varchar AS TRA_CLASS_ORG, 
                        src:TRA_CODE::varchar AS TRA_CODE, 
                        src:TRA_CREATED::datetime AS TRA_CREATED, 
                        src:TRA_DATE::datetime AS TRA_DATE, 
                        src:TRA_DCKCODE::varchar AS TRA_DCKCODE, 
                        src:TRA_DCKLINE::numeric(38, 10) AS TRA_DCKLINE, 
                        src:TRA_DESC::varchar AS TRA_DESC, 
                        src:TRA_FROMCODE::varchar AS TRA_FROMCODE, 
                        src:TRA_FROMCODE_ORG::varchar AS TRA_FROMCODE_ORG, 
                        src:TRA_FROMENTITY::varchar AS TRA_FROMENTITY, 
                        src:TRA_FROMRENTITY::varchar AS TRA_FROMRENTITY, 
                        src:TRA_FROMRTYPE::varchar AS TRA_FROMRTYPE, 
                        src:TRA_FROMTYPE::varchar AS TRA_FROMTYPE, 
                        src:TRA_INTERFACE::datetime AS TRA_INTERFACE, 
                        src:TRA_INVALLOCATION::numeric(38, 10) AS TRA_INVALLOCATION, 
                        src:TRA_IPTRANSMITTED::varchar AS TRA_IPTRANSMITTED, 
                        src:TRA_JECATEGORY::varchar AS TRA_JECATEGORY, 
                        src:TRA_JESOURCE::varchar AS TRA_JESOURCE, 
                        src:TRA_LASTSAVED::datetime AS TRA_LASTSAVED, 
                        src:TRA_ORDER::varchar AS TRA_ORDER, 
                        src:TRA_ORDER_ORG::varchar AS TRA_ORDER_ORG, 
                        src:TRA_ORG::varchar AS TRA_ORG, 
                        src:TRA_PERS::varchar AS TRA_PERS, 
                        src:TRA_PRIDEST::varchar AS TRA_PRIDEST, 
                        src:TRA_PRINTED::varchar AS TRA_PRINTED, 
                        src:TRA_PRIORIG::varchar AS TRA_PRIORIG, 
                        src:TRA_RELATEDWO::varchar AS TRA_RELATEDWO, 
                        src:TRA_REQ::varchar AS TRA_REQ, 
                        src:TRA_ROUTEPARENT::varchar AS TRA_ROUTEPARENT, 
                        src:TRA_RSTATUS::varchar AS TRA_RSTATUS, 
                        src:TRA_RTYPE::varchar AS TRA_RTYPE, 
                        src:TRA_SOURCECODE::varchar AS TRA_SOURCECODE, 
                        src:TRA_SOURCESYSTEM::varchar AS TRA_SOURCESYSTEM, 
                        src:TRA_STATUS::varchar AS TRA_STATUS, 
                        src:TRA_TOCODE::varchar AS TRA_TOCODE, 
                        src:TRA_TOCODE_ORG::varchar AS TRA_TOCODE_ORG, 
                        src:TRA_TOENTITY::varchar AS TRA_TOENTITY, 
                        src:TRA_TORENTITY::varchar AS TRA_TORENTITY, 
                        src:TRA_TORTYPE::varchar AS TRA_TORTYPE, 
                        src:TRA_TOTYPE::varchar AS TRA_TOTYPE, 
                        src:TRA_TYPE::varchar AS TRA_TYPE, 
                        src:TRA_UDFCHAR01::varchar AS TRA_UDFCHAR01, 
                        src:TRA_UDFCHAR02::varchar AS TRA_UDFCHAR02, 
                        src:TRA_UDFCHAR03::varchar AS TRA_UDFCHAR03, 
                        src:TRA_UDFCHAR04::varchar AS TRA_UDFCHAR04, 
                        src:TRA_UDFCHAR05::varchar AS TRA_UDFCHAR05, 
                        src:TRA_UDFCHAR06::varchar AS TRA_UDFCHAR06, 
                        src:TRA_UDFCHAR07::varchar AS TRA_UDFCHAR07, 
                        src:TRA_UDFCHAR08::varchar AS TRA_UDFCHAR08, 
                        src:TRA_UDFCHAR09::varchar AS TRA_UDFCHAR09, 
                        src:TRA_UDFCHAR10::varchar AS TRA_UDFCHAR10, 
                        src:TRA_UDFCHAR11::varchar AS TRA_UDFCHAR11, 
                        src:TRA_UDFCHAR12::varchar AS TRA_UDFCHAR12, 
                        src:TRA_UDFCHAR13::varchar AS TRA_UDFCHAR13, 
                        src:TRA_UDFCHAR14::varchar AS TRA_UDFCHAR14, 
                        src:TRA_UDFCHAR15::varchar AS TRA_UDFCHAR15, 
                        src:TRA_UDFCHAR16::varchar AS TRA_UDFCHAR16, 
                        src:TRA_UDFCHAR17::varchar AS TRA_UDFCHAR17, 
                        src:TRA_UDFCHAR18::varchar AS TRA_UDFCHAR18, 
                        src:TRA_UDFCHAR19::varchar AS TRA_UDFCHAR19, 
                        src:TRA_UDFCHAR20::varchar AS TRA_UDFCHAR20, 
                        src:TRA_UDFCHAR21::varchar AS TRA_UDFCHAR21, 
                        src:TRA_UDFCHAR22::varchar AS TRA_UDFCHAR22, 
                        src:TRA_UDFCHAR23::varchar AS TRA_UDFCHAR23, 
                        src:TRA_UDFCHAR24::varchar AS TRA_UDFCHAR24, 
                        src:TRA_UDFCHAR25::varchar AS TRA_UDFCHAR25, 
                        src:TRA_UDFCHAR26::varchar AS TRA_UDFCHAR26, 
                        src:TRA_UDFCHAR27::varchar AS TRA_UDFCHAR27, 
                        src:TRA_UDFCHAR28::varchar AS TRA_UDFCHAR28, 
                        src:TRA_UDFCHAR29::varchar AS TRA_UDFCHAR29, 
                        src:TRA_UDFCHAR30::varchar AS TRA_UDFCHAR30, 
                        src:TRA_UDFCHKBOX01::varchar AS TRA_UDFCHKBOX01, 
                        src:TRA_UDFCHKBOX02::varchar AS TRA_UDFCHKBOX02, 
                        src:TRA_UDFCHKBOX03::varchar AS TRA_UDFCHKBOX03, 
                        src:TRA_UDFCHKBOX04::varchar AS TRA_UDFCHKBOX04, 
                        src:TRA_UDFCHKBOX05::varchar AS TRA_UDFCHKBOX05, 
                        src:TRA_UDFDATE01::datetime AS TRA_UDFDATE01, 
                        src:TRA_UDFDATE02::datetime AS TRA_UDFDATE02, 
                        src:TRA_UDFDATE03::datetime AS TRA_UDFDATE03, 
                        src:TRA_UDFDATE04::datetime AS TRA_UDFDATE04, 
                        src:TRA_UDFDATE05::datetime AS TRA_UDFDATE05, 
                        src:TRA_UDFNUM01::numeric(38, 10) AS TRA_UDFNUM01, 
                        src:TRA_UDFNUM02::numeric(38, 10) AS TRA_UDFNUM02, 
                        src:TRA_UDFNUM03::numeric(38, 10) AS TRA_UDFNUM03, 
                        src:TRA_UDFNUM04::numeric(38, 10) AS TRA_UDFNUM04, 
                        src:TRA_UDFNUM05::numeric(38, 10) AS TRA_UDFNUM05, 
                        src:TRA_UPDATECOUNT::numeric(38, 10) AS TRA_UPDATECOUNT, 
                        src:TRA_UPDATED::datetime AS TRA_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:TRA_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:TRA_CODE  ORDER BY 
            src:TRA_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5TRANSACTIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRA_ACD), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRA_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRA_DATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRA_DCKLINE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRA_INTERFACE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRA_INVALLOCATION), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRA_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRA_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRA_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRA_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRA_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRA_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRA_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRA_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRA_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRA_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRA_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRA_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRA_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRA_LASTSAVED), '1900-01-01')) is not null