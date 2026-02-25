CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ORDERS AS SELECT
                        src:ORD_ACD::numeric(38, 10) AS ORD_ACD, 
                        src:ORD_APPROV::datetime AS ORD_APPROV, 
                        src:ORD_ATTENTIONTO::varchar AS ORD_ATTENTIONTO, 
                        src:ORD_AUTH::varchar AS ORD_AUTH, 
                        src:ORD_BLANKETORDER::varchar AS ORD_BLANKETORDER, 
                        src:ORD_BLANKETRELEASE::numeric(38, 10) AS ORD_BLANKETRELEASE, 
                        src:ORD_BUYER::varchar AS ORD_BUYER, 
                        src:ORD_CLASS::varchar AS ORD_CLASS, 
                        src:ORD_CLASS_ORG::varchar AS ORD_CLASS_ORG, 
                        src:ORD_CODE::varchar AS ORD_CODE, 
                        src:ORD_CONTRACT::varchar AS ORD_CONTRACT, 
                        src:ORD_CONTROLNO::varchar AS ORD_CONTROLNO, 
                        src:ORD_CONVERTTOASN::varchar AS ORD_CONVERTTOASN, 
                        src:ORD_CREATED::datetime AS ORD_CREATED, 
                        src:ORD_CREDITCARD::numeric(38, 10) AS ORD_CREDITCARD, 
                        src:ORD_CURR::varchar AS ORD_CURR, 
                        src:ORD_DATE::datetime AS ORD_DATE, 
                        src:ORD_DELADDRESS::varchar AS ORD_DELADDRESS, 
                        src:ORD_DESC::varchar AS ORD_DESC, 
                        src:ORD_DFLTAUTH::varchar AS ORD_DFLTAUTH, 
                        src:ORD_DISCOUNT::numeric(38, 10) AS ORD_DISCOUNT, 
                        src:ORD_DISCPERC::numeric(38, 10) AS ORD_DISCPERC, 
                        src:ORD_DUE::datetime AS ORD_DUE, 
                        src:ORD_EXCH::numeric(38, 10) AS ORD_EXCH, 
                        src:ORD_EXCHFROMDUAL::numeric(38, 10) AS ORD_EXCHFROMDUAL, 
                        src:ORD_EXCHTODUAL::numeric(38, 10) AS ORD_EXCHTODUAL, 
                        src:ORD_FOBPOINT::varchar AS ORD_FOBPOINT, 
                        src:ORD_FREIGHTTERMS::varchar AS ORD_FREIGHTTERMS, 
                        src:ORD_INTERFACE::datetime AS ORD_INTERFACE, 
                        src:ORD_IPTRANSMITTED::varchar AS ORD_IPTRANSMITTED, 
                        src:ORD_JECATEGORY::varchar AS ORD_JECATEGORY, 
                        src:ORD_JESOURCE::varchar AS ORD_JESOURCE, 
                        src:ORD_LASTSAVED::datetime AS ORD_LASTSAVED, 
                        src:ORD_LASTSTATUSUPDATE::datetime AS ORD_LASTSTATUSUPDATE, 
                        src:ORD_ORG::varchar AS ORD_ORG, 
                        src:ORD_ORIGIN::varchar AS ORD_ORIGIN, 
                        src:ORD_PACKAGETRACKINGNUMBER::varchar AS ORD_PACKAGETRACKINGNUMBER, 
                        src:ORD_PAYBYMETHOD::varchar AS ORD_PAYBYMETHOD, 
                        src:ORD_PAYMENTTERMS::varchar AS ORD_PAYMENTTERMS, 
                        src:ORD_PRICE::numeric(38, 10) AS ORD_PRICE, 
                        src:ORD_PRINTED::varchar AS ORD_PRINTED, 
                        src:ORD_REVISED::datetime AS ORD_REVISED, 
                        src:ORD_REVISION::numeric(38, 10) AS ORD_REVISION, 
                        src:ORD_REVISIONREASON::varchar AS ORD_REVISIONREASON, 
                        src:ORD_RSTATUS::varchar AS ORD_RSTATUS, 
                        src:ORD_RTYPE::varchar AS ORD_RTYPE, 
                        src:ORD_SHIPVIA::varchar AS ORD_SHIPVIA, 
                        src:ORD_SOURCECODE::varchar AS ORD_SOURCECODE, 
                        src:ORD_SOURCESYSTEM::varchar AS ORD_SOURCESYSTEM, 
                        src:ORD_STATUS::varchar AS ORD_STATUS, 
                        src:ORD_STORE::varchar AS ORD_STORE, 
                        src:ORD_SUPPLIER::varchar AS ORD_SUPPLIER, 
                        src:ORD_SUPPLIER_ORG::varchar AS ORD_SUPPLIER_ORG, 
                        src:ORD_TYPE::varchar AS ORD_TYPE, 
                        src:ORD_UDFCHAR01::varchar AS ORD_UDFCHAR01, 
                        src:ORD_UDFCHAR02::varchar AS ORD_UDFCHAR02, 
                        src:ORD_UDFCHAR03::varchar AS ORD_UDFCHAR03, 
                        src:ORD_UDFCHAR04::varchar AS ORD_UDFCHAR04, 
                        src:ORD_UDFCHAR05::varchar AS ORD_UDFCHAR05, 
                        src:ORD_UDFCHAR06::varchar AS ORD_UDFCHAR06, 
                        src:ORD_UDFCHAR07::varchar AS ORD_UDFCHAR07, 
                        src:ORD_UDFCHAR08::varchar AS ORD_UDFCHAR08, 
                        src:ORD_UDFCHAR09::varchar AS ORD_UDFCHAR09, 
                        src:ORD_UDFCHAR10::varchar AS ORD_UDFCHAR10, 
                        src:ORD_UDFCHAR11::varchar AS ORD_UDFCHAR11, 
                        src:ORD_UDFCHAR12::varchar AS ORD_UDFCHAR12, 
                        src:ORD_UDFCHAR13::varchar AS ORD_UDFCHAR13, 
                        src:ORD_UDFCHAR14::varchar AS ORD_UDFCHAR14, 
                        src:ORD_UDFCHAR15::varchar AS ORD_UDFCHAR15, 
                        src:ORD_UDFCHAR16::varchar AS ORD_UDFCHAR16, 
                        src:ORD_UDFCHAR17::varchar AS ORD_UDFCHAR17, 
                        src:ORD_UDFCHAR18::varchar AS ORD_UDFCHAR18, 
                        src:ORD_UDFCHAR19::varchar AS ORD_UDFCHAR19, 
                        src:ORD_UDFCHAR20::varchar AS ORD_UDFCHAR20, 
                        src:ORD_UDFCHAR21::varchar AS ORD_UDFCHAR21, 
                        src:ORD_UDFCHAR22::varchar AS ORD_UDFCHAR22, 
                        src:ORD_UDFCHAR23::varchar AS ORD_UDFCHAR23, 
                        src:ORD_UDFCHAR24::varchar AS ORD_UDFCHAR24, 
                        src:ORD_UDFCHAR25::varchar AS ORD_UDFCHAR25, 
                        src:ORD_UDFCHAR26::varchar AS ORD_UDFCHAR26, 
                        src:ORD_UDFCHAR27::varchar AS ORD_UDFCHAR27, 
                        src:ORD_UDFCHAR28::varchar AS ORD_UDFCHAR28, 
                        src:ORD_UDFCHAR29::varchar AS ORD_UDFCHAR29, 
                        src:ORD_UDFCHAR30::varchar AS ORD_UDFCHAR30, 
                        src:ORD_UDFCHKBOX01::varchar AS ORD_UDFCHKBOX01, 
                        src:ORD_UDFCHKBOX02::varchar AS ORD_UDFCHKBOX02, 
                        src:ORD_UDFCHKBOX03::varchar AS ORD_UDFCHKBOX03, 
                        src:ORD_UDFCHKBOX04::varchar AS ORD_UDFCHKBOX04, 
                        src:ORD_UDFCHKBOX05::varchar AS ORD_UDFCHKBOX05, 
                        src:ORD_UDFDATE01::datetime AS ORD_UDFDATE01, 
                        src:ORD_UDFDATE02::datetime AS ORD_UDFDATE02, 
                        src:ORD_UDFDATE03::datetime AS ORD_UDFDATE03, 
                        src:ORD_UDFDATE04::datetime AS ORD_UDFDATE04, 
                        src:ORD_UDFDATE05::datetime AS ORD_UDFDATE05, 
                        src:ORD_UDFNUM01::numeric(38, 10) AS ORD_UDFNUM01, 
                        src:ORD_UDFNUM02::numeric(38, 10) AS ORD_UDFNUM02, 
                        src:ORD_UDFNUM03::numeric(38, 10) AS ORD_UDFNUM03, 
                        src:ORD_UDFNUM04::numeric(38, 10) AS ORD_UDFNUM04, 
                        src:ORD_UDFNUM05::numeric(38, 10) AS ORD_UDFNUM05, 
                        src:ORD_UPDATECOUNT::numeric(38, 10) AS ORD_UPDATECOUNT, 
                        src:ORD_UPDATED::datetime AS ORD_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ORD_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:ORD_CODE,
                src:ORD_ORG  ORDER BY 
            src:ORD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ORDERS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD_ACD), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORD_APPROV), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD_BLANKETRELEASE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORD_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD_CREDITCARD), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORD_DATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD_DISCOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD_DISCPERC), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORD_DUE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD_EXCH), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD_EXCHFROMDUAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD_EXCHTODUAL), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORD_INTERFACE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORD_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORD_LASTSTATUSUPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD_PRICE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORD_REVISED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD_REVISION), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORD_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORD_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORD_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORD_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORD_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORD_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORD_LASTSAVED), '1900-01-01')) is not null