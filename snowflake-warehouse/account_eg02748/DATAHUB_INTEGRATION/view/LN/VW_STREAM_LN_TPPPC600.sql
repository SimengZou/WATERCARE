CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TPPPC600 AS SELECT
                        src:amnt::numeric(38, 17) AS amnt, 
                        src:appl::integer AS appl, 
                        src:appl_kw::varchar AS appl_kw, 
                        src:bamt::numeric(38, 17) AS bamt, 
                        src:base::varchar AS base, 
                        src:base_vrsn_ref_compnr::integer AS base_vrsn_ref_compnr, 
                        src:bclc::numeric(38, 17) AS bclc, 
                        src:bcur::varchar AS bcur, 
                        src:bprc::numeric(38, 17) AS bprc, 
                        src:brte::numeric(38, 17) AS brte, 
                        src:btba::numeric(38, 17) AS btba, 
                        src:btyp::integer AS btyp, 
                        src:btyp_kw::varchar AS btyp_kw, 
                        src:cact::varchar AS cact, 
                        src:calc::numeric(38, 17) AS calc, 
                        src:cccp::varchar AS cccp, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cldt::datetime AS cldt, 
                        src:compnr::integer AS compnr, 
                        src:coob::varchar AS coob, 
                        src:cotp::integer AS cotp, 
                        src:cotp_kw::varchar AS cotp_kw, 
                        src:cprj::varchar AS cprj, 
                        src:cspa::varchar AS cspa, 
                        src:ctdt::datetime AS ctdt, 
                        src:deleted::boolean AS deleted, 
                        src:hour::numeric(38, 17) AS hour, 
                        src:levl::integer AS levl, 
                        src:ohcs::integer AS ohcs, 
                        src:ohst::integer AS ohst, 
                        src:ohst_kw::varchar AS ohst_kw, 
                        src:perc::numeric(38, 17) AS perc, 
                        src:rate::numeric(38, 17) AS rate, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sern::integer AS sern, 
                        src:tbap::numeric(38, 17) AS tbap, 
                        src:tcob::varchar AS tcob, 
                        src:tcob_ref_compnr::integer AS tcob_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:trdt::datetime AS trdt, 
                        src:unit::varchar AS unit, 
                        src:user::varchar AS user, 
                        src:username::varchar AS username, 
                        src:vrsn::integer AS vrsn, 
                        src:year::integer AS year, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'LN' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'LN' = 'IPS'
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
                                        
                src:cotp,
                src:compnr,
                src:user,
                src:coob,
                src:cprj,
                src:base,
                src:cspa,
                src:ohcs,
                src:cact,
                src:sern  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPPC600 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:appl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bamt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:base_vrsn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bclc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bprc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:brte), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:btba), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:btyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:calc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cldt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cotp), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ctdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hour), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:levl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ohcs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ohst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:perc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sern), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tbap), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcob_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:trdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vrsn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null