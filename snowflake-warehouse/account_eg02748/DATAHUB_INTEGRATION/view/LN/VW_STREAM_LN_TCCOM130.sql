CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TCCOM130 AS SELECT
                        src:afal::varchar AS afal, 
                        src:afal_ref_compnr::integer AS afal_ref_compnr, 
                        src:bldg::object AS bldg, 
                        src:blfl::object AS blfl, 
                        src:blun::object AS blun, 
                        src:cadr::varchar AS cadr, 
                        src:ccal::varchar AS ccal, 
                        src:ccal_ref_compnr::integer AS ccal_ref_compnr, 
                        src:ccit::varchar AS ccit, 
                        src:ccty::varchar AS ccty, 
                        src:ccty_cste_ccit_ref_compnr::integer AS ccty_cste_ccit_ref_compnr, 
                        src:ccty_cste_ref_compnr::integer AS ccty_cste_ref_compnr, 
                        src:ccty_ref_compnr::integer AS ccty_ref_compnr, 
                        src:cnty::object AS cnty, 
                        src:coaf::varchar AS coaf, 
                        src:coaf_ref_compnr::integer AS coaf_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:crdt::datetime AS crdt, 
                        src:crid::integer AS crid, 
                        src:crte::varchar AS crte, 
                        src:crte_ref_compnr::integer AS crte_ref_compnr, 
                        src:cste::varchar AS cste, 
                        src:defa::integer AS defa, 
                        src:defa_kw::varchar AS defa_kw, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:dtlm::datetime AS dtlm, 
                        src:etyp::integer AS etyp, 
                        src:etyp_kw::varchar AS etyp_kw, 
                        src:ezty::varchar AS ezty, 
                        src:ezty_ref_compnr::integer AS ezty_ref_compnr, 
                        src:foid::integer AS foid, 
                        src:fovn::varchar AS fovn, 
                        src:geoc::varchar AS geoc, 
                        src:glat::numeric(38, 17) AS glat, 
                        src:glon::numeric(38, 17) AS glon, 
                        src:hono::object AS hono, 
                        src:icty::varchar AS icty, 
                        src:inet::object AS inet, 
                        src:info::object AS info, 
                        src:lmus::varchar AS lmus, 
                        src:ln01::object AS ln01, 
                        src:ln02::object AS ln02, 
                        src:ln03::object AS ln03, 
                        src:ln04::object AS ln04, 
                        src:ln05::object AS ln05, 
                        src:ln06::object AS ln06, 
                        src:lvdt::datetime AS lvdt, 
                        src:nama::object AS nama, 
                        src:namb::object AS namb, 
                        src:namc::object AS namc, 
                        src:namd::object AS namd, 
                        src:namf::object AS namf, 
                        src:pobn::object AS pobn, 
                        src:pstc::object AS pstc, 
                        src:seak::object AS seak, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sita::varchar AS sita, 
                        src:smsa::object AS smsa, 
                        src:tefx::varchar AS tefx, 
                        src:telp::varchar AS telp, 
                        src:telx::varchar AS telx, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:tzid::varchar AS tzid, 
                        src:tzid_ref_compnr::integer AS tzid_ref_compnr, 
                        src:user::varchar AS user, 
                        src:username::varchar AS username, 
                        src:valr::integer AS valr, 
                        src:valr_kw::varchar AS valr_kw, 
                        src:vatl::varchar AS vatl, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:cadr,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCCOM130 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:afal_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccty_cste_ccit_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccty_cste_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:coaf_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:crdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crid), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:defa), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:dtlm), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:etyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ezty_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:foid), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:glat), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:glon), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:lvdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tzid_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:valr), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null