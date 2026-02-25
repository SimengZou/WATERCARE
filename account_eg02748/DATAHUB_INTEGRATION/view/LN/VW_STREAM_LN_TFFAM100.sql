CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM100 AS SELECT
                        src:aacq_1::numeric(38, 17) AS aacq_1, 
                        src:aacq_2::numeric(38, 17) AS aacq_2, 
                        src:aacq_3::numeric(38, 17) AS aacq_3, 
                        src:acmc_1::numeric(38, 17) AS acmc_1, 
                        src:acmc_2::numeric(38, 17) AS acmc_2, 
                        src:acmc_3::numeric(38, 17) AS acmc_3, 
                        src:acmd::date AS acmd, 
                        src:aext::varchar AS aext, 
                        src:agrp::varchar AS agrp, 
                        src:agrp_ref_compnr::integer AS agrp_ref_compnr, 
                        src:anbr::varchar AS anbr, 
                        src:ascd::integer AS ascd, 
                        src:ascd_kw::varchar AS ascd_kw, 
                        src:atbc_1::numeric(38, 17) AS atbc_1, 
                        src:atbc_2::numeric(38, 17) AS atbc_2, 
                        src:atbc_3::numeric(38, 17) AS atbc_3, 
                        src:auto::integer AS auto, 
                        src:auto_kw::varchar AS auto_kw, 
                        src:bpct::numeric(38, 17) AS bpct, 
                        src:bpid::varchar AS bpid, 
                        src:bpid_ref_compnr::integer AS bpid_ref_compnr, 
                        src:c263::numeric(38, 17) AS c263, 
                        src:cact::varchar AS cact, 
                        src:capi::numeric(38, 17) AS capi, 
                        src:cdf_aid1::object AS cdf_aid1, 
                        src:cdf_akey::object AS cdf_akey, 
                        src:cdf_asid::object AS cdf_asid, 
                        src:cdf_atyp::varchar AS cdf_atyp, 
                        src:cdf_atyp_ref_compnr::integer AS cdf_atyp_ref_compnr, 
                        src:cdf_htir::varchar AS cdf_htir, 
                        src:cdf_ipst::integer AS cdf_ipst, 
                        src:cdf_ipst_kw::varchar AS cdf_ipst_kw, 
                        src:cdf_utyp::varchar AS cdf_utyp, 
                        src:cdf_utyp_ref_compnr::integer AS cdf_utyp_ref_compnr, 
                        src:cola::integer AS cola, 
                        src:cola_kw::varchar AS cola_kw, 
                        src:comp::integer AS comp, 
                        src:compnr::integer AS compnr, 
                        src:cost::numeric(38, 17) AS cost, 
                        src:cspa::varchar AS cspa, 
                        src:csth_1::numeric(38, 17) AS csth_1, 
                        src:csth_2::numeric(38, 17) AS csth_2, 
                        src:csth_3::numeric(38, 17) AS csth_3, 
                        src:ctgy::varchar AS ctgy, 
                        src:ctgy_ref_compnr::integer AS ctgy_ref_compnr, 
                        src:ctgy_sctg_ref_compnr::integer AS ctgy_sctg_ref_compnr, 
                        src:curr::varchar AS curr, 
                        src:curr_ref_compnr::integer AS curr_ref_compnr, 
                        src:date::date AS date, 
                        src:dbfg::integer AS dbfg, 
                        src:dbfg_kw::varchar AS dbfg_kw, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:imag::varchar AS imag, 
                        src:itca::numeric(38, 17) AS itca, 
                        src:itcd::integer AS itcd, 
                        src:itcd_kw::varchar AS itcd_kw, 
                        src:item::object AS item, 
                        src:list::integer AS list, 
                        src:list_kw::varchar AS list_kw, 
                        src:ltdd::numeric(38, 17) AS ltdd, 
                        src:newu::integer AS newu, 
                        src:newu_kw::varchar AS newu_kw, 
                        src:oqty::integer AS oqty, 
                        src:owco::integer AS owco, 
                        src:owco_ref_compnr::integer AS owco_ref_compnr, 
                        src:ownc::integer AS ownc, 
                        src:ownc_kw::varchar AS ownc_kw, 
                        src:ownd::varchar AS ownd, 
                        src:ownd_ref_compnr::integer AS ownd_ref_compnr, 
                        src:pnbr::varchar AS pnbr, 
                        src:prcm::integer AS prcm, 
                        src:prod::integer AS prod, 
                        src:proj::varchar AS proj, 
                        src:pudt::date AS pudt, 
                        src:ramt::numeric(38, 17) AS ramt, 
                        src:ratd::datetime AS ratd, 
                        src:rate_1::numeric(38, 17) AS rate_1, 
                        src:rate_2::numeric(38, 17) AS rate_2, 
                        src:rate_3::numeric(38, 17) AS rate_3, 
                        src:ratf_1::integer AS ratf_1, 
                        src:ratf_2::integer AS ratf_2, 
                        src:ratf_3::integer AS ratf_3, 
                        src:ratu::integer AS ratu, 
                        src:ratu_kw::varchar AS ratu_kw, 
                        src:rcmp::integer AS rcmp, 
                        src:rext::varchar AS rext, 
                        src:rnbr::varchar AS rnbr, 
                        src:rtyp::varchar AS rtyp, 
                        src:ryer::integer AS ryer, 
                        src:s179::numeric(38, 17) AS s179, 
                        src:salv::numeric(38, 17) AS salv, 
                        src:sctg::varchar AS sctg, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:shft::numeric(38, 17) AS shft, 
                        src:simu::integer AS simu, 
                        src:simu_kw::varchar AS simu_kw, 
                        src:snbr::varchar AS snbr, 
                        src:srvc::date AS srvc, 
                        src:stat::integer AS stat, 
                        src:stat_kw::varchar AS stat_kw, 
                        src:tagn::object AS tagn, 
                        src:timestamp::datetime AS timestamp, 
                        src:tqty::integer AS tqty, 
                        src:trfg::integer AS trfg, 
                        src:trfg_kw::varchar AS trfg_kw, 
                        src:tuam_1::numeric(38, 17) AS tuam_1, 
                        src:tuam_2::numeric(38, 17) AS tuam_2, 
                        src:tuam_3::numeric(38, 17) AS tuam_3, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:txtb::integer AS txtb, 
                        src:txtb_ref_compnr::integer AS txtb_ref_compnr, 
                        src:ucsh::integer AS ucsh, 
                        src:ucsh_kw::varchar AS ucsh_kw, 
                        src:username::varchar AS username, 
                        src:vinb::integer AS vinb, 
                        src:vinb_kw::varchar AS vinb_kw, 
                        src:vint::varchar AS vint, 
                        src:vint_ref_compnr::integer AS vint_ref_compnr, 
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
                                        
                src:anbr,
                src:compnr,
                src:aext  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM100 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aacq_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aacq_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aacq_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acmc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acmc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acmc_3), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:acmd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:agrp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atbc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atbc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atbc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:auto), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bpct), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:c263), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:capi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_atyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_ipst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_utyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cola), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:comp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cost), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csth_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csth_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csth_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctgy_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctgy_sctg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:curr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:date), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dbfg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itca), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itcd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:list), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ltdd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:newu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oqty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:owco), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:owco_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ownc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ownd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prcm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prod), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:pudt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ramt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ratd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ryer), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:s179), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:salv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:shft), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:simu), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:srvc), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stat), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tqty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trfg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tuam_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tuam_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tuam_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ucsh), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vinb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vint_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null