CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TPPSS200 AS SELECT
                        src:acmn::varchar AS acmn, 
                        src:acmn_ref_compnr::integer AS acmn_ref_compnr, 
                        src:actp::integer AS actp, 
                        src:addr::varchar AS addr, 
                        src:addr_ref_compnr::integer AS addr_ref_compnr, 
                        src:aext::varchar AS aext, 
                        src:afdt::datetime AS afdt, 
                        src:afrt::integer AS afrt, 
                        src:afrt_kw::varchar AS afrt_kw, 
                        src:anbr::varchar AS anbr, 
                        src:asdt::datetime AS asdt, 
                        src:avtp::varchar AS avtp, 
                        src:avtp_ref_compnr::integer AS avtp_ref_compnr, 
                        src:blbl::integer AS blbl, 
                        src:blbl_kw::varchar AS blbl_kw, 
                        src:cact::varchar AS cact, 
                        src:capa::integer AS capa, 
                        src:capa_kw::varchar AS capa_kw, 
                        src:capt::integer AS capt, 
                        src:capt_kw::varchar AS capt_kw, 
                        src:ccal::varchar AS ccal, 
                        src:ccat::varchar AS ccat, 
                        src:ccat_ref_compnr::integer AS ccat_ref_compnr, 
                        src:cctr::integer AS cctr, 
                        src:cctr_kw::varchar AS cctr_kw, 
                        src:cdf_awip::numeric(38, 17) AS cdf_awip, 
                        src:cdf_camt::numeric(38, 17) AS cdf_camt, 
                        src:cdf_depm::integer AS cdf_depm, 
                        src:cdf_depm_kw::varchar AS cdf_depm_kw, 
                        src:cdf_uamt::numeric(38, 17) AS cdf_uamt, 
                        src:cloc::varchar AS cloc, 
                        src:cnsd::datetime AS cnsd, 
                        src:cnst::integer AS cnst, 
                        src:cnst_kw::varchar AS cnst_kw, 
                        src:coel::varchar AS coel, 
                        src:coel_rcmp::integer AS coel_rcmp, 
                        src:compnr::integer AS compnr, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cloc_ref_compnr::integer AS cprj_cloc_ref_compnr, 
                        src:cprj_cpla_lact_ref_compnr::integer AS cprj_cpla_lact_ref_compnr, 
                        src:cprj_cpla_pact_ref_compnr::integer AS cprj_cpla_pact_ref_compnr, 
                        src:cprj_cpla_ref_compnr::integer AS cprj_cpla_ref_compnr, 
                        src:cprj_cstl_ref_compnr::integer AS cprj_cstl_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:creg::varchar AS creg, 
                        src:creg_ref_compnr::integer AS creg_ref_compnr, 
                        src:csec::varchar AS csec, 
                        src:csec_ref_compnr::integer AS csec_ref_compnr, 
                        src:cspt::integer AS cspt, 
                        src:cspt_kw::varchar AS cspt_kw, 
                        src:cstg::varchar AS cstg, 
                        src:cstg_ref_compnr::integer AS cstg_ref_compnr, 
                        src:cstl::varchar AS cstl, 
                        src:cstv::numeric(38, 17) AS cstv, 
                        src:cuni::varchar AS cuni, 
                        src:cuni_ref_compnr::integer AS cuni_ref_compnr, 
                        src:cuti::varchar AS cuti, 
                        src:cuti_ref_compnr::integer AS cuti_ref_compnr, 
                        src:cuvc::varchar AS cuvc, 
                        src:cuvc_ref_compnr::integer AS cuvc_ref_compnr, 
                        src:ddln::datetime AS ddln, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:dfac::varchar AS dfac, 
                        src:dfac_ref_compnr::integer AS dfac_ref_compnr, 
                        src:dura::numeric(38, 17) AS dura, 
                        src:dwar::varchar AS dwar, 
                        src:dwar_ref_compnr::integer AS dwar_ref_compnr, 
                        src:efdt::datetime AS efdt, 
                        src:eoid::varchar AS eoid, 
                        src:eoty::integer AS eoty, 
                        src:eoty_kw::varchar AS eoty_kw, 
                        src:esdt::datetime AS esdt, 
                        src:ffhr::numeric(38, 17) AS ffhr, 
                        src:fref::numeric(38, 17) AS fref, 
                        src:freq::numeric(38, 17) AS freq, 
                        src:glat::numeric(38, 17) AS glat, 
                        src:glon::numeric(38, 17) AS glon, 
                        src:gris::integer AS gris, 
                        src:hbyn::integer AS hbyn, 
                        src:hbyn_kw::varchar AS hbyn_kw, 
                        src:lact::varchar AS lact, 
                        src:levl::integer AS levl, 
                        src:lfdt::datetime AS lfdt, 
                        src:litm::varchar AS litm, 
                        src:litm_ref_compnr::integer AS litm_ref_compnr, 
                        src:llnr::integer AS llnr, 
                        src:lsdt::datetime AS lsdt, 
                        src:lvps::integer AS lvps, 
                        src:lvps_kw::varchar AS lvps_kw, 
                        src:mamt::numeric(38, 17) AS mamt, 
                        src:mevl::integer AS mevl, 
                        src:mevl_kw::varchar AS mevl_kw, 
                        src:ohdt::datetime AS ohdt, 
                        src:pact::varchar AS pact, 
                        src:pcom::numeric(38, 17) AS pcom, 
                        src:plmc::integer AS plmc, 
                        src:plmp::varchar AS plmp, 
                        src:prin::integer AS prin, 
                        src:prin_kw::varchar AS prin_kw, 
                        src:pris::numeric(38, 17) AS pris, 
                        src:prms::numeric(38, 17) AS prms, 
                        src:prnd::numeric(38, 17) AS prnd, 
                        src:prsp::numeric(38, 17) AS prsp, 
                        src:prss::numeric(38, 17) AS prss, 
                        src:prst::numeric(38, 17) AS prst, 
                        src:pwar::varchar AS pwar, 
                        src:pwar_ref_compnr::integer AS pwar_ref_compnr, 
                        src:qans::numeric(38, 17) AS qans, 
                        src:quan::numeric(38, 17) AS quan, 
                        src:qutm::numeric(38, 17) AS qutm, 
                        src:rcod::varchar AS rcod, 
                        src:rcod_ref_compnr::integer AS rcod_ref_compnr, 
                        src:rdas::datetime AS rdas, 
                        src:rdse::datetime AS rdse, 
                        src:rent::integer AS rent, 
                        src:rent_kw::varchar AS rent_kw, 
                        src:rfac::varchar AS rfac, 
                        src:rfsa_1::integer AS rfsa_1, 
                        src:rfsa_2::integer AS rfsa_2, 
                        src:rfsa_3::integer AS rfsa_3, 
                        src:rfse_1::integer AS rfse_1, 
                        src:rfse_2::integer AS rfse_2, 
                        src:rfse_3::integer AS rfse_3, 
                        src:rout::varchar AS rout, 
                        src:rtsa_1::numeric(38, 17) AS rtsa_1, 
                        src:rtsa_2::numeric(38, 17) AS rtsa_2, 
                        src:rtsa_3::numeric(38, 17) AS rtsa_3, 
                        src:rtse_1::numeric(38, 17) AS rtse_1, 
                        src:rtse_2::numeric(38, 17) AS rtse_2, 
                        src:rtse_3::numeric(38, 17) AS rtse_3, 
                        src:sacu::varchar AS sacu, 
                        src:sacu_ref_compnr::integer AS sacu_ref_compnr, 
                        src:scmd::integer AS scmd, 
                        src:scmd_kw::varchar AS scmd_kw, 
                        src:sdch::integer AS sdch, 
                        src:sdch_kw::varchar AS sdch_kw, 
                        src:seak::object AS seak, 
                        src:secu::varchar AS secu, 
                        src:secu_ref_compnr::integer AS secu_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:setl::integer AS setl, 
                        src:setl_kw::varchar AS setl_kw, 
                        src:sfdt::datetime AS sfdt, 
                        src:ssdt::datetime AS ssdt, 
                        src:stat::integer AS stat, 
                        src:stat_kw::varchar AS stat_kw, 
                        src:tact::integer AS tact, 
                        src:tact_kw::varchar AS tact_kw, 
                        src:tfhr::numeric(38, 17) AS tfhr, 
                        src:timestamp::datetime AS timestamp, 
                        src:tmud::varchar AS tmud, 
                        src:tmud_ref_compnr::integer AS tmud_ref_compnr, 
                        src:totf::numeric(38, 17) AS totf, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:username::varchar AS username, 
                        src:wast::integer AS wast, 
                        src:wast_kw::varchar AS wast_kw, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:compnr,
                src:cprj,
                src:cpla,
                src:cact  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPSS200 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acmn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:actp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:addr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:afdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:afrt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:asdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:avtp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blbl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:capa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:capt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cctr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_awip), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_camt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_depm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_uamt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cnsd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cnst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:coel_rcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cloc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cpla_lact_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cpla_pact_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cspt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ddln), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dfac_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dura), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:efdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eoty), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:esdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ffhr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fref), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:freq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:glat), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:glon), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gris), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hbyn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:levl), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:lfdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:litm_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:llnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:lsdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lvps), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mamt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mevl), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ohdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcom), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:plmc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pris), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prms), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prnd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prsp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prss), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qans), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:quan), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qutm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rdas), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rdse), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rent), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfsa_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfsa_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfsa_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfse_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfse_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfse_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtsa_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtsa_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtsa_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtse_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtse_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtse_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sacu_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scmd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sdch), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:secu_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:setl), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sfdt), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ssdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stat), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tact), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tfhr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tmud_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:totf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wast), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null