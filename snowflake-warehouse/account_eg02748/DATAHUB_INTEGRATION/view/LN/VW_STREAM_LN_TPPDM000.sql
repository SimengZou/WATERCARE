CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TPPDM000 AS SELECT
                        src:achs::integer AS achs, 
                        src:achs_kw::varchar AS achs_kw, 
                        src:acus::integer AS acus, 
                        src:acus_kw::varchar AS acus_kw, 
                        src:bghi::integer AS bghi, 
                        src:bghi_kw::varchar AS bghi_kw, 
                        src:bgnw::integer AS bgnw, 
                        src:bgnw_kw::varchar AS bgnw_kw, 
                        src:cacc::integer AS cacc, 
                        src:cacc_kw::varchar AS cacc_kw, 
                        src:cact::integer AS cact, 
                        src:cact_kw::varchar AS cact_kw, 
                        src:cacu::integer AS cacu, 
                        src:cacu_kw::varchar AS cacu_kw, 
                        src:capp::varchar AS capp, 
                        src:capp_ref_compnr::integer AS capp_ref_compnr, 
                        src:ccpr::integer AS ccpr, 
                        src:ccpr_kw::varchar AS ccpr_kw, 
                        src:ccus::integer AS ccus, 
                        src:ccus_kw::varchar AS ccus_kw, 
                        src:cdf_acnl::integer AS cdf_acnl, 
                        src:cdf_acnl_kw::varchar AS cdf_acnl_kw, 
                        src:cdf_ocrl::integer AS cdf_ocrl, 
                        src:cdf_ocrl_kw::varchar AS cdf_ocrl_kw, 
                        src:cdf_scrl::integer AS cdf_scrl, 
                        src:cdf_scrl_kw::varchar AS cdf_scrl_kw, 
                        src:cgch::integer AS cgch, 
                        src:cgch_kw::varchar AS cgch_kw, 
                        src:citg::varchar AS citg, 
                        src:citg_ref_compnr::integer AS citg_ref_compnr, 
                        src:citt::varchar AS citt, 
                        src:citt_ref_compnr::integer AS citt_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cpac::integer AS cpac, 
                        src:cpac_kw::varchar AS cpac_kw, 
                        src:cprc::integer AS cprc, 
                        src:cprc_kw::varchar AS cprc_kw, 
                        src:cprt::integer AS cprt, 
                        src:cprt_kw::varchar AS cprt_kw, 
                        src:cpru::integer AS cpru, 
                        src:cpru_kw::varchar AS cpru_kw, 
                        src:cpsp::integer AS cpsp, 
                        src:cpsp_kw::varchar AS cpsp_kw, 
                        src:cpst::integer AS cpst, 
                        src:cpst_kw::varchar AS cpst_kw, 
                        src:crtp::varchar AS crtp, 
                        src:crtp_ref_compnr::integer AS crtp_ref_compnr, 
                        src:cspc::integer AS cspc, 
                        src:cspc_kw::varchar AS cspc_kw, 
                        src:cspt::integer AS cspt, 
                        src:cspt_kw::varchar AS cspt_kw, 
                        src:cspu::integer AS cspu, 
                        src:cspu_kw::varchar AS cspu_kw, 
                        src:cstc::integer AS cstc, 
                        src:cstc_kw::varchar AS cstc_kw, 
                        src:cstt::integer AS cstt, 
                        src:cstt_kw::varchar AS cstt_kw, 
                        src:cstu::integer AS cstu, 
                        src:cstu_kw::varchar AS cstu_kw, 
                        src:ctut::varchar AS ctut, 
                        src:ctut_ref_compnr::integer AS ctut_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dpca::varchar AS dpca, 
                        src:dpca_ref_compnr::integer AS dpca_ref_compnr, 
                        src:dpch::varchar AS dpch, 
                        src:dpch_ref_compnr::integer AS dpch_ref_compnr, 
                        src:dpci::varchar AS dpci, 
                        src:dpci_ref_compnr::integer AS dpci_ref_compnr, 
                        src:dsca::object AS dsca, 
                        src:egrp::varchar AS egrp, 
                        src:egrp_essr_ref_compnr::integer AS egrp_essr_ref_compnr, 
                        src:elhs::integer AS elhs, 
                        src:elhs_kw::varchar AS elhs_kw, 
                        src:elus::integer AS elus, 
                        src:elus_kw::varchar AS elus_kw, 
                        src:essr::varchar AS essr, 
                        src:ests::varchar AS ests, 
                        src:exus::integer AS exus, 
                        src:exus_kw::varchar AS exus_kw, 
                        src:frvt::numeric(38, 17) AS frvt, 
                        src:indt::datetime AS indt, 
                        src:lccc::integer AS lccc, 
                        src:lccc_kw::varchar AS lccc_kw, 
                        src:lexp::integer AS lexp, 
                        src:lexp_kw::varchar AS lexp_kw, 
                        src:ngrp::varchar AS ngrp, 
                        src:ngrp_ests_ref_compnr::integer AS ngrp_ests_ref_compnr, 
                        src:ngrp_prsr_ref_compnr::integer AS ngrp_prsr_ref_compnr, 
                        src:ngrp_ref_compnr::integer AS ngrp_ref_compnr, 
                        src:nlci::integer AS nlci, 
                        src:nleq::integer AS nleq, 
                        src:nlit::integer AS nlit, 
                        src:nlsb::integer AS nlsb, 
                        src:nlta::integer AS nlta, 
                        src:ohac::integer AS ohac, 
                        src:ohac_kw::varchar AS ohac_kw, 
                        src:ohhc::integer AS ohhc, 
                        src:ohhc_kw::varchar AS ohhc_kw, 
                        src:ohim::integer AS ohim, 
                        src:ohim_kw::varchar AS ohim_kw, 
                        src:pcmp::integer AS pcmp, 
                        src:pcmp_kw::varchar AS pcmp_kw, 
                        src:pcsp::integer AS pcsp, 
                        src:pcsp_kw::varchar AS pcsp_kw, 
                        src:plmc::integer AS plmc, 
                        src:plmc_ref_compnr::integer AS plmc_ref_compnr, 
                        src:plmi::integer AS plmi, 
                        src:plmi_kw::varchar AS plmi_kw, 
                        src:prbs::integer AS prbs, 
                        src:prbs_kw::varchar AS prbs_kw, 
                        src:prhs::integer AS prhs, 
                        src:prhs_kw::varchar AS prhs_kw, 
                        src:prsr::varchar AS prsr, 
                        src:pwar::varchar AS pwar, 
                        src:pwar_ref_compnr::integer AS pwar_ref_compnr, 
                        src:rcap::varchar AS rcap, 
                        src:rcap_ref_compnr::integer AS rcap_ref_compnr, 
                        src:rcfe::varchar AS rcfe, 
                        src:rcfe_ref_compnr::integer AS rcfe_ref_compnr, 
                        src:rcpp::varchar AS rcpp, 
                        src:rcpp_ref_compnr::integer AS rcpp_ref_compnr, 
                        src:rcpt::varchar AS rcpt, 
                        src:rcpt_ref_compnr::integer AS rcpt_ref_compnr, 
                        src:rrtp::varchar AS rrtp, 
                        src:rrtp_ref_compnr::integer AS rrtp_ref_compnr, 
                        src:rscs::varchar AS rscs, 
                        src:rtus::integer AS rtus, 
                        src:rtus_kw::varchar AS rtus_kw, 
                        src:rtyp::varchar AS rtyp, 
                        src:rtyp_ref_compnr::integer AS rtyp_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:serr::varchar AS serr, 
                        src:text::integer AS text, 
                        src:text_ref_compnr::integer AS text_ref_compnr, 
                        src:tgrp::varchar AS tgrp, 
                        src:tgrp_ref_compnr::integer AS tgrp_ref_compnr, 
                        src:tgrp_tpsr_ref_compnr::integer AS tgrp_tpsr_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:tpsr::varchar AS tpsr, 
                        src:tpte::varchar AS tpte, 
                        src:tpte_ref_compnr::integer AS tpte_ref_compnr, 
                        src:upra::integer AS upra, 
                        src:upra_kw::varchar AS upra_kw, 
                        src:username::varchar AS username, 
                        src:uset::varchar AS uset, 
                        src:uset_ref_compnr::integer AS uset_ref_compnr, 
                        src:usev::integer AS usev, 
                        src:usev_kw::varchar AS usev_kw, 
                        src:uwka::integer AS uwka, 
                        src:uwka_kw::varchar AS uwka_kw, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:indt,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM000 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:achs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acus), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bghi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bgnw), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cacc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cact), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cacu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:capp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccus), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_acnl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_ocrl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_scrl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cgch), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpru), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpsp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crtp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cspc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cspt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cspu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctut_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dpca_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dpch_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dpci_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:egrp_essr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:elhs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:elus), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:exus), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:frvt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:indt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lccc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lexp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ngrp_ests_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ngrp_prsr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ngrp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nlci), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nleq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nlit), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nlsb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nlta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ohac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ohhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ohim), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcsp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:plmc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:plmc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:plmi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prbs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prhs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcap_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcfe_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcpp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcpt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrtp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtus), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tgrp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tgrp_tpsr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tpte_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:upra), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uset_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:usev), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uwka), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null