CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TPPDM600 AS SELECT
                        src:abpp::integer AS abpp, 
                        src:abpp_kw::varchar AS abpp_kw, 
                        src:abrf::integer AS abrf, 
                        src:abrf_kw::varchar AS abrf_kw, 
                        src:acgm::integer AS acgm, 
                        src:acgm_kw::varchar AS acgm_kw, 
                        src:acmp::integer AS acmp, 
                        src:acmp_kw::varchar AS acmp_kw, 
                        src:acpc::numeric(38, 17) AS acpc, 
                        src:aerf::numeric(38, 17) AS aerf, 
                        src:aext::varchar AS aext, 
                        src:agty::integer AS agty, 
                        src:agty_kw::varchar AS agty_kw, 
                        src:anbr::varchar AS anbr, 
                        src:aprp::numeric(38, 17) AS aprp, 
                        src:arpc::numeric(38, 17) AS arpc, 
                        src:arrl::numeric(38, 17) AS arrl, 
                        src:arrm::integer AS arrm, 
                        src:arrm_kw::varchar AS arrm_kw, 
                        src:arrt::numeric(38, 17) AS arrt, 
                        src:ascd::object AS ascd, 
                        src:avtp::varchar AS avtp, 
                        src:avtp_ref_compnr::integer AS avtp_ref_compnr, 
                        src:bdmt::integer AS bdmt, 
                        src:bdmt_kw::varchar AS bdmt_kw, 
                        src:bfpp::integer AS bfpp, 
                        src:bfpp_kw::varchar AS bfpp_kw, 
                        src:bfrf::integer AS bfrf, 
                        src:bfrf_kw::varchar AS bfrf_kw, 
                        src:bqua::numeric(38, 17) AS bqua, 
                        src:btco::integer AS btco, 
                        src:btco_kw::varchar AS btco_kw, 
                        src:btdt::datetime AS btdt, 
                        src:btpr::integer AS btpr, 
                        src:btpr_kw::varchar AS btpr_kw, 
                        src:capp::integer AS capp, 
                        src:capp_kw::varchar AS capp_kw, 
                        src:carr::varchar AS carr, 
                        src:carr_ref_compnr::integer AS carr_ref_compnr, 
                        src:ccal::varchar AS ccal, 
                        src:ccam::varchar AS ccam, 
                        src:ccam_ref_compnr::integer AS ccam_ref_compnr, 
                        src:ccat::varchar AS ccat, 
                        src:ccat_ref_compnr::integer AS ccat_ref_compnr, 
                        src:ccdt::datetime AS ccdt, 
                        src:ccot::varchar AS ccot, 
                        src:ccot_ref_compnr::integer AS ccot_ref_compnr, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cdf_ampc::varchar AS cdf_ampc, 
                        src:cdf_ampc_ref_compnr::integer AS cdf_ampc_ref_compnr, 
                        src:cdf_ampd::varchar AS cdf_ampd, 
                        src:cdf_awip::numeric(38, 17) AS cdf_awip, 
                        src:cdf_bcpc::integer AS cdf_bcpc, 
                        src:cdf_bcpc_kw::varchar AS cdf_bcpc_kw, 
                        src:cdf_camt::numeric(38, 17) AS cdf_camt, 
                        src:cdf_cpin::integer AS cdf_cpin, 
                        src:cdf_cpin_kw::varchar AS cdf_cpin_kw, 
                        src:cdf_fwcc::integer AS cdf_fwcc, 
                        src:cdf_fwcc_kw::varchar AS cdf_fwcc_kw, 
                        src:cdf_fwdc::integer AS cdf_fwdc, 
                        src:cdf_fwdc_ref_compnr::integer AS cdf_fwdc_ref_compnr, 
                        src:cdf_fwic::integer AS cdf_fwic, 
                        src:cdf_fwic_kw::varchar AS cdf_fwic_kw, 
                        src:cdf_fwpc::object AS cdf_fwpc, 
                        src:cdf_grow::integer AS cdf_grow, 
                        src:cdf_locn::integer AS cdf_locn, 
                        src:cdf_locn_kw::varchar AS cdf_locn_kw, 
                        src:cdf_lprj::varchar AS cdf_lprj, 
                        src:cdf_lser::integer AS cdf_lser, 
                        src:cdf_pacd::datetime AS cdf_pacd, 
                        src:cdf_pnam::object AS cdf_pnam, 
                        src:cdf_prjc::integer AS cdf_prjc, 
                        src:cdf_prjc_kw::varchar AS cdf_prjc_kw, 
                        src:cdf_prju::integer AS cdf_prju, 
                        src:cdf_prju_ref_compnr::integer AS cdf_prju_ref_compnr, 
                        src:cdf_renw::integer AS cdf_renw, 
                        src:cdf_sdtl::integer AS cdf_sdtl, 
                        src:cdf_sdtl_kw::varchar AS cdf_sdtl_kw, 
                        src:cdf_sqtl::integer AS cdf_sqtl, 
                        src:cdf_sqtl_kw::varchar AS cdf_sqtl_kw, 
                        src:cdf_svcc::integer AS cdf_svcc, 
                        src:cdf_svcc_kw::varchar AS cdf_svcc_kw, 
                        src:cdf_tcac::numeric(38, 17) AS cdf_tcac, 
                        src:cdf_tcbl::numeric(38, 17) AS cdf_tcbl, 
                        src:cdf_tcfc::numeric(38, 17) AS cdf_tcfc, 
                        src:cdf_tcfl::integer AS cdf_tcfl, 
                        src:cdf_tcfl_kw::varchar AS cdf_tcfl_kw, 
                        src:cdf_tflc::integer AS cdf_tflc, 
                        src:cdf_tflc_kw::varchar AS cdf_tflc_kw, 
                        src:cdf_tlct::integer AS cdf_tlct, 
                        src:cdf_tlct_ref_compnr::integer AS cdf_tlct_ref_compnr, 
                        src:cdf_tmtl::integer AS cdf_tmtl, 
                        src:cdf_tmtl_kw::varchar AS cdf_tmtl_kw, 
                        src:cdf_trfl::integer AS cdf_trfl, 
                        src:cdf_trfl_kw::varchar AS cdf_trfl_kw, 
                        src:cdf_uamt::numeric(38, 17) AS cdf_uamt, 
                        src:cdis::varchar AS cdis, 
                        src:cdis_ref_compnr::integer AS cdis_ref_compnr, 
                        src:cfac::varchar AS cfac, 
                        src:cfac_ref_compnr::integer AS cfac_ref_compnr, 
                        src:cfpr::integer AS cfpr, 
                        src:cfye::integer AS cfye, 
                        src:cldr::varchar AS cldr, 
                        src:cldr_ref_compnr::integer AS cldr_ref_compnr, 
                        src:cmog::integer AS cmog, 
                        src:cmog_kw::varchar AS cmog_kw, 
                        src:cmpc::integer AS cmpc, 
                        src:cmpc_kw::varchar AS cmpc_kw, 
                        src:cmud::varchar AS cmud, 
                        src:cmud_ref_compnr::integer AS cmud_ref_compnr, 
                        src:cobs::varchar AS cobs, 
                        src:cobs_ref_compnr::integer AS cobs_ref_compnr, 
                        src:cogc::numeric(38, 17) AS cogc, 
                        src:cogs::integer AS cogs, 
                        src:cogs_kw::varchar AS cogs_kw, 
                        src:compnr::integer AS compnr, 
                        src:copr::numeric(38, 17) AS copr, 
                        src:coty::integer AS coty, 
                        src:coty_kw::varchar AS coty_kw, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ccal_ref_compnr::integer AS cprj_ccal_ref_compnr, 
                        src:cprj_cpla_ref_compnr::integer AS cprj_cpla_ref_compnr, 
                        src:cprj_cpla_rsac_ref_compnr::integer AS cprj_cpla_rsac_ref_compnr, 
                        src:cprj_cspa_ref_compnr::integer AS cprj_cspa_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cprq::integer AS cprq, 
                        src:cprq_kw::varchar AS cprq_kw, 
                        src:creg::varchar AS creg, 
                        src:creg_ref_compnr::integer AS creg_ref_compnr, 
                        src:crte::varchar AS crte, 
                        src:crte_ref_compnr::integer AS crte_ref_compnr, 
                        src:crtp::varchar AS crtp, 
                        src:crtp_ref_compnr::integer AS crtp_ref_compnr, 
                        src:csec::varchar AS csec, 
                        src:csec_ref_compnr::integer AS csec_ref_compnr, 
                        src:cspa::varchar AS cspa, 
                        src:cstg::varchar AS cstg, 
                        src:cstg_ref_compnr::integer AS cstg_ref_compnr, 
                        src:cuni::varchar AS cuni, 
                        src:cuni_ref_compnr::integer AS cuni_ref_compnr, 
                        src:dacd::varchar AS dacd, 
                        src:decd::varchar AS decd, 
                        src:deleted::boolean AS deleted, 
                        src:dlau::datetime AS dlau, 
                        src:dldt::datetime AS dldt, 
                        src:dpcd::varchar AS dpcd, 
                        src:dsca::object AS dsca, 
                        src:dscb::object AS dscb, 
                        src:dscc::object AS dscc, 
                        src:dscd::object AS dscd, 
                        src:dwar::varchar AS dwar, 
                        src:dwar_ref_compnr::integer AS dwar_ref_compnr, 
                        src:earf::numeric(38, 17) AS earf, 
                        src:edbg::datetime AS edbg, 
                        src:entu::varchar AS entu, 
                        src:entu_rcmp::integer AS entu_rcmp, 
                        src:espt::numeric(38, 17) AS espt, 
                        src:fddt::datetime AS fddt, 
                        src:guid::varchar AS guid, 
                        src:habo::integer AS habo, 
                        src:habo_kw::varchar AS habo_kw, 
                        src:hbpc::numeric(38, 17) AS hbpc, 
                        src:hebo::integer AS hebo, 
                        src:hebo_kw::varchar AS hebo_kw, 
                        src:igcd::integer AS igcd, 
                        src:igcd_kw::varchar AS igcd_kw, 
                        src:inst::varchar AS inst, 
                        src:intp::integer AS intp, 
                        src:intp_kw::varchar AS intp_kw, 
                        src:invm::integer AS invm, 
                        src:invm_kw::varchar AS invm_kw, 
                        src:invr::integer AS invr, 
                        src:invr_kw::varchar AS invr_kw, 
                        src:ipin::integer AS ipin, 
                        src:ipin_kw::varchar AS ipin_kw, 
                        src:kopr::integer AS kopr, 
                        src:kopr_kw::varchar AS kopr_kw, 
                        src:kptm::integer AS kptm, 
                        src:kptm_kw::varchar AS kptm_kw, 
                        src:lcdt::datetime AS lcdt, 
                        src:lcla::varchar AS lcla, 
                        src:lclg::varchar AS lclg, 
                        src:lmdt::datetime AS lmdt, 
                        src:lmus::varchar AS lmus, 
                        src:logc::varchar AS logc, 
                        src:logh::varchar AS logh, 
                        src:lpdt::datetime AS lpdt, 
                        src:ltrc::datetime AS ltrc, 
                        src:ltrs::datetime AS ltrs, 
                        src:lwha::integer AS lwha, 
                        src:lwha_kw::varchar AS lwha_kw, 
                        src:lwhb::integer AS lwhb, 
                        src:lwhb_kw::varchar AS lwhb_kw, 
                        src:lwhc::integer AS lwhc, 
                        src:lwhc_kw::varchar AS lwhc_kw, 
                        src:lwhd::integer AS lwhd, 
                        src:lwhd_kw::varchar AS lwhd_kw, 
                        src:lwta::integer AS lwta, 
                        src:lwta_kw::varchar AS lwta_kw, 
                        src:lwtb::integer AS lwtb, 
                        src:lwtb_kw::varchar AS lwtb_kw, 
                        src:lwtc::integer AS lwtc, 
                        src:lwtc_kw::varchar AS lwtc_kw, 
                        src:mprj::varchar AS mprj, 
                        src:mprj_rcmp::integer AS mprj_rcmp, 
                        src:ncmp::integer AS ncmp, 
                        src:njsp::integer AS njsp, 
                        src:ofes::varchar AS ofes, 
                        src:ofes_ref_compnr::integer AS ofes_ref_compnr, 
                        src:ofpr::varchar AS ofpr, 
                        src:ofpr_ref_compnr::integer AS ofpr_ref_compnr, 
                        src:padr::varchar AS padr, 
                        src:padr_ref_compnr::integer AS padr_ref_compnr, 
                        src:pcba::numeric(38, 17) AS pcba, 
                        src:pcbq::numeric(38, 17) AS pcbq, 
                        src:pceq::numeric(38, 17) AS pceq, 
                        src:pcic::numeric(38, 17) AS pcic, 
                        src:pcit::numeric(38, 17) AS pcit, 
                        src:pcmp::numeric(38, 17) AS pcmp, 
                        src:pcrd::datetime AS pcrd, 
                        src:pcsb::numeric(38, 17) AS pcsb, 
                        src:pcts::numeric(38, 17) AS pcts, 
                        src:pcwa::numeric(38, 17) AS pcwa, 
                        src:peri::integer AS peri, 
                        src:plcd::integer AS plcd, 
                        src:plcd_kw::varchar AS plcd_kw, 
                        src:plgr::varchar AS plgr, 
                        src:plgr_ref_compnr::integer AS plgr_ref_compnr, 
                        src:pmfc::varchar AS pmfc, 
                        src:pmfc_rcmp::integer AS pmfc_rcmp, 
                        src:pmfc_ref_compnr::integer AS pmfc_ref_compnr, 
                        src:pmng::varchar AS pmng, 
                        src:pmng_ref_compnr::integer AS pmng_ref_compnr, 
                        src:potp::varchar AS potp, 
                        src:potp_ref_compnr::integer AS potp_ref_compnr, 
                        src:preq::integer AS preq, 
                        src:preq_kw::varchar AS preq_kw, 
                        src:prgm::varchar AS prgm, 
                        src:prgm_ref_compnr::integer AS prgm_ref_compnr, 
                        src:pric::integer AS pric, 
                        src:pric_kw::varchar AS pric_kw, 
                        src:prit::integer AS prit, 
                        src:prit_kw::varchar AS prit_kw, 
                        src:pror::integer AS pror, 
                        src:pror_kw::varchar AS pror_kw, 
                        src:prpc::numeric(38, 17) AS prpc, 
                        src:prre::integer AS prre, 
                        src:prre_kw::varchar AS prre_kw, 
                        src:prsb::integer AS prsb, 
                        src:prsb_kw::varchar AS prsb_kw, 
                        src:prts::integer AS prts, 
                        src:prts_kw::varchar AS prts_kw, 
                        src:pscd::object AS pscd, 
                        src:pseq::integer AS pseq, 
                        src:psic::integer AS psic, 
                        src:psit::integer AS psit, 
                        src:pssc::integer AS pssc, 
                        src:psta::integer AS psta, 
                        src:pstf::integer AS pstf, 
                        src:pstf_kw::varchar AS pstf_kw, 
                        src:psts::integer AS psts, 
                        src:psts_kw::varchar AS psts_kw, 
                        src:ptcs::numeric(38, 17) AS ptcs, 
                        src:pwar::varchar AS pwar, 
                        src:pwar_ref_compnr::integer AS pwar_ref_compnr, 
                        src:ratc::numeric(38, 17) AS ratc, 
                        src:rate_1::numeric(38, 17) AS rate_1, 
                        src:rate_2::numeric(38, 17) AS rate_2, 
                        src:rate_3::numeric(38, 17) AS rate_3, 
                        src:ratf_1::integer AS ratf_1, 
                        src:ratf_2::integer AS ratf_2, 
                        src:ratf_3::integer AS ratf_3, 
                        src:rats::numeric(38, 17) AS rats, 
                        src:rbqu::numeric(38, 17) AS rbqu, 
                        src:rcmc_1::numeric(38, 17) AS rcmc_1, 
                        src:rcmc_2::numeric(38, 17) AS rcmc_2, 
                        src:rcmc_3::numeric(38, 17) AS rcmc_3, 
                        src:rdat::datetime AS rdat, 
                        src:rdbg::datetime AS rdbg, 
                        src:revl::numeric(38, 17) AS revl, 
                        src:revr::integer AS revr, 
                        src:revr_kw::varchar AS revr_kw, 
                        src:revt::numeric(38, 17) AS revt, 
                        src:rfcu::object AS rfcu, 
                        src:ripr::varchar AS ripr, 
                        src:ripr_ref_compnr::integer AS ripr_ref_compnr, 
                        src:rjct::integer AS rjct, 
                        src:rjct_kw::varchar AS rjct_kw, 
                        src:rpcb::numeric(38, 17) AS rpcb, 
                        src:rrtp::varchar AS rrtp, 
                        src:rrtp_ref_compnr::integer AS rrtp_ref_compnr, 
                        src:rsac::varchar AS rsac, 
                        src:rsip::varchar AS rsip, 
                        src:rsmc_1::numeric(38, 17) AS rsmc_1, 
                        src:rsmc_2::numeric(38, 17) AS rsmc_2, 
                        src:rsmc_3::numeric(38, 17) AS rsmc_3, 
                        src:rsrv::integer AS rsrv, 
                        src:rtyp::varchar AS rtyp, 
                        src:rtyp_ref_compnr::integer AS rtyp_ref_compnr, 
                        src:sadr::varchar AS sadr, 
                        src:sadr_ref_compnr::integer AS sadr_ref_compnr, 
                        src:seak::object AS seak, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:stdt::datetime AS stdt, 
                        src:stus::integer AS stus, 
                        src:stus_kw::varchar AS stus_kw, 
                        src:tbdg::integer AS tbdg, 
                        src:tbdg_kw::varchar AS tbdg_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:tmpl::integer AS tmpl, 
                        src:tmpl_kw::varchar AS tmpl_kw, 
                        src:tpac::varchar AS tpac, 
                        src:uobs::integer AS uobs, 
                        src:uobs_kw::varchar AS uobs_kw, 
                        src:upal::integer AS upal, 
                        src:upal_kw::varchar AS upal_kw, 
                        src:user::varchar AS user, 
                        src:username::varchar AS username, 
                        src:ussu::integer AS ussu, 
                        src:ussu_kw::varchar AS ussu_kw, 
                        src:wipc::numeric(38, 17) AS wipc, 
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
                                        
                src:compnr,
                src:cprj  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM600 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:abpp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:abrf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acgm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acpc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aerf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:agty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aprp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:arpc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:arrl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:arrm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:arrt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:avtp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bdmt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bfpp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bfrf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bqua), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:btco), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:btdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:btpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:capp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:carr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccam_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ccdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccot_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_ampc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_awip), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_bcpc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_camt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_cpin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_fwcc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_fwdc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_fwdc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_fwic), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_grow), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_locn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_lser), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cdf_pacd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_prju), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_prju_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_renw), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_sdtl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_sqtl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_svcc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_tcac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_tcbl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_tcfc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_tcfl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_tflc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_tlct), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_tlct_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_tmtl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_trfl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdf_uamt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdis_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfac_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfye), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cldr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmog), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmpc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmud_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cobs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cogc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cogs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:coty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_ccal_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cpla_rsac_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crtp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:dlau), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:dldt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:earf), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:edbg), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:entu_rcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:espt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:fddt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:habo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hbpc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hebo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:igcd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:intp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:invm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:invr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ipin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:kopr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:kptm), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:lcdt), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:lmdt), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:lpdt), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ltrc), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ltrs), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lwha), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lwhb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lwhd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lwta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lwtb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lwtc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mprj_rcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ncmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:njsp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofes_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofpr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:padr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcba), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcbq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pceq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcic), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcit), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcmp), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:pcrd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcsb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcts), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcwa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:peri), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:plcd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:plgr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pmfc_rcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pmfc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pmng_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:potp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:preq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prgm_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pric), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prit), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pror), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prpc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prre), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prsb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prts), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pseq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:psic), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:psit), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pssc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:psta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pstf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:psts), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptcs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rats), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rbqu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcmc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcmc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcmc_3), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rdat), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rdbg), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:revl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:revr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:revt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ripr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rjct), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rpcb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrtp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rsmc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rsmc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rsmc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rsrv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sadr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:stdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stus), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tbdg), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tmpl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uobs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:upal), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ussu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wipc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null