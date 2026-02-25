CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TCCOM000 AS SELECT
                        src:acfg::integer AS acfg, 
                        src:acfg_kw::varchar AS acfg_kw, 
                        src:adcc::integer AS adcc, 
                        src:adcc_kw::varchar AS adcc_kw, 
                        src:alhp::integer AS alhp, 
                        src:alhp_kw::varchar AS alhp_kw, 
                        src:altm::integer AS altm, 
                        src:altm_kw::varchar AS altm_kw, 
                        src:arcc::integer AS arcc, 
                        src:arcc_kw::varchar AS arcc_kw, 
                        src:ardc::integer AS ardc, 
                        src:asci::integer AS asci, 
                        src:asci_kw::varchar AS asci_kw, 
                        src:asec::integer AS asec, 
                        src:asec_kw::varchar AS asec_kw, 
                        src:bape::integer AS bape, 
                        src:bape_kw::varchar AS bape_kw, 
                        src:bcmi::integer AS bcmi, 
                        src:bcmi_kw::varchar AS bcmi_kw, 
                        src:beid::object AS beid, 
                        src:bgci::integer AS bgci, 
                        src:bgci_kw::varchar AS bgci_kw, 
                        src:bulg::integer AS bulg, 
                        src:bulg_kw::varchar AS bulg_kw, 
                        src:cadr::varchar AS cadr, 
                        src:cadr_ref_compnr::integer AS cadr_ref_compnr, 
                        src:capp::integer AS capp, 
                        src:capp_kw::varchar AS capp_kw, 
                        src:cbrk::integer AS cbrk, 
                        src:cbrk_kw::varchar AS cbrk_kw, 
                        src:cbrn::varchar AS cbrn, 
                        src:cbtp::varchar AS cbtp, 
                        src:ccty::varchar AS ccty, 
                        src:ccty_ref_compnr::integer AS ccty_ref_compnr, 
                        src:cfbs::integer AS cfbs, 
                        src:cfbs_kw::varchar AS cfbs_kw, 
                        src:cfri::integer AS cfri, 
                        src:cfri_kw::varchar AS cfri_kw, 
                        src:cinv::integer AS cinv, 
                        src:cinv_kw::varchar AS cinv_kw, 
                        src:clan::varchar AS clan, 
                        src:clan_ref_compnr::integer AS clan_ref_compnr, 
                        src:clbi::integer AS clbi, 
                        src:clbi_kw::varchar AS clbi_kw, 
                        src:cmai::varchar AS cmai, 
                        src:compnr::integer AS compnr, 
                        src:corg::integer AS corg, 
                        src:corg_kw::varchar AS corg_kw, 
                        src:cpli::integer AS cpli, 
                        src:cpli_kw::varchar AS cpli_kw, 
                        src:crid::object AS crid, 
                        src:csli::integer AS csli, 
                        src:csli_kw::varchar AS csli_kw, 
                        src:darc::integer AS darc, 
                        src:dasr::integer AS dasr, 
                        src:dasr_kw::varchar AS dasr_kw, 
                        src:deleted::boolean AS deleted, 
                        src:deln::integer AS deln, 
                        src:deln_kw::varchar AS deln_kw, 
                        src:dsca::object AS dsca, 
                        src:dsri::integer AS dsri, 
                        src:dsri_kw::varchar AS dsri_kw, 
                        src:duns::integer AS duns, 
                        src:eaas::integer AS eaas, 
                        src:eaas_kw::varchar AS eaas_kw, 
                        src:ecmi::integer AS ecmi, 
                        src:ecmi_kw::varchar AS ecmi_kw, 
                        src:erac::integer AS erac, 
                        src:erac_kw::varchar AS erac_kw, 
                        src:eupl::integer AS eupl, 
                        src:eupl_kw::varchar AS eupl_kw, 
                        src:eusl::integer AS eusl, 
                        src:eusl_kw::varchar AS eusl_kw, 
                        src:eust::integer AS eust, 
                        src:eust_kw::varchar AS eust_kw, 
                        src:ffpl::integer AS ffpl, 
                        src:ffpl_kw::varchar AS ffpl_kw, 
                        src:fidn::varchar AS fidn, 
                        src:fini::integer AS fini, 
                        src:fini_kw::varchar AS fini_kw, 
                        src:fovn::varchar AS fovn, 
                        src:frmi::integer AS frmi, 
                        src:frmi_kw::varchar AS frmi_kw, 
                        src:fstk::integer AS fstk, 
                        src:fstk_kw::varchar AS fstk_kw, 
                        src:grpl::integer AS grpl, 
                        src:grpl_kw::varchar AS grpl_kw, 
                        src:gtci::integer AS gtci, 
                        src:gtci_kw::varchar AS gtci_kw, 
                        src:gtro::integer AS gtro, 
                        src:gtro_kw::varchar AS gtro_kw, 
                        src:hcst::integer AS hcst, 
                        src:hcst_kw::varchar AS hcst_kw, 
                        src:ilwh::integer AS ilwh, 
                        src:ilwh_kw::varchar AS ilwh_kw, 
                        src:indt::datetime AS indt, 
                        src:inf1::object AS inf1, 
                        src:inf2::object AS inf2, 
                        src:inf3::object AS inf3, 
                        src:inf4::object AS inf4, 
                        src:inpr::integer AS inpr, 
                        src:inpr_kw::varchar AS inpr_kw, 
                        src:intr::integer AS intr, 
                        src:intr_kw::varchar AS intr_kw, 
                        src:itpd::integer AS itpd, 
                        src:itpd_kw::varchar AS itpd_kw, 
                        src:itri::integer AS itri, 
                        src:itri_kw::varchar AS itri_kw, 
                        src:jspm::integer AS jspm, 
                        src:jspm_kw::varchar AS jspm_kw, 
                        src:lago::integer AS lago, 
                        src:lago_kw::varchar AS lago_kw, 
                        src:larg::integer AS larg, 
                        src:larg_kw::varchar AS larg_kw, 
                        src:lbra::integer AS lbra, 
                        src:lbra_kw::varchar AS lbra_kw, 
                        src:lche::integer AS lche, 
                        src:lche_kw::varchar AS lche_kw, 
                        src:lchl::integer AS lchl, 
                        src:lchl_kw::varchar AS lchl_kw, 
                        src:lchn::integer AS lchn, 
                        src:lchn_kw::varchar AS lchn_kw, 
                        src:lcol::integer AS lcol, 
                        src:lcol_kw::varchar AS lcol_kw, 
                        src:lcsi::integer AS lcsi, 
                        src:lcsi_kw::varchar AS lcsi_kw, 
                        src:lcze::integer AS lcze, 
                        src:lcze_kw::varchar AS lcze_kw, 
                        src:ldeu::integer AS ldeu, 
                        src:ldeu_kw::varchar AS ldeu_kw, 
                        src:lesp::integer AS lesp, 
                        src:lesp_kw::varchar AS lesp_kw, 
                        src:lgbr::integer AS lgbr, 
                        src:lgbr_kw::varchar AS lgbr_kw, 
                        src:lgid::object AS lgid, 
                        src:lhrv::integer AS lhrv, 
                        src:lhrv_kw::varchar AS lhrv_kw, 
                        src:lhun::integer AS lhun, 
                        src:lhun_kw::varchar AS lhun_kw, 
                        src:lidn::integer AS lidn, 
                        src:lidn_kw::varchar AS lidn_kw, 
                        src:lind::integer AS lind, 
                        src:lind_kw::varchar AS lind_kw, 
                        src:lisr::integer AS lisr, 
                        src:lisr_kw::varchar AS lisr_kw, 
                        src:lita::integer AS lita, 
                        src:lita_kw::varchar AS lita_kw, 
                        src:lmex::integer AS lmex, 
                        src:lmex_kw::varchar AS lmex_kw, 
                        src:lmys::integer AS lmys, 
                        src:lmys_kw::varchar AS lmys_kw, 
                        src:lnor::integer AS lnor, 
                        src:lnor_kw::varchar AS lnor_kw, 
                        src:lnwi::integer AS lnwi, 
                        src:lnwi_kw::varchar AS lnwi_kw, 
                        src:lper::integer AS lper, 
                        src:lper_kw::varchar AS lper_kw, 
                        src:lphl::integer AS lphl, 
                        src:lphl_kw::varchar AS lphl_kw, 
                        src:lpol::integer AS lpol, 
                        src:lpol_kw::varchar AS lpol_kw, 
                        src:lpor::integer AS lpor, 
                        src:lpor_kw::varchar AS lpor_kw, 
                        src:lrom::integer AS lrom, 
                        src:lrom_kw::varchar AS lrom_kw, 
                        src:lrus::integer AS lrus, 
                        src:lrus_kw::varchar AS lrus_kw, 
                        src:lsau::integer AS lsau, 
                        src:lsau_kw::varchar AS lsau_kw, 
                        src:lsrb::integer AS lsrb, 
                        src:lsrb_kw::varchar AS lsrb_kw, 
                        src:lsvk::integer AS lsvk, 
                        src:lsvk_kw::varchar AS lsvk_kw, 
                        src:lsvn::integer AS lsvn, 
                        src:lsvn_kw::varchar AS lsvn_kw, 
                        src:ltha::integer AS ltha, 
                        src:ltha_kw::varchar AS ltha_kw, 
                        src:ltur::integer AS ltur, 
                        src:ltur_kw::varchar AS ltur_kw, 
                        src:lvnm::integer AS lvnm, 
                        src:lvnm_kw::varchar AS lvnm_kw, 
                        src:mejs::integer AS mejs, 
                        src:mejs_kw::varchar AS mejs_kw, 
                        src:mesa::integer AS mesa, 
                        src:mesa_kw::varchar AS mesa_kw, 
                        src:mfsi::integer AS mfsi, 
                        src:mfsi_kw::varchar AS mfsi_kw, 
                        src:mnfc::integer AS mnfc, 
                        src:mnfc_kw::varchar AS mnfc_kw, 
                        src:mpni::integer AS mpni, 
                        src:mpni_kw::varchar AS mpni_kw, 
                        src:mprs::integer AS mprs, 
                        src:mprs_kw::varchar AS mprs_kw, 
                        src:mstc::integer AS mstc, 
                        src:mstc_kw::varchar AS mstc_kw, 
                        src:nama::object AS nama, 
                        src:ncmp::integer AS ncmp, 
                        src:odmu::integer AS odmu, 
                        src:odmu_kw::varchar AS odmu_kw, 
                        src:opsi::integer AS opsi, 
                        src:opsi_kw::varchar AS opsi_kw, 
                        src:owne::integer AS owne, 
                        src:owne_kw::varchar AS owne_kw, 
                        src:owni::integer AS owni, 
                        src:owni_kw::varchar AS owni_kw, 
                        src:pcfi::integer AS pcfi, 
                        src:pcfi_kw::varchar AS pcfi_kw, 
                        src:pcmd::integer AS pcmd, 
                        src:pcmd_kw::varchar AS pcmd_kw, 
                        src:pcsi::integer AS pcsi, 
                        src:pcsi_kw::varchar AS pcsi_kw, 
                        src:pdim::integer AS pdim, 
                        src:pdim_kw::varchar AS pdim_kw, 
                        src:pedi::integer AS pedi, 
                        src:pedi_kw::varchar AS pedi_kw, 
                        src:plai::integer AS plai, 
                        src:plai_kw::varchar AS plai_kw, 
                        src:plma::integer AS plma, 
                        src:plma_kw::varchar AS plma_kw, 
                        src:plnp::integer AS plnp, 
                        src:plnp_kw::varchar AS plnp_kw, 
                        src:ppeg::integer AS ppeg, 
                        src:ppeg_kw::varchar AS ppeg_kw, 
                        src:prci::integer AS prci, 
                        src:prci_kw::varchar AS prci_kw, 
                        src:prji::integer AS prji, 
                        src:prji_kw::varchar AS prji_kw, 
                        src:psch::integer AS psch, 
                        src:psch_kw::varchar AS psch_kw, 
                        src:ptri::integer AS ptri, 
                        src:ptri_kw::varchar AS ptri_kw, 
                        src:qumi::integer AS qumi, 
                        src:qumi_kw::varchar AS qumi_kw, 
                        src:rdes::integer AS rdes, 
                        src:rdes_kw::varchar AS rdes_kw, 
                        src:repr::varchar AS repr, 
                        src:rpti::integer AS rpti, 
                        src:rpti_kw::varchar AS rpti_kw, 
                        src:rsbs::integer AS rsbs, 
                        src:rsbs_kw::varchar AS rsbs_kw, 
                        src:scbs::integer AS scbs, 
                        src:scbs_kw::varchar AS scbs_kw, 
                        src:schi::integer AS schi, 
                        src:schi_kw::varchar AS schi_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:siac::object AS siac, 
                        src:simd::integer AS simd, 
                        src:simd_kw::varchar AS simd_kw, 
                        src:slbp::integer AS slbp, 
                        src:slbp_kw::varchar AS slbp_kw, 
                        src:slbs::integer AS slbs, 
                        src:slbs_kw::varchar AS slbs_kw, 
                        src:smfm::integer AS smfm, 
                        src:smfm_kw::varchar AS smfm_kw, 
                        src:smfs::integer AS smfs, 
                        src:smfs_kw::varchar AS smfs_kw, 
                        src:smii::integer AS smii, 
                        src:smii_kw::varchar AS smii_kw, 
                        src:spri::integer AS spri, 
                        src:spri_kw::varchar AS spri_kw, 
                        src:spsp::integer AS spsp, 
                        src:spsp_kw::varchar AS spsp_kw, 
                        src:srvi::integer AS srvi, 
                        src:srvi_kw::varchar AS srvi_kw, 
                        src:ssmf::integer AS ssmf, 
                        src:ssmf_kw::varchar AS ssmf_kw, 
                        src:stat::integer AS stat, 
                        src:stat_kw::varchar AS stat_kw, 
                        src:stbi::integer AS stbi, 
                        src:stbi_kw::varchar AS stbi_kw, 
                        src:tati::integer AS tati, 
                        src:tati_kw::varchar AS tati_kw, 
                        src:taxd::integer AS taxd, 
                        src:taxd_kw::varchar AS taxd_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:tlgi::integer AS tlgi, 
                        src:tlgi_kw::varchar AS tlgi_kw, 
                        src:trcn::integer AS trcn, 
                        src:trcn_kw::varchar AS trcn_kw, 
                        src:ucpu::integer AS ucpu, 
                        src:ucpu_kw::varchar AS ucpu_kw, 
                        src:ucsl::integer AS ucsl, 
                        src:ucsl_kw::varchar AS ucsl_kw, 
                        src:unef::integer AS unef, 
                        src:unef_kw::varchar AS unef_kw, 
                        src:username::varchar AS username, 
                        src:usup::integer AS usup, 
                        src:usup_kw::varchar AS usup_kw, 
                        src:uwht::integer AS uwht, 
                        src:uwht_kw::varchar AS uwht_kw, 
                        src:vmic::integer AS vmic, 
                        src:vmic_kw::varchar AS vmic_kw, 
                        src:vmis::integer AS vmis, 
                        src:vmis_kw::varchar AS vmis_kw, 
                        src:wmbi::integer AS wmbi, 
                        src:wmbi_kw::varchar AS wmbi_kw, 
                        src:wrhi::integer AS wrhi, 
                        src:wrhi_kw::varchar AS wrhi_kw, 
                        src:xtmm::integer AS xtmm, 
                        src:xtmm_kw::varchar AS xtmm_kw, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                src:ncmp,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCCOM000 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acfg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adcc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alhp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:altm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:arcc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ardc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:asci), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:asec), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bape), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bcmi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bgci), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bulg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:capp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cbrk), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfbs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfri), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cinv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clbi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:corg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpli), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csli), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:darc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dasr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:deln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dsri), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:duns), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eaas), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ecmi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:erac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eupl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eusl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eust), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ffpl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fini), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:frmi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fstk), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:grpl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gtci), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gtro), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hcst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ilwh), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:indt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:intr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itpd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itri), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:jspm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lago), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:larg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lbra), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lche), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lchl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lchn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcol), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcsi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcze), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldeu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lesp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lgbr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lhrv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lhun), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lidn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lind), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lisr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lita), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lmex), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lmys), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lnor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lnwi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lper), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lphl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lpol), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lpor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lrom), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lrus), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lsau), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lsrb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lsvk), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lsvn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ltha), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ltur), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lvnm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mejs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mesa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mfsi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mnfc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mpni), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mprs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mstc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ncmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:odmu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:opsi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:owne), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:owni), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcfi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcmd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcsi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pdim), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pedi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:plai), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:plma), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:plnp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ppeg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prci), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prji), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:psch), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptri), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qumi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rdes), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rpti), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rsbs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scbs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:schi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:simd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:slbp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:slbs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:smfm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:smfs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:smii), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spri), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spsp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:srvi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ssmf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stat), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stbi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tati), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:taxd), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tlgi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trcn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ucpu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ucsl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:unef), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:usup), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uwht), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vmic), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vmis), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wmbi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wrhi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:xtmm), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null