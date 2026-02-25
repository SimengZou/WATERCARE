CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_WHWMD200 AS SELECT
                        src:aipd::integer AS aipd, 
                        src:aipd_kw::varchar AS aipd_kw, 
                        src:aoit::integer AS aoit, 
                        src:aoit_kw::varchar AS aoit_kw, 
                        src:arlo::varchar AS arlo, 
                        src:bcyc::integer AS bcyc, 
                        src:bcyc_kw::varchar AS bcyc_kw, 
                        src:binb::integer AS binb, 
                        src:binb_kw::varchar AS binb_kw, 
                        src:bloc::varchar AS bloc, 
                        src:bloc_ref_compnr::integer AS bloc_ref_compnr, 
                        src:bout::integer AS bout, 
                        src:bout_kw::varchar AS bout_kw, 
                        src:ccal::varchar AS ccal, 
                        src:ccal_ref_compnr::integer AS ccal_ref_compnr, 
                        src:cdap::integer AS cdap, 
                        src:cdap_kw::varchar AS cdap_kw, 
                        src:cdcr::integer AS cdcr, 
                        src:cdcr_kw::varchar AS cdcr_kw, 
                        src:cdlt::numeric(38, 17) AS cdlt, 
                        src:cdlu::integer AS cdlu, 
                        src:cdlu_kw::varchar AS cdlu_kw, 
                        src:cdoa::integer AS cdoa, 
                        src:cdoa_kw::varchar AS cdoa_kw, 
                        src:cdpd::varchar AS cdpd, 
                        src:cdpd_ref_compnr::integer AS cdpd_ref_compnr, 
                        src:cdrd::varchar AS cdrd, 
                        src:cdrd_ref_compnr::integer AS cdrd_ref_compnr, 
                        src:cdro::integer AS cdro, 
                        src:cdro_kw::varchar AS cdro_kw, 
                        src:coka::integer AS coka, 
                        src:compnr::integer AS compnr, 
                        src:cons::integer AS cons, 
                        src:cons_kw::varchar AS cons_kw, 
                        src:copa::integer AS copa, 
                        src:copc::integer AS copc, 
                        src:copp::integer AS copp, 
                        src:copr::integer AS copr, 
                        src:copu::integer AS copu, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_arlo_ref_compnr::integer AS cwar_arlo_ref_compnr, 
                        src:cwar_palo_ref_compnr::integer AS cwar_palo_ref_compnr, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:cwar_relo_ref_compnr::integer AS cwar_relo_ref_compnr, 
                        src:cwar_stlo_ref_compnr::integer AS cwar_stlo_ref_compnr, 
                        src:ddti::integer AS ddti, 
                        src:ddti_kw::varchar AS ddti_kw, 
                        src:ddwi::integer AS ddwi, 
                        src:ddwi_kw::varchar AS ddwi_kw, 
                        src:ddwr::integer AS ddwr, 
                        src:ddwr_kw::varchar AS ddwr_kw, 
                        src:deleted::boolean AS deleted, 
                        src:diws::integer AS diws, 
                        src:diws_kw::varchar AS diws_kw, 
                        src:dmdt::integer AS dmdt, 
                        src:dmdt_kw::varchar AS dmdt_kw, 
                        src:dmph::numeric(38, 17) AS dmph, 
                        src:dmsi::integer AS dmsi, 
                        src:dmsi_kw::varchar AS dmsi_kw, 
                        src:dmsl::varchar AS dmsl, 
                        src:dmsl_ref_compnr::integer AS dmsl_ref_compnr, 
                        src:dmsp::integer AS dmsp, 
                        src:dmsp_kw::varchar AS dmsp_kw, 
                        src:dmsr::integer AS dmsr, 
                        src:dmsr_kw::varchar AS dmsr_kw, 
                        src:dmss::integer AS dmss, 
                        src:dmss_kw::varchar AS dmss_kw, 
                        src:dphi::numeric(38, 17) AS dphi, 
                        src:dprl::integer AS dprl, 
                        src:dprl_kw::varchar AS dprl_kw, 
                        src:dptr::integer AS dptr, 
                        src:dptr_kw::varchar AS dptr_kw, 
                        src:dycd::integer AS dycd, 
                        src:dycd_kw::varchar AS dycd_kw, 
                        src:fire::integer AS fire, 
                        src:fire_kw::varchar AS fire_kw, 
                        src:flhu::integer AS flhu, 
                        src:flhu_kw::varchar AS flhu_kw, 
                        src:flup::integer AS flup, 
                        src:flup_kw::varchar AS flup_kw, 
                        src:free::numeric(38, 17) AS free, 
                        src:frlo::integer AS frlo, 
                        src:frlo_kw::varchar AS frlo_kw, 
                        src:geha::integer AS geha, 
                        src:geha_kw::varchar AS geha_kw, 
                        src:gehc::integer AS gehc, 
                        src:gehc_kw::varchar AS gehc_kw, 
                        src:gehp::integer AS gehp, 
                        src:gehp_kw::varchar AS gehp_kw, 
                        src:gehr::integer AS gehr, 
                        src:gehr_kw::varchar AS gehr_kw, 
                        src:gehu::integer AS gehu, 
                        src:gehu_kw::varchar AS gehu_kw, 
                        src:ghps::integer AS ghps, 
                        src:ghps_kw::varchar AS ghps_kw, 
                        src:gnsh::integer AS gnsh, 
                        src:gnsh_kw::varchar AS gnsh_kw, 
                        src:ialt::integer AS ialt, 
                        src:ialt_kw::varchar AS ialt_kw, 
                        src:ieod::integer AS ieod, 
                        src:ieod_kw::varchar AS ieod_kw, 
                        src:ilmc::varchar AS ilmc, 
                        src:ilmc_ref_compnr::integer AS ilmc_ref_compnr, 
                        src:iltm::numeric(38, 17) AS iltm, 
                        src:iltu::integer AS iltu, 
                        src:iltu_kw::varchar AS iltu_kw, 
                        src:imdp::varchar AS imdp, 
                        src:imdp_ref_compnr::integer AS imdp_ref_compnr, 
                        src:itlo::integer AS itlo, 
                        src:itlo_kw::varchar AS itlo_kw, 
                        src:karu::varchar AS karu, 
                        src:karu_ref_compnr::integer AS karu_ref_compnr, 
                        src:kdev::varchar AS kdev, 
                        src:kmsk::varchar AS kmsk, 
                        src:kmsk_ref_compnr::integer AS kmsk_ref_compnr, 
                        src:laba::varchar AS laba, 
                        src:laba_ref_compnr::integer AS laba_ref_compnr, 
                        src:labc::varchar AS labc, 
                        src:labc_ref_compnr::integer AS labc_ref_compnr, 
                        src:labi::varchar AS labi, 
                        src:labi_ref_compnr::integer AS labi_ref_compnr, 
                        src:labo::varchar AS labo, 
                        src:labo_ref_compnr::integer AS labo_ref_compnr, 
                        src:labr::varchar AS labr, 
                        src:labr_ref_compnr::integer AS labr_ref_compnr, 
                        src:lolo::integer AS lolo, 
                        src:lolo_kw::varchar AS lolo_kw, 
                        src:lpad::integer AS lpad, 
                        src:lpad_kw::varchar AS lpad_kw, 
                        src:lpas::integer AS lpas, 
                        src:lpas_kw::varchar AS lpas_kw, 
                        src:lpcc::integer AS lpcc, 
                        src:lpcc_kw::varchar AS lpcc_kw, 
                        src:lppi::integer AS lppi, 
                        src:lppi_kw::varchar AS lppi_kw, 
                        src:lpre::integer AS lpre, 
                        src:lpre_kw::varchar AS lpre_kw, 
                        src:mada::integer AS mada, 
                        src:mada_kw::varchar AS mada_kw, 
                        src:masi::numeric(38, 17) AS masi, 
                        src:masu::integer AS masu, 
                        src:masu_kw::varchar AS masu_kw, 
                        src:matt::numeric(38, 17) AS matt, 
                        src:matu::integer AS matu, 
                        src:matu_kw::varchar AS matu_kw, 
                        src:mcca::integer AS mcca, 
                        src:mcca_kw::varchar AS mcca_kw, 
                        src:mipa::integer AS mipa, 
                        src:mipa_kw::varchar AS mipa_kw, 
                        src:misi::numeric(38, 17) AS misi, 
                        src:misu::integer AS misu, 
                        src:misu_kw::varchar AS misu_kw, 
                        src:mitt::numeric(38, 17) AS mitt, 
                        src:mitu::integer AS mitu, 
                        src:mitu_kw::varchar AS mitu_kw, 
                        src:mopa::integer AS mopa, 
                        src:mopa_kw::varchar AS mopa_kw, 
                        src:mrhu::integer AS mrhu, 
                        src:mrhu_kw::varchar AS mrhu_kw, 
                        src:olmc::varchar AS olmc, 
                        src:olmc_ref_compnr::integer AS olmc_ref_compnr, 
                        src:oltm::numeric(38, 17) AS oltm, 
                        src:oltu::integer AS oltu, 
                        src:oltu_kw::varchar AS oltu_kw, 
                        src:orbo::integer AS orbo, 
                        src:orbo_kw::varchar AS orbo_kw, 
                        src:palo::varchar AS palo, 
                        src:prla::integer AS prla, 
                        src:prla_kw::varchar AS prla_kw, 
                        src:prlc::integer AS prlc, 
                        src:prlc_kw::varchar AS prlc_kw, 
                        src:prlp::integer AS prlp, 
                        src:prlp_kw::varchar AS prlp_kw, 
                        src:prlr::integer AS prlr, 
                        src:prlr_kw::varchar AS prlr_kw, 
                        src:prlu::integer AS prlu, 
                        src:prlu_kw::varchar AS prlu_kw, 
                        src:prmp::integer AS prmp, 
                        src:prmp_kw::varchar AS prmp_kw, 
                        src:prmr::integer AS prmr, 
                        src:prmr_kw::varchar AS prmr_kw, 
                        src:prmu::integer AS prmu, 
                        src:prmu_kw::varchar AS prmu_kw, 
                        src:prva::integer AS prva, 
                        src:prva_kw::varchar AS prva_kw, 
                        src:qfma::numeric(38, 17) AS qfma, 
                        src:qfmi::numeric(38, 17) AS qfmi, 
                        src:qmoo::integer AS qmoo, 
                        src:qmoo_kw::varchar AS qmoo_kw, 
                        src:qwrh::varchar AS qwrh, 
                        src:qwrh_ref_compnr::integer AS qwrh_ref_compnr, 
                        src:ract::varchar AS ract, 
                        src:ract_ref_compnr::integer AS ract_ref_compnr, 
                        src:reje::integer AS reje, 
                        src:reje_kw::varchar AS reje_kw, 
                        src:relo::varchar AS relo, 
                        src:rfsa::integer AS rfsa, 
                        src:rfsa_kw::varchar AS rfsa_kw, 
                        src:rilt::numeric(38, 17) AS rilt, 
                        src:rilu::integer AS rilu, 
                        src:rilu_kw::varchar AS rilu_kw, 
                        src:rkbs::integer AS rkbs, 
                        src:rkbs_kw::varchar AS rkbs_kw, 
                        src:rolt::numeric(38, 17) AS rolt, 
                        src:rolu::integer AS rolu, 
                        src:rolu_kw::varchar AS rolu_kw, 
                        src:rsdn::integer AS rsdn, 
                        src:rsdn_kw::varchar AS rsdn_kw, 
                        src:rsrc::integer AS rsrc, 
                        src:rsrc_kw::varchar AS rsrc_kw, 
                        src:rtcf::integer AS rtcf, 
                        src:rtcf_kw::varchar AS rtcf_kw, 
                        src:scom::integer AS scom, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sfwh::integer AS sfwh, 
                        src:sfwh_kw::varchar AS sfwh_kw, 
                        src:shhu::integer AS shhu, 
                        src:shhu_kw::varchar AS shhu_kw, 
                        src:shtw::varchar AS shtw, 
                        src:shtw_ref_compnr::integer AS shtw_ref_compnr, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
                        src:sloc::integer AS sloc, 
                        src:sloc_kw::varchar AS sloc_kw, 
                        src:smsh::integer AS smsh, 
                        src:smsh_kw::varchar AS smsh_kw, 
                        src:spps::integer AS spps, 
                        src:spps_kw::varchar AS spps_kw, 
                        src:sscc::integer AS sscc, 
                        src:sscc_kw::varchar AS sscc_kw, 
                        src:stlo::varchar AS stlo, 
                        src:sups::integer AS sups, 
                        src:sups_kw::varchar AS sups_kw, 
                        src:supw::varchar AS supw, 
                        src:supw_ref_compnr::integer AS supw_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:trdc::integer AS trdc, 
                        src:trdc_kw::varchar AS trdc_kw, 
                        src:ufpl::integer AS ufpl, 
                        src:ufpl_kw::varchar AS ufpl_kw, 
                        src:uhii::integer AS uhii, 
                        src:uhii_kw::varchar AS uhii_kw, 
                        src:uhin::integer AS uhin, 
                        src:uhin_kw::varchar AS uhin_kw, 
                        src:uhir::integer AS uhir, 
                        src:uhir_kw::varchar AS uhir_kw, 
                        src:uhis::integer AS uhis, 
                        src:uhis_kw::varchar AS uhis_kw, 
                        src:uhnd::integer AS uhnd, 
                        src:uhnd_kw::varchar AS uhnd_kw, 
                        src:uhoa::integer AS uhoa, 
                        src:uhoa_kw::varchar AS uhoa_kw, 
                        src:uhri::integer AS uhri, 
                        src:uhri_kw::varchar AS uhri_kw, 
                        src:uidt::integer AS uidt, 
                        src:uidt_kw::varchar AS uidt_kw, 
                        src:uolo::integer AS uolo, 
                        src:uolo_kw::varchar AS uolo_kw, 
                        src:username::varchar AS username, 
                        src:usmd::integer AS usmd, 
                        src:usmd_kw::varchar AS usmd_kw, 
                        src:usse::integer AS usse, 
                        src:usse_kw::varchar AS usse_kw, 
                        src:uudl::integer AS uudl, 
                        src:uudl_kw::varchar AS uudl_kw, 
                        src:uwtr::integer AS uwtr, 
                        src:uwtr_kw::varchar AS uwtr_kw, 
                        src:vpad::integer AS vpad, 
                        src:vpad_kw::varchar AS vpad_kw, 
                        src:vpcc::integer AS vpcc, 
                        src:vpcc_kw::varchar AS vpcc_kw, 
                        src:wvgr::varchar AS wvgr, 
                        src:wvgr_ref_compnr::integer AS wvgr_ref_compnr, 
                        src:zere::integer AS zere, 
                        src:zere_kw::varchar AS zere_kw, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                src:cwar  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHWMD200 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aipd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aoit), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bcyc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:binb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bloc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bout), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdap), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdcr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdlt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdlu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdoa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdpd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdrd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdro), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:coka), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cons), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwar_arlo_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwar_palo_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwar_relo_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwar_stlo_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ddti), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ddwi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ddwr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:diws), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmdt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmph), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmsi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmsl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmsp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmsr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmss), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dphi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dprl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dptr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dycd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fire), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:flhu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:flup), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:free), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:frlo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:geha), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gehc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gehp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gehr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gehu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ghps), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gnsh), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ialt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ieod), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ilmc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iltm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iltu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:imdp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itlo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:karu_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:kmsk_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:laba_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:labc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:labi_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:labo_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:labr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lolo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lpad), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lpas), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lpcc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lppi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lpre), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mada), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:masi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:masu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:matt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:matu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mcca), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mipa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:misi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:misu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mitt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mitu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mopa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mrhu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:olmc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oltm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oltu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:orbo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prla), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prlc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prlp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prlr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prlu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prmr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prmu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prva), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qfma), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qfmi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qmoo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qwrh_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ract_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:reje), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfsa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rilt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rilu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rkbs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rolt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rolu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rsdn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rsrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtcf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scom), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sfwh), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:shhu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:shtw_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sloc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:smsh), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spps), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sscc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sups), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:supw_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trdc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ufpl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uhii), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uhin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uhir), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uhis), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uhnd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uhoa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uhri), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uidt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uolo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:usmd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:usse), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uudl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uwtr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vpad), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vpcc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wvgr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:zere), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null