CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TPPPC200 AS SELECT
                        src:afpy::varchar AS afpy, 
                        src:alin::integer AS alin, 
                        src:alin_kw::varchar AS alin_kw, 
                        src:amoc::numeric(38, 17) AS amoc, 
                        src:amoc_cntc::numeric(38, 17) AS amoc_cntc, 
                        src:amoc_dwhc::numeric(38, 17) AS amoc_dwhc, 
                        src:amoc_prjc::numeric(38, 17) AS amoc_prjc, 
                        src:amoc_refc::numeric(38, 17) AS amoc_refc, 
                        src:amos::numeric(38, 17) AS amos, 
                        src:amos_cntc::numeric(38, 17) AS amos_cntc, 
                        src:amos_dwhc::numeric(38, 17) AS amos_dwhc, 
                        src:amos_homc::numeric(38, 17) AS amos_homc, 
                        src:amos_prjc::numeric(38, 17) AS amos_prjc, 
                        src:amos_refc::numeric(38, 17) AS amos_refc, 
                        src:amos_rpc1::numeric(38, 17) AS amos_rpc1, 
                        src:amos_rpc2::numeric(38, 17) AS amos_rpc2, 
                        src:base::varchar AS base, 
                        src:base_vrsn_ref_compnr::integer AS base_vrsn_ref_compnr, 
                        src:blbl::integer AS blbl, 
                        src:blbl_kw::varchar AS blbl_kw, 
                        src:bona::varchar AS bona, 
                        src:borf::varchar AS borf, 
                        src:bpcl::varchar AS bpcl, 
                        src:bpcl_ref_compnr::integer AS bpcl_ref_compnr, 
                        src:bpct::varchar AS bpct, 
                        src:bpct_ref_compnr::integer AS bpct_ref_compnr, 
                        src:bprc::numeric(38, 17) AS bprc, 
                        src:btxt::integer AS btxt, 
                        src:btxt_ref_compnr::integer AS btxt_ref_compnr, 
                        src:bwln::integer AS bwln, 
                        src:bwln_kw::varchar AS bwln_kw, 
                        src:cact::varchar AS cact, 
                        src:ccco::varchar AS ccco, 
                        src:ccco_ref_compnr::integer AS ccco_ref_compnr, 
                        src:ccma::varchar AS ccma, 
                        src:ccma_ref_compnr::integer AS ccma_ref_compnr, 
                        src:cdf_sref::object AS cdf_sref, 
                        src:cdf_susr::object AS cdf_susr, 
                        src:cdoc::object AS cdoc, 
                        src:cdoc_ref_compnr::integer AS cdoc_ref_compnr, 
                        src:cdru::datetime AS cdru, 
                        src:cdru_cser_ref_compnr::integer AS cdru_cser_ref_compnr, 
                        src:ceno::varchar AS ceno, 
                        src:cequ::varchar AS cequ, 
                        src:chpr::numeric(38, 17) AS chpr, 
                        src:cico::varchar AS cico, 
                        src:clin::integer AS clin, 
                        src:cnln::varchar AS cnln, 
                        src:cohc_1::numeric(38, 17) AS cohc_1, 
                        src:cohc_2::numeric(38, 17) AS cohc_2, 
                        src:cohc_3::numeric(38, 17) AS cohc_3, 
                        src:cohd_1::numeric(38, 17) AS cohd_1, 
                        src:cohd_2::numeric(38, 17) AS cohd_2, 
                        src:cohd_3::numeric(38, 17) AS cohd_3, 
                        src:comf::integer AS comf, 
                        src:comf_kw::varchar AS comf_kw, 
                        src:comm_acts::varchar AS comm_acts, 
                        src:compnr::integer AS compnr, 
                        src:cono::varchar AS cono, 
                        src:cono_cnln_ref_compnr::integer AS cono_cnln_ref_compnr, 
                        src:coob::varchar AS coob, 
                        src:cotp::integer AS cotp, 
                        src:cotp_kw::varchar AS cotp_kw, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cequ::varchar AS cprj_cequ, 
                        src:cprj_cico::varchar AS cprj_cico, 
                        src:cprj_cpla_cact_ref_compnr::integer AS cprj_cpla_cact_ref_compnr, 
                        src:cprj_cspa_ref_compnr::integer AS cprj_cspa_ref_compnr, 
                        src:cprj_cstl_ref_compnr::integer AS cprj_cstl_ref_compnr, 
                        src:cprj_csub::varchar AS cprj_csub, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cprj_task::varchar AS cprj_task, 
                        src:cptc::varchar AS cptc, 
                        src:cptc_ref_compnr::integer AS cptc_ref_compnr, 
                        src:cptc_year_peri_ref_compnr::integer AS cptc_year_peri_ref_compnr, 
                        src:cptf::varchar AS cptf, 
                        src:cptf_fyea_fper_ref_compnr::integer AS cptf_fyea_fper_ref_compnr, 
                        src:cptf_ref_compnr::integer AS cptf_ref_compnr, 
                        src:cpth::varchar AS cpth, 
                        src:cser::integer AS cser, 
                        src:cspa::varchar AS cspa, 
                        src:cstl::varchar AS cstl, 
                        src:csts::integer AS csts, 
                        src:csts_kw::varchar AS csts_kw, 
                        src:cstv::numeric(38, 17) AS cstv, 
                        src:csub::varchar AS csub, 
                        src:cuni::varchar AS cuni, 
                        src:cuni_ref_compnr::integer AS cuni_ref_compnr, 
                        src:curc::varchar AS curc, 
                        src:curc_ref_compnr::integer AS curc_ref_compnr, 
                        src:curs::varchar AS curs, 
                        src:curs_ref_compnr::integer AS curs_ref_compnr, 
                        src:cuti::varchar AS cuti, 
                        src:cuti_ref_compnr::integer AS cuti_ref_compnr, 
                        src:cuvc::varchar AS cuvc, 
                        src:cuvc_ref_compnr::integer AS cuvc_ref_compnr, 
                        src:cvat::varchar AS cvat, 
                        src:cvat_ref_compnr::integer AS cvat_ref_compnr, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:cwgt::varchar AS cwgt, 
                        src:cwgt_ref_compnr::integer AS cwgt_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:drun::datetime AS drun, 
                        src:drun_serb_ref_compnr::integer AS drun_serb_ref_compnr, 
                        src:ecom::integer AS ecom, 
                        src:effn::integer AS effn, 
                        src:effn_ref_compnr::integer AS effn_ref_compnr, 
                        src:efph::integer AS efph, 
                        src:efph_ref_compnr::integer AS efph_ref_compnr, 
                        src:emno::varchar AS emno, 
                        src:emno_ref_compnr::integer AS emno_ref_compnr, 
                        src:enid::varchar AS enid, 
                        src:entu::varchar AS entu, 
                        src:enty::integer AS enty, 
                        src:enty_kw::varchar AS enty_kw, 
                        src:exmt::integer AS exmt, 
                        src:exmt_kw::varchar AS exmt_kw, 
                        src:fcmp::integer AS fcmp, 
                        src:fcor::integer AS fcor, 
                        src:fcrt::integer AS fcrt, 
                        src:fcrt_kw::varchar AS fcrt_kw, 
                        src:fdoc::integer AS fdoc, 
                        src:fitr::integer AS fitr, 
                        src:fitr_kw::varchar AS fitr_kw, 
                        src:flin::integer AS flin, 
                        src:fper::integer AS fper, 
                        src:frdc::datetime AS frdc, 
                        src:frgd::datetime AS frgd, 
                        src:fsrl::integer AS fsrl, 
                        src:ftln::integer AS ftln, 
                        src:ftty::varchar AS ftty, 
                        src:fyea::integer AS fyea, 
                        src:guid::varchar AS guid, 
                        src:hbnr::integer AS hbnr, 
                        src:hbyn::integer AS hbyn, 
                        src:hbyn_kw::varchar AS hbyn_kw, 
                        src:hper::integer AS hper, 
                        src:hyea::integer AS hyea, 
                        src:idoc::integer AS idoc, 
                        src:ifbp::varchar AS ifbp, 
                        src:ifbp_ref_compnr::integer AS ifbp_ref_compnr, 
                        src:intc::integer AS intc, 
                        src:intc_kw::varchar AS intc_kw, 
                        src:inti::integer AS inti, 
                        src:inti_kw::varchar AS inti_kw, 
                        src:invo::integer AS invo, 
                        src:invo_kw::varchar AS invo_kw, 
                        src:isln::integer AS isln, 
                        src:itco::integer AS itco, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:itgd::varchar AS itgd, 
                        src:itln::integer AS itln, 
                        src:itor::varchar AS itor, 
                        src:ityp::varchar AS ityp, 
                        src:koor::integer AS koor, 
                        src:koor_kw::varchar AS koor_kw, 
                        src:lcln::integer AS lcln, 
                        src:lcor::integer AS lcor, 
                        src:ltdt::datetime AS ltdt, 
                        src:ncmp::integer AS ncmp, 
                        src:ntbl::integer AS ntbl, 
                        src:ntbl_kw::varchar AS ntbl_kw, 
                        src:oboi::varchar AS oboi, 
                        src:obon::varchar AS obon, 
                        src:obor::varchar AS obor, 
                        src:ocmp::integer AS ocmp, 
                        src:ohpr::numeric(38, 17) AS ohpr, 
                        src:oitm::varchar AS oitm, 
                        src:oitm_ref_compnr::integer AS oitm_ref_compnr, 
                        src:oldf::integer AS oldf, 
                        src:oldf_kw::varchar AS oldf_kw, 
                        src:orno::varchar AS orno, 
                        src:otbp::varchar AS otbp, 
                        src:otbp_ref_compnr::integer AS otbp_ref_compnr, 
                        src:ovhd::varchar AS ovhd, 
                        src:ovhd_ref_compnr::integer AS ovhd_ref_compnr, 
                        src:peri::integer AS peri, 
                        src:phit::varchar AS phit, 
                        src:phit_ref_compnr::integer AS phit_ref_compnr, 
                        src:pono::integer AS pono, 
                        src:potf::integer AS potf, 
                        src:potf_kw::varchar AS potf_kw, 
                        src:prdt::datetime AS prdt, 
                        src:pric::numeric(38, 17) AS pric, 
                        src:pris::numeric(38, 17) AS pris, 
                        src:pstt::integer AS pstt, 
                        src:pstt_kw::varchar AS pstt_kw, 
                        src:ptyc::integer AS ptyc, 
                        src:ptyc_kw::varchar AS ptyc_kw, 
                        src:quan::numeric(38, 17) AS quan, 
                        src:quan_base_time::numeric(38, 17) AS quan_base_time, 
                        src:quan_buar::numeric(38, 17) AS quan_buar, 
                        src:quan_buln::numeric(38, 17) AS quan_buln, 
                        src:quan_bupc::numeric(38, 17) AS quan_bupc, 
                        src:quan_buvl::numeric(38, 17) AS quan_buvl, 
                        src:quan_buwg::numeric(38, 17) AS quan_buwg, 
                        src:rcln::integer AS rcln, 
                        src:rcno::varchar AS rcno, 
                        src:rcod::varchar AS rcod, 
                        src:rcod_ref_compnr::integer AS rcod_ref_compnr, 
                        src:rdas::datetime AS rdas, 
                        src:rdat::datetime AS rdat, 
                        src:reas::varchar AS reas, 
                        src:reas_ref_compnr::integer AS reas_ref_compnr, 
                        src:retx::integer AS retx, 
                        src:retx_kw::varchar AS retx_kw, 
                        src:rgdt::datetime AS rgdt, 
                        src:rlin::integer AS rlin, 
                        src:rsqn::integer AS rsqn, 
                        src:rtcc_1::numeric(38, 17) AS rtcc_1, 
                        src:rtcc_2::numeric(38, 17) AS rtcc_2, 
                        src:rtcc_3::numeric(38, 17) AS rtcc_3, 
                        src:rtcs_1::numeric(38, 17) AS rtcs_1, 
                        src:rtcs_2::numeric(38, 17) AS rtcs_2, 
                        src:rtcs_3::numeric(38, 17) AS rtcs_3, 
                        src:rtfc_1::integer AS rtfc_1, 
                        src:rtfc_2::integer AS rtfc_2, 
                        src:rtfc_3::integer AS rtfc_3, 
                        src:rtfs_1::integer AS rtfs_1, 
                        src:rtfs_2::integer AS rtfs_2, 
                        src:rtfs_3::integer AS rtfs_3, 
                        src:rttp::integer AS rttp, 
                        src:rttp_kw::varchar AS rttp_kw, 
                        src:rtyp::varchar AS rtyp, 
                        src:rtyp_ref_compnr::integer AS rtyp_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:serb::integer AS serb, 
                        src:serh::integer AS serh, 
                        src:sern::integer AS sern, 
                        src:snec::integer AS snec, 
                        src:spln::integer AS spln, 
                        src:srnb::integer AS srnb, 
                        src:srni::integer AS srni, 
                        src:sttl::integer AS sttl, 
                        src:sttl_kw::varchar AS sttl_kw, 
                        src:task::varchar AS task, 
                        src:time::numeric(38, 17) AS time, 
                        src:timestamp::datetime AS timestamp, 
                        src:tror::integer AS tror, 
                        src:tror_kw::varchar AS tror_kw, 
                        src:trsl::integer AS trsl, 
                        src:trsl_kw::varchar AS trsl_kw, 
                        src:txct::varchar AS txct, 
                        src:txct_cvat_ref_compnr::integer AS txct_cvat_ref_compnr, 
                        src:txct_ref_compnr::integer AS txct_ref_compnr, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:user::varchar AS user, 
                        src:username::varchar AS username, 
                        src:vrln::integer AS vrln, 
                        src:vrsn::integer AS vrsn, 
                        src:xgui::varchar AS xgui, 
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
                                        
                src:coob,
                src:cspa,
                src:cprj,
                src:cotp,
                src:compnr,
                src:sern  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPPC200 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amoc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amoc_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amoc_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amoc_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amoc_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amos), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amos_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amos_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amos_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amos_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amos_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amos_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amos_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:base_vrsn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blbl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bpct_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bprc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:btxt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:btxt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bwln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccma_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdoc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cdru), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdru_cser_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:chpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cohc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cohc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cohc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cohd_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cohd_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cohd_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:comf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cotp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cptc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cptc_year_peri_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cptf_fyea_fper_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cptf_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cser), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csts), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:curc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:curs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwgt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:drun), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:drun_serb_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ecom), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:effn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:efph), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:efph_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:enty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:exmt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcrt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fdoc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fitr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:flin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fper), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:frdc), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:frgd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fsrl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ftln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fyea), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hbnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hbyn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hper), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hyea), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:idoc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:intc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inti), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:invo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:isln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itco), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:koor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcor), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ltdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ncmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ntbl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ocmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ohpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oitm_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oldf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ovhd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:peri), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:phit_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pono), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:potf), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:prdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pric), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pris), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pstt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptyc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:quan), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:quan_base_time), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:quan_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:quan_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:quan_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:quan_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:quan_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rdas), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:reas_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:retx), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rgdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rlin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rsqn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtcc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtcc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtcc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtcs_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtcs_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtcs_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtfc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtfc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtfc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtfs_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtfs_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtfs_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rttp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:serb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:serh), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sern), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:snec), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:srnb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:srni), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sttl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:time), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tror), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trsl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txct_cvat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txct_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vrln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vrsn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null