CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TISFC001 AS SELECT
                        src:aapd::integer AS aapd, 
                        src:aapd_kw::varchar AS aapd_kw, 
                        src:adld::datetime AS adld, 
                        src:adld_actd::numeric(38, 17) AS adld_actd, 
                        src:adld_deea::numeric(38, 17) AS adld_deea, 
                        src:adld_dela::numeric(38, 17) AS adld_dela, 
                        src:apdt::datetime AS apdt, 
                        src:aprp::integer AS aprp, 
                        src:aprp_kw::varchar AS aprp_kw, 
                        src:asrp::integer AS asrp, 
                        src:asrp_kw::varchar AS asrp_kw, 
                        src:awtc::integer AS awtc, 
                        src:awtc_kw::varchar AS awtc_kw, 
                        src:bfep::integer AS bfep, 
                        src:bfep_kw::varchar AS bfep_kw, 
                        src:bfhr::integer AS bfhr, 
                        src:bfhr_kw::varchar AS bfhr_kw, 
                        src:bloc::integer AS bloc, 
                        src:bloc_kw::varchar AS bloc_kw, 
                        src:bmdl::varchar AS bmdl, 
                        src:bmrv::varchar AS bmrv, 
                        src:cada::datetime AS cada, 
                        src:cctt::integer AS cctt, 
                        src:cctt_kw::varchar AS cctt_kw, 
                        src:cdld::datetime AS cdld, 
                        src:chel::integer AS chel, 
                        src:chel_kw::varchar AS chel_kw, 
                        src:clco::varchar AS clco, 
                        src:clco_fcmp::integer AS clco_fcmp, 
                        src:clco_ref_compnr::integer AS clco_ref_compnr, 
                        src:cldt::datetime AS cldt, 
                        src:clot::varchar AS clot, 
                        src:clpd::integer AS clpd, 
                        src:clpd_kw::varchar AS clpd_kw, 
                        src:cmdt::datetime AS cmdt, 
                        src:cncd::varchar AS cncd, 
                        src:compnr::integer AS compnr, 
                        src:covn::integer AS covn, 
                        src:covn_kw::varchar AS covn_kw, 
                        src:cpla::integer AS cpla, 
                        src:cpla_kw::varchar AS cpla_kw, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:crpr_1::integer AS crpr_1, 
                        src:crpr_2::integer AS crpr_2, 
                        src:crpr_3::integer AS crpr_3, 
                        src:crpr_4::integer AS crpr_4, 
                        src:crpr_kw_1::varchar AS crpr_kw_1, 
                        src:crpr_kw_2::varchar AS crpr_kw_2, 
                        src:crpr_kw_3::varchar AS crpr_kw_3, 
                        src:crpr_kw_4::varchar AS crpr_kw_4, 
                        src:cutl::integer AS cutl, 
                        src:cutl_kw::varchar AS cutl_kw, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:defc::integer AS defc, 
                        src:defc_kw::varchar AS defc_kw, 
                        src:deleted::boolean AS deleted, 
                        src:dstp::integer AS dstp, 
                        src:dstp_kw::varchar AS dstp_kw, 
                        src:efdt::datetime AS efdt, 
                        src:fain::integer AS fain, 
                        src:fain_kw::varchar AS fain_kw, 
                        src:fres::integer AS fres, 
                        src:fres_kw::varchar AS fres_kw, 
                        src:grid::varchar AS grid, 
                        src:grid_ref_compnr::integer AS grid_ref_compnr, 
                        src:huid::varchar AS huid, 
                        src:huqt::numeric(38, 17) AS huqt, 
                        src:insn::integer AS insn, 
                        src:insn_kw::varchar AS insn_kw, 
                        src:insp::integer AS insp, 
                        src:insp_kw::varchar AS insp_kw, 
                        src:irev::varchar AS irev, 
                        src:lbdv::varchar AS lbdv, 
                        src:lbid::varchar AS lbid, 
                        src:lblo::varchar AS lblo, 
                        src:lbnc::integer AS lbnc, 
                        src:lbpb::integer AS lbpb, 
                        src:lbpb_kw::varchar AS lbpb_kw, 
                        src:lbpr::integer AS lbpr, 
                        src:lbpr_kw::varchar AS lbpr_kw, 
                        src:lbtp::integer AS lbtp, 
                        src:lbtp_kw::varchar AS lbtp_kw, 
                        src:matl::integer AS matl, 
                        src:matl_kw::varchar AS matl_kw, 
                        src:matn::integer AS matn, 
                        src:matn_kw::varchar AS matn_kw, 
                        src:mfre::integer AS mfre, 
                        src:mfre_kw::varchar AS mfre_kw, 
                        src:mitm::varchar AS mitm, 
                        src:mitm_opro_rcmp::integer AS mitm_opro_rcmp, 
                        src:mitm_ref_compnr::integer AS mitm_ref_compnr, 
                        src:mitm_site_ref_compnr::integer AS mitm_site_ref_compnr, 
                        src:mmno::integer AS mmno, 
                        src:mmno_kw::varchar AS mmno_kw, 
                        src:mmpo::integer AS mmpo, 
                        src:mmpo_kw::varchar AS mmpo_kw, 
                        src:mocp::integer AS mocp, 
                        src:mocp_kw::varchar AS mocp_kw, 
                        src:odpr::integer AS odpr, 
                        src:odpr_kw::varchar AS odpr_kw, 
                        src:odpt::integer AS odpt, 
                        src:odpt_kw::varchar AS odpt_kw, 
                        src:ofbp::varchar AS ofbp, 
                        src:ofbp_ref_compnr::integer AS ofbp_ref_compnr, 
                        src:oprn::integer AS oprn, 
                        src:oprn_kw::varchar AS oprn_kw, 
                        src:opro::varchar AS opro, 
                        src:oprp::integer AS oprp, 
                        src:oprp_kw::varchar AS oprp_kw, 
                        src:osta::integer AS osta, 
                        src:osta_kw::varchar AS osta_kw, 
                        src:owns::integer AS owns, 
                        src:owns_kw::varchar AS owns_kw, 
                        src:pdno::varchar AS pdno, 
                        src:pkdf::varchar AS pkdf, 
                        src:pldt::datetime AS pldt, 
                        src:pldt_pltd::numeric(38, 17) AS pldt_pltd, 
                        src:plgr::varchar AS plgr, 
                        src:plid::varchar AS plid, 
                        src:plid_ref_compnr::integer AS plid_ref_compnr, 
                        src:poas::varchar AS poas, 
                        src:prcd::integer AS prcd, 
                        src:prdt::datetime AS prdt, 
                        src:prlb::integer AS prlb, 
                        src:prlb_kw::varchar AS prlb_kw, 
                        src:qbfd::numeric(38, 17) AS qbfd, 
                        src:qbfd_buar::numeric(38, 17) AS qbfd_buar, 
                        src:qbfd_buln::numeric(38, 17) AS qbfd_buln, 
                        src:qbfd_bupc::numeric(38, 17) AS qbfd_bupc, 
                        src:qbfd_butm::numeric(38, 17) AS qbfd_butm, 
                        src:qbfd_buvl::numeric(38, 17) AS qbfd_buvl, 
                        src:qbfd_buwg::numeric(38, 17) AS qbfd_buwg, 
                        src:qdlv::numeric(38, 17) AS qdlv, 
                        src:qdlv_buar::numeric(38, 17) AS qdlv_buar, 
                        src:qdlv_buln::numeric(38, 17) AS qdlv_buln, 
                        src:qdlv_bupc::numeric(38, 17) AS qdlv_bupc, 
                        src:qdlv_butm::numeric(38, 17) AS qdlv_butm, 
                        src:qdlv_buvl::numeric(38, 17) AS qdlv_buvl, 
                        src:qdlv_buwg::numeric(38, 17) AS qdlv_buwg, 
                        src:qntl::numeric(38, 17) AS qntl, 
                        src:qntl_buar::numeric(38, 17) AS qntl_buar, 
                        src:qntl_buln::numeric(38, 17) AS qntl_buln, 
                        src:qntl_bupc::numeric(38, 17) AS qntl_bupc, 
                        src:qntl_butm::numeric(38, 17) AS qntl_butm, 
                        src:qntl_buvl::numeric(38, 17) AS qntl_buvl, 
                        src:qntl_buwg::numeric(38, 17) AS qntl_buwg, 
                        src:qoor::numeric(38, 17) AS qoor, 
                        src:qqar::numeric(38, 17) AS qqar, 
                        src:qqar_buar::numeric(38, 17) AS qqar_buar, 
                        src:qqar_buln::numeric(38, 17) AS qqar_buln, 
                        src:qqar_bupc::numeric(38, 17) AS qqar_bupc, 
                        src:qqar_butm::numeric(38, 17) AS qqar_butm, 
                        src:qqar_buvl::numeric(38, 17) AS qqar_buvl, 
                        src:qqar_buwg::numeric(38, 17) AS qqar_buwg, 
                        src:qrdr::numeric(38, 17) AS qrdr, 
                        src:qrdr_buar::numeric(38, 17) AS qrdr_buar, 
                        src:qrdr_buln::numeric(38, 17) AS qrdr_buln, 
                        src:qrdr_bupc::numeric(38, 17) AS qrdr_bupc, 
                        src:qrdr_butm::numeric(38, 17) AS qrdr_butm, 
                        src:qrdr_buvl::numeric(38, 17) AS qrdr_buvl, 
                        src:qrdr_buwg::numeric(38, 17) AS qrdr_buwg, 
                        src:qrjc::numeric(38, 17) AS qrjc, 
                        src:qrjc_buar::numeric(38, 17) AS qrjc_buar, 
                        src:qrjc_buln::numeric(38, 17) AS qrjc_buln, 
                        src:qrjc_bupc::numeric(38, 17) AS qrjc_bupc, 
                        src:qrjc_butm::numeric(38, 17) AS qrjc_butm, 
                        src:qrjc_buvl::numeric(38, 17) AS qrjc_buvl, 
                        src:qrjc_buwg::numeric(38, 17) AS qrjc_buwg, 
                        src:qtbf::numeric(38, 17) AS qtbf, 
                        src:qtbf_buar::numeric(38, 17) AS qtbf_buar, 
                        src:qtbf_buln::numeric(38, 17) AS qtbf_buln, 
                        src:qtbf_bupc::numeric(38, 17) AS qtbf_bupc, 
                        src:qtbf_butm::numeric(38, 17) AS qtbf_butm, 
                        src:qtbf_buvl::numeric(38, 17) AS qtbf_buvl, 
                        src:qtbf_buwg::numeric(38, 17) AS qtbf_buwg, 
                        src:qtdl::numeric(38, 17) AS qtdl, 
                        src:qtdl_buar::numeric(38, 17) AS qtdl_buar, 
                        src:qtdl_buln::numeric(38, 17) AS qtdl_buln, 
                        src:qtdl_bupc::numeric(38, 17) AS qtdl_bupc, 
                        src:qtdl_butm::numeric(38, 17) AS qtdl_butm, 
                        src:qtdl_buvl::numeric(38, 17) AS qtdl_buvl, 
                        src:qtdl_buwg::numeric(38, 17) AS qtdl_buwg, 
                        src:qtor::numeric(38, 17) AS qtor, 
                        src:qtor_buar::numeric(38, 17) AS qtor_buar, 
                        src:qtor_buln::numeric(38, 17) AS qtor_buln, 
                        src:qtor_bupc::numeric(38, 17) AS qtor_bupc, 
                        src:qtor_butm::numeric(38, 17) AS qtor_butm, 
                        src:qtor_buvl::numeric(38, 17) AS qtor_buvl, 
                        src:qtor_buwg::numeric(38, 17) AS qtor_buwg, 
                        src:rdld::datetime AS rdld, 
                        src:recn::integer AS recn, 
                        src:recn_kw::varchar AS recn_kw, 
                        src:reco::varchar AS reco, 
                        src:reco_ref_compnr::integer AS reco_ref_compnr, 
                        src:revi::varchar AS revi, 
                        src:rgrp::varchar AS rgrp, 
                        src:rgrp_ref_compnr::integer AS rgrp_ref_compnr, 
                        src:rorv::varchar AS rorv, 
                        src:rouc::varchar AS rouc, 
                        src:roul::integer AS roul, 
                        src:roul_kw::varchar AS roul_kw, 
                        src:rswc::integer AS rswc, 
                        src:rswc_kw::varchar AS rswc_kw, 
                        src:rwko::integer AS rwko, 
                        src:rwko_kw::varchar AS rwko_kw, 
                        src:rwtp::integer AS rwtp, 
                        src:rwtp_kw::varchar AS rwtp_kw, 
                        src:sawl::integer AS sawl, 
                        src:sawl_kw::varchar AS sawl_kw, 
                        src:schd::integer AS schd, 
                        src:schd_kw::varchar AS schd_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sfpl::varchar AS sfpl, 
                        src:sfpl_ref_compnr::integer AS sfpl_ref_compnr, 
                        src:shsp::integer AS shsp, 
                        src:shsp_kw::varchar AS shsp_kw, 
                        src:site::varchar AS site, 
                        src:site_plgr_ref_compnr::integer AS site_plgr_ref_compnr, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
                        src:smfs::integer AS smfs, 
                        src:smfs_kw::varchar AS smfs_kw, 
                        src:spcn::varchar AS spcn, 
                        src:srnl::integer AS srnl, 
                        src:srnl_kw::varchar AS srnl_kw, 
                        src:subc::integer AS subc, 
                        src:subc_kw::varchar AS subc_kw, 
                        src:subn::integer AS subn, 
                        src:subn_kw::varchar AS subn_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:trcp::varchar AS trcp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:username::varchar AS username, 
                        src:vers::integer AS vers, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                src:pdno  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TISFC001 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aapd), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:adld), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adld_actd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adld_deea), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adld_dela), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:apdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aprp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:asrp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:awtc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bfep), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bfhr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bloc), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cada), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cctt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cdld), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:chel), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clco_fcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clco_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cldt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clpd), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cmdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:covn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpla), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crpr_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crpr_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crpr_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crpr_4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cutl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:defc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dstp), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:efdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fain), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fres), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:grid_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:huqt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:insn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:insp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lbnc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lbpb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lbpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lbtp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:matl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:matn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mfre), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mitm_opro_rcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mitm_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mitm_site_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mmno), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mmpo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mocp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:odpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:odpt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oprn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oprp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:osta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:owns), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:pldt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pldt_pltd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:plid_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prcd), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:prdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prlb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qbfd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qbfd_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qbfd_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qbfd_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qbfd_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qbfd_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qbfd_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qdlv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qdlv_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qdlv_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qdlv_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qdlv_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qdlv_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qdlv_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qntl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qntl_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qntl_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qntl_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qntl_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qntl_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qntl_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qqar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qqar_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qqar_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qqar_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qqar_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qqar_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qqar_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrdr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrdr_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrdr_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrdr_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrdr_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrdr_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrdr_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrjc_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrjc_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrjc_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrjc_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrjc_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrjc_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbf_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbf_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbf_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbf_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbf_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbf_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtdl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtdl_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtdl_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtdl_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtdl_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtdl_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtdl_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtor_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtor_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtor_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtor_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtor_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtor_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rdld), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:recn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:reco_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rgrp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:roul), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rswc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rwko), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rwtp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sawl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:schd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sfpl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:shsp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:site_plgr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:smfs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:srnl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:subc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:subn), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vers), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null