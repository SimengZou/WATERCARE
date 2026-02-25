CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TISFC001_ERROR AS SELECT src, 'LN_TISFC001' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aapd), '0'), 38, 10) is null and 
                    src:aapd is not null) THEN
                    'aapd contains a non-numeric value : \'' || AS_VARCHAR(src:aapd) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:adld), '1900-01-01')) is null and 
                    src:adld is not null) THEN
                    'adld contains a non-timestamp value : \'' || AS_VARCHAR(src:adld) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adld_actd), '0'), 38, 10) is null and 
                    src:adld_actd is not null) THEN
                    'adld_actd contains a non-numeric value : \'' || AS_VARCHAR(src:adld_actd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adld_deea), '0'), 38, 10) is null and 
                    src:adld_deea is not null) THEN
                    'adld_deea contains a non-numeric value : \'' || AS_VARCHAR(src:adld_deea) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adld_dela), '0'), 38, 10) is null and 
                    src:adld_dela is not null) THEN
                    'adld_dela contains a non-numeric value : \'' || AS_VARCHAR(src:adld_dela) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:apdt), '1900-01-01')) is null and 
                    src:apdt is not null) THEN
                    'apdt contains a non-timestamp value : \'' || AS_VARCHAR(src:apdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aprp), '0'), 38, 10) is null and 
                    src:aprp is not null) THEN
                    'aprp contains a non-numeric value : \'' || AS_VARCHAR(src:aprp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asrp), '0'), 38, 10) is null and 
                    src:asrp is not null) THEN
                    'asrp contains a non-numeric value : \'' || AS_VARCHAR(src:asrp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awtc), '0'), 38, 10) is null and 
                    src:awtc is not null) THEN
                    'awtc contains a non-numeric value : \'' || AS_VARCHAR(src:awtc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfep), '0'), 38, 10) is null and 
                    src:bfep is not null) THEN
                    'bfep contains a non-numeric value : \'' || AS_VARCHAR(src:bfep) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfhr), '0'), 38, 10) is null and 
                    src:bfhr is not null) THEN
                    'bfhr contains a non-numeric value : \'' || AS_VARCHAR(src:bfhr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bloc), '0'), 38, 10) is null and 
                    src:bloc is not null) THEN
                    'bloc contains a non-numeric value : \'' || AS_VARCHAR(src:bloc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cada), '1900-01-01')) is null and 
                    src:cada is not null) THEN
                    'cada contains a non-timestamp value : \'' || AS_VARCHAR(src:cada) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctt), '0'), 38, 10) is null and 
                    src:cctt is not null) THEN
                    'cctt contains a non-numeric value : \'' || AS_VARCHAR(src:cctt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdld), '1900-01-01')) is null and 
                    src:cdld is not null) THEN
                    'cdld contains a non-timestamp value : \'' || AS_VARCHAR(src:cdld) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chel), '0'), 38, 10) is null and 
                    src:chel is not null) THEN
                    'chel contains a non-numeric value : \'' || AS_VARCHAR(src:chel) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clco_fcmp), '0'), 38, 10) is null and 
                    src:clco_fcmp is not null) THEN
                    'clco_fcmp contains a non-numeric value : \'' || AS_VARCHAR(src:clco_fcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clco_ref_compnr), '0'), 38, 10) is null and 
                    src:clco_ref_compnr is not null) THEN
                    'clco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cldt), '1900-01-01')) is null and 
                    src:cldt is not null) THEN
                    'cldt contains a non-timestamp value : \'' || AS_VARCHAR(src:cldt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clpd), '0'), 38, 10) is null and 
                    src:clpd is not null) THEN
                    'clpd contains a non-numeric value : \'' || AS_VARCHAR(src:clpd) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cmdt), '1900-01-01')) is null and 
                    src:cmdt is not null) THEN
                    'cmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:cmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:covn), '0'), 38, 10) is null and 
                    src:covn is not null) THEN
                    'covn contains a non-numeric value : \'' || AS_VARCHAR(src:covn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpla), '0'), 38, 10) is null and 
                    src:cpla is not null) THEN
                    'cpla contains a non-numeric value : \'' || AS_VARCHAR(src:cpla) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crpr_1), '0'), 38, 10) is null and 
                    src:crpr_1 is not null) THEN
                    'crpr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:crpr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crpr_2), '0'), 38, 10) is null and 
                    src:crpr_2 is not null) THEN
                    'crpr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:crpr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crpr_3), '0'), 38, 10) is null and 
                    src:crpr_3 is not null) THEN
                    'crpr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:crpr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crpr_4), '0'), 38, 10) is null and 
                    src:crpr_4 is not null) THEN
                    'crpr_4 contains a non-numeric value : \'' || AS_VARCHAR(src:crpr_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cutl), '0'), 38, 10) is null and 
                    src:cutl is not null) THEN
                    'cutl contains a non-numeric value : \'' || AS_VARCHAR(src:cutl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:defc), '0'), 38, 10) is null and 
                    src:defc is not null) THEN
                    'defc contains a non-numeric value : \'' || AS_VARCHAR(src:defc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dstp), '0'), 38, 10) is null and 
                    src:dstp is not null) THEN
                    'dstp contains a non-numeric value : \'' || AS_VARCHAR(src:dstp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) THEN
                    'efdt contains a non-timestamp value : \'' || AS_VARCHAR(src:efdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fain), '0'), 38, 10) is null and 
                    src:fain is not null) THEN
                    'fain contains a non-numeric value : \'' || AS_VARCHAR(src:fain) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fres), '0'), 38, 10) is null and 
                    src:fres is not null) THEN
                    'fres contains a non-numeric value : \'' || AS_VARCHAR(src:fres) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grid_ref_compnr), '0'), 38, 10) is null and 
                    src:grid_ref_compnr is not null) THEN
                    'grid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:huqt), '0'), 38, 10) is null and 
                    src:huqt is not null) THEN
                    'huqt contains a non-numeric value : \'' || AS_VARCHAR(src:huqt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insn), '0'), 38, 10) is null and 
                    src:insn is not null) THEN
                    'insn contains a non-numeric value : \'' || AS_VARCHAR(src:insn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insp), '0'), 38, 10) is null and 
                    src:insp is not null) THEN
                    'insp contains a non-numeric value : \'' || AS_VARCHAR(src:insp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lbnc), '0'), 38, 10) is null and 
                    src:lbnc is not null) THEN
                    'lbnc contains a non-numeric value : \'' || AS_VARCHAR(src:lbnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lbpb), '0'), 38, 10) is null and 
                    src:lbpb is not null) THEN
                    'lbpb contains a non-numeric value : \'' || AS_VARCHAR(src:lbpb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lbpr), '0'), 38, 10) is null and 
                    src:lbpr is not null) THEN
                    'lbpr contains a non-numeric value : \'' || AS_VARCHAR(src:lbpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lbtp), '0'), 38, 10) is null and 
                    src:lbtp is not null) THEN
                    'lbtp contains a non-numeric value : \'' || AS_VARCHAR(src:lbtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:matl), '0'), 38, 10) is null and 
                    src:matl is not null) THEN
                    'matl contains a non-numeric value : \'' || AS_VARCHAR(src:matl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:matn), '0'), 38, 10) is null and 
                    src:matn is not null) THEN
                    'matn contains a non-numeric value : \'' || AS_VARCHAR(src:matn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mfre), '0'), 38, 10) is null and 
                    src:mfre is not null) THEN
                    'mfre contains a non-numeric value : \'' || AS_VARCHAR(src:mfre) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mitm_opro_rcmp), '0'), 38, 10) is null and 
                    src:mitm_opro_rcmp is not null) THEN
                    'mitm_opro_rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:mitm_opro_rcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mitm_ref_compnr), '0'), 38, 10) is null and 
                    src:mitm_ref_compnr is not null) THEN
                    'mitm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mitm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mitm_site_ref_compnr), '0'), 38, 10) is null and 
                    src:mitm_site_ref_compnr is not null) THEN
                    'mitm_site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mitm_site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mmno), '0'), 38, 10) is null and 
                    src:mmno is not null) THEN
                    'mmno contains a non-numeric value : \'' || AS_VARCHAR(src:mmno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mmpo), '0'), 38, 10) is null and 
                    src:mmpo is not null) THEN
                    'mmpo contains a non-numeric value : \'' || AS_VARCHAR(src:mmpo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mocp), '0'), 38, 10) is null and 
                    src:mocp is not null) THEN
                    'mocp contains a non-numeric value : \'' || AS_VARCHAR(src:mocp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odpr), '0'), 38, 10) is null and 
                    src:odpr is not null) THEN
                    'odpr contains a non-numeric value : \'' || AS_VARCHAR(src:odpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odpt), '0'), 38, 10) is null and 
                    src:odpt is not null) THEN
                    'odpt contains a non-numeric value : \'' || AS_VARCHAR(src:odpt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oprn), '0'), 38, 10) is null and 
                    src:oprn is not null) THEN
                    'oprn contains a non-numeric value : \'' || AS_VARCHAR(src:oprn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oprp), '0'), 38, 10) is null and 
                    src:oprp is not null) THEN
                    'oprp contains a non-numeric value : \'' || AS_VARCHAR(src:oprp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osta), '0'), 38, 10) is null and 
                    src:osta is not null) THEN
                    'osta contains a non-numeric value : \'' || AS_VARCHAR(src:osta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owns), '0'), 38, 10) is null and 
                    src:owns is not null) THEN
                    'owns contains a non-numeric value : \'' || AS_VARCHAR(src:owns) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pldt), '1900-01-01')) is null and 
                    src:pldt is not null) THEN
                    'pldt contains a non-timestamp value : \'' || AS_VARCHAR(src:pldt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pldt_pltd), '0'), 38, 10) is null and 
                    src:pldt_pltd is not null) THEN
                    'pldt_pltd contains a non-numeric value : \'' || AS_VARCHAR(src:pldt_pltd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plid_ref_compnr), '0'), 38, 10) is null and 
                    src:plid_ref_compnr is not null) THEN
                    'plid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:plid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prcd), '0'), 38, 10) is null and 
                    src:prcd is not null) THEN
                    'prcd contains a non-numeric value : \'' || AS_VARCHAR(src:prcd) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) THEN
                    'prdt contains a non-timestamp value : \'' || AS_VARCHAR(src:prdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prlb), '0'), 38, 10) is null and 
                    src:prlb is not null) THEN
                    'prlb contains a non-numeric value : \'' || AS_VARCHAR(src:prlb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd), '0'), 38, 10) is null and 
                    src:qbfd is not null) THEN
                    'qbfd contains a non-numeric value : \'' || AS_VARCHAR(src:qbfd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_buar), '0'), 38, 10) is null and 
                    src:qbfd_buar is not null) THEN
                    'qbfd_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qbfd_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_buln), '0'), 38, 10) is null and 
                    src:qbfd_buln is not null) THEN
                    'qbfd_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qbfd_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_bupc), '0'), 38, 10) is null and 
                    src:qbfd_bupc is not null) THEN
                    'qbfd_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qbfd_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_butm), '0'), 38, 10) is null and 
                    src:qbfd_butm is not null) THEN
                    'qbfd_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qbfd_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_buvl), '0'), 38, 10) is null and 
                    src:qbfd_buvl is not null) THEN
                    'qbfd_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qbfd_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_buwg), '0'), 38, 10) is null and 
                    src:qbfd_buwg is not null) THEN
                    'qbfd_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qbfd_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdlv), '0'), 38, 10) is null and 
                    src:qdlv is not null) THEN
                    'qdlv contains a non-numeric value : \'' || AS_VARCHAR(src:qdlv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdlv_buar), '0'), 38, 10) is null and 
                    src:qdlv_buar is not null) THEN
                    'qdlv_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qdlv_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdlv_buln), '0'), 38, 10) is null and 
                    src:qdlv_buln is not null) THEN
                    'qdlv_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qdlv_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdlv_bupc), '0'), 38, 10) is null and 
                    src:qdlv_bupc is not null) THEN
                    'qdlv_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qdlv_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdlv_butm), '0'), 38, 10) is null and 
                    src:qdlv_butm is not null) THEN
                    'qdlv_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qdlv_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdlv_buvl), '0'), 38, 10) is null and 
                    src:qdlv_buvl is not null) THEN
                    'qdlv_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qdlv_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdlv_buwg), '0'), 38, 10) is null and 
                    src:qdlv_buwg is not null) THEN
                    'qdlv_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qdlv_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qntl), '0'), 38, 10) is null and 
                    src:qntl is not null) THEN
                    'qntl contains a non-numeric value : \'' || AS_VARCHAR(src:qntl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qntl_buar), '0'), 38, 10) is null and 
                    src:qntl_buar is not null) THEN
                    'qntl_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qntl_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qntl_buln), '0'), 38, 10) is null and 
                    src:qntl_buln is not null) THEN
                    'qntl_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qntl_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qntl_bupc), '0'), 38, 10) is null and 
                    src:qntl_bupc is not null) THEN
                    'qntl_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qntl_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qntl_butm), '0'), 38, 10) is null and 
                    src:qntl_butm is not null) THEN
                    'qntl_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qntl_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qntl_buvl), '0'), 38, 10) is null and 
                    src:qntl_buvl is not null) THEN
                    'qntl_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qntl_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qntl_buwg), '0'), 38, 10) is null and 
                    src:qntl_buwg is not null) THEN
                    'qntl_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qntl_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor), '0'), 38, 10) is null and 
                    src:qoor is not null) THEN
                    'qoor contains a non-numeric value : \'' || AS_VARCHAR(src:qoor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar), '0'), 38, 10) is null and 
                    src:qqar is not null) THEN
                    'qqar contains a non-numeric value : \'' || AS_VARCHAR(src:qqar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buar), '0'), 38, 10) is null and 
                    src:qqar_buar is not null) THEN
                    'qqar_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buln), '0'), 38, 10) is null and 
                    src:qqar_buln is not null) THEN
                    'qqar_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_bupc), '0'), 38, 10) is null and 
                    src:qqar_bupc is not null) THEN
                    'qqar_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_butm), '0'), 38, 10) is null and 
                    src:qqar_butm is not null) THEN
                    'qqar_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buvl), '0'), 38, 10) is null and 
                    src:qqar_buvl is not null) THEN
                    'qqar_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buwg), '0'), 38, 10) is null and 
                    src:qqar_buwg is not null) THEN
                    'qqar_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrdr), '0'), 38, 10) is null and 
                    src:qrdr is not null) THEN
                    'qrdr contains a non-numeric value : \'' || AS_VARCHAR(src:qrdr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrdr_buar), '0'), 38, 10) is null and 
                    src:qrdr_buar is not null) THEN
                    'qrdr_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qrdr_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrdr_buln), '0'), 38, 10) is null and 
                    src:qrdr_buln is not null) THEN
                    'qrdr_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qrdr_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrdr_bupc), '0'), 38, 10) is null and 
                    src:qrdr_bupc is not null) THEN
                    'qrdr_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qrdr_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrdr_butm), '0'), 38, 10) is null and 
                    src:qrdr_butm is not null) THEN
                    'qrdr_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qrdr_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrdr_buvl), '0'), 38, 10) is null and 
                    src:qrdr_buvl is not null) THEN
                    'qrdr_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qrdr_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrdr_buwg), '0'), 38, 10) is null and 
                    src:qrdr_buwg is not null) THEN
                    'qrdr_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qrdr_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc), '0'), 38, 10) is null and 
                    src:qrjc is not null) THEN
                    'qrjc contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buar), '0'), 38, 10) is null and 
                    src:qrjc_buar is not null) THEN
                    'qrjc_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buln), '0'), 38, 10) is null and 
                    src:qrjc_buln is not null) THEN
                    'qrjc_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_bupc), '0'), 38, 10) is null and 
                    src:qrjc_bupc is not null) THEN
                    'qrjc_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_butm), '0'), 38, 10) is null and 
                    src:qrjc_butm is not null) THEN
                    'qrjc_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buvl), '0'), 38, 10) is null and 
                    src:qrjc_buvl is not null) THEN
                    'qrjc_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buwg), '0'), 38, 10) is null and 
                    src:qrjc_buwg is not null) THEN
                    'qrjc_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf), '0'), 38, 10) is null and 
                    src:qtbf is not null) THEN
                    'qtbf contains a non-numeric value : \'' || AS_VARCHAR(src:qtbf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_buar), '0'), 38, 10) is null and 
                    src:qtbf_buar is not null) THEN
                    'qtbf_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qtbf_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_buln), '0'), 38, 10) is null and 
                    src:qtbf_buln is not null) THEN
                    'qtbf_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qtbf_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_bupc), '0'), 38, 10) is null and 
                    src:qtbf_bupc is not null) THEN
                    'qtbf_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qtbf_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_butm), '0'), 38, 10) is null and 
                    src:qtbf_butm is not null) THEN
                    'qtbf_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qtbf_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_buvl), '0'), 38, 10) is null and 
                    src:qtbf_buvl is not null) THEN
                    'qtbf_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qtbf_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_buwg), '0'), 38, 10) is null and 
                    src:qtbf_buwg is not null) THEN
                    'qtbf_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qtbf_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtdl), '0'), 38, 10) is null and 
                    src:qtdl is not null) THEN
                    'qtdl contains a non-numeric value : \'' || AS_VARCHAR(src:qtdl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtdl_buar), '0'), 38, 10) is null and 
                    src:qtdl_buar is not null) THEN
                    'qtdl_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qtdl_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtdl_buln), '0'), 38, 10) is null and 
                    src:qtdl_buln is not null) THEN
                    'qtdl_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qtdl_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtdl_bupc), '0'), 38, 10) is null and 
                    src:qtdl_bupc is not null) THEN
                    'qtdl_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qtdl_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtdl_butm), '0'), 38, 10) is null and 
                    src:qtdl_butm is not null) THEN
                    'qtdl_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qtdl_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtdl_buvl), '0'), 38, 10) is null and 
                    src:qtdl_buvl is not null) THEN
                    'qtdl_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qtdl_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtdl_buwg), '0'), 38, 10) is null and 
                    src:qtdl_buwg is not null) THEN
                    'qtdl_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qtdl_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor), '0'), 38, 10) is null and 
                    src:qtor is not null) THEN
                    'qtor contains a non-numeric value : \'' || AS_VARCHAR(src:qtor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_buar), '0'), 38, 10) is null and 
                    src:qtor_buar is not null) THEN
                    'qtor_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qtor_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_buln), '0'), 38, 10) is null and 
                    src:qtor_buln is not null) THEN
                    'qtor_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qtor_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_bupc), '0'), 38, 10) is null and 
                    src:qtor_bupc is not null) THEN
                    'qtor_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qtor_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_butm), '0'), 38, 10) is null and 
                    src:qtor_butm is not null) THEN
                    'qtor_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qtor_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_buvl), '0'), 38, 10) is null and 
                    src:qtor_buvl is not null) THEN
                    'qtor_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qtor_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_buwg), '0'), 38, 10) is null and 
                    src:qtor_buwg is not null) THEN
                    'qtor_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qtor_buwg) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdld), '1900-01-01')) is null and 
                    src:rdld is not null) THEN
                    'rdld contains a non-timestamp value : \'' || AS_VARCHAR(src:rdld) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:recn), '0'), 38, 10) is null and 
                    src:recn is not null) THEN
                    'recn contains a non-numeric value : \'' || AS_VARCHAR(src:recn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reco_ref_compnr), '0'), 38, 10) is null and 
                    src:reco_ref_compnr is not null) THEN
                    'reco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:reco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rgrp_ref_compnr), '0'), 38, 10) is null and 
                    src:rgrp_ref_compnr is not null) THEN
                    'rgrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rgrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:roul), '0'), 38, 10) is null and 
                    src:roul is not null) THEN
                    'roul contains a non-numeric value : \'' || AS_VARCHAR(src:roul) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rswc), '0'), 38, 10) is null and 
                    src:rswc is not null) THEN
                    'rswc contains a non-numeric value : \'' || AS_VARCHAR(src:rswc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rwko), '0'), 38, 10) is null and 
                    src:rwko is not null) THEN
                    'rwko contains a non-numeric value : \'' || AS_VARCHAR(src:rwko) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rwtp), '0'), 38, 10) is null and 
                    src:rwtp is not null) THEN
                    'rwtp contains a non-numeric value : \'' || AS_VARCHAR(src:rwtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sawl), '0'), 38, 10) is null and 
                    src:sawl is not null) THEN
                    'sawl contains a non-numeric value : \'' || AS_VARCHAR(src:sawl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schd), '0'), 38, 10) is null and 
                    src:schd is not null) THEN
                    'schd contains a non-numeric value : \'' || AS_VARCHAR(src:schd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfpl_ref_compnr), '0'), 38, 10) is null and 
                    src:sfpl_ref_compnr is not null) THEN
                    'sfpl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfpl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shsp), '0'), 38, 10) is null and 
                    src:shsp is not null) THEN
                    'shsp contains a non-numeric value : \'' || AS_VARCHAR(src:shsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_plgr_ref_compnr), '0'), 38, 10) is null and 
                    src:site_plgr_ref_compnr is not null) THEN
                    'site_plgr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_plgr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smfs), '0'), 38, 10) is null and 
                    src:smfs is not null) THEN
                    'smfs contains a non-numeric value : \'' || AS_VARCHAR(src:smfs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srnl), '0'), 38, 10) is null and 
                    src:srnl is not null) THEN
                    'srnl contains a non-numeric value : \'' || AS_VARCHAR(src:srnl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) THEN
                    'subc contains a non-numeric value : \'' || AS_VARCHAR(src:subc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subn), '0'), 38, 10) is null and 
                    src:subn is not null) THEN
                    'subn contains a non-numeric value : \'' || AS_VARCHAR(src:subn) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vers), '0'), 38, 10) is null and 
                    src:vers is not null) THEN
                    'vers contains a non-numeric value : \'' || AS_VARCHAR(src:vers) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null) THEN
                'sequencenumber contains a non-timestamp value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aapd), '0'), 38, 10) is null and 
                    src:aapd is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:adld), '1900-01-01')) is null and 
                    src:adld is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adld_actd), '0'), 38, 10) is null and 
                    src:adld_actd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adld_deea), '0'), 38, 10) is null and 
                    src:adld_deea is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adld_dela), '0'), 38, 10) is null and 
                    src:adld_dela is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:apdt), '1900-01-01')) is null and 
                    src:apdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aprp), '0'), 38, 10) is null and 
                    src:aprp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asrp), '0'), 38, 10) is null and 
                    src:asrp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awtc), '0'), 38, 10) is null and 
                    src:awtc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfep), '0'), 38, 10) is null and 
                    src:bfep is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfhr), '0'), 38, 10) is null and 
                    src:bfhr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bloc), '0'), 38, 10) is null and 
                    src:bloc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cada), '1900-01-01')) is null and 
                    src:cada is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctt), '0'), 38, 10) is null and 
                    src:cctt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdld), '1900-01-01')) is null and 
                    src:cdld is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chel), '0'), 38, 10) is null and 
                    src:chel is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clco_fcmp), '0'), 38, 10) is null and 
                    src:clco_fcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clco_ref_compnr), '0'), 38, 10) is null and 
                    src:clco_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cldt), '1900-01-01')) is null and 
                    src:cldt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clpd), '0'), 38, 10) is null and 
                    src:clpd is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cmdt), '1900-01-01')) is null and 
                    src:cmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:covn), '0'), 38, 10) is null and 
                    src:covn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpla), '0'), 38, 10) is null and 
                    src:cpla is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crpr_1), '0'), 38, 10) is null and 
                    src:crpr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crpr_2), '0'), 38, 10) is null and 
                    src:crpr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crpr_3), '0'), 38, 10) is null and 
                    src:crpr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crpr_4), '0'), 38, 10) is null and 
                    src:crpr_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cutl), '0'), 38, 10) is null and 
                    src:cutl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:defc), '0'), 38, 10) is null and 
                    src:defc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dstp), '0'), 38, 10) is null and 
                    src:dstp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fain), '0'), 38, 10) is null and 
                    src:fain is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fres), '0'), 38, 10) is null and 
                    src:fres is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grid_ref_compnr), '0'), 38, 10) is null and 
                    src:grid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:huqt), '0'), 38, 10) is null and 
                    src:huqt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insn), '0'), 38, 10) is null and 
                    src:insn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insp), '0'), 38, 10) is null and 
                    src:insp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lbnc), '0'), 38, 10) is null and 
                    src:lbnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lbpb), '0'), 38, 10) is null and 
                    src:lbpb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lbpr), '0'), 38, 10) is null and 
                    src:lbpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lbtp), '0'), 38, 10) is null and 
                    src:lbtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:matl), '0'), 38, 10) is null and 
                    src:matl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:matn), '0'), 38, 10) is null and 
                    src:matn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mfre), '0'), 38, 10) is null and 
                    src:mfre is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mitm_opro_rcmp), '0'), 38, 10) is null and 
                    src:mitm_opro_rcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mitm_ref_compnr), '0'), 38, 10) is null and 
                    src:mitm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mitm_site_ref_compnr), '0'), 38, 10) is null and 
                    src:mitm_site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mmno), '0'), 38, 10) is null and 
                    src:mmno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mmpo), '0'), 38, 10) is null and 
                    src:mmpo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mocp), '0'), 38, 10) is null and 
                    src:mocp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odpr), '0'), 38, 10) is null and 
                    src:odpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odpt), '0'), 38, 10) is null and 
                    src:odpt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oprn), '0'), 38, 10) is null and 
                    src:oprn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oprp), '0'), 38, 10) is null and 
                    src:oprp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osta), '0'), 38, 10) is null and 
                    src:osta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owns), '0'), 38, 10) is null and 
                    src:owns is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pldt), '1900-01-01')) is null and 
                    src:pldt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pldt_pltd), '0'), 38, 10) is null and 
                    src:pldt_pltd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plid_ref_compnr), '0'), 38, 10) is null and 
                    src:plid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prcd), '0'), 38, 10) is null and 
                    src:prcd is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prlb), '0'), 38, 10) is null and 
                    src:prlb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd), '0'), 38, 10) is null and 
                    src:qbfd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_buar), '0'), 38, 10) is null and 
                    src:qbfd_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_buln), '0'), 38, 10) is null and 
                    src:qbfd_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_bupc), '0'), 38, 10) is null and 
                    src:qbfd_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_butm), '0'), 38, 10) is null and 
                    src:qbfd_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_buvl), '0'), 38, 10) is null and 
                    src:qbfd_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_buwg), '0'), 38, 10) is null and 
                    src:qbfd_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdlv), '0'), 38, 10) is null and 
                    src:qdlv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdlv_buar), '0'), 38, 10) is null and 
                    src:qdlv_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdlv_buln), '0'), 38, 10) is null and 
                    src:qdlv_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdlv_bupc), '0'), 38, 10) is null and 
                    src:qdlv_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdlv_butm), '0'), 38, 10) is null and 
                    src:qdlv_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdlv_buvl), '0'), 38, 10) is null and 
                    src:qdlv_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdlv_buwg), '0'), 38, 10) is null and 
                    src:qdlv_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qntl), '0'), 38, 10) is null and 
                    src:qntl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qntl_buar), '0'), 38, 10) is null and 
                    src:qntl_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qntl_buln), '0'), 38, 10) is null and 
                    src:qntl_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qntl_bupc), '0'), 38, 10) is null and 
                    src:qntl_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qntl_butm), '0'), 38, 10) is null and 
                    src:qntl_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qntl_buvl), '0'), 38, 10) is null and 
                    src:qntl_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qntl_buwg), '0'), 38, 10) is null and 
                    src:qntl_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor), '0'), 38, 10) is null and 
                    src:qoor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar), '0'), 38, 10) is null and 
                    src:qqar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buar), '0'), 38, 10) is null and 
                    src:qqar_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buln), '0'), 38, 10) is null and 
                    src:qqar_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_bupc), '0'), 38, 10) is null and 
                    src:qqar_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_butm), '0'), 38, 10) is null and 
                    src:qqar_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buvl), '0'), 38, 10) is null and 
                    src:qqar_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buwg), '0'), 38, 10) is null and 
                    src:qqar_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrdr), '0'), 38, 10) is null and 
                    src:qrdr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrdr_buar), '0'), 38, 10) is null and 
                    src:qrdr_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrdr_buln), '0'), 38, 10) is null and 
                    src:qrdr_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrdr_bupc), '0'), 38, 10) is null and 
                    src:qrdr_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrdr_butm), '0'), 38, 10) is null and 
                    src:qrdr_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrdr_buvl), '0'), 38, 10) is null and 
                    src:qrdr_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrdr_buwg), '0'), 38, 10) is null and 
                    src:qrdr_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc), '0'), 38, 10) is null and 
                    src:qrjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buar), '0'), 38, 10) is null and 
                    src:qrjc_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buln), '0'), 38, 10) is null and 
                    src:qrjc_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_bupc), '0'), 38, 10) is null and 
                    src:qrjc_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_butm), '0'), 38, 10) is null and 
                    src:qrjc_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buvl), '0'), 38, 10) is null and 
                    src:qrjc_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buwg), '0'), 38, 10) is null and 
                    src:qrjc_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf), '0'), 38, 10) is null and 
                    src:qtbf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_buar), '0'), 38, 10) is null and 
                    src:qtbf_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_buln), '0'), 38, 10) is null and 
                    src:qtbf_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_bupc), '0'), 38, 10) is null and 
                    src:qtbf_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_butm), '0'), 38, 10) is null and 
                    src:qtbf_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_buvl), '0'), 38, 10) is null and 
                    src:qtbf_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_buwg), '0'), 38, 10) is null and 
                    src:qtbf_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtdl), '0'), 38, 10) is null and 
                    src:qtdl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtdl_buar), '0'), 38, 10) is null and 
                    src:qtdl_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtdl_buln), '0'), 38, 10) is null and 
                    src:qtdl_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtdl_bupc), '0'), 38, 10) is null and 
                    src:qtdl_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtdl_butm), '0'), 38, 10) is null and 
                    src:qtdl_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtdl_buvl), '0'), 38, 10) is null and 
                    src:qtdl_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtdl_buwg), '0'), 38, 10) is null and 
                    src:qtdl_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor), '0'), 38, 10) is null and 
                    src:qtor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_buar), '0'), 38, 10) is null and 
                    src:qtor_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_buln), '0'), 38, 10) is null and 
                    src:qtor_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_bupc), '0'), 38, 10) is null and 
                    src:qtor_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_butm), '0'), 38, 10) is null and 
                    src:qtor_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_buvl), '0'), 38, 10) is null and 
                    src:qtor_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_buwg), '0'), 38, 10) is null and 
                    src:qtor_buwg is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdld), '1900-01-01')) is null and 
                    src:rdld is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:recn), '0'), 38, 10) is null and 
                    src:recn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reco_ref_compnr), '0'), 38, 10) is null and 
                    src:reco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rgrp_ref_compnr), '0'), 38, 10) is null and 
                    src:rgrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:roul), '0'), 38, 10) is null and 
                    src:roul is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rswc), '0'), 38, 10) is null and 
                    src:rswc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rwko), '0'), 38, 10) is null and 
                    src:rwko is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rwtp), '0'), 38, 10) is null and 
                    src:rwtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sawl), '0'), 38, 10) is null and 
                    src:sawl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schd), '0'), 38, 10) is null and 
                    src:schd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfpl_ref_compnr), '0'), 38, 10) is null and 
                    src:sfpl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shsp), '0'), 38, 10) is null and 
                    src:shsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_plgr_ref_compnr), '0'), 38, 10) is null and 
                    src:site_plgr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smfs), '0'), 38, 10) is null and 
                    src:smfs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srnl), '0'), 38, 10) is null and 
                    src:srnl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subn), '0'), 38, 10) is null and 
                    src:subn is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vers), '0'), 38, 10) is null and 
                    src:vers is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)