CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDSLS401_ERROR AS SELECT src, 'LN_TDSLS401' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acsl), '0'), 38, 10) is null and 
                    src:acsl is not null) THEN
                    'acsl contains a non-numeric value : \'' || AS_VARCHAR(src:acsl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:airp), '0'), 38, 10) is null and 
                    src:airp is not null) THEN
                    'airp contains a non-numeric value : \'' || AS_VARCHAR(src:airp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:akcd_ref_compnr), '0'), 38, 10) is null and 
                    src:akcd_ref_compnr is not null) THEN
                    'akcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:akcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amgr_dtwc), '0'), 38, 10) is null and 
                    src:amgr_dtwc is not null) THEN
                    'amgr_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:amgr_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amgr_lclc), '0'), 38, 10) is null and 
                    src:amgr_lclc is not null) THEN
                    'amgr_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:amgr_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amgr_rfrc), '0'), 38, 10) is null and 
                    src:amgr_rfrc is not null) THEN
                    'amgr_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:amgr_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amgr_rpc1), '0'), 38, 10) is null and 
                    src:amgr_rpc1 is not null) THEN
                    'amgr_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:amgr_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amgr_rpc2), '0'), 38, 10) is null and 
                    src:amgr_rpc2 is not null) THEN
                    'amgr_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:amgr_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amgr_trnc), '0'), 38, 10) is null and 
                    src:amgr_trnc is not null) THEN
                    'amgr_trnc contains a non-numeric value : \'' || AS_VARCHAR(src:amgr_trnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld), '0'), 38, 10) is null and 
                    src:amld is not null) THEN
                    'amld contains a non-numeric value : \'' || AS_VARCHAR(src:amld) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld_dtwc), '0'), 38, 10) is null and 
                    src:amld_dtwc is not null) THEN
                    'amld_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:amld_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld_lclc), '0'), 38, 10) is null and 
                    src:amld_lclc is not null) THEN
                    'amld_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:amld_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld_rfrc), '0'), 38, 10) is null and 
                    src:amld_rfrc is not null) THEN
                    'amld_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:amld_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld_rpc1), '0'), 38, 10) is null and 
                    src:amld_rpc1 is not null) THEN
                    'amld_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:amld_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld_rpc2), '0'), 38, 10) is null and 
                    src:amld_rpc2 is not null) THEN
                    'amld_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:amld_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_dtwc), '0'), 38, 10) is null and 
                    src:amnt_dtwc is not null) THEN
                    'amnt_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_lclc), '0'), 38, 10) is null and 
                    src:amnt_lclc is not null) THEN
                    'amnt_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rfrc), '0'), 38, 10) is null and 
                    src:amnt_rfrc is not null) THEN
                    'amnt_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc1), '0'), 38, 10) is null and 
                    src:amnt_rpc1 is not null) THEN
                    'amnt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc2), '0'), 38, 10) is null and 
                    src:amnt_rpc2 is not null) THEN
                    'amnt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_trnc), '0'), 38, 10) is null and 
                    src:amnt_trnc is not null) THEN
                    'amnt_trnc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_trnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod), '0'), 38, 10) is null and 
                    src:amod is not null) THEN
                    'amod contains a non-numeric value : \'' || AS_VARCHAR(src:amod) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod_dtwc), '0'), 38, 10) is null and 
                    src:amod_dtwc is not null) THEN
                    'amod_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:amod_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod_lclc), '0'), 38, 10) is null and 
                    src:amod_lclc is not null) THEN
                    'amod_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:amod_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod_rfrc), '0'), 38, 10) is null and 
                    src:amod_rfrc is not null) THEN
                    'amod_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:amod_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod_rpc1), '0'), 38, 10) is null and 
                    src:amod_rpc1 is not null) THEN
                    'amod_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:amod_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod_rpc2), '0'), 38, 10) is null and 
                    src:amod_rpc2 is not null) THEN
                    'amod_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:amod_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is null and 
                    src:atse_ref_compnr is not null) THEN
                    'atse_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:atse_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrb), '0'), 38, 10) is null and 
                    src:bgrb is not null) THEN
                    'bgrb contains a non-numeric value : \'' || AS_VARCHAR(src:bgrb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrq), '0'), 38, 10) is null and 
                    src:bgrq is not null) THEN
                    'bgrq contains a non-numeric value : \'' || AS_VARCHAR(src:bgrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bind), '0'), 38, 10) is null and 
                    src:bind is not null) THEN
                    'bind contains a non-numeric value : \'' || AS_VARCHAR(src:bind) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bkyn), '0'), 38, 10) is null and 
                    src:bkyn is not null) THEN
                    'bkyn contains a non-numeric value : \'' || AS_VARCHAR(src:bkyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) THEN
                    'bptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bqco), '0'), 38, 10) is null and 
                    src:bqco is not null) THEN
                    'bqco contains a non-numeric value : \'' || AS_VARCHAR(src:bqco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casi_ref_compnr), '0'), 38, 10) is null and 
                    src:casi_ref_compnr is not null) THEN
                    'casi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:casi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccof_ref_compnr), '0'), 38, 10) is null and 
                    src:ccof_ref_compnr is not null) THEN
                    'ccof_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccof_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) THEN
                    'ccty_cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) THEN
                    'cfrw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfrw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chan_ref_compnr), '0'), 38, 10) is null and 
                    src:chan_ref_compnr is not null) THEN
                    'chan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:chan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is null and 
                    src:citt_ref_compnr is not null) THEN
                    'citt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:citt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clgr_ref_compnr), '0'), 38, 10) is null and 
                    src:clgr_ref_compnr is not null) THEN
                    'clgr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clgr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clyn), '0'), 38, 10) is null and 
                    src:clyn is not null) THEN
                    'clyn contains a non-numeric value : \'' || AS_VARCHAR(src:clyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnig), '0'), 38, 10) is null and 
                    src:cnig is not null) THEN
                    'cnig contains a non-numeric value : \'' || AS_VARCHAR(src:cnig) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocp_dtwc), '0'), 38, 10) is null and 
                    src:cocp_dtwc is not null) THEN
                    'cocp_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:cocp_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocp_lclc), '0'), 38, 10) is null and 
                    src:cocp_lclc is not null) THEN
                    'cocp_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:cocp_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocp_rfrc), '0'), 38, 10) is null and 
                    src:cocp_rfrc is not null) THEN
                    'cocp_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:cocp_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocp_rpc1), '0'), 38, 10) is null and 
                    src:cocp_rpc1 is not null) THEN
                    'cocp_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:cocp_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocp_rpc2), '0'), 38, 10) is null and 
                    src:cocp_rpc2 is not null) THEN
                    'cocp_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:cocp_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocp_trnc), '0'), 38, 10) is null and 
                    src:cocp_trnc is not null) THEN
                    'cocp_trnc contains a non-numeric value : \'' || AS_VARCHAR(src:cocp_trnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) THEN
                    'cono_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_1), '0'), 38, 10) is null and 
                    src:copr_1 is not null) THEN
                    'copr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:copr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_2), '0'), 38, 10) is null and 
                    src:copr_2 is not null) THEN
                    'copr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:copr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_3), '0'), 38, 10) is null and 
                    src:copr_3 is not null) THEN
                    'copr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:copr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cphl), '0'), 38, 10) is null and 
                    src:cphl is not null) THEN
                    'cphl contains a non-numeric value : \'' || AS_VARCHAR(src:cphl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpon), '0'), 38, 10) is null and 
                    src:cpon is not null) THEN
                    'cpon contains a non-numeric value : \'' || AS_VARCHAR(src:cpon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpva), '0'), 38, 10) is null and 
                    src:cpva is not null) THEN
                    'cpva contains a non-numeric value : \'' || AS_VARCHAR(src:cpva) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpva_rcmp), '0'), 38, 10) is null and 
                    src:cpva_rcmp is not null) THEN
                    'cpva_rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:cpva_rcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crbn), '0'), 38, 10) is null and 
                    src:crbn is not null) THEN
                    'crbn contains a non-numeric value : \'' || AS_VARCHAR(src:crbn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crcd_ref_compnr), '0'), 38, 10) is null and 
                    src:crcd_ref_compnr is not null) THEN
                    'crcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) THEN
                    'crte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctcd_ref_compnr), '0'), 38, 10) is null and 
                    src:ctcd_ref_compnr is not null) THEN
                    'ctcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cubs_ref_compnr), '0'), 38, 10) is null and 
                    src:cubs_ref_compnr is not null) THEN
                    'cubs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cubs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cucp_dtwc), '0'), 38, 10) is null and 
                    src:cucp_dtwc is not null) THEN
                    'cucp_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:cucp_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cucp_lclc), '0'), 38, 10) is null and 
                    src:cucp_lclc is not null) THEN
                    'cucp_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:cucp_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cucp_rfrc), '0'), 38, 10) is null and 
                    src:cucp_rfrc is not null) THEN
                    'cucp_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:cucp_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cucp_rpc1), '0'), 38, 10) is null and 
                    src:cucp_rpc1 is not null) THEN
                    'cucp_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:cucp_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cucp_rpc2), '0'), 38, 10) is null and 
                    src:cucp_rpc2 is not null) THEN
                    'cucp_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:cucp_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cucp_trnc), '0'), 38, 10) is null and 
                    src:cucp_trnc is not null) THEN
                    'cucp_trnc contains a non-numeric value : \'' || AS_VARCHAR(src:cucp_trnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupq_ref_compnr), '0'), 38, 10) is null and 
                    src:cupq_ref_compnr is not null) THEN
                    'cupq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cupq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cups_ref_compnr), '0'), 38, 10) is null and 
                    src:cups_ref_compnr is not null) THEN
                    'cups_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cups_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqs_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqs_ref_compnr is not null) THEN
                    'cuqs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuqs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuva), '0'), 38, 10) is null and 
                    src:cuva is not null) THEN
                    'cuva contains a non-numeric value : \'' || AS_VARCHAR(src:cuva) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvbs), '0'), 38, 10) is null and 
                    src:cvbs is not null) THEN
                    'cvbs contains a non-numeric value : \'' || AS_VARCHAR(src:cvbs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvpb), '0'), 38, 10) is null and 
                    src:cvpb is not null) THEN
                    'cvpb contains a non-numeric value : \'' || AS_VARCHAR(src:cvpb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvpq), '0'), 38, 10) is null and 
                    src:cvpq is not null) THEN
                    'cvpq contains a non-numeric value : \'' || AS_VARCHAR(src:cvpq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvps), '0'), 38, 10) is null and 
                    src:cvps is not null) THEN
                    'cvps contains a non-numeric value : \'' || AS_VARCHAR(src:cvps) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvqs), '0'), 38, 10) is null and 
                    src:cvqs is not null) THEN
                    'cvqs contains a non-numeric value : \'' || AS_VARCHAR(src:cvqs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) THEN
                    'cwoc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwoc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:damt), '0'), 38, 10) is null and 
                    src:damt is not null) THEN
                    'damt contains a non-numeric value : \'' || AS_VARCHAR(src:damt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ddch), '0'), 38, 10) is null and 
                    src:ddch is not null) THEN
                    'ddch contains a non-numeric value : \'' || AS_VARCHAR(src:ddch) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddta), '1900-01-01')) is null and 
                    src:ddta is not null) THEN
                    'ddta contains a non-timestamp value : \'' || AS_VARCHAR(src:ddta) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddtb), '1900-01-01')) is null and 
                    src:ddtb is not null) THEN
                    'ddtb contains a non-timestamp value : \'' || AS_VARCHAR(src:ddtb) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddtc), '1900-01-01')) is null and 
                    src:ddtc is not null) THEN
                    'ddtc contains a non-timestamp value : \'' || AS_VARCHAR(src:ddtc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddtd), '1900-01-01')) is null and 
                    src:ddtd is not null) THEN
                    'ddtd contains a non-timestamp value : \'' || AS_VARCHAR(src:ddtd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delc_ref_compnr), '0'), 38, 10) is null and 
                    src:delc_ref_compnr is not null) THEN
                    'delc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:delc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_1), '0'), 38, 10) is null and 
                    src:disc_1 is not null) THEN
                    'disc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_10), '0'), 38, 10) is null and 
                    src:disc_10 is not null) THEN
                    'disc_10 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_11), '0'), 38, 10) is null and 
                    src:disc_11 is not null) THEN
                    'disc_11 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_2), '0'), 38, 10) is null and 
                    src:disc_2 is not null) THEN
                    'disc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_3), '0'), 38, 10) is null and 
                    src:disc_3 is not null) THEN
                    'disc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_4), '0'), 38, 10) is null and 
                    src:disc_4 is not null) THEN
                    'disc_4 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_5), '0'), 38, 10) is null and 
                    src:disc_5 is not null) THEN
                    'disc_5 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_6), '0'), 38, 10) is null and 
                    src:disc_6 is not null) THEN
                    'disc_6 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_7), '0'), 38, 10) is null and 
                    src:disc_7 is not null) THEN
                    'disc_7 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_8), '0'), 38, 10) is null and 
                    src:disc_8 is not null) THEN
                    'disc_8 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_9), '0'), 38, 10) is null and 
                    src:disc_9 is not null) THEN
                    'disc_9 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_9) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dldt), '1900-01-01')) is null and 
                    src:dldt is not null) THEN
                    'dldt contains a non-timestamp value : \'' || AS_VARCHAR(src:dldt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dlld), '1900-01-01')) is null and 
                    src:dlld is not null) THEN
                    'dlld contains a non-timestamp value : \'' || AS_VARCHAR(src:dlld) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dltp), '0'), 38, 10) is null and 
                    src:dltp is not null) THEN
                    'dltp contains a non-numeric value : \'' || AS_VARCHAR(src:dltp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_1), '0'), 38, 10) is null and 
                    src:dmse_1 is not null) THEN
                    'dmse_1 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_10), '0'), 38, 10) is null and 
                    src:dmse_10 is not null) THEN
                    'dmse_10 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_11), '0'), 38, 10) is null and 
                    src:dmse_11 is not null) THEN
                    'dmse_11 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_2), '0'), 38, 10) is null and 
                    src:dmse_2 is not null) THEN
                    'dmse_2 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_3), '0'), 38, 10) is null and 
                    src:dmse_3 is not null) THEN
                    'dmse_3 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_4), '0'), 38, 10) is null and 
                    src:dmse_4 is not null) THEN
                    'dmse_4 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_5), '0'), 38, 10) is null and 
                    src:dmse_5 is not null) THEN
                    'dmse_5 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_6), '0'), 38, 10) is null and 
                    src:dmse_6 is not null) THEN
                    'dmse_6 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_7), '0'), 38, 10) is null and 
                    src:dmse_7 is not null) THEN
                    'dmse_7 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_8), '0'), 38, 10) is null and 
                    src:dmse_8 is not null) THEN
                    'dmse_8 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_9), '0'), 38, 10) is null and 
                    src:dmse_9 is not null) THEN
                    'dmse_9 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_1), '0'), 38, 10) is null and 
                    src:dmth_1 is not null) THEN
                    'dmth_1 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_10), '0'), 38, 10) is null and 
                    src:dmth_10 is not null) THEN
                    'dmth_10 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_11), '0'), 38, 10) is null and 
                    src:dmth_11 is not null) THEN
                    'dmth_11 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_2), '0'), 38, 10) is null and 
                    src:dmth_2 is not null) THEN
                    'dmth_2 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_3), '0'), 38, 10) is null and 
                    src:dmth_3 is not null) THEN
                    'dmth_3 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_4), '0'), 38, 10) is null and 
                    src:dmth_4 is not null) THEN
                    'dmth_4 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_5), '0'), 38, 10) is null and 
                    src:dmth_5 is not null) THEN
                    'dmth_5 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_6), '0'), 38, 10) is null and 
                    src:dmth_6 is not null) THEN
                    'dmth_6 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_7), '0'), 38, 10) is null and 
                    src:dmth_7 is not null) THEN
                    'dmth_7 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_8), '0'), 38, 10) is null and 
                    src:dmth_8 is not null) THEN
                    'dmth_8 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_9), '0'), 38, 10) is null and 
                    src:dmth_9 is not null) THEN
                    'dmth_9 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_1), '0'), 38, 10) is null and 
                    src:dmty_1 is not null) THEN
                    'dmty_1 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_10), '0'), 38, 10) is null and 
                    src:dmty_10 is not null) THEN
                    'dmty_10 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_11), '0'), 38, 10) is null and 
                    src:dmty_11 is not null) THEN
                    'dmty_11 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_2), '0'), 38, 10) is null and 
                    src:dmty_2 is not null) THEN
                    'dmty_2 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_3), '0'), 38, 10) is null and 
                    src:dmty_3 is not null) THEN
                    'dmty_3 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_4), '0'), 38, 10) is null and 
                    src:dmty_4 is not null) THEN
                    'dmty_4 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_5), '0'), 38, 10) is null and 
                    src:dmty_5 is not null) THEN
                    'dmty_5 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_6), '0'), 38, 10) is null and 
                    src:dmty_6 is not null) THEN
                    'dmty_6 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_7), '0'), 38, 10) is null and 
                    src:dmty_7 is not null) THEN
                    'dmty_7 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_8), '0'), 38, 10) is null and 
                    src:dmty_8 is not null) THEN
                    'dmty_8 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_9), '0'), 38, 10) is null and 
                    src:dmty_9 is not null) THEN
                    'dmty_9 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_1), '0'), 38, 10) is null and 
                    src:dorg_1 is not null) THEN
                    'dorg_1 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_10), '0'), 38, 10) is null and 
                    src:dorg_10 is not null) THEN
                    'dorg_10 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_11), '0'), 38, 10) is null and 
                    src:dorg_11 is not null) THEN
                    'dorg_11 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_2), '0'), 38, 10) is null and 
                    src:dorg_2 is not null) THEN
                    'dorg_2 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_3), '0'), 38, 10) is null and 
                    src:dorg_3 is not null) THEN
                    'dorg_3 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_4), '0'), 38, 10) is null and 
                    src:dorg_4 is not null) THEN
                    'dorg_4 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_5), '0'), 38, 10) is null and 
                    src:dorg_5 is not null) THEN
                    'dorg_5 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_6), '0'), 38, 10) is null and 
                    src:dorg_6 is not null) THEN
                    'dorg_6 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_7), '0'), 38, 10) is null and 
                    src:dorg_7 is not null) THEN
                    'dorg_7 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_8), '0'), 38, 10) is null and 
                    src:dorg_8 is not null) THEN
                    'dorg_8 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_9), '0'), 38, 10) is null and 
                    src:dorg_9 is not null) THEN
                    'dorg_9 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtrm), '0'), 38, 10) is null and 
                    src:dtrm is not null) THEN
                    'dtrm contains a non-numeric value : \'' || AS_VARCHAR(src:dtrm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) THEN
                    'effn contains a non-numeric value : \'' || AS_VARCHAR(src:effn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) THEN
                    'effn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:effn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elgb), '0'), 38, 10) is null and 
                    src:elgb is not null) THEN
                    'elgb contains a non-numeric value : \'' || AS_VARCHAR(src:elgb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) THEN
                    'exmt contains a non-numeric value : \'' || AS_VARCHAR(src:exmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcop_1), '0'), 38, 10) is null and 
                    src:fcop_1 is not null) THEN
                    'fcop_1 contains a non-numeric value : \'' || AS_VARCHAR(src:fcop_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcop_2), '0'), 38, 10) is null and 
                    src:fcop_2 is not null) THEN
                    'fcop_2 contains a non-numeric value : \'' || AS_VARCHAR(src:fcop_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcop_3), '0'), 38, 10) is null and 
                    src:fcop_3 is not null) THEN
                    'fcop_3 contains a non-numeric value : \'' || AS_VARCHAR(src:fcop_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fram), '0'), 38, 10) is null and 
                    src:fram is not null) THEN
                    'fram contains a non-numeric value : \'' || AS_VARCHAR(src:fram) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frbn), '0'), 38, 10) is null and 
                    src:frbn is not null) THEN
                    'frbn contains a non-numeric value : \'' || AS_VARCHAR(src:frbn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frin), '0'), 38, 10) is null and 
                    src:frin is not null) THEN
                    'frin contains a non-numeric value : \'' || AS_VARCHAR(src:frin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fxcp_1), '0'), 38, 10) is null and 
                    src:fxcp_1 is not null) THEN
                    'fxcp_1 contains a non-numeric value : \'' || AS_VARCHAR(src:fxcp_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fxcp_2), '0'), 38, 10) is null and 
                    src:fxcp_2 is not null) THEN
                    'fxcp_2 contains a non-numeric value : \'' || AS_VARCHAR(src:fxcp_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fxcp_3), '0'), 38, 10) is null and 
                    src:fxcp_3 is not null) THEN
                    'fxcp_3 contains a non-numeric value : \'' || AS_VARCHAR(src:fxcp_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gefo), '0'), 38, 10) is null and 
                    src:gefo is not null) THEN
                    'gefo contains a non-numeric value : \'' || AS_VARCHAR(src:gefo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:infr), '0'), 38, 10) is null and 
                    src:infr is not null) THEN
                    'infr contains a non-numeric value : \'' || AS_VARCHAR(src:infr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:invd), '1900-01-01')) is null and 
                    src:invd is not null) THEN
                    'invd contains a non-timestamp value : \'' || AS_VARCHAR(src:invd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invn), '0'), 38, 10) is null and 
                    src:invn is not null) THEN
                    'invn contains a non-numeric value : \'' || AS_VARCHAR(src:invn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:isss), '0'), 38, 10) is null and 
                    src:isss is not null) THEN
                    'isss contains a non-numeric value : \'' || AS_VARCHAR(src:isss) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_atse_ref_compnr), '0'), 38, 10) is null and 
                    src:item_atse_ref_compnr is not null) THEN
                    'item_atse_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_atse_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_atse_site_ref_compnr), '0'), 38, 10) is null and 
                    src:item_atse_site_ref_compnr is not null) THEN
                    'item_atse_site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_atse_site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_atse_stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:item_atse_stsi_ref_compnr is not null) THEN
                    'item_atse_stsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_atse_stsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_site_ref_compnr), '0'), 38, 10) is null and 
                    src:item_site_ref_compnr is not null) THEN
                    'item_site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:item_stsi_ref_compnr is not null) THEN
                    'item_stsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_stsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcrq), '0'), 38, 10) is null and 
                    src:lcrq is not null) THEN
                    'lcrq contains a non-numeric value : \'' || AS_VARCHAR(src:lcrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_1), '0'), 38, 10) is null and 
                    src:ldam_1 is not null) THEN
                    'ldam_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_10), '0'), 38, 10) is null and 
                    src:ldam_10 is not null) THEN
                    'ldam_10 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_11), '0'), 38, 10) is null and 
                    src:ldam_11 is not null) THEN
                    'ldam_11 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_2), '0'), 38, 10) is null and 
                    src:ldam_2 is not null) THEN
                    'ldam_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_3), '0'), 38, 10) is null and 
                    src:ldam_3 is not null) THEN
                    'ldam_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_4), '0'), 38, 10) is null and 
                    src:ldam_4 is not null) THEN
                    'ldam_4 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_5), '0'), 38, 10) is null and 
                    src:ldam_5 is not null) THEN
                    'ldam_5 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_6), '0'), 38, 10) is null and 
                    src:ldam_6 is not null) THEN
                    'ldam_6 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_7), '0'), 38, 10) is null and 
                    src:ldam_7 is not null) THEN
                    'ldam_7 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_8), '0'), 38, 10) is null and 
                    src:ldam_8 is not null) THEN
                    'ldam_8 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_9), '0'), 38, 10) is null and 
                    src:ldam_9 is not null) THEN
                    'ldam_9 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leng), '0'), 38, 10) is null and 
                    src:leng is not null) THEN
                    'leng contains a non-numeric value : \'' || AS_VARCHAR(src:leng) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmbi), '0'), 38, 10) is null and 
                    src:lmbi is not null) THEN
                    'lmbi contains a non-numeric value : \'' || AS_VARCHAR(src:lmbi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lnst), '0'), 38, 10) is null and 
                    src:lnst is not null) THEN
                    'lnst contains a non-numeric value : \'' || AS_VARCHAR(src:lnst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsel), '0'), 38, 10) is null and 
                    src:lsel is not null) THEN
                    'lsel contains a non-numeric value : \'' || AS_VARCHAR(src:lsel) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lseq), '0'), 38, 10) is null and 
                    src:lseq is not null) THEN
                    'lseq contains a non-numeric value : \'' || AS_VARCHAR(src:lseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:modi), '0'), 38, 10) is null and 
                    src:modi is not null) THEN
                    'modi contains a non-numeric value : \'' || AS_VARCHAR(src:modi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:motv_ref_compnr), '0'), 38, 10) is null and 
                    src:motv_ref_compnr is not null) THEN
                    'motv_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:motv_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mprm), '0'), 38, 10) is null and 
                    src:mprm is not null) THEN
                    'mprm contains a non-numeric value : \'' || AS_VARCHAR(src:mprm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt), '0'), 38, 10) is null and 
                    src:oamt is not null) THEN
                    'oamt contains a non-numeric value : \'' || AS_VARCHAR(src:oamt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:odat), '1900-01-01')) is null and 
                    src:odat is not null) THEN
                    'odat contains a non-timestamp value : \'' || AS_VARCHAR(src:odat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oltp), '0'), 38, 10) is null and 
                    src:oltp is not null) THEN
                    'oltp contains a non-numeric value : \'' || AS_VARCHAR(src:oltp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opol), '0'), 38, 10) is null and 
                    src:opol is not null) THEN
                    'opol contains a non-numeric value : \'' || AS_VARCHAR(src:opol) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opri), '0'), 38, 10) is null and 
                    src:opri is not null) THEN
                    'opri contains a non-numeric value : \'' || AS_VARCHAR(src:opri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oprs), '0'), 38, 10) is null and 
                    src:oprs is not null) THEN
                    'oprs contains a non-numeric value : \'' || AS_VARCHAR(src:oprs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_cosn_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_cosn_ref_compnr is not null) THEN
                    'orno_cosn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:orno_cosn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_ref_compnr is not null) THEN
                    'orno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:orno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owns), '0'), 38, 10) is null and 
                    src:owns is not null) THEN
                    'owns contains a non-numeric value : \'' || AS_VARCHAR(src:owns) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcad), '0'), 38, 10) is null and 
                    src:pcad is not null) THEN
                    'pcad contains a non-numeric value : \'' || AS_VARCHAR(src:pcad) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcod_ref_compnr), '0'), 38, 10) is null and 
                    src:pcod_ref_compnr is not null) THEN
                    'pcod_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pcod_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmnt), '0'), 38, 10) is null and 
                    src:pmnt is not null) THEN
                    'pmnt contains a non-numeric value : \'' || AS_VARCHAR(src:pmnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmse), '0'), 38, 10) is null and 
                    src:pmse is not null) THEN
                    'pmse contains a non-numeric value : \'' || AS_VARCHAR(src:pmse) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:porg), '0'), 38, 10) is null and 
                    src:porg is not null) THEN
                    'porg contains a non-numeric value : \'' || AS_VARCHAR(src:porg) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) THEN
                    'prdt contains a non-timestamp value : \'' || AS_VARCHAR(src:prdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) THEN
                    'pric contains a non-numeric value : \'' || AS_VARCHAR(src:pric) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_dtwc), '0'), 38, 10) is null and 
                    src:pric_dtwc is not null) THEN
                    'pric_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:pric_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_lclc), '0'), 38, 10) is null and 
                    src:pric_lclc is not null) THEN
                    'pric_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:pric_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_rfrc), '0'), 38, 10) is null and 
                    src:pric_rfrc is not null) THEN
                    'pric_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:pric_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_rpc1), '0'), 38, 10) is null and 
                    src:pric_rpc1 is not null) THEN
                    'pric_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:pric_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_rpc2), '0'), 38, 10) is null and 
                    src:pric_rpc2 is not null) THEN
                    'pric_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:pric_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pror), '0'), 38, 10) is null and 
                    src:pror is not null) THEN
                    'pror contains a non-numeric value : \'' || AS_VARCHAR(src:pror) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsg_ref_compnr), '0'), 38, 10) is null and 
                    src:prsg_ref_compnr is not null) THEN
                    'prsg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prsg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbo), '0'), 38, 10) is null and 
                    src:qbbo is not null) THEN
                    'qbbo contains a non-numeric value : \'' || AS_VARCHAR(src:qbbo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbo_buar), '0'), 38, 10) is null and 
                    src:qbbo_buar is not null) THEN
                    'qbbo_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qbbo_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbo_buln), '0'), 38, 10) is null and 
                    src:qbbo_buln is not null) THEN
                    'qbbo_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qbbo_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbo_bupc), '0'), 38, 10) is null and 
                    src:qbbo_bupc is not null) THEN
                    'qbbo_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qbbo_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbo_butm), '0'), 38, 10) is null and 
                    src:qbbo_butm is not null) THEN
                    'qbbo_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qbbo_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbo_buvl), '0'), 38, 10) is null and 
                    src:qbbo_buvl is not null) THEN
                    'qbbo_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qbbo_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbo_buwg), '0'), 38, 10) is null and 
                    src:qbbo_buwg is not null) THEN
                    'qbbo_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qbbo_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbo_trnu), '0'), 38, 10) is null and 
                    src:qbbo_trnu is not null) THEN
                    'qbbo_trnu contains a non-numeric value : \'' || AS_VARCHAR(src:qbbo_trnu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicm), '0'), 38, 10) is null and 
                    src:qicm is not null) THEN
                    'qicm contains a non-numeric value : \'' || AS_VARCHAR(src:qicm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl), '0'), 38, 10) is null and 
                    src:qidl is not null) THEN
                    'qidl contains a non-numeric value : \'' || AS_VARCHAR(src:qidl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl_buar), '0'), 38, 10) is null and 
                    src:qidl_buar is not null) THEN
                    'qidl_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qidl_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl_buln), '0'), 38, 10) is null and 
                    src:qidl_buln is not null) THEN
                    'qidl_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qidl_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl_bupc), '0'), 38, 10) is null and 
                    src:qidl_bupc is not null) THEN
                    'qidl_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qidl_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl_butm), '0'), 38, 10) is null and 
                    src:qidl_butm is not null) THEN
                    'qidl_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qidl_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl_buvl), '0'), 38, 10) is null and 
                    src:qidl_buvl is not null) THEN
                    'qidl_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qidl_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl_buwg), '0'), 38, 10) is null and 
                    src:qidl_buwg is not null) THEN
                    'qidl_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qidl_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl_trnu), '0'), 38, 10) is null and 
                    src:qidl_trnu is not null) THEN
                    'qidl_trnu contains a non-numeric value : \'' || AS_VARCHAR(src:qidl_trnu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qobh_invu), '0'), 38, 10) is null and 
                    src:qobh_invu is not null) THEN
                    'qobh_invu contains a non-numeric value : \'' || AS_VARCHAR(src:qobh_invu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qohb), '0'), 38, 10) is null and 
                    src:qohb is not null) THEN
                    'qohb contains a non-numeric value : \'' || AS_VARCHAR(src:qohb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qohb_buar), '0'), 38, 10) is null and 
                    src:qohb_buar is not null) THEN
                    'qohb_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qohb_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qohb_buln), '0'), 38, 10) is null and 
                    src:qohb_buln is not null) THEN
                    'qohb_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qohb_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qohb_bupc), '0'), 38, 10) is null and 
                    src:qohb_bupc is not null) THEN
                    'qohb_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qohb_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qohb_butm), '0'), 38, 10) is null and 
                    src:qohb_butm is not null) THEN
                    'qohb_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qohb_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qohb_buvl), '0'), 38, 10) is null and 
                    src:qohb_buvl is not null) THEN
                    'qohb_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qohb_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qohb_buwg), '0'), 38, 10) is null and 
                    src:qohb_buwg is not null) THEN
                    'qohb_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qohb_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor), '0'), 38, 10) is null and 
                    src:qoor is not null) THEN
                    'qoor contains a non-numeric value : \'' || AS_VARCHAR(src:qoor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_buar), '0'), 38, 10) is null and 
                    src:qoor_buar is not null) THEN
                    'qoor_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qoor_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_buln), '0'), 38, 10) is null and 
                    src:qoor_buln is not null) THEN
                    'qoor_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qoor_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_bupc), '0'), 38, 10) is null and 
                    src:qoor_bupc is not null) THEN
                    'qoor_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qoor_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_butm), '0'), 38, 10) is null and 
                    src:qoor_butm is not null) THEN
                    'qoor_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qoor_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_buvl), '0'), 38, 10) is null and 
                    src:qoor_buvl is not null) THEN
                    'qoor_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qoor_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_buwg), '0'), 38, 10) is null and 
                    src:qoor_buwg is not null) THEN
                    'qoor_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qoor_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_invu), '0'), 38, 10) is null and 
                    src:qoor_invu is not null) THEN
                    'qoor_invu contains a non-numeric value : \'' || AS_VARCHAR(src:qoor_invu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpbo), '0'), 38, 10) is null and 
                    src:qpbo is not null) THEN
                    'qpbo contains a non-numeric value : \'' || AS_VARCHAR(src:qpbo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpdl), '0'), 38, 10) is null and 
                    src:qpdl is not null) THEN
                    'qpdl contains a non-numeric value : \'' || AS_VARCHAR(src:qpdl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpor), '0'), 38, 10) is null and 
                    src:qpor is not null) THEN
                    'qpor contains a non-numeric value : \'' || AS_VARCHAR(src:qpor) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ratd), '1900-01-01')) is null and 
                    src:ratd is not null) THEN
                    'ratd contains a non-timestamp value : \'' || AS_VARCHAR(src:ratd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) THEN
                    'ratf_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) THEN
                    'ratf_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) THEN
                    'ratf_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_1), '0'), 38, 10) is null and 
                    src:rats_1 is not null) THEN
                    'rats_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rats_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_2), '0'), 38, 10) is null and 
                    src:rats_2 is not null) THEN
                    'rats_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rats_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_3), '0'), 38, 10) is null and 
                    src:rats_3 is not null) THEN
                    'rats_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rats_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) THEN
                    'rcod_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcod_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdta), '1900-01-01')) is null and 
                    src:rdta is not null) THEN
                    'rdta contains a non-timestamp value : \'' || AS_VARCHAR(src:rdta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ruso), '0'), 38, 10) is null and 
                    src:ruso is not null) THEN
                    'ruso contains a non-numeric value : \'' || AS_VARCHAR(src:ruso) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbim), '0'), 38, 10) is null and 
                    src:sbim is not null) THEN
                    'sbim contains a non-numeric value : \'' || AS_VARCHAR(src:sbim) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scmp), '0'), 38, 10) is null and 
                    src:scmp is not null) THEN
                    'scmp contains a non-numeric value : \'' || AS_VARCHAR(src:scmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scon), '0'), 38, 10) is null and 
                    src:scon is not null) THEN
                    'scon contains a non-numeric value : \'' || AS_VARCHAR(src:scon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdsc), '0'), 38, 10) is null and 
                    src:sdsc is not null) THEN
                    'sdsc contains a non-numeric value : \'' || AS_VARCHAR(src:sdsc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serv_ref_compnr), '0'), 38, 10) is null and 
                    src:serv_ref_compnr is not null) THEN
                    'serv_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:serv_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shln), '0'), 38, 10) is null and 
                    src:shln is not null) THEN
                    'shln contains a non-numeric value : \'' || AS_VARCHAR(src:shln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) THEN
                    'sqnb contains a non-numeric value : \'' || AS_VARCHAR(src:sqnb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssel), '0'), 38, 10) is null and 
                    src:ssel is not null) THEN
                    'ssel contains a non-numeric value : \'' || AS_VARCHAR(src:ssel) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_dlpt_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_dlpt_ref_compnr is not null) THEN
                    'stad_dlpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_dlpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) THEN
                    'stad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) THEN
                    'stbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is null and 
                    src:stcn_ref_compnr is not null) THEN
                    'stcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stdc), '0'), 38, 10) is null and 
                    src:stdc is not null) THEN
                    'stdc contains a non-numeric value : \'' || AS_VARCHAR(src:stdc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:stsi_ref_compnr is not null) THEN
                    'stsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stwh_ref_compnr), '0'), 38, 10) is null and 
                    src:stwh_ref_compnr is not null) THEN
                    'stwh_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stwh_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is null and 
                    src:styp_ref_compnr is not null) THEN
                    'styp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:styp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) THEN
                    'subc contains a non-numeric value : \'' || AS_VARCHAR(src:subc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:susi), '0'), 38, 10) is null and 
                    src:susi is not null) THEN
                    'susi contains a non-numeric value : \'' || AS_VARCHAR(src:susi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term), '0'), 38, 10) is null and 
                    src:term is not null) THEN
                    'term contains a non-numeric value : \'' || AS_VARCHAR(src:term) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:thic), '0'), 38, 10) is null and 
                    src:thic is not null) THEN
                    'thic contains a non-numeric value : \'' || AS_VARCHAR(src:thic) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprd), '0'), 38, 10) is null and 
                    src:tprd is not null) THEN
                    'tprd contains a non-numeric value : \'' || AS_VARCHAR(src:tprd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprd_dtwc), '0'), 38, 10) is null and 
                    src:tprd_dtwc is not null) THEN
                    'tprd_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:tprd_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprd_lclc), '0'), 38, 10) is null and 
                    src:tprd_lclc is not null) THEN
                    'tprd_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:tprd_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprd_rfrc), '0'), 38, 10) is null and 
                    src:tprd_rfrc is not null) THEN
                    'tprd_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:tprd_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprd_rpc1), '0'), 38, 10) is null and 
                    src:tprd_rpc1 is not null) THEN
                    'tprd_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:tprd_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprd_rpc2), '0'), 38, 10) is null and 
                    src:tprd_rpc2 is not null) THEN
                    'tprd_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:tprd_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vcop_1), '0'), 38, 10) is null and 
                    src:vcop_1 is not null) THEN
                    'vcop_1 contains a non-numeric value : \'' || AS_VARCHAR(src:vcop_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vcop_2), '0'), 38, 10) is null and 
                    src:vcop_2 is not null) THEN
                    'vcop_2 contains a non-numeric value : \'' || AS_VARCHAR(src:vcop_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vcop_3), '0'), 38, 10) is null and 
                    src:vcop_3 is not null) THEN
                    'vcop_3 contains a non-numeric value : \'' || AS_VARCHAR(src:vcop_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:widt), '0'), 38, 10) is null and 
                    src:widt is not null) THEN
                    'widt contains a non-numeric value : \'' || AS_VARCHAR(src:widt) || '\'' WHEN 
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
                                    
                src:orno,
                src:sqnb,
                src:pono,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDSLS401 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acsl), '0'), 38, 10) is null and 
                    src:acsl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:airp), '0'), 38, 10) is null and 
                    src:airp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:akcd_ref_compnr), '0'), 38, 10) is null and 
                    src:akcd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amgr_dtwc), '0'), 38, 10) is null and 
                    src:amgr_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amgr_lclc), '0'), 38, 10) is null and 
                    src:amgr_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amgr_rfrc), '0'), 38, 10) is null and 
                    src:amgr_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amgr_rpc1), '0'), 38, 10) is null and 
                    src:amgr_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amgr_rpc2), '0'), 38, 10) is null and 
                    src:amgr_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amgr_trnc), '0'), 38, 10) is null and 
                    src:amgr_trnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld), '0'), 38, 10) is null and 
                    src:amld is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld_dtwc), '0'), 38, 10) is null and 
                    src:amld_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld_lclc), '0'), 38, 10) is null and 
                    src:amld_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld_rfrc), '0'), 38, 10) is null and 
                    src:amld_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld_rpc1), '0'), 38, 10) is null and 
                    src:amld_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld_rpc2), '0'), 38, 10) is null and 
                    src:amld_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_dtwc), '0'), 38, 10) is null and 
                    src:amnt_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_lclc), '0'), 38, 10) is null and 
                    src:amnt_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rfrc), '0'), 38, 10) is null and 
                    src:amnt_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc1), '0'), 38, 10) is null and 
                    src:amnt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc2), '0'), 38, 10) is null and 
                    src:amnt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_trnc), '0'), 38, 10) is null and 
                    src:amnt_trnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod), '0'), 38, 10) is null and 
                    src:amod is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod_dtwc), '0'), 38, 10) is null and 
                    src:amod_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod_lclc), '0'), 38, 10) is null and 
                    src:amod_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod_rfrc), '0'), 38, 10) is null and 
                    src:amod_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod_rpc1), '0'), 38, 10) is null and 
                    src:amod_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod_rpc2), '0'), 38, 10) is null and 
                    src:amod_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is null and 
                    src:atse_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrb), '0'), 38, 10) is null and 
                    src:bgrb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrq), '0'), 38, 10) is null and 
                    src:bgrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bind), '0'), 38, 10) is null and 
                    src:bind is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bkyn), '0'), 38, 10) is null and 
                    src:bkyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bqco), '0'), 38, 10) is null and 
                    src:bqco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casi_ref_compnr), '0'), 38, 10) is null and 
                    src:casi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccof_ref_compnr), '0'), 38, 10) is null and 
                    src:ccof_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chan_ref_compnr), '0'), 38, 10) is null and 
                    src:chan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is null and 
                    src:citt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clgr_ref_compnr), '0'), 38, 10) is null and 
                    src:clgr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clyn), '0'), 38, 10) is null and 
                    src:clyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnig), '0'), 38, 10) is null and 
                    src:cnig is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocp_dtwc), '0'), 38, 10) is null and 
                    src:cocp_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocp_lclc), '0'), 38, 10) is null and 
                    src:cocp_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocp_rfrc), '0'), 38, 10) is null and 
                    src:cocp_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocp_rpc1), '0'), 38, 10) is null and 
                    src:cocp_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocp_rpc2), '0'), 38, 10) is null and 
                    src:cocp_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocp_trnc), '0'), 38, 10) is null and 
                    src:cocp_trnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_1), '0'), 38, 10) is null and 
                    src:copr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_2), '0'), 38, 10) is null and 
                    src:copr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_3), '0'), 38, 10) is null and 
                    src:copr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cphl), '0'), 38, 10) is null and 
                    src:cphl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpon), '0'), 38, 10) is null and 
                    src:cpon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpva), '0'), 38, 10) is null and 
                    src:cpva is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpva_rcmp), '0'), 38, 10) is null and 
                    src:cpva_rcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crbn), '0'), 38, 10) is null and 
                    src:crbn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crcd_ref_compnr), '0'), 38, 10) is null and 
                    src:crcd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctcd_ref_compnr), '0'), 38, 10) is null and 
                    src:ctcd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cubs_ref_compnr), '0'), 38, 10) is null and 
                    src:cubs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cucp_dtwc), '0'), 38, 10) is null and 
                    src:cucp_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cucp_lclc), '0'), 38, 10) is null and 
                    src:cucp_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cucp_rfrc), '0'), 38, 10) is null and 
                    src:cucp_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cucp_rpc1), '0'), 38, 10) is null and 
                    src:cucp_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cucp_rpc2), '0'), 38, 10) is null and 
                    src:cucp_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cucp_trnc), '0'), 38, 10) is null and 
                    src:cucp_trnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupq_ref_compnr), '0'), 38, 10) is null and 
                    src:cupq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cups_ref_compnr), '0'), 38, 10) is null and 
                    src:cups_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqs_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuva), '0'), 38, 10) is null and 
                    src:cuva is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvbs), '0'), 38, 10) is null and 
                    src:cvbs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvpb), '0'), 38, 10) is null and 
                    src:cvpb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvpq), '0'), 38, 10) is null and 
                    src:cvpq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvps), '0'), 38, 10) is null and 
                    src:cvps is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvqs), '0'), 38, 10) is null and 
                    src:cvqs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:damt), '0'), 38, 10) is null and 
                    src:damt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ddch), '0'), 38, 10) is null and 
                    src:ddch is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddta), '1900-01-01')) is null and 
                    src:ddta is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddtb), '1900-01-01')) is null and 
                    src:ddtb is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddtc), '1900-01-01')) is null and 
                    src:ddtc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddtd), '1900-01-01')) is null and 
                    src:ddtd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delc_ref_compnr), '0'), 38, 10) is null and 
                    src:delc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_1), '0'), 38, 10) is null and 
                    src:disc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_10), '0'), 38, 10) is null and 
                    src:disc_10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_11), '0'), 38, 10) is null and 
                    src:disc_11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_2), '0'), 38, 10) is null and 
                    src:disc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_3), '0'), 38, 10) is null and 
                    src:disc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_4), '0'), 38, 10) is null and 
                    src:disc_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_5), '0'), 38, 10) is null and 
                    src:disc_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_6), '0'), 38, 10) is null and 
                    src:disc_6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_7), '0'), 38, 10) is null and 
                    src:disc_7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_8), '0'), 38, 10) is null and 
                    src:disc_8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_9), '0'), 38, 10) is null and 
                    src:disc_9 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dldt), '1900-01-01')) is null and 
                    src:dldt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dlld), '1900-01-01')) is null and 
                    src:dlld is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dltp), '0'), 38, 10) is null and 
                    src:dltp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_1), '0'), 38, 10) is null and 
                    src:dmse_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_10), '0'), 38, 10) is null and 
                    src:dmse_10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_11), '0'), 38, 10) is null and 
                    src:dmse_11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_2), '0'), 38, 10) is null and 
                    src:dmse_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_3), '0'), 38, 10) is null and 
                    src:dmse_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_4), '0'), 38, 10) is null and 
                    src:dmse_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_5), '0'), 38, 10) is null and 
                    src:dmse_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_6), '0'), 38, 10) is null and 
                    src:dmse_6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_7), '0'), 38, 10) is null and 
                    src:dmse_7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_8), '0'), 38, 10) is null and 
                    src:dmse_8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_9), '0'), 38, 10) is null and 
                    src:dmse_9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_1), '0'), 38, 10) is null and 
                    src:dmth_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_10), '0'), 38, 10) is null and 
                    src:dmth_10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_11), '0'), 38, 10) is null and 
                    src:dmth_11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_2), '0'), 38, 10) is null and 
                    src:dmth_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_3), '0'), 38, 10) is null and 
                    src:dmth_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_4), '0'), 38, 10) is null and 
                    src:dmth_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_5), '0'), 38, 10) is null and 
                    src:dmth_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_6), '0'), 38, 10) is null and 
                    src:dmth_6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_7), '0'), 38, 10) is null and 
                    src:dmth_7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_8), '0'), 38, 10) is null and 
                    src:dmth_8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_9), '0'), 38, 10) is null and 
                    src:dmth_9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_1), '0'), 38, 10) is null and 
                    src:dmty_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_10), '0'), 38, 10) is null and 
                    src:dmty_10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_11), '0'), 38, 10) is null and 
                    src:dmty_11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_2), '0'), 38, 10) is null and 
                    src:dmty_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_3), '0'), 38, 10) is null and 
                    src:dmty_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_4), '0'), 38, 10) is null and 
                    src:dmty_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_5), '0'), 38, 10) is null and 
                    src:dmty_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_6), '0'), 38, 10) is null and 
                    src:dmty_6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_7), '0'), 38, 10) is null and 
                    src:dmty_7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_8), '0'), 38, 10) is null and 
                    src:dmty_8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_9), '0'), 38, 10) is null and 
                    src:dmty_9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_1), '0'), 38, 10) is null and 
                    src:dorg_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_10), '0'), 38, 10) is null and 
                    src:dorg_10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_11), '0'), 38, 10) is null and 
                    src:dorg_11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_2), '0'), 38, 10) is null and 
                    src:dorg_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_3), '0'), 38, 10) is null and 
                    src:dorg_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_4), '0'), 38, 10) is null and 
                    src:dorg_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_5), '0'), 38, 10) is null and 
                    src:dorg_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_6), '0'), 38, 10) is null and 
                    src:dorg_6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_7), '0'), 38, 10) is null and 
                    src:dorg_7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_8), '0'), 38, 10) is null and 
                    src:dorg_8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_9), '0'), 38, 10) is null and 
                    src:dorg_9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtrm), '0'), 38, 10) is null and 
                    src:dtrm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elgb), '0'), 38, 10) is null and 
                    src:elgb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcop_1), '0'), 38, 10) is null and 
                    src:fcop_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcop_2), '0'), 38, 10) is null and 
                    src:fcop_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcop_3), '0'), 38, 10) is null and 
                    src:fcop_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fram), '0'), 38, 10) is null and 
                    src:fram is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frbn), '0'), 38, 10) is null and 
                    src:frbn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frin), '0'), 38, 10) is null and 
                    src:frin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fxcp_1), '0'), 38, 10) is null and 
                    src:fxcp_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fxcp_2), '0'), 38, 10) is null and 
                    src:fxcp_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fxcp_3), '0'), 38, 10) is null and 
                    src:fxcp_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gefo), '0'), 38, 10) is null and 
                    src:gefo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:infr), '0'), 38, 10) is null and 
                    src:infr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:invd), '1900-01-01')) is null and 
                    src:invd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invn), '0'), 38, 10) is null and 
                    src:invn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:isss), '0'), 38, 10) is null and 
                    src:isss is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_atse_ref_compnr), '0'), 38, 10) is null and 
                    src:item_atse_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_atse_site_ref_compnr), '0'), 38, 10) is null and 
                    src:item_atse_site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_atse_stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:item_atse_stsi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_site_ref_compnr), '0'), 38, 10) is null and 
                    src:item_site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:item_stsi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcrq), '0'), 38, 10) is null and 
                    src:lcrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_1), '0'), 38, 10) is null and 
                    src:ldam_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_10), '0'), 38, 10) is null and 
                    src:ldam_10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_11), '0'), 38, 10) is null and 
                    src:ldam_11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_2), '0'), 38, 10) is null and 
                    src:ldam_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_3), '0'), 38, 10) is null and 
                    src:ldam_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_4), '0'), 38, 10) is null and 
                    src:ldam_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_5), '0'), 38, 10) is null and 
                    src:ldam_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_6), '0'), 38, 10) is null and 
                    src:ldam_6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_7), '0'), 38, 10) is null and 
                    src:ldam_7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_8), '0'), 38, 10) is null and 
                    src:ldam_8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_9), '0'), 38, 10) is null and 
                    src:ldam_9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leng), '0'), 38, 10) is null and 
                    src:leng is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmbi), '0'), 38, 10) is null and 
                    src:lmbi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lnst), '0'), 38, 10) is null and 
                    src:lnst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsel), '0'), 38, 10) is null and 
                    src:lsel is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lseq), '0'), 38, 10) is null and 
                    src:lseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:modi), '0'), 38, 10) is null and 
                    src:modi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:motv_ref_compnr), '0'), 38, 10) is null and 
                    src:motv_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mprm), '0'), 38, 10) is null and 
                    src:mprm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt), '0'), 38, 10) is null and 
                    src:oamt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:odat), '1900-01-01')) is null and 
                    src:odat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oltp), '0'), 38, 10) is null and 
                    src:oltp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opol), '0'), 38, 10) is null and 
                    src:opol is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opri), '0'), 38, 10) is null and 
                    src:opri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oprs), '0'), 38, 10) is null and 
                    src:oprs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_cosn_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_cosn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owns), '0'), 38, 10) is null and 
                    src:owns is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcad), '0'), 38, 10) is null and 
                    src:pcad is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcod_ref_compnr), '0'), 38, 10) is null and 
                    src:pcod_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmnt), '0'), 38, 10) is null and 
                    src:pmnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmse), '0'), 38, 10) is null and 
                    src:pmse is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:porg), '0'), 38, 10) is null and 
                    src:porg is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_dtwc), '0'), 38, 10) is null and 
                    src:pric_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_lclc), '0'), 38, 10) is null and 
                    src:pric_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_rfrc), '0'), 38, 10) is null and 
                    src:pric_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_rpc1), '0'), 38, 10) is null and 
                    src:pric_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_rpc2), '0'), 38, 10) is null and 
                    src:pric_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pror), '0'), 38, 10) is null and 
                    src:pror is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsg_ref_compnr), '0'), 38, 10) is null and 
                    src:prsg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbo), '0'), 38, 10) is null and 
                    src:qbbo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbo_buar), '0'), 38, 10) is null and 
                    src:qbbo_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbo_buln), '0'), 38, 10) is null and 
                    src:qbbo_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbo_bupc), '0'), 38, 10) is null and 
                    src:qbbo_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbo_butm), '0'), 38, 10) is null and 
                    src:qbbo_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbo_buvl), '0'), 38, 10) is null and 
                    src:qbbo_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbo_buwg), '0'), 38, 10) is null and 
                    src:qbbo_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbo_trnu), '0'), 38, 10) is null and 
                    src:qbbo_trnu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicm), '0'), 38, 10) is null and 
                    src:qicm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl), '0'), 38, 10) is null and 
                    src:qidl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl_buar), '0'), 38, 10) is null and 
                    src:qidl_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl_buln), '0'), 38, 10) is null and 
                    src:qidl_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl_bupc), '0'), 38, 10) is null and 
                    src:qidl_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl_butm), '0'), 38, 10) is null and 
                    src:qidl_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl_buvl), '0'), 38, 10) is null and 
                    src:qidl_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl_buwg), '0'), 38, 10) is null and 
                    src:qidl_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl_trnu), '0'), 38, 10) is null and 
                    src:qidl_trnu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qobh_invu), '0'), 38, 10) is null and 
                    src:qobh_invu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qohb), '0'), 38, 10) is null and 
                    src:qohb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qohb_buar), '0'), 38, 10) is null and 
                    src:qohb_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qohb_buln), '0'), 38, 10) is null and 
                    src:qohb_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qohb_bupc), '0'), 38, 10) is null and 
                    src:qohb_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qohb_butm), '0'), 38, 10) is null and 
                    src:qohb_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qohb_buvl), '0'), 38, 10) is null and 
                    src:qohb_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qohb_buwg), '0'), 38, 10) is null and 
                    src:qohb_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor), '0'), 38, 10) is null and 
                    src:qoor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_buar), '0'), 38, 10) is null and 
                    src:qoor_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_buln), '0'), 38, 10) is null and 
                    src:qoor_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_bupc), '0'), 38, 10) is null and 
                    src:qoor_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_butm), '0'), 38, 10) is null and 
                    src:qoor_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_buvl), '0'), 38, 10) is null and 
                    src:qoor_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_buwg), '0'), 38, 10) is null and 
                    src:qoor_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_invu), '0'), 38, 10) is null and 
                    src:qoor_invu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpbo), '0'), 38, 10) is null and 
                    src:qpbo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpdl), '0'), 38, 10) is null and 
                    src:qpdl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpor), '0'), 38, 10) is null and 
                    src:qpor is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ratd), '1900-01-01')) is null and 
                    src:ratd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_1), '0'), 38, 10) is null and 
                    src:rats_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_2), '0'), 38, 10) is null and 
                    src:rats_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_3), '0'), 38, 10) is null and 
                    src:rats_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdta), '1900-01-01')) is null and 
                    src:rdta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ruso), '0'), 38, 10) is null and 
                    src:ruso is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbim), '0'), 38, 10) is null and 
                    src:sbim is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scmp), '0'), 38, 10) is null and 
                    src:scmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scon), '0'), 38, 10) is null and 
                    src:scon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdsc), '0'), 38, 10) is null and 
                    src:sdsc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serv_ref_compnr), '0'), 38, 10) is null and 
                    src:serv_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shln), '0'), 38, 10) is null and 
                    src:shln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssel), '0'), 38, 10) is null and 
                    src:ssel is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_dlpt_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_dlpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is null and 
                    src:stcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stdc), '0'), 38, 10) is null and 
                    src:stdc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:stsi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stwh_ref_compnr), '0'), 38, 10) is null and 
                    src:stwh_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is null and 
                    src:styp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:susi), '0'), 38, 10) is null and 
                    src:susi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term), '0'), 38, 10) is null and 
                    src:term is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:thic), '0'), 38, 10) is null and 
                    src:thic is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprd), '0'), 38, 10) is null and 
                    src:tprd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprd_dtwc), '0'), 38, 10) is null and 
                    src:tprd_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprd_lclc), '0'), 38, 10) is null and 
                    src:tprd_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprd_rfrc), '0'), 38, 10) is null and 
                    src:tprd_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprd_rpc1), '0'), 38, 10) is null and 
                    src:tprd_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprd_rpc2), '0'), 38, 10) is null and 
                    src:tprd_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vcop_1), '0'), 38, 10) is null and 
                    src:vcop_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vcop_2), '0'), 38, 10) is null and 
                    src:vcop_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vcop_3), '0'), 38, 10) is null and 
                    src:vcop_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:widt), '0'), 38, 10) is null and 
                    src:widt is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)