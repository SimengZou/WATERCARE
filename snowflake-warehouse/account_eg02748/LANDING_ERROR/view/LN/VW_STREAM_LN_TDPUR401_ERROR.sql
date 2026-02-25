CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDPUR401_ERROR AS SELECT src, 'LN_TDPUR401' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:akcd_ref_compnr), '0'), 38, 10) is null and 
                    src:akcd_ref_compnr is not null) THEN
                    'akcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:akcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld), '0'), 38, 10) is null and 
                    src:amld is not null) THEN
                    'amld contains a non-numeric value : \'' || AS_VARCHAR(src:amld) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod), '0'), 38, 10) is null and 
                    src:amod is not null) THEN
                    'amod contains a non-numeric value : \'' || AS_VARCHAR(src:amod) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appr), '0'), 38, 10) is null and 
                    src:appr is not null) THEN
                    'appr contains a non-numeric value : \'' || AS_VARCHAR(src:appr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is null and 
                    src:atse_ref_compnr is not null) THEN
                    'atse_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:atse_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrb), '0'), 38, 10) is null and 
                    src:bgrb is not null) THEN
                    'bgrb contains a non-numeric value : \'' || AS_VARCHAR(src:bgrb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrq), '0'), 38, 10) is null and 
                    src:bgrq is not null) THEN
                    'bgrq contains a non-numeric value : \'' || AS_VARCHAR(src:bgrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgxc), '0'), 38, 10) is null and 
                    src:bgxc is not null) THEN
                    'bgxc contains a non-numeric value : \'' || AS_VARCHAR(src:bgxc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bkyn), '0'), 38, 10) is null and 
                    src:bkyn is not null) THEN
                    'bkyn contains a non-numeric value : \'' || AS_VARCHAR(src:bkyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) THEN
                    'bptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btsp), '0'), 38, 10) is null and 
                    src:btsp is not null) THEN
                    'btsp contains a non-numeric value : \'' || AS_VARCHAR(src:btsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btx1), '0'), 38, 10) is null and 
                    src:btx1 is not null) THEN
                    'btx1 contains a non-numeric value : \'' || AS_VARCHAR(src:btx1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btx1_ref_compnr), '0'), 38, 10) is null and 
                    src:btx1_ref_compnr is not null) THEN
                    'btx1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:btx1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btx2), '0'), 38, 10) is null and 
                    src:btx2 is not null) THEN
                    'btx2 contains a non-numeric value : \'' || AS_VARCHAR(src:btx2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btx2_ref_compnr), '0'), 38, 10) is null and 
                    src:btx2_ref_compnr is not null) THEN
                    'btx2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:btx2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) THEN
                    'cadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casi_ref_compnr), '0'), 38, 10) is null and 
                    src:casi_ref_compnr is not null) THEN
                    'casi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:casi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) THEN
                    'ccco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) THEN
                    'ccty_cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cdf_cono_ref_compnr is not null) THEN
                    'cdf_cono_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_cono_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_cpon), '0'), 38, 10) is null and 
                    src:cdf_cpon is not null) THEN
                    'cdf_cpon contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_cpon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) THEN
                    'cfrw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfrw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is null and 
                    src:citt_ref_compnr is not null) THEN
                    'citt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:citt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clip), '0'), 38, 10) is null and 
                    src:clip is not null) THEN
                    'clip contains a non-numeric value : \'' || AS_VARCHAR(src:clip) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clyn), '0'), 38, 10) is null and 
                    src:clyn is not null) THEN
                    'clyn contains a non-numeric value : \'' || AS_VARCHAR(src:clyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) THEN
                    'cmnf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmnf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnig), '0'), 38, 10) is null and 
                    src:cnig is not null) THEN
                    'cnig contains a non-numeric value : \'' || AS_VARCHAR(src:cnig) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comm), '0'), 38, 10) is null and 
                    src:comm is not null) THEN
                    'comm contains a non-numeric value : \'' || AS_VARCHAR(src:comm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coop_1), '0'), 38, 10) is null and 
                    src:coop_1 is not null) THEN
                    'coop_1 contains a non-numeric value : \'' || AS_VARCHAR(src:coop_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coop_2), '0'), 38, 10) is null and 
                    src:coop_2 is not null) THEN
                    'coop_2 contains a non-numeric value : \'' || AS_VARCHAR(src:coop_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coop_3), '0'), 38, 10) is null and 
                    src:coop_3 is not null) THEN
                    'coop_3 contains a non-numeric value : \'' || AS_VARCHAR(src:coop_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coop_trnc), '0'), 38, 10) is null and 
                    src:coop_trnc is not null) THEN
                    'coop_trnc contains a non-numeric value : \'' || AS_VARCHAR(src:coop_trnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_1), '0'), 38, 10) is null and 
                    src:copr_1 is not null) THEN
                    'copr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:copr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_2), '0'), 38, 10) is null and 
                    src:copr_2 is not null) THEN
                    'copr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:copr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_3), '0'), 38, 10) is null and 
                    src:copr_3 is not null) THEN
                    'copr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:copr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_trnc), '0'), 38, 10) is null and 
                    src:copr_trnc is not null) THEN
                    'copr_trnc contains a non-numeric value : \'' || AS_VARCHAR(src:copr_trnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corg), '0'), 38, 10) is null and 
                    src:corg is not null) THEN
                    'corg contains a non-numeric value : \'' || AS_VARCHAR(src:corg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcp_ref_compnr is not null) THEN
                    'cpcp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpcp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpon), '0'), 38, 10) is null and 
                    src:cpon is not null) THEN
                    'cpon contains a non-numeric value : \'' || AS_VARCHAR(src:cpon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpva), '0'), 38, 10) is null and 
                    src:cpva is not null) THEN
                    'cpva contains a non-numeric value : \'' || AS_VARCHAR(src:cpva) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crbn), '0'), 38, 10) is null and 
                    src:crbn is not null) THEN
                    'crbn contains a non-numeric value : \'' || AS_VARCHAR(src:crbn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crcd_ref_compnr), '0'), 38, 10) is null and 
                    src:crcd_ref_compnr is not null) THEN
                    'crcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crej_ref_compnr), '0'), 38, 10) is null and 
                    src:crej_ref_compnr is not null) THEN
                    'crej_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crej_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crrf), '0'), 38, 10) is null and 
                    src:crrf is not null) THEN
                    'crrf contains a non-numeric value : \'' || AS_VARCHAR(src:crrf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csqn), '0'), 38, 10) is null and 
                    src:csqn is not null) THEN
                    'csqn contains a non-numeric value : \'' || AS_VARCHAR(src:csqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctcd_ref_compnr), '0'), 38, 10) is null and 
                    src:ctcd_ref_compnr is not null) THEN
                    'ctcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctrj_ref_compnr), '0'), 38, 10) is null and 
                    src:ctrj_ref_compnr is not null) THEN
                    'ctrj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctrj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cubp_ref_compnr), '0'), 38, 10) is null and 
                    src:cubp_ref_compnr is not null) THEN
                    'cubp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cubp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupb_ref_compnr), '0'), 38, 10) is null and 
                    src:cupb_ref_compnr is not null) THEN
                    'cupb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cupb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupp_ref_compnr), '0'), 38, 10) is null and 
                    src:cupp_ref_compnr is not null) THEN
                    'cupp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cupp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupq_ref_compnr), '0'), 38, 10) is null and 
                    src:cupq_ref_compnr is not null) THEN
                    'cupq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cupq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqp_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqp_ref_compnr is not null) THEN
                    'cuqp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuqp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuva), '0'), 38, 10) is null and 
                    src:cuva is not null) THEN
                    'cuva contains a non-numeric value : \'' || AS_VARCHAR(src:cuva) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvbp), '0'), 38, 10) is null and 
                    src:cvbp is not null) THEN
                    'cvbp contains a non-numeric value : \'' || AS_VARCHAR(src:cvbp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvpb), '0'), 38, 10) is null and 
                    src:cvpb is not null) THEN
                    'cvpb contains a non-numeric value : \'' || AS_VARCHAR(src:cvpb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvpp), '0'), 38, 10) is null and 
                    src:cvpp is not null) THEN
                    'cvpp contains a non-numeric value : \'' || AS_VARCHAR(src:cvpp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvpq), '0'), 38, 10) is null and 
                    src:cvpq is not null) THEN
                    'cvpq contains a non-numeric value : \'' || AS_VARCHAR(src:cvpq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvqp), '0'), 38, 10) is null and 
                    src:cvqp is not null) THEN
                    'cvqp contains a non-numeric value : \'' || AS_VARCHAR(src:cvqp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:damt), '0'), 38, 10) is null and 
                    src:damt is not null) THEN
                    'damt contains a non-numeric value : \'' || AS_VARCHAR(src:damt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ddon), '0'), 38, 10) is null and 
                    src:ddon is not null) THEN
                    'ddon contains a non-numeric value : \'' || AS_VARCHAR(src:ddon) || '\'' WHEN 
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddte), '1900-01-01')) is null and 
                    src:ddte is not null) THEN
                    'ddte contains a non-timestamp value : \'' || AS_VARCHAR(src:ddte) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddtf), '1900-01-01')) is null and 
                    src:ddtf is not null) THEN
                    'ddtf contains a non-timestamp value : \'' || AS_VARCHAR(src:ddtf) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etpc), '0'), 38, 10) is null and 
                    src:etpc is not null) THEN
                    'etpc contains a non-numeric value : \'' || AS_VARCHAR(src:etpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) THEN
                    'exmt contains a non-numeric value : \'' || AS_VARCHAR(src:exmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fire), '0'), 38, 10) is null and 
                    src:fire is not null) THEN
                    'fire contains a non-numeric value : \'' || AS_VARCHAR(src:fire) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gefo), '0'), 38, 10) is null and 
                    src:gefo is not null) THEN
                    'gefo contains a non-numeric value : \'' || AS_VARCHAR(src:gefo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iamt), '0'), 38, 10) is null and 
                    src:iamt is not null) THEN
                    'iamt contains a non-numeric value : \'' || AS_VARCHAR(src:iamt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:invd), '1900-01-01')) is null and 
                    src:invd is not null) THEN
                    'invd contains a non-timestamp value : \'' || AS_VARCHAR(src:invd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issp), '0'), 38, 10) is null and 
                    src:issp is not null) THEN
                    'issp contains a non-numeric value : \'' || AS_VARCHAR(src:issp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcam_trnc), '0'), 38, 10) is null and 
                    src:lcam_trnc is not null) THEN
                    'lcam_trnc contains a non-numeric value : \'' || AS_VARCHAR(src:lcam_trnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lccl_ref_compnr), '0'), 38, 10) is null and 
                    src:lccl_ref_compnr is not null) THEN
                    'lccl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lccl_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ldat), '1900-01-01')) is null and 
                    src:ldat is not null) THEN
                    'ldat contains a non-timestamp value : \'' || AS_VARCHAR(src:ldat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leng), '0'), 38, 10) is null and 
                    src:leng is not null) THEN
                    'leng contains a non-numeric value : \'' || AS_VARCHAR(src:leng) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lnst), '0'), 38, 10) is null and 
                    src:lnst is not null) THEN
                    'lnst contains a non-numeric value : \'' || AS_VARCHAR(src:lnst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsel), '0'), 38, 10) is null and 
                    src:lsel is not null) THEN
                    'lsel contains a non-numeric value : \'' || AS_VARCHAR(src:lsel) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lseq), '0'), 38, 10) is null and 
                    src:lseq is not null) THEN
                    'lseq contains a non-numeric value : \'' || AS_VARCHAR(src:lseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpnr_cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:mpnr_cmnf_ref_compnr is not null) THEN
                    'mpnr_cmnf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mpnr_cmnf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpsn_ref_compnr), '0'), 38, 10) is null and 
                    src:mpsn_ref_compnr is not null) THEN
                    'mpsn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mpsn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt), '0'), 38, 10) is null and 
                    src:oamt is not null) THEN
                    'oamt contains a non-numeric value : \'' || AS_VARCHAR(src:oamt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:odat), '1900-01-01')) is null and 
                    src:odat is not null) THEN
                    'odat contains a non-timestamp value : \'' || AS_VARCHAR(src:odat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oltp), '0'), 38, 10) is null and 
                    src:oltp is not null) THEN
                    'oltp contains a non-numeric value : \'' || AS_VARCHAR(src:oltp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_cosn_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_cosn_ref_compnr is not null) THEN
                    'orno_cosn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:orno_cosn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_ref_compnr is not null) THEN
                    'orno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:orno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ownr_ref_compnr), '0'), 38, 10) is null and 
                    src:ownr_ref_compnr is not null) THEN
                    'ownr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ownr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owns), '0'), 38, 10) is null and 
                    src:owns is not null) THEN
                    'owns contains a non-numeric value : \'' || AS_VARCHAR(src:owns) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paft), '0'), 38, 10) is null and 
                    src:paft is not null) THEN
                    'paft contains a non-numeric value : \'' || AS_VARCHAR(src:paft) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paya_ref_compnr), '0'), 38, 10) is null and 
                    src:paya_ref_compnr is not null) THEN
                    'paya_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:paya_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pegd), '0'), 38, 10) is null and 
                    src:pegd is not null) THEN
                    'pegd contains a non-numeric value : \'' || AS_VARCHAR(src:pegd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmnd), '0'), 38, 10) is null and 
                    src:pmnd is not null) THEN
                    'pmnd contains a non-numeric value : \'' || AS_VARCHAR(src:pmnd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmni), '0'), 38, 10) is null and 
                    src:pmni is not null) THEN
                    'pmni contains a non-numeric value : \'' || AS_VARCHAR(src:pmni) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) THEN
                    'pric contains a non-numeric value : \'' || AS_VARCHAR(src:pric) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsg_ref_compnr), '0'), 38, 10) is null and 
                    src:prsg_ref_compnr is not null) THEN
                    'prsg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prsg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pseq), '0'), 38, 10) is null and 
                    src:pseq is not null) THEN
                    'pseq contains a non-numeric value : \'' || AS_VARCHAR(src:pseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpe_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpe_ref_compnr is not null) THEN
                    'ptpe_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpe_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc), '0'), 38, 10) is null and 
                    src:qbbc is not null) THEN
                    'qbbc contains a non-numeric value : \'' || AS_VARCHAR(src:qbbc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc_buar), '0'), 38, 10) is null and 
                    src:qbbc_buar is not null) THEN
                    'qbbc_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qbbc_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc_buln), '0'), 38, 10) is null and 
                    src:qbbc_buln is not null) THEN
                    'qbbc_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qbbc_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc_bupc), '0'), 38, 10) is null and 
                    src:qbbc_bupc is not null) THEN
                    'qbbc_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qbbc_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc_butm), '0'), 38, 10) is null and 
                    src:qbbc_butm is not null) THEN
                    'qbbc_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qbbc_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc_buvl), '0'), 38, 10) is null and 
                    src:qbbc_buvl is not null) THEN
                    'qbbc_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qbbc_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc_buwg), '0'), 38, 10) is null and 
                    src:qbbc_buwg is not null) THEN
                    'qbbc_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qbbc_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc_invu), '0'), 38, 10) is null and 
                    src:qbbc_invu is not null) THEN
                    'qbbc_invu contains a non-numeric value : \'' || AS_VARCHAR(src:qbbc_invu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc_trnu), '0'), 38, 10) is null and 
                    src:qbbc_trnu is not null) THEN
                    'qbbc_trnu contains a non-numeric value : \'' || AS_VARCHAR(src:qbbc_trnu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap), '0'), 38, 10) is null and 
                    src:qiap is not null) THEN
                    'qiap contains a non-numeric value : \'' || AS_VARCHAR(src:qiap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap_buar), '0'), 38, 10) is null and 
                    src:qiap_buar is not null) THEN
                    'qiap_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qiap_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap_buln), '0'), 38, 10) is null and 
                    src:qiap_buln is not null) THEN
                    'qiap_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qiap_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap_bupc), '0'), 38, 10) is null and 
                    src:qiap_bupc is not null) THEN
                    'qiap_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qiap_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap_butm), '0'), 38, 10) is null and 
                    src:qiap_butm is not null) THEN
                    'qiap_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qiap_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap_buvl), '0'), 38, 10) is null and 
                    src:qiap_buvl is not null) THEN
                    'qiap_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qiap_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap_buwg), '0'), 38, 10) is null and 
                    src:qiap_buwg is not null) THEN
                    'qiap_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qiap_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap_trnu), '0'), 38, 10) is null and 
                    src:qiap_trnu is not null) THEN
                    'qiap_trnu contains a non-numeric value : \'' || AS_VARCHAR(src:qiap_trnu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo), '0'), 38, 10) is null and 
                    src:qibo is not null) THEN
                    'qibo contains a non-numeric value : \'' || AS_VARCHAR(src:qibo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo_buar), '0'), 38, 10) is null and 
                    src:qibo_buar is not null) THEN
                    'qibo_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qibo_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo_buln), '0'), 38, 10) is null and 
                    src:qibo_buln is not null) THEN
                    'qibo_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qibo_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo_bupc), '0'), 38, 10) is null and 
                    src:qibo_bupc is not null) THEN
                    'qibo_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qibo_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo_butm), '0'), 38, 10) is null and 
                    src:qibo_butm is not null) THEN
                    'qibo_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qibo_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo_buvl), '0'), 38, 10) is null and 
                    src:qibo_buvl is not null) THEN
                    'qibo_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qibo_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo_buwg), '0'), 38, 10) is null and 
                    src:qibo_buwg is not null) THEN
                    'qibo_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qibo_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo_invu), '0'), 38, 10) is null and 
                    src:qibo_invu is not null) THEN
                    'qibo_invu contains a non-numeric value : \'' || AS_VARCHAR(src:qibo_invu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo_trnu), '0'), 38, 10) is null and 
                    src:qibo_trnu is not null) THEN
                    'qibo_trnu contains a non-numeric value : \'' || AS_VARCHAR(src:qibo_trnu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibp), '0'), 38, 10) is null and 
                    src:qibp is not null) THEN
                    'qibp contains a non-numeric value : \'' || AS_VARCHAR(src:qibp) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv), '0'), 38, 10) is null and 
                    src:qiiv is not null) THEN
                    'qiiv contains a non-numeric value : \'' || AS_VARCHAR(src:qiiv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv_buar), '0'), 38, 10) is null and 
                    src:qiiv_buar is not null) THEN
                    'qiiv_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qiiv_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv_buln), '0'), 38, 10) is null and 
                    src:qiiv_buln is not null) THEN
                    'qiiv_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qiiv_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv_bupc), '0'), 38, 10) is null and 
                    src:qiiv_bupc is not null) THEN
                    'qiiv_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qiiv_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv_butm), '0'), 38, 10) is null and 
                    src:qiiv_butm is not null) THEN
                    'qiiv_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qiiv_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv_buvl), '0'), 38, 10) is null and 
                    src:qiiv_buvl is not null) THEN
                    'qiiv_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qiiv_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv_buwg), '0'), 38, 10) is null and 
                    src:qiiv_buwg is not null) THEN
                    'qiiv_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qiiv_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv_trnu), '0'), 38, 10) is null and 
                    src:qiiv_trnu is not null) THEN
                    'qiiv_trnu contains a non-numeric value : \'' || AS_VARCHAR(src:qiiv_trnu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qips), '0'), 38, 10) is null and 
                    src:qips is not null) THEN
                    'qips contains a non-numeric value : \'' || AS_VARCHAR(src:qips) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj), '0'), 38, 10) is null and 
                    src:qirj is not null) THEN
                    'qirj contains a non-numeric value : \'' || AS_VARCHAR(src:qirj) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj_buar), '0'), 38, 10) is null and 
                    src:qirj_buar is not null) THEN
                    'qirj_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qirj_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj_buln), '0'), 38, 10) is null and 
                    src:qirj_buln is not null) THEN
                    'qirj_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qirj_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj_bupc), '0'), 38, 10) is null and 
                    src:qirj_bupc is not null) THEN
                    'qirj_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qirj_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj_butm), '0'), 38, 10) is null and 
                    src:qirj_butm is not null) THEN
                    'qirj_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qirj_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj_buvl), '0'), 38, 10) is null and 
                    src:qirj_buvl is not null) THEN
                    'qirj_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qirj_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj_buwg), '0'), 38, 10) is null and 
                    src:qirj_buwg is not null) THEN
                    'qirj_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qirj_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj_trnu), '0'), 38, 10) is null and 
                    src:qirj_trnu is not null) THEN
                    'qirj_trnu contains a non-numeric value : \'' || AS_VARCHAR(src:qirj_trnu) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpap), '0'), 38, 10) is null and 
                    src:qpap is not null) THEN
                    'qpap contains a non-numeric value : \'' || AS_VARCHAR(src:qpap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpbc), '0'), 38, 10) is null and 
                    src:qpbc is not null) THEN
                    'qpbc contains a non-numeric value : \'' || AS_VARCHAR(src:qpbc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpbo), '0'), 38, 10) is null and 
                    src:qpbo is not null) THEN
                    'qpbo contains a non-numeric value : \'' || AS_VARCHAR(src:qpbo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpbp), '0'), 38, 10) is null and 
                    src:qpbp is not null) THEN
                    'qpbp contains a non-numeric value : \'' || AS_VARCHAR(src:qpbp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpdl), '0'), 38, 10) is null and 
                    src:qpdl is not null) THEN
                    'qpdl contains a non-numeric value : \'' || AS_VARCHAR(src:qpdl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpiv), '0'), 38, 10) is null and 
                    src:qpiv is not null) THEN
                    'qpiv contains a non-numeric value : \'' || AS_VARCHAR(src:qpiv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpor), '0'), 38, 10) is null and 
                    src:qpor is not null) THEN
                    'qpor contains a non-numeric value : \'' || AS_VARCHAR(src:qpor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpps), '0'), 38, 10) is null and 
                    src:qpps is not null) THEN
                    'qpps contains a non-numeric value : \'' || AS_VARCHAR(src:qpps) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qprj), '0'), 38, 10) is null and 
                    src:qprj is not null) THEN
                    'qprj contains a non-numeric value : \'' || AS_VARCHAR(src:qprj) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qual), '0'), 38, 10) is null and 
                    src:qual is not null) THEN
                    'qual contains a non-numeric value : \'' || AS_VARCHAR(src:qual) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) THEN
                    'rcod_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcod_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdta), '1900-01-01')) is null and 
                    src:rdta is not null) THEN
                    'rdta contains a non-timestamp value : \'' || AS_VARCHAR(src:rdta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) THEN
                    'rseq contains a non-numeric value : \'' || AS_VARCHAR(src:rseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsta), '0'), 38, 10) is null and 
                    src:rsta is not null) THEN
                    'rsta contains a non-numeric value : \'' || AS_VARCHAR(src:rsta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbim), '0'), 38, 10) is null and 
                    src:sbim is not null) THEN
                    'sbim contains a non-numeric value : \'' || AS_VARCHAR(src:sbim) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbmt_ref_compnr), '0'), 38, 10) is null and 
                    src:sbmt_ref_compnr is not null) THEN
                    'sbmt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sbmt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sclb), '0'), 38, 10) is null and 
                    src:sclb is not null) THEN
                    'sclb contains a non-numeric value : \'' || AS_VARCHAR(src:sclb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdsc), '0'), 38, 10) is null and 
                    src:sdsc is not null) THEN
                    'sdsc contains a non-numeric value : \'' || AS_VARCHAR(src:sdsc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serv_ref_compnr), '0'), 38, 10) is null and 
                    src:serv_ref_compnr is not null) THEN
                    'serv_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:serv_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfad_ref_compnr), '0'), 38, 10) is null and 
                    src:sfad_ref_compnr is not null) THEN
                    'sfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) THEN
                    'sfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:sfcn_ref_compnr is not null) THEN
                    'sfcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfsi_ref_compnr), '0'), 38, 10) is null and 
                    src:sfsi_ref_compnr is not null) THEN
                    'sfsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spss_ref_compnr), '0'), 38, 10) is null and 
                    src:spss_ref_compnr is not null) THEN
                    'spss_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:spss_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) THEN
                    'sqnb contains a non-numeric value : \'' || AS_VARCHAR(src:sqnb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stdc), '0'), 38, 10) is null and 
                    src:stdc is not null) THEN
                    'stdc contains a non-numeric value : \'' || AS_VARCHAR(src:stdc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsc), '0'), 38, 10) is null and 
                    src:stsc is not null) THEN
                    'stsc contains a non-numeric value : \'' || AS_VARCHAR(src:stsc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsd), '0'), 38, 10) is null and 
                    src:stsd is not null) THEN
                    'stsd contains a non-numeric value : \'' || AS_VARCHAR(src:stsd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) THEN
                    'subc contains a non-numeric value : \'' || AS_VARCHAR(src:subc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subs_ref_compnr), '0'), 38, 10) is null and 
                    src:subs_ref_compnr is not null) THEN
                    'subs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:subs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tapr), '0'), 38, 10) is null and 
                    src:tapr is not null) THEN
                    'tapr contains a non-numeric value : \'' || AS_VARCHAR(src:tapr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:taxp), '0'), 38, 10) is null and 
                    src:taxp is not null) THEN
                    'taxp contains a non-numeric value : \'' || AS_VARCHAR(src:taxp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:thic), '0'), 38, 10) is null and 
                    src:thic is not null) THEN
                    'thic contains a non-numeric value : \'' || AS_VARCHAR(src:thic) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpbk_ref_compnr), '0'), 38, 10) is null and 
                    src:tpbk_ref_compnr is not null) THEN
                    'tpbk_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tpbk_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpbk_tpbl_ref_compnr), '0'), 38, 10) is null and 
                    src:tpbk_tpbl_ref_compnr is not null) THEN
                    'tpbk_tpbl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tpbk_tpbl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpbl), '0'), 38, 10) is null and 
                    src:tpbl is not null) THEN
                    'tpbl contains a non-numeric value : \'' || AS_VARCHAR(src:tpbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vryn), '0'), 38, 10) is null and 
                    src:vryn is not null) THEN
                    'vryn contains a non-numeric value : \'' || AS_VARCHAR(src:vryn) || '\'' WHEN 
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
                                    
                src:pono,
                src:compnr,
                src:orno,
                src:sqnb  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR401 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:akcd_ref_compnr), '0'), 38, 10) is null and 
                    src:akcd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld), '0'), 38, 10) is null and 
                    src:amld is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod), '0'), 38, 10) is null and 
                    src:amod is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appr), '0'), 38, 10) is null and 
                    src:appr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is null and 
                    src:atse_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrb), '0'), 38, 10) is null and 
                    src:bgrb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrq), '0'), 38, 10) is null and 
                    src:bgrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgxc), '0'), 38, 10) is null and 
                    src:bgxc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bkyn), '0'), 38, 10) is null and 
                    src:bkyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btsp), '0'), 38, 10) is null and 
                    src:btsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btx1), '0'), 38, 10) is null and 
                    src:btx1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btx1_ref_compnr), '0'), 38, 10) is null and 
                    src:btx1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btx2), '0'), 38, 10) is null and 
                    src:btx2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btx2_ref_compnr), '0'), 38, 10) is null and 
                    src:btx2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casi_ref_compnr), '0'), 38, 10) is null and 
                    src:casi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cdf_cono_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_cpon), '0'), 38, 10) is null and 
                    src:cdf_cpon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is null and 
                    src:citt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clip), '0'), 38, 10) is null and 
                    src:clip is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clyn), '0'), 38, 10) is null and 
                    src:clyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnig), '0'), 38, 10) is null and 
                    src:cnig is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comm), '0'), 38, 10) is null and 
                    src:comm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coop_1), '0'), 38, 10) is null and 
                    src:coop_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coop_2), '0'), 38, 10) is null and 
                    src:coop_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coop_3), '0'), 38, 10) is null and 
                    src:coop_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coop_trnc), '0'), 38, 10) is null and 
                    src:coop_trnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_1), '0'), 38, 10) is null and 
                    src:copr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_2), '0'), 38, 10) is null and 
                    src:copr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_3), '0'), 38, 10) is null and 
                    src:copr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_trnc), '0'), 38, 10) is null and 
                    src:copr_trnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corg), '0'), 38, 10) is null and 
                    src:corg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpon), '0'), 38, 10) is null and 
                    src:cpon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpva), '0'), 38, 10) is null and 
                    src:cpva is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crbn), '0'), 38, 10) is null and 
                    src:crbn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crcd_ref_compnr), '0'), 38, 10) is null and 
                    src:crcd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crej_ref_compnr), '0'), 38, 10) is null and 
                    src:crej_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crrf), '0'), 38, 10) is null and 
                    src:crrf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csqn), '0'), 38, 10) is null and 
                    src:csqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctcd_ref_compnr), '0'), 38, 10) is null and 
                    src:ctcd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctrj_ref_compnr), '0'), 38, 10) is null and 
                    src:ctrj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cubp_ref_compnr), '0'), 38, 10) is null and 
                    src:cubp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupb_ref_compnr), '0'), 38, 10) is null and 
                    src:cupb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupp_ref_compnr), '0'), 38, 10) is null and 
                    src:cupp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupq_ref_compnr), '0'), 38, 10) is null and 
                    src:cupq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqp_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuva), '0'), 38, 10) is null and 
                    src:cuva is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvbp), '0'), 38, 10) is null and 
                    src:cvbp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvpb), '0'), 38, 10) is null and 
                    src:cvpb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvpp), '0'), 38, 10) is null and 
                    src:cvpp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvpq), '0'), 38, 10) is null and 
                    src:cvpq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvqp), '0'), 38, 10) is null and 
                    src:cvqp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:damt), '0'), 38, 10) is null and 
                    src:damt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ddon), '0'), 38, 10) is null and 
                    src:ddon is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddta), '1900-01-01')) is null and 
                    src:ddta is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddtb), '1900-01-01')) is null and 
                    src:ddtb is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddtc), '1900-01-01')) is null and 
                    src:ddtc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddtd), '1900-01-01')) is null and 
                    src:ddtd is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddte), '1900-01-01')) is null and 
                    src:ddte is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddtf), '1900-01-01')) is null and 
                    src:ddtf is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etpc), '0'), 38, 10) is null and 
                    src:etpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fire), '0'), 38, 10) is null and 
                    src:fire is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gefo), '0'), 38, 10) is null and 
                    src:gefo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iamt), '0'), 38, 10) is null and 
                    src:iamt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:invd), '1900-01-01')) is null and 
                    src:invd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issp), '0'), 38, 10) is null and 
                    src:issp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcam_trnc), '0'), 38, 10) is null and 
                    src:lcam_trnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lccl_ref_compnr), '0'), 38, 10) is null and 
                    src:lccl_ref_compnr is not null) or 
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ldat), '1900-01-01')) is null and 
                    src:ldat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leng), '0'), 38, 10) is null and 
                    src:leng is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lnst), '0'), 38, 10) is null and 
                    src:lnst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsel), '0'), 38, 10) is null and 
                    src:lsel is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lseq), '0'), 38, 10) is null and 
                    src:lseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpnr_cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:mpnr_cmnf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpsn_ref_compnr), '0'), 38, 10) is null and 
                    src:mpsn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt), '0'), 38, 10) is null and 
                    src:oamt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:odat), '1900-01-01')) is null and 
                    src:odat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oltp), '0'), 38, 10) is null and 
                    src:oltp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_cosn_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_cosn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ownr_ref_compnr), '0'), 38, 10) is null and 
                    src:ownr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owns), '0'), 38, 10) is null and 
                    src:owns is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paft), '0'), 38, 10) is null and 
                    src:paft is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paya_ref_compnr), '0'), 38, 10) is null and 
                    src:paya_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pegd), '0'), 38, 10) is null and 
                    src:pegd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmnd), '0'), 38, 10) is null and 
                    src:pmnd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmni), '0'), 38, 10) is null and 
                    src:pmni is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmnt), '0'), 38, 10) is null and 
                    src:pmnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmse), '0'), 38, 10) is null and 
                    src:pmse is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:porg), '0'), 38, 10) is null and 
                    src:porg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsg_ref_compnr), '0'), 38, 10) is null and 
                    src:prsg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pseq), '0'), 38, 10) is null and 
                    src:pseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpe_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpe_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc), '0'), 38, 10) is null and 
                    src:qbbc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc_buar), '0'), 38, 10) is null and 
                    src:qbbc_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc_buln), '0'), 38, 10) is null and 
                    src:qbbc_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc_bupc), '0'), 38, 10) is null and 
                    src:qbbc_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc_butm), '0'), 38, 10) is null and 
                    src:qbbc_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc_buvl), '0'), 38, 10) is null and 
                    src:qbbc_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc_buwg), '0'), 38, 10) is null and 
                    src:qbbc_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc_invu), '0'), 38, 10) is null and 
                    src:qbbc_invu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbbc_trnu), '0'), 38, 10) is null and 
                    src:qbbc_trnu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap), '0'), 38, 10) is null and 
                    src:qiap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap_buar), '0'), 38, 10) is null and 
                    src:qiap_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap_buln), '0'), 38, 10) is null and 
                    src:qiap_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap_bupc), '0'), 38, 10) is null and 
                    src:qiap_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap_butm), '0'), 38, 10) is null and 
                    src:qiap_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap_buvl), '0'), 38, 10) is null and 
                    src:qiap_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap_buwg), '0'), 38, 10) is null and 
                    src:qiap_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap_trnu), '0'), 38, 10) is null and 
                    src:qiap_trnu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo), '0'), 38, 10) is null and 
                    src:qibo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo_buar), '0'), 38, 10) is null and 
                    src:qibo_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo_buln), '0'), 38, 10) is null and 
                    src:qibo_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo_bupc), '0'), 38, 10) is null and 
                    src:qibo_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo_butm), '0'), 38, 10) is null and 
                    src:qibo_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo_buvl), '0'), 38, 10) is null and 
                    src:qibo_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo_buwg), '0'), 38, 10) is null and 
                    src:qibo_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo_invu), '0'), 38, 10) is null and 
                    src:qibo_invu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibo_trnu), '0'), 38, 10) is null and 
                    src:qibo_trnu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qibp), '0'), 38, 10) is null and 
                    src:qibp is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv), '0'), 38, 10) is null and 
                    src:qiiv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv_buar), '0'), 38, 10) is null and 
                    src:qiiv_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv_buln), '0'), 38, 10) is null and 
                    src:qiiv_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv_bupc), '0'), 38, 10) is null and 
                    src:qiiv_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv_butm), '0'), 38, 10) is null and 
                    src:qiiv_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv_buvl), '0'), 38, 10) is null and 
                    src:qiiv_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv_buwg), '0'), 38, 10) is null and 
                    src:qiiv_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv_trnu), '0'), 38, 10) is null and 
                    src:qiiv_trnu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qips), '0'), 38, 10) is null and 
                    src:qips is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj), '0'), 38, 10) is null and 
                    src:qirj is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj_buar), '0'), 38, 10) is null and 
                    src:qirj_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj_buln), '0'), 38, 10) is null and 
                    src:qirj_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj_bupc), '0'), 38, 10) is null and 
                    src:qirj_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj_butm), '0'), 38, 10) is null and 
                    src:qirj_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj_buvl), '0'), 38, 10) is null and 
                    src:qirj_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj_buwg), '0'), 38, 10) is null and 
                    src:qirj_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj_trnu), '0'), 38, 10) is null and 
                    src:qirj_trnu is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpap), '0'), 38, 10) is null and 
                    src:qpap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpbc), '0'), 38, 10) is null and 
                    src:qpbc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpbo), '0'), 38, 10) is null and 
                    src:qpbo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpbp), '0'), 38, 10) is null and 
                    src:qpbp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpdl), '0'), 38, 10) is null and 
                    src:qpdl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpiv), '0'), 38, 10) is null and 
                    src:qpiv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpor), '0'), 38, 10) is null and 
                    src:qpor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpps), '0'), 38, 10) is null and 
                    src:qpps is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qprj), '0'), 38, 10) is null and 
                    src:qprj is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qual), '0'), 38, 10) is null and 
                    src:qual is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdta), '1900-01-01')) is null and 
                    src:rdta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsta), '0'), 38, 10) is null and 
                    src:rsta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbim), '0'), 38, 10) is null and 
                    src:sbim is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbmt_ref_compnr), '0'), 38, 10) is null and 
                    src:sbmt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sclb), '0'), 38, 10) is null and 
                    src:sclb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdsc), '0'), 38, 10) is null and 
                    src:sdsc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serv_ref_compnr), '0'), 38, 10) is null and 
                    src:serv_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfad_ref_compnr), '0'), 38, 10) is null and 
                    src:sfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:sfcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfsi_ref_compnr), '0'), 38, 10) is null and 
                    src:sfsi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spss_ref_compnr), '0'), 38, 10) is null and 
                    src:spss_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stdc), '0'), 38, 10) is null and 
                    src:stdc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsc), '0'), 38, 10) is null and 
                    src:stsc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsd), '0'), 38, 10) is null and 
                    src:stsd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subs_ref_compnr), '0'), 38, 10) is null and 
                    src:subs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tapr), '0'), 38, 10) is null and 
                    src:tapr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:taxp), '0'), 38, 10) is null and 
                    src:taxp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:thic), '0'), 38, 10) is null and 
                    src:thic is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpbk_ref_compnr), '0'), 38, 10) is null and 
                    src:tpbk_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpbk_tpbl_ref_compnr), '0'), 38, 10) is null and 
                    src:tpbk_tpbl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpbl), '0'), 38, 10) is null and 
                    src:tpbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vryn), '0'), 38, 10) is null and 
                    src:vryn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:widt), '0'), 38, 10) is null and 
                    src:widt is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)