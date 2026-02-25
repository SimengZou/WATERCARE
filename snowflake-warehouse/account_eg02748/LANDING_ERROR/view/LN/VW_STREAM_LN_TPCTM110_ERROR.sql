CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPCTM110_ERROR AS SELECT src, 'LN_TPCTM110' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acpn), '0'), 38, 10) is null and 
                    src:acpn is not null) THEN
                    'acpn contains a non-numeric value : \'' || AS_VARCHAR(src:acpn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adtp), '0'), 38, 10) is null and 
                    src:adtp is not null) THEN
                    'adtp contains a non-numeric value : \'' || AS_VARCHAR(src:adtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:advn), '0'), 38, 10) is null and 
                    src:advn is not null) THEN
                    'advn contains a non-numeric value : \'' || AS_VARCHAR(src:advn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bcrp), '0'), 38, 10) is null and 
                    src:bcrp is not null) THEN
                    'bcrp contains a non-numeric value : \'' || AS_VARCHAR(src:bcrp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrb), '0'), 38, 10) is null and 
                    src:bgrb is not null) THEN
                    'bgrb contains a non-numeric value : \'' || AS_VARCHAR(src:bgrb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrq), '0'), 38, 10) is null and 
                    src:bgrq is not null) THEN
                    'bgrq contains a non-numeric value : \'' || AS_VARCHAR(src:bgrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blck), '0'), 38, 10) is null and 
                    src:blck is not null) THEN
                    'blck contains a non-numeric value : \'' || AS_VARCHAR(src:blck) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blcl_ref_compnr), '0'), 38, 10) is null and 
                    src:blcl_ref_compnr is not null) THEN
                    'blcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:blcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) THEN
                    'bptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) THEN
                    'cbrn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbrn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccam_ref_compnr), '0'), 38, 10) is null and 
                    src:ccam_ref_compnr is not null) THEN
                    'ccam_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccam_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccat_ref_compnr is not null) THEN
                    'ccat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccot_ref_compnr), '0'), 38, 10) is null and 
                    src:ccot_ref_compnr is not null) THEN
                    'ccot_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccot_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) THEN
                    'ccrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ceil), '0'), 38, 10) is null and 
                    src:ceil is not null) THEN
                    'ceil contains a non-numeric value : \'' || AS_VARCHAR(src:ceil) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfac_ref_compnr), '0'), 38, 10) is null and 
                    src:cfac_ref_compnr is not null) THEN
                    'cfac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) THEN
                    'cfrw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfrw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cidm_ref_compnr), '0'), 38, 10) is null and 
                    src:cidm_ref_compnr is not null) THEN
                    'cidm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cidm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinm_ref_compnr), '0'), 38, 10) is null and 
                    src:cinm_ref_compnr is not null) THEN
                    'cinm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cinm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clam), '0'), 38, 10) is null and 
                    src:clam is not null) THEN
                    'clam contains a non-numeric value : \'' || AS_VARCHAR(src:clam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clam_cntc), '0'), 38, 10) is null and 
                    src:clam_cntc is not null) THEN
                    'clam_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:clam_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clam_dwhc), '0'), 38, 10) is null and 
                    src:clam_dwhc is not null) THEN
                    'clam_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:clam_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clam_homc), '0'), 38, 10) is null and 
                    src:clam_homc is not null) THEN
                    'clam_homc contains a non-numeric value : \'' || AS_VARCHAR(src:clam_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clam_prjc), '0'), 38, 10) is null and 
                    src:clam_prjc is not null) THEN
                    'clam_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:clam_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clam_refc), '0'), 38, 10) is null and 
                    src:clam_refc is not null) THEN
                    'clam_refc contains a non-numeric value : \'' || AS_VARCHAR(src:clam_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clam_rpc1), '0'), 38, 10) is null and 
                    src:clam_rpc1 is not null) THEN
                    'clam_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:clam_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clam_rpc2), '0'), 38, 10) is null and 
                    src:clam_rpc2 is not null) THEN
                    'clam_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:clam_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmng_ref_compnr), '0'), 38, 10) is null and 
                    src:cmng_ref_compnr is not null) THEN
                    'cmng_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmng_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnln_fcmp), '0'), 38, 10) is null and 
                    src:cnln_fcmp is not null) THEN
                    'cnln_fcmp contains a non-numeric value : \'' || AS_VARCHAR(src:cnln_fcmp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:codt), '1900-01-01')) is null and 
                    src:codt is not null) THEN
                    'codt contains a non-timestamp value : \'' || AS_VARCHAR(src:codt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) THEN
                    'cono_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr), '0'), 38, 10) is null and 
                    src:copr is not null) THEN
                    'copr contains a non-numeric value : \'' || AS_VARCHAR(src:copr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_cntc), '0'), 38, 10) is null and 
                    src:copr_cntc is not null) THEN
                    'copr_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:copr_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_dwhc), '0'), 38, 10) is null and 
                    src:copr_dwhc is not null) THEN
                    'copr_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:copr_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_homc), '0'), 38, 10) is null and 
                    src:copr_homc is not null) THEN
                    'copr_homc contains a non-numeric value : \'' || AS_VARCHAR(src:copr_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_prjc), '0'), 38, 10) is null and 
                    src:copr_prjc is not null) THEN
                    'copr_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:copr_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_refc), '0'), 38, 10) is null and 
                    src:copr_refc is not null) THEN
                    'copr_refc contains a non-numeric value : \'' || AS_VARCHAR(src:copr_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_rpc1), '0'), 38, 10) is null and 
                    src:copr_rpc1 is not null) THEN
                    'copr_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:copr_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_rpc2), '0'), 38, 10) is null and 
                    src:copr_rpc2 is not null) THEN
                    'copr_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:copr_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coty), '0'), 38, 10) is null and 
                    src:coty is not null) THEN
                    'coty contains a non-numeric value : \'' || AS_VARCHAR(src:coty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_bver_bnum_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_bver_bnum_ref_compnr is not null) THEN
                    'cprj_bver_bnum_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_bver_bnum_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) THEN
                    'creg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:creg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is null and 
                    src:crep_ref_compnr is not null) THEN
                    'crep_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crep_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is null and 
                    src:csec_ref_compnr is not null) THEN
                    'csec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstg_ref_compnr), '0'), 38, 10) is null and 
                    src:cstg_ref_compnr is not null) THEN
                    'cstg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cstg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csts), '0'), 38, 10) is null and 
                    src:csts is not null) THEN
                    'csts contains a non-numeric value : \'' || AS_VARCHAR(src:csts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyp), '0'), 38, 10) is null and 
                    src:ctyp is not null) THEN
                    'ctyp contains a non-numeric value : \'' || AS_VARCHAR(src:ctyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) THEN
                    'cuvc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuvc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfis_ref_compnr), '0'), 38, 10) is null and 
                    src:dfis_ref_compnr is not null) THEN
                    'dfis_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dfis_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc), '0'), 38, 10) is null and 
                    src:disc is not null) THEN
                    'disc contains a non-numeric value : \'' || AS_VARCHAR(src:disc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) THEN
                    'efdt contains a non-timestamp value : \'' || AS_VARCHAR(src:efdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:enip), '0'), 38, 10) is null and 
                    src:enip is not null) THEN
                    'enip contains a non-numeric value : \'' || AS_VARCHAR(src:enip) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esam), '0'), 38, 10) is null and 
                    src:esam is not null) THEN
                    'esam contains a non-numeric value : \'' || AS_VARCHAR(src:esam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esam_cntc), '0'), 38, 10) is null and 
                    src:esam_cntc is not null) THEN
                    'esam_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:esam_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esam_dwhc), '0'), 38, 10) is null and 
                    src:esam_dwhc is not null) THEN
                    'esam_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:esam_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esam_homc), '0'), 38, 10) is null and 
                    src:esam_homc is not null) THEN
                    'esam_homc contains a non-numeric value : \'' || AS_VARCHAR(src:esam_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esam_prjc), '0'), 38, 10) is null and 
                    src:esam_prjc is not null) THEN
                    'esam_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:esam_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esam_refc), '0'), 38, 10) is null and 
                    src:esam_refc is not null) THEN
                    'esam_refc contains a non-numeric value : \'' || AS_VARCHAR(src:esam_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esam_rpc1), '0'), 38, 10) is null and 
                    src:esam_rpc1 is not null) THEN
                    'esam_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:esam_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esam_rpc2), '0'), 38, 10) is null and 
                    src:esam_rpc2 is not null) THEN
                    'esam_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:esam_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esin), '0'), 38, 10) is null and 
                    src:esin is not null) THEN
                    'esin contains a non-numeric value : \'' || AS_VARCHAR(src:esin) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) THEN
                    'exdt contains a non-timestamp value : \'' || AS_VARCHAR(src:exdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) THEN
                    'exmt contains a non-numeric value : \'' || AS_VARCHAR(src:exmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is null and 
                    src:exrs_ref_compnr is not null) THEN
                    'exrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:exrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcrt), '0'), 38, 10) is null and 
                    src:fcrt is not null) THEN
                    'fcrt contains a non-numeric value : \'' || AS_VARCHAR(src:fcrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdis), '0'), 38, 10) is null and 
                    src:fdis is not null) THEN
                    'fdis contains a non-numeric value : \'' || AS_VARCHAR(src:fdis) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flmt), '0'), 38, 10) is null and 
                    src:flmt is not null) THEN
                    'flmt contains a non-numeric value : \'' || AS_VARCHAR(src:flmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flmt_cntc), '0'), 38, 10) is null and 
                    src:flmt_cntc is not null) THEN
                    'flmt_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:flmt_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flmt_dwhc), '0'), 38, 10) is null and 
                    src:flmt_dwhc is not null) THEN
                    'flmt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:flmt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flmt_homc), '0'), 38, 10) is null and 
                    src:flmt_homc is not null) THEN
                    'flmt_homc contains a non-numeric value : \'' || AS_VARCHAR(src:flmt_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flmt_prjc), '0'), 38, 10) is null and 
                    src:flmt_prjc is not null) THEN
                    'flmt_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:flmt_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flmt_refc), '0'), 38, 10) is null and 
                    src:flmt_refc is not null) THEN
                    'flmt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:flmt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flmt_rpc1), '0'), 38, 10) is null and 
                    src:flmt_rpc1 is not null) THEN
                    'flmt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:flmt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flmt_rpc2), '0'), 38, 10) is null and 
                    src:flmt_rpc2 is not null) THEN
                    'flmt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:flmt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frvt), '0'), 38, 10) is null and 
                    src:frvt is not null) THEN
                    'frvt contains a non-numeric value : \'' || AS_VARCHAR(src:frvt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbpc), '0'), 38, 10) is null and 
                    src:hbpc is not null) THEN
                    'hbpc contains a non-numeric value : \'' || AS_VARCHAR(src:hbpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hldb), '0'), 38, 10) is null and 
                    src:hldb is not null) THEN
                    'hldb contains a non-numeric value : \'' || AS_VARCHAR(src:hldb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hrsn_ref_compnr), '0'), 38, 10) is null and 
                    src:hrsn_ref_compnr is not null) THEN
                    'hrsn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:hrsn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invm), '0'), 38, 10) is null and 
                    src:invm is not null) THEN
                    'invm contains a non-numeric value : \'' || AS_VARCHAR(src:invm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itad_ref_compnr), '0'), 38, 10) is null and 
                    src:itad_ref_compnr is not null) THEN
                    'itad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) THEN
                    'itbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itcn_ref_compnr), '0'), 38, 10) is null and 
                    src:itcn_ref_compnr is not null) THEN
                    'itcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod), '0'), 38, 10) is null and 
                    src:itod is not null) THEN
                    'itod contains a non-numeric value : \'' || AS_VARCHAR(src:itod) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_cntc), '0'), 38, 10) is null and 
                    src:itod_cntc is not null) THEN
                    'itod_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:itod_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_dwhc), '0'), 38, 10) is null and 
                    src:itod_dwhc is not null) THEN
                    'itod_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:itod_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_homc), '0'), 38, 10) is null and 
                    src:itod_homc is not null) THEN
                    'itod_homc contains a non-numeric value : \'' || AS_VARCHAR(src:itod_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_prjc), '0'), 38, 10) is null and 
                    src:itod_prjc is not null) THEN
                    'itod_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:itod_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_refc), '0'), 38, 10) is null and 
                    src:itod_refc is not null) THEN
                    'itod_refc contains a non-numeric value : \'' || AS_VARCHAR(src:itod_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_rpc1), '0'), 38, 10) is null and 
                    src:itod_rpc1 is not null) THEN
                    'itod_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:itod_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_rpc2), '0'), 38, 10) is null and 
                    src:itod_rpc2 is not null) THEN
                    'itod_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:itod_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam), '0'), 38, 10) is null and 
                    src:ivam is not null) THEN
                    'ivam contains a non-numeric value : \'' || AS_VARCHAR(src:ivam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcrs_ref_compnr), '0'), 38, 10) is null and 
                    src:lcrs_ref_compnr is not null) THEN
                    'lcrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) THEN
                    'lmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpad), '0'), 38, 10) is null and 
                    src:lpad is not null) THEN
                    'lpad contains a non-numeric value : \'' || AS_VARCHAR(src:lpad) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpin), '0'), 38, 10) is null and 
                    src:lpin is not null) THEN
                    'lpin contains a non-numeric value : \'' || AS_VARCHAR(src:lpin) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltti), '1900-01-01')) is null and 
                    src:ltti is not null) THEN
                    'ltti contains a non-timestamp value : \'' || AS_VARCHAR(src:ltti) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mfad_ref_compnr), '0'), 38, 10) is null and 
                    src:mfad_ref_compnr is not null) THEN
                    'mfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mftp), '0'), 38, 10) is null and 
                    src:mftp is not null) THEN
                    'mftp contains a non-numeric value : \'' || AS_VARCHAR(src:mftp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mftx), '0'), 38, 10) is null and 
                    src:mftx is not null) THEN
                    'mftx contains a non-numeric value : \'' || AS_VARCHAR(src:mftx) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mftx_ref_compnr), '0'), 38, 10) is null and 
                    src:mftx_ref_compnr is not null) THEN
                    'mftx_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mftx_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) THEN
                    'ofad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) THEN
                    'ofcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrp_ref_compnr), '0'), 38, 10) is null and 
                    src:osrp_ref_compnr is not null) THEN
                    'osrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:osrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcba), '0'), 38, 10) is null and 
                    src:pcba is not null) THEN
                    'pcba contains a non-numeric value : \'' || AS_VARCHAR(src:pcba) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pceq), '0'), 38, 10) is null and 
                    src:pceq is not null) THEN
                    'pceq contains a non-numeric value : \'' || AS_VARCHAR(src:pceq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcic), '0'), 38, 10) is null and 
                    src:pcic is not null) THEN
                    'pcic contains a non-numeric value : \'' || AS_VARCHAR(src:pcic) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcit), '0'), 38, 10) is null and 
                    src:pcit is not null) THEN
                    'pcit contains a non-numeric value : \'' || AS_VARCHAR(src:pcit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcnt_ref_compnr), '0'), 38, 10) is null and 
                    src:pcnt_ref_compnr is not null) THEN
                    'pcnt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pcnt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcsb), '0'), 38, 10) is null and 
                    src:pcsb is not null) THEN
                    'pcsb contains a non-numeric value : \'' || AS_VARCHAR(src:pcsb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcts), '0'), 38, 10) is null and 
                    src:pcts is not null) THEN
                    'pcts contains a non-numeric value : \'' || AS_VARCHAR(src:pcts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcwa), '0'), 38, 10) is null and 
                    src:pcwa is not null) THEN
                    'pcwa contains a non-numeric value : \'' || AS_VARCHAR(src:pcwa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is null and 
                    src:pfad_ref_compnr is not null) THEN
                    'pfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) THEN
                    'pfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:pfcn_ref_compnr is not null) THEN
                    'pfcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfob), '0'), 38, 10) is null and 
                    src:pfob is not null) THEN
                    'pfob contains a non-numeric value : \'' || AS_VARCHAR(src:pfob) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgpm), '0'), 38, 10) is null and 
                    src:pgpm is not null) THEN
                    'pgpm contains a non-numeric value : \'' || AS_VARCHAR(src:pgpm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plpc), '0'), 38, 10) is null and 
                    src:plpc is not null) THEN
                    'plpc contains a non-numeric value : \'' || AS_VARCHAR(src:plpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:poin), '0'), 38, 10) is null and 
                    src:poin is not null) THEN
                    'poin contains a non-numeric value : \'' || AS_VARCHAR(src:poin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppim_ref_compnr), '0'), 38, 10) is null and 
                    src:ppim_ref_compnr is not null) THEN
                    'ppim_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ppim_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pppc), '0'), 38, 10) is null and 
                    src:pppc is not null) THEN
                    'pppc contains a non-numeric value : \'' || AS_VARCHAR(src:pppc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prrt_ref_compnr), '0'), 38, 10) is null and 
                    src:prrt_ref_compnr is not null) THEN
                    'prrt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prrt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prst), '0'), 38, 10) is null and 
                    src:prst is not null) THEN
                    'prst contains a non-numeric value : \'' || AS_VARCHAR(src:prst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prtx), '0'), 38, 10) is null and 
                    src:prtx is not null) THEN
                    'prtx contains a non-numeric value : \'' || AS_VARCHAR(src:prtx) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpp_ref_compnr is not null) THEN
                    'ptpp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptsh), '0'), 38, 10) is null and 
                    src:ptsh is not null) THEN
                    'ptsh contains a non-numeric value : \'' || AS_VARCHAR(src:ptsh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_1), '0'), 38, 10) is null and 
                    src:rate_1 is not null) THEN
                    'rate_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rate_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_2), '0'), 38, 10) is null and 
                    src:rate_2 is not null) THEN
                    'rate_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rate_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_3), '0'), 38, 10) is null and 
                    src:rate_3 is not null) THEN
                    'rate_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rate_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) THEN
                    'ratf_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) THEN
                    'ratf_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) THEN
                    'ratf_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) THEN
                    'rdat contains a non-timestamp value : \'' || AS_VARCHAR(src:rdat) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rddt), '1900-01-01')) is null and 
                    src:rddt is not null) THEN
                    'rddt contains a non-timestamp value : \'' || AS_VARCHAR(src:rddt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) THEN
                    'rtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sadr_ref_compnr), '0'), 38, 10) is null and 
                    src:sadr_ref_compnr is not null) THEN
                    'sadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:speq), '0'), 38, 10) is null and 
                    src:speq is not null) THEN
                    'speq contains a non-numeric value : \'' || AS_VARCHAR(src:speq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spic), '0'), 38, 10) is null and 
                    src:spic is not null) THEN
                    'spic contains a non-numeric value : \'' || AS_VARCHAR(src:spic) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spit), '0'), 38, 10) is null and 
                    src:spit is not null) THEN
                    'spit contains a non-numeric value : \'' || AS_VARCHAR(src:spit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsb), '0'), 38, 10) is null and 
                    src:spsb is not null) THEN
                    'spsb contains a non-numeric value : \'' || AS_VARCHAR(src:spsb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spts), '0'), 38, 10) is null and 
                    src:spts is not null) THEN
                    'spts contains a non-numeric value : \'' || AS_VARCHAR(src:spts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srvo_ref_compnr), '0'), 38, 10) is null and 
                    src:srvo_ref_compnr is not null) THEN
                    'srvo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:srvo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) THEN
                    'stad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) THEN
                    'stbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is null and 
                    src:stcn_ref_compnr is not null) THEN
                    'stcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is null and 
                    src:styp_ref_compnr is not null) THEN
                    'styp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:styp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcst), '0'), 38, 10) is null and 
                    src:tcst is not null) THEN
                    'tcst contains a non-numeric value : \'' || AS_VARCHAR(src:tcst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) THEN
                    'text contains a non-numeric value : \'' || AS_VARCHAR(src:text) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) THEN
                    'text_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:text_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tins), '0'), 38, 10) is null and 
                    src:tins is not null) THEN
                    'tins contains a non-numeric value : \'' || AS_VARCHAR(src:tins) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trpr), '0'), 38, 10) is null and 
                    src:trpr is not null) THEN
                    'trpr contains a non-numeric value : \'' || AS_VARCHAR(src:trpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubnk), '0'), 38, 10) is null and 
                    src:ubnk is not null) THEN
                    'ubnk contains a non-numeric value : \'' || AS_VARCHAR(src:ubnk) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ulpd), '0'), 38, 10) is null and 
                    src:ulpd is not null) THEN
                    'ulpd contains a non-numeric value : \'' || AS_VARCHAR(src:ulpd) || '\'' WHEN 
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
                src:cono,
                src:cnln  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPCTM110 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acpn), '0'), 38, 10) is null and 
                    src:acpn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adtp), '0'), 38, 10) is null and 
                    src:adtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:advn), '0'), 38, 10) is null and 
                    src:advn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bcrp), '0'), 38, 10) is null and 
                    src:bcrp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrb), '0'), 38, 10) is null and 
                    src:bgrb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrq), '0'), 38, 10) is null and 
                    src:bgrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blck), '0'), 38, 10) is null and 
                    src:blck is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blcl_ref_compnr), '0'), 38, 10) is null and 
                    src:blcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccam_ref_compnr), '0'), 38, 10) is null and 
                    src:ccam_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccot_ref_compnr), '0'), 38, 10) is null and 
                    src:ccot_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ceil), '0'), 38, 10) is null and 
                    src:ceil is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfac_ref_compnr), '0'), 38, 10) is null and 
                    src:cfac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cidm_ref_compnr), '0'), 38, 10) is null and 
                    src:cidm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinm_ref_compnr), '0'), 38, 10) is null and 
                    src:cinm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clam), '0'), 38, 10) is null and 
                    src:clam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clam_cntc), '0'), 38, 10) is null and 
                    src:clam_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clam_dwhc), '0'), 38, 10) is null and 
                    src:clam_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clam_homc), '0'), 38, 10) is null and 
                    src:clam_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clam_prjc), '0'), 38, 10) is null and 
                    src:clam_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clam_refc), '0'), 38, 10) is null and 
                    src:clam_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clam_rpc1), '0'), 38, 10) is null and 
                    src:clam_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clam_rpc2), '0'), 38, 10) is null and 
                    src:clam_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmng_ref_compnr), '0'), 38, 10) is null and 
                    src:cmng_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnln_fcmp), '0'), 38, 10) is null and 
                    src:cnln_fcmp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:codt), '1900-01-01')) is null and 
                    src:codt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr), '0'), 38, 10) is null and 
                    src:copr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_cntc), '0'), 38, 10) is null and 
                    src:copr_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_dwhc), '0'), 38, 10) is null and 
                    src:copr_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_homc), '0'), 38, 10) is null and 
                    src:copr_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_prjc), '0'), 38, 10) is null and 
                    src:copr_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_refc), '0'), 38, 10) is null and 
                    src:copr_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_rpc1), '0'), 38, 10) is null and 
                    src:copr_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_rpc2), '0'), 38, 10) is null and 
                    src:copr_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coty), '0'), 38, 10) is null and 
                    src:coty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_bver_bnum_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_bver_bnum_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is null and 
                    src:crep_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is null and 
                    src:csec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstg_ref_compnr), '0'), 38, 10) is null and 
                    src:cstg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csts), '0'), 38, 10) is null and 
                    src:csts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyp), '0'), 38, 10) is null and 
                    src:ctyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfis_ref_compnr), '0'), 38, 10) is null and 
                    src:dfis_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc), '0'), 38, 10) is null and 
                    src:disc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:enip), '0'), 38, 10) is null and 
                    src:enip is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esam), '0'), 38, 10) is null and 
                    src:esam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esam_cntc), '0'), 38, 10) is null and 
                    src:esam_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esam_dwhc), '0'), 38, 10) is null and 
                    src:esam_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esam_homc), '0'), 38, 10) is null and 
                    src:esam_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esam_prjc), '0'), 38, 10) is null and 
                    src:esam_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esam_refc), '0'), 38, 10) is null and 
                    src:esam_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esam_rpc1), '0'), 38, 10) is null and 
                    src:esam_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esam_rpc2), '0'), 38, 10) is null and 
                    src:esam_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esin), '0'), 38, 10) is null and 
                    src:esin is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is null and 
                    src:exrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcrt), '0'), 38, 10) is null and 
                    src:fcrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdis), '0'), 38, 10) is null and 
                    src:fdis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flmt), '0'), 38, 10) is null and 
                    src:flmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flmt_cntc), '0'), 38, 10) is null and 
                    src:flmt_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flmt_dwhc), '0'), 38, 10) is null and 
                    src:flmt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flmt_homc), '0'), 38, 10) is null and 
                    src:flmt_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flmt_prjc), '0'), 38, 10) is null and 
                    src:flmt_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flmt_refc), '0'), 38, 10) is null and 
                    src:flmt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flmt_rpc1), '0'), 38, 10) is null and 
                    src:flmt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flmt_rpc2), '0'), 38, 10) is null and 
                    src:flmt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frvt), '0'), 38, 10) is null and 
                    src:frvt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbpc), '0'), 38, 10) is null and 
                    src:hbpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hldb), '0'), 38, 10) is null and 
                    src:hldb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hrsn_ref_compnr), '0'), 38, 10) is null and 
                    src:hrsn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invm), '0'), 38, 10) is null and 
                    src:invm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itad_ref_compnr), '0'), 38, 10) is null and 
                    src:itad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itcn_ref_compnr), '0'), 38, 10) is null and 
                    src:itcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod), '0'), 38, 10) is null and 
                    src:itod is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_cntc), '0'), 38, 10) is null and 
                    src:itod_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_dwhc), '0'), 38, 10) is null and 
                    src:itod_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_homc), '0'), 38, 10) is null and 
                    src:itod_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_prjc), '0'), 38, 10) is null and 
                    src:itod_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_refc), '0'), 38, 10) is null and 
                    src:itod_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_rpc1), '0'), 38, 10) is null and 
                    src:itod_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_rpc2), '0'), 38, 10) is null and 
                    src:itod_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam), '0'), 38, 10) is null and 
                    src:ivam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcrs_ref_compnr), '0'), 38, 10) is null and 
                    src:lcrs_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpad), '0'), 38, 10) is null and 
                    src:lpad is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpin), '0'), 38, 10) is null and 
                    src:lpin is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltti), '1900-01-01')) is null and 
                    src:ltti is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mfad_ref_compnr), '0'), 38, 10) is null and 
                    src:mfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mftp), '0'), 38, 10) is null and 
                    src:mftp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mftx), '0'), 38, 10) is null and 
                    src:mftx is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mftx_ref_compnr), '0'), 38, 10) is null and 
                    src:mftx_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrp_ref_compnr), '0'), 38, 10) is null and 
                    src:osrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcba), '0'), 38, 10) is null and 
                    src:pcba is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pceq), '0'), 38, 10) is null and 
                    src:pceq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcic), '0'), 38, 10) is null and 
                    src:pcic is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcit), '0'), 38, 10) is null and 
                    src:pcit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcnt_ref_compnr), '0'), 38, 10) is null and 
                    src:pcnt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcsb), '0'), 38, 10) is null and 
                    src:pcsb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcts), '0'), 38, 10) is null and 
                    src:pcts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcwa), '0'), 38, 10) is null and 
                    src:pcwa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is null and 
                    src:pfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:pfcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfob), '0'), 38, 10) is null and 
                    src:pfob is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgpm), '0'), 38, 10) is null and 
                    src:pgpm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plpc), '0'), 38, 10) is null and 
                    src:plpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:poin), '0'), 38, 10) is null and 
                    src:poin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppim_ref_compnr), '0'), 38, 10) is null and 
                    src:ppim_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pppc), '0'), 38, 10) is null and 
                    src:pppc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prrt_ref_compnr), '0'), 38, 10) is null and 
                    src:prrt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prst), '0'), 38, 10) is null and 
                    src:prst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prtx), '0'), 38, 10) is null and 
                    src:prtx is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptsh), '0'), 38, 10) is null and 
                    src:ptsh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_1), '0'), 38, 10) is null and 
                    src:rate_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_2), '0'), 38, 10) is null and 
                    src:rate_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_3), '0'), 38, 10) is null and 
                    src:rate_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rddt), '1900-01-01')) is null and 
                    src:rddt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sadr_ref_compnr), '0'), 38, 10) is null and 
                    src:sadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:speq), '0'), 38, 10) is null and 
                    src:speq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spic), '0'), 38, 10) is null and 
                    src:spic is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spit), '0'), 38, 10) is null and 
                    src:spit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsb), '0'), 38, 10) is null and 
                    src:spsb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spts), '0'), 38, 10) is null and 
                    src:spts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srvo_ref_compnr), '0'), 38, 10) is null and 
                    src:srvo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is null and 
                    src:stcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is null and 
                    src:styp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcst), '0'), 38, 10) is null and 
                    src:tcst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tins), '0'), 38, 10) is null and 
                    src:tins is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trpr), '0'), 38, 10) is null and 
                    src:trpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubnk), '0'), 38, 10) is null and 
                    src:ubnk is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ulpd), '0'), 38, 10) is null and 
                    src:ulpd is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)