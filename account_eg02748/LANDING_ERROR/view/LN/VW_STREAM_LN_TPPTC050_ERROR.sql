CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPTC050_ERROR AS SELECT src, 'LN_TPPTC050' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acgm), '0'), 38, 10) is null and 
                    src:acgm is not null) THEN
                    'acgm contains a non-numeric value : \'' || AS_VARCHAR(src:acgm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aerf), '0'), 38, 10) is null and 
                    src:aerf is not null) THEN
                    'aerf contains a non-numeric value : \'' || AS_VARCHAR(src:aerf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aprp), '0'), 38, 10) is null and 
                    src:aprp is not null) THEN
                    'aprp contains a non-numeric value : \'' || AS_VARCHAR(src:aprp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arpc), '0'), 38, 10) is null and 
                    src:arpc is not null) THEN
                    'arpc contains a non-numeric value : \'' || AS_VARCHAR(src:arpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arrl), '0'), 38, 10) is null and 
                    src:arrl is not null) THEN
                    'arrl contains a non-numeric value : \'' || AS_VARCHAR(src:arrl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arrm), '0'), 38, 10) is null and 
                    src:arrm is not null) THEN
                    'arrm contains a non-numeric value : \'' || AS_VARCHAR(src:arrm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arrt), '0'), 38, 10) is null and 
                    src:arrt is not null) THEN
                    'arrt contains a non-numeric value : \'' || AS_VARCHAR(src:arrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadc_ref_compnr), '0'), 38, 10) is null and 
                    src:cadc_ref_compnr is not null) THEN
                    'cadc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) THEN
                    'cbrn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbrn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccam_ref_compnr), '0'), 38, 10) is null and 
                    src:ccam_ref_compnr is not null) THEN
                    'ccam_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccam_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccat_ref_compnr is not null) THEN
                    'ccat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ceil), '0'), 38, 10) is null and 
                    src:ceil is not null) THEN
                    'ceil contains a non-numeric value : \'' || AS_VARCHAR(src:ceil) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfac_ref_compnr), '0'), 38, 10) is null and 
                    src:cfac_ref_compnr is not null) THEN
                    'cfac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chic), '0'), 38, 10) is null and 
                    src:chic is not null) THEN
                    'chic contains a non-numeric value : \'' || AS_VARCHAR(src:chic) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:codt), '1900-01-01')) is null and 
                    src:codt is not null) THEN
                    'codt contains a non-timestamp value : \'' || AS_VARCHAR(src:codt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cogs), '0'), 38, 10) is null and 
                    src:cogs is not null) THEN
                    'cogs contains a non-numeric value : \'' || AS_VARCHAR(src:cogs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcu_ref_compnr is not null) THEN
                    'cpcu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpcu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) THEN
                    'creg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:creg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cres_ref_compnr), '0'), 38, 10) is null and 
                    src:cres_ref_compnr is not null) THEN
                    'cres_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cres_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is null and 
                    src:csec_ref_compnr is not null) THEN
                    'csec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csst), '0'), 38, 10) is null and 
                    src:csst is not null) THEN
                    'csst contains a non-numeric value : \'' || AS_VARCHAR(src:csst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstg_ref_compnr), '0'), 38, 10) is null and 
                    src:cstg_ref_compnr is not null) THEN
                    'cstg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cstg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) THEN
                    'cstv contains a non-numeric value : \'' || AS_VARCHAR(src:cstv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) THEN
                    'cuvc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuvc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:earf), '0'), 38, 10) is null and 
                    src:earf is not null) THEN
                    'earf contains a non-numeric value : \'' || AS_VARCHAR(src:earf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) THEN
                    'emno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:emno_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:escu_ref_compnr), '0'), 38, 10) is null and 
                    src:escu_ref_compnr is not null) THEN
                    'escu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:escu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:excc), '0'), 38, 10) is null and 
                    src:excc is not null) THEN
                    'excc contains a non-numeric value : \'' || AS_VARCHAR(src:excc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmp), '0'), 38, 10) is null and 
                    src:fcmp is not null) THEN
                    'fcmp contains a non-numeric value : \'' || AS_VARCHAR(src:fcmp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:fidt), '1900-01-01')) is null and 
                    src:fidt is not null) THEN
                    'fidt contains a non-timestamp value : \'' || AS_VARCHAR(src:fidt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbnr), '0'), 38, 10) is null and 
                    src:hbnr is not null) THEN
                    'hbnr contains a non-numeric value : \'' || AS_VARCHAR(src:hbnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbyn), '0'), 38, 10) is null and 
                    src:hbyn is not null) THEN
                    'hbyn contains a non-numeric value : \'' || AS_VARCHAR(src:hbyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) THEN
                    'idoc contains a non-numeric value : \'' || AS_VARCHAR(src:idoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam), '0'), 38, 10) is null and 
                    src:ivam is not null) THEN
                    'ivam contains a non-numeric value : \'' || AS_VARCHAR(src:ivam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_cntc), '0'), 38, 10) is null and 
                    src:ivam_cntc is not null) THEN
                    'ivam_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:ivam_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_dwhc), '0'), 38, 10) is null and 
                    src:ivam_dwhc is not null) THEN
                    'ivam_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:ivam_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_homc), '0'), 38, 10) is null and 
                    src:ivam_homc is not null) THEN
                    'ivam_homc contains a non-numeric value : \'' || AS_VARCHAR(src:ivam_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_prjc), '0'), 38, 10) is null and 
                    src:ivam_prjc is not null) THEN
                    'ivam_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:ivam_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_refc), '0'), 38, 10) is null and 
                    src:ivam_refc is not null) THEN
                    'ivam_refc contains a non-numeric value : \'' || AS_VARCHAR(src:ivam_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_rpc1), '0'), 38, 10) is null and 
                    src:ivam_rpc1 is not null) THEN
                    'ivam_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:ivam_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_rpc2), '0'), 38, 10) is null and 
                    src:ivam_rpc2 is not null) THEN
                    'ivam_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:ivam_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpsl), '0'), 38, 10) is null and 
                    src:lpsl is not null) THEN
                    'lpsl contains a non-numeric value : \'' || AS_VARCHAR(src:lpsl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:njsp), '0'), 38, 10) is null and 
                    src:njsp is not null) THEN
                    'njsp contains a non-numeric value : \'' || AS_VARCHAR(src:njsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pacu_ref_compnr), '0'), 38, 10) is null and 
                    src:pacu_ref_compnr is not null) THEN
                    'pacu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pacu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmp), '0'), 38, 10) is null and 
                    src:pcmp is not null) THEN
                    'pcmp contains a non-numeric value : \'' || AS_VARCHAR(src:pcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prpc), '0'), 38, 10) is null and 
                    src:prpc is not null) THEN
                    'prpc contains a non-numeric value : \'' || AS_VARCHAR(src:prpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva), '0'), 38, 10) is null and 
                    src:prva is not null) THEN
                    'prva contains a non-numeric value : \'' || AS_VARCHAR(src:prva) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva_cntc), '0'), 38, 10) is null and 
                    src:prva_cntc is not null) THEN
                    'prva_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:prva_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva_dwhc), '0'), 38, 10) is null and 
                    src:prva_dwhc is not null) THEN
                    'prva_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:prva_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva_homc), '0'), 38, 10) is null and 
                    src:prva_homc is not null) THEN
                    'prva_homc contains a non-numeric value : \'' || AS_VARCHAR(src:prva_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva_prjc), '0'), 38, 10) is null and 
                    src:prva_prjc is not null) THEN
                    'prva_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:prva_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva_refc), '0'), 38, 10) is null and 
                    src:prva_refc is not null) THEN
                    'prva_refc contains a non-numeric value : \'' || AS_VARCHAR(src:prva_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva_rpc1), '0'), 38, 10) is null and 
                    src:prva_rpc1 is not null) THEN
                    'prva_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:prva_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva_rpc2), '0'), 38, 10) is null and 
                    src:prva_rpc2 is not null) THEN
                    'prva_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:prva_rpc2) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdap), '1900-01-01')) is null and 
                    src:rdap is not null) THEN
                    'rdap contains a non-timestamp value : \'' || AS_VARCHAR(src:rdap) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) THEN
                    'rdat contains a non-timestamp value : \'' || AS_VARCHAR(src:rdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revl), '0'), 38, 10) is null and 
                    src:revl is not null) THEN
                    'revl contains a non-numeric value : \'' || AS_VARCHAR(src:revl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revr), '0'), 38, 10) is null and 
                    src:revr is not null) THEN
                    'revr contains a non-numeric value : \'' || AS_VARCHAR(src:revr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revt), '0'), 38, 10) is null and 
                    src:revt is not null) THEN
                    'revt contains a non-numeric value : \'' || AS_VARCHAR(src:revt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_1), '0'), 38, 10) is null and 
                    src:rtcc_1 is not null) THEN
                    'rtcc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_2), '0'), 38, 10) is null and 
                    src:rtcc_2 is not null) THEN
                    'rtcc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_3), '0'), 38, 10) is null and 
                    src:rtcc_3 is not null) THEN
                    'rtcc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcp_1), '0'), 38, 10) is null and 
                    src:rtcp_1 is not null) THEN
                    'rtcp_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcp_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcp_2), '0'), 38, 10) is null and 
                    src:rtcp_2 is not null) THEN
                    'rtcp_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcp_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcp_3), '0'), 38, 10) is null and 
                    src:rtcp_3 is not null) THEN
                    'rtcp_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcp_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_1), '0'), 38, 10) is null and 
                    src:rtfc_1 is not null) THEN
                    'rtfc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_2), '0'), 38, 10) is null and 
                    src:rtfc_2 is not null) THEN
                    'rtfc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_3), '0'), 38, 10) is null and 
                    src:rtfc_3 is not null) THEN
                    'rtfc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfp_1), '0'), 38, 10) is null and 
                    src:rtfp_1 is not null) THEN
                    'rtfp_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfp_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfp_2), '0'), 38, 10) is null and 
                    src:rtfp_2 is not null) THEN
                    'rtfp_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfp_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfp_3), '0'), 38, 10) is null and 
                    src:rtfp_3 is not null) THEN
                    'rtfp_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfp_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sadr_ref_compnr), '0'), 38, 10) is null and 
                    src:sadr_ref_compnr is not null) THEN
                    'sadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) THEN
                    'stdt contains a non-timestamp value : \'' || AS_VARCHAR(src:stdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stlm), '0'), 38, 10) is null and 
                    src:stlm is not null) THEN
                    'stlm contains a non-numeric value : \'' || AS_VARCHAR(src:stlm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stls), '0'), 38, 10) is null and 
                    src:stls is not null) THEN
                    'stls contains a non-numeric value : \'' || AS_VARCHAR(src:stls) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stlt), '0'), 38, 10) is null and 
                    src:stlt is not null) THEN
                    'stlt contains a non-numeric value : \'' || AS_VARCHAR(src:stlt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is null and 
                    src:styp_ref_compnr is not null) THEN
                    'styp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:styp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trsl), '0'), 38, 10) is null and 
                    src:trsl is not null) THEN
                    'trsl contains a non-numeric value : \'' || AS_VARCHAR(src:trsl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
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
                src:cprj,
                src:cstl  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPTC050 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acgm), '0'), 38, 10) is null and 
                    src:acgm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aerf), '0'), 38, 10) is null and 
                    src:aerf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aprp), '0'), 38, 10) is null and 
                    src:aprp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arpc), '0'), 38, 10) is null and 
                    src:arpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arrl), '0'), 38, 10) is null and 
                    src:arrl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arrm), '0'), 38, 10) is null and 
                    src:arrm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arrt), '0'), 38, 10) is null and 
                    src:arrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadc_ref_compnr), '0'), 38, 10) is null and 
                    src:cadc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccam_ref_compnr), '0'), 38, 10) is null and 
                    src:ccam_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ceil), '0'), 38, 10) is null and 
                    src:ceil is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfac_ref_compnr), '0'), 38, 10) is null and 
                    src:cfac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chic), '0'), 38, 10) is null and 
                    src:chic is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:codt), '1900-01-01')) is null and 
                    src:codt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cogs), '0'), 38, 10) is null and 
                    src:cogs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cres_ref_compnr), '0'), 38, 10) is null and 
                    src:cres_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is null and 
                    src:csec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csst), '0'), 38, 10) is null and 
                    src:csst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstg_ref_compnr), '0'), 38, 10) is null and 
                    src:cstg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:earf), '0'), 38, 10) is null and 
                    src:earf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:escu_ref_compnr), '0'), 38, 10) is null and 
                    src:escu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:excc), '0'), 38, 10) is null and 
                    src:excc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmp), '0'), 38, 10) is null and 
                    src:fcmp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:fidt), '1900-01-01')) is null and 
                    src:fidt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbnr), '0'), 38, 10) is null and 
                    src:hbnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbyn), '0'), 38, 10) is null and 
                    src:hbyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam), '0'), 38, 10) is null and 
                    src:ivam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_cntc), '0'), 38, 10) is null and 
                    src:ivam_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_dwhc), '0'), 38, 10) is null and 
                    src:ivam_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_homc), '0'), 38, 10) is null and 
                    src:ivam_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_prjc), '0'), 38, 10) is null and 
                    src:ivam_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_refc), '0'), 38, 10) is null and 
                    src:ivam_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_rpc1), '0'), 38, 10) is null and 
                    src:ivam_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_rpc2), '0'), 38, 10) is null and 
                    src:ivam_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpsl), '0'), 38, 10) is null and 
                    src:lpsl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:njsp), '0'), 38, 10) is null and 
                    src:njsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pacu_ref_compnr), '0'), 38, 10) is null and 
                    src:pacu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmp), '0'), 38, 10) is null and 
                    src:pcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prpc), '0'), 38, 10) is null and 
                    src:prpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva), '0'), 38, 10) is null and 
                    src:prva is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva_cntc), '0'), 38, 10) is null and 
                    src:prva_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva_dwhc), '0'), 38, 10) is null and 
                    src:prva_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva_homc), '0'), 38, 10) is null and 
                    src:prva_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva_prjc), '0'), 38, 10) is null and 
                    src:prva_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva_refc), '0'), 38, 10) is null and 
                    src:prva_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva_rpc1), '0'), 38, 10) is null and 
                    src:prva_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva_rpc2), '0'), 38, 10) is null and 
                    src:prva_rpc2 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdap), '1900-01-01')) is null and 
                    src:rdap is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revl), '0'), 38, 10) is null and 
                    src:revl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revr), '0'), 38, 10) is null and 
                    src:revr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revt), '0'), 38, 10) is null and 
                    src:revt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_1), '0'), 38, 10) is null and 
                    src:rtcc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_2), '0'), 38, 10) is null and 
                    src:rtcc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_3), '0'), 38, 10) is null and 
                    src:rtcc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcp_1), '0'), 38, 10) is null and 
                    src:rtcp_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcp_2), '0'), 38, 10) is null and 
                    src:rtcp_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcp_3), '0'), 38, 10) is null and 
                    src:rtcp_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_1), '0'), 38, 10) is null and 
                    src:rtfc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_2), '0'), 38, 10) is null and 
                    src:rtfc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_3), '0'), 38, 10) is null and 
                    src:rtfc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfp_1), '0'), 38, 10) is null and 
                    src:rtfp_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfp_2), '0'), 38, 10) is null and 
                    src:rtfp_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfp_3), '0'), 38, 10) is null and 
                    src:rtfp_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sadr_ref_compnr), '0'), 38, 10) is null and 
                    src:sadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stlm), '0'), 38, 10) is null and 
                    src:stlm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stls), '0'), 38, 10) is null and 
                    src:stls is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stlt), '0'), 38, 10) is null and 
                    src:stlt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is null and 
                    src:styp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trsl), '0'), 38, 10) is null and 
                    src:trsl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)