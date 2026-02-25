CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPDM700_ERROR AS SELECT src, 'LN_TPPDM700' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acpn), '0'), 38, 10) is null and 
                    src:acpn is not null) THEN
                    'acpn contains a non-numeric value : \'' || AS_VARCHAR(src:acpn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amts), '0'), 38, 10) is null and 
                    src:amts is not null) THEN
                    'amts contains a non-numeric value : \'' || AS_VARCHAR(src:amts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amts_cntc), '0'), 38, 10) is null and 
                    src:amts_cntc is not null) THEN
                    'amts_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:amts_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amts_dwhc), '0'), 38, 10) is null and 
                    src:amts_dwhc is not null) THEN
                    'amts_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:amts_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amts_homc), '0'), 38, 10) is null and 
                    src:amts_homc is not null) THEN
                    'amts_homc contains a non-numeric value : \'' || AS_VARCHAR(src:amts_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amts_prjc), '0'), 38, 10) is null and 
                    src:amts_prjc is not null) THEN
                    'amts_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:amts_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amts_refc), '0'), 38, 10) is null and 
                    src:amts_refc is not null) THEN
                    'amts_refc contains a non-numeric value : \'' || AS_VARCHAR(src:amts_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amts_rpc1), '0'), 38, 10) is null and 
                    src:amts_rpc1 is not null) THEN
                    'amts_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:amts_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amts_rpc2), '0'), 38, 10) is null and 
                    src:amts_rpc2 is not null) THEN
                    'amts_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:amts_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aqan), '0'), 38, 10) is null and 
                    src:aqan is not null) THEN
                    'aqan contains a non-numeric value : \'' || AS_VARCHAR(src:aqan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:baor), '0'), 38, 10) is null and 
                    src:baor is not null) THEN
                    'baor contains a non-numeric value : \'' || AS_VARCHAR(src:baor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrb), '0'), 38, 10) is null and 
                    src:bgrb is not null) THEN
                    'bgrb contains a non-numeric value : \'' || AS_VARCHAR(src:bgrb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrq), '0'), 38, 10) is null and 
                    src:bgrq is not null) THEN
                    'bgrq contains a non-numeric value : \'' || AS_VARCHAR(src:bgrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blck), '0'), 38, 10) is null and 
                    src:blck is not null) THEN
                    'blck contains a non-numeric value : \'' || AS_VARCHAR(src:blck) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) THEN
                    'bptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bqan), '0'), 38, 10) is null and 
                    src:bqan is not null) THEN
                    'bqan contains a non-numeric value : \'' || AS_VARCHAR(src:bqan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:buni_ref_compnr), '0'), 38, 10) is null and 
                    src:buni_ref_compnr is not null) THEN
                    'buni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:buni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt), '0'), 38, 10) is null and 
                    src:camt is not null) THEN
                    'camt contains a non-numeric value : \'' || AS_VARCHAR(src:camt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_cntc), '0'), 38, 10) is null and 
                    src:camt_cntc is not null) THEN
                    'camt_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_dwhc), '0'), 38, 10) is null and 
                    src:camt_dwhc is not null) THEN
                    'camt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_homc), '0'), 38, 10) is null and 
                    src:camt_homc is not null) THEN
                    'camt_homc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_prjc), '0'), 38, 10) is null and 
                    src:camt_prjc is not null) THEN
                    'camt_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_refc), '0'), 38, 10) is null and 
                    src:camt_refc is not null) THEN
                    'camt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_rpc1), '0'), 38, 10) is null and 
                    src:camt_rpc1 is not null) THEN
                    'camt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_rpc2), '0'), 38, 10) is null and 
                    src:camt_rpc2 is not null) THEN
                    'camt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:carr_ref_compnr), '0'), 38, 10) is null and 
                    src:carr_ref_compnr is not null) THEN
                    'carr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:carr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) THEN
                    'ccco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccfm), '0'), 38, 10) is null and 
                    src:ccfm is not null) THEN
                    'ccfm contains a non-numeric value : \'' || AS_VARCHAR(src:ccfm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cddt), '1900-01-01')) is null and 
                    src:cddt is not null) THEN
                    'cddt contains a non-timestamp value : \'' || AS_VARCHAR(src:cddt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is null and 
                    src:citt_ref_compnr is not null) THEN
                    'citt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:citt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cnel_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cnel_ref_compnr is not null) THEN
                    'cnpr_cnel_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cnpr_cnel_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cnpl_cnac_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cnpl_cnac_ref_compnr is not null) THEN
                    'cnpr_cnpl_cnac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cnpr_cnpl_cnac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_ref_compnr is not null) THEN
                    'cnpr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cnpr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocu_ref_compnr), '0'), 38, 10) is null and 
                    src:cocu_ref_compnr is not null) THEN
                    'cocu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cocu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_ref_compnr is not null) THEN
                    'cono_cnln_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_cnln_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) THEN
                    'cono_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) THEN
                    'cprj_cpla_cact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_cact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_milt_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_milt_ref_compnr is not null) THEN
                    'cprj_cpla_milt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_milt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) THEN
                    'cprj_cpla_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) THEN
                    'cprj_cspa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cspa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) THEN
                    'cprj_cstl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cstl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpva), '0'), 38, 10) is null and 
                    src:cpva is not null) THEN
                    'cpva contains a non-numeric value : \'' || AS_VARCHAR(src:cpva) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cqan), '0'), 38, 10) is null and 
                    src:cqan is not null) THEN
                    'cqan contains a non-numeric value : \'' || AS_VARCHAR(src:cqan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crbn), '0'), 38, 10) is null and 
                    src:crbn is not null) THEN
                    'crbn contains a non-numeric value : \'' || AS_VARCHAR(src:crbn) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) THEN
                    'crte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspg), '0'), 38, 10) is null and 
                    src:cspg is not null) THEN
                    'cspg contains a non-numeric value : \'' || AS_VARCHAR(src:cspg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) THEN
                    'cstv contains a non-numeric value : \'' || AS_VARCHAR(src:cstv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv_cntc), '0'), 38, 10) is null and 
                    src:cstv_cntc is not null) THEN
                    'cstv_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:cstv_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv_dwhc), '0'), 38, 10) is null and 
                    src:cstv_dwhc is not null) THEN
                    'cstv_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:cstv_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv_homc), '0'), 38, 10) is null and 
                    src:cstv_homc is not null) THEN
                    'cstv_homc contains a non-numeric value : \'' || AS_VARCHAR(src:cstv_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv_prjc), '0'), 38, 10) is null and 
                    src:cstv_prjc is not null) THEN
                    'cstv_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:cstv_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv_refc), '0'), 38, 10) is null and 
                    src:cstv_refc is not null) THEN
                    'cstv_refc contains a non-numeric value : \'' || AS_VARCHAR(src:cstv_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv_rpc1), '0'), 38, 10) is null and 
                    src:cstv_rpc1 is not null) THEN
                    'cstv_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:cstv_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv_rpc2), '0'), 38, 10) is null and 
                    src:cstv_rpc2 is not null) THEN
                    'cstv_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:cstv_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delt), '0'), 38, 10) is null and 
                    src:delt is not null) THEN
                    'delt contains a non-numeric value : \'' || AS_VARCHAR(src:delt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc), '0'), 38, 10) is null and 
                    src:disc is not null) THEN
                    'disc contains a non-numeric value : \'' || AS_VARCHAR(src:disc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlor), '0'), 38, 10) is null and 
                    src:dlor is not null) THEN
                    'dlor contains a non-numeric value : \'' || AS_VARCHAR(src:dlor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlvr), '0'), 38, 10) is null and 
                    src:dlvr is not null) THEN
                    'dlvr contains a non-numeric value : \'' || AS_VARCHAR(src:dlvr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg), '0'), 38, 10) is null and 
                    src:dorg is not null) THEN
                    'dorg contains a non-numeric value : \'' || AS_VARCHAR(src:dorg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dspr), '0'), 38, 10) is null and 
                    src:dspr is not null) THEN
                    'dspr contains a non-numeric value : \'' || AS_VARCHAR(src:dspr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) THEN
                    'effn contains a non-numeric value : \'' || AS_VARCHAR(src:effn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) THEN
                    'effn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:effn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) THEN
                    'exmt contains a non-numeric value : \'' || AS_VARCHAR(src:exmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is null and 
                    src:exrs_ref_compnr is not null) THEN
                    'exrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:exrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:iddt), '1900-01-01')) is null and 
                    src:iddt is not null) THEN
                    'iddt contains a non-timestamp value : \'' || AS_VARCHAR(src:iddt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_site_ref_compnr), '0'), 38, 10) is null and 
                    src:item_site_ref_compnr is not null) THEN
                    'item_site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcrq), '0'), 38, 10) is null and 
                    src:lcrq is not null) THEN
                    'lcrq contains a non-numeric value : \'' || AS_VARCHAR(src:lcrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam), '0'), 38, 10) is null and 
                    src:ldam is not null) THEN
                    'ldam contains a non-numeric value : \'' || AS_VARCHAR(src:ldam) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) THEN
                    'lmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lssn_ref_compnr), '0'), 38, 10) is null and 
                    src:lssn_ref_compnr is not null) THEN
                    'lssn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lssn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltsl), '0'), 38, 10) is null and 
                    src:ltsl is not null) THEN
                    'ltsl contains a non-numeric value : \'' || AS_VARCHAR(src:ltsl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oqan), '0'), 38, 10) is null and 
                    src:oqan is not null) THEN
                    'oqan contains a non-numeric value : \'' || AS_VARCHAR(src:oqan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osch), '0'), 38, 10) is null and 
                    src:osch is not null) THEN
                    'osch contains a non-numeric value : \'' || AS_VARCHAR(src:osch) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ouni_ref_compnr), '0'), 38, 10) is null and 
                    src:ouni_ref_compnr is not null) THEN
                    'ouni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ouni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pass), '0'), 38, 10) is null and 
                    src:pass is not null) THEN
                    'pass contains a non-numeric value : \'' || AS_VARCHAR(src:pass) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pddt), '1900-01-01')) is null and 
                    src:pddt is not null) THEN
                    'pddt contains a non-timestamp value : \'' || AS_VARCHAR(src:pddt) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) THEN
                    'pris contains a non-numeric value : \'' || AS_VARCHAR(src:pris) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsg_ref_compnr), '0'), 38, 10) is null and 
                    src:prsg_ref_compnr is not null) THEN
                    'prsg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prsg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rddt), '1900-01-01')) is null and 
                    src:rddt is not null) THEN
                    'rddt contains a non-timestamp value : \'' || AS_VARCHAR(src:rddt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfdl), '0'), 38, 10) is null and 
                    src:rfdl is not null) THEN
                    'rfdl contains a non-numeric value : \'' || AS_VARCHAR(src:rfdl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfic), '0'), 38, 10) is null and 
                    src:rfic is not null) THEN
                    'rfic contains a non-numeric value : \'' || AS_VARCHAR(src:rfic) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfid), '0'), 38, 10) is null and 
                    src:rfid is not null) THEN
                    'rfid contains a non-numeric value : \'' || AS_VARCHAR(src:rfid) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfsc), '0'), 38, 10) is null and 
                    src:rfsc is not null) THEN
                    'rfsc contains a non-numeric value : \'' || AS_VARCHAR(src:rfsc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown), '0'), 38, 10) is null and 
                    src:rown is not null) THEN
                    'rown contains a non-numeric value : \'' || AS_VARCHAR(src:rown) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rqan), '0'), 38, 10) is null and 
                    src:rqan is not null) THEN
                    'rqan contains a non-numeric value : \'' || AS_VARCHAR(src:rqan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrsn_ref_compnr), '0'), 38, 10) is null and 
                    src:rrsn_ref_compnr is not null) THEN
                    'rrsn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rrsn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_1), '0'), 38, 10) is null and 
                    src:rtcc_1 is not null) THEN
                    'rtcc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_2), '0'), 38, 10) is null and 
                    src:rtcc_2 is not null) THEN
                    'rtcc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_3), '0'), 38, 10) is null and 
                    src:rtcc_3 is not null) THEN
                    'rtcc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_1), '0'), 38, 10) is null and 
                    src:rtcs_1 is not null) THEN
                    'rtcs_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcs_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_2), '0'), 38, 10) is null and 
                    src:rtcs_2 is not null) THEN
                    'rtcs_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcs_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_3), '0'), 38, 10) is null and 
                    src:rtcs_3 is not null) THEN
                    'rtcs_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcs_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_1), '0'), 38, 10) is null and 
                    src:rtfc_1 is not null) THEN
                    'rtfc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_2), '0'), 38, 10) is null and 
                    src:rtfc_2 is not null) THEN
                    'rtfc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_3), '0'), 38, 10) is null and 
                    src:rtfc_3 is not null) THEN
                    'rtfc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_1), '0'), 38, 10) is null and 
                    src:rtfs_1 is not null) THEN
                    'rtfs_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfs_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_2), '0'), 38, 10) is null and 
                    src:rtfs_2 is not null) THEN
                    'rtfs_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfs_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_3), '0'), 38, 10) is null and 
                    src:rtfs_3 is not null) THEN
                    'rtfs_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfs_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtor), '0'), 38, 10) is null and 
                    src:rtor is not null) THEN
                    'rtor contains a non-numeric value : \'' || AS_VARCHAR(src:rtor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sacu_ref_compnr), '0'), 38, 10) is null and 
                    src:sacu_ref_compnr is not null) THEN
                    'sacu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sacu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt), '0'), 38, 10) is null and 
                    src:samt is not null) THEN
                    'samt contains a non-numeric value : \'' || AS_VARCHAR(src:samt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_cntc), '0'), 38, 10) is null and 
                    src:samt_cntc is not null) THEN
                    'samt_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:samt_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_dwhc), '0'), 38, 10) is null and 
                    src:samt_dwhc is not null) THEN
                    'samt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:samt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_homc), '0'), 38, 10) is null and 
                    src:samt_homc is not null) THEN
                    'samt_homc contains a non-numeric value : \'' || AS_VARCHAR(src:samt_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_prjc), '0'), 38, 10) is null and 
                    src:samt_prjc is not null) THEN
                    'samt_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:samt_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_refc), '0'), 38, 10) is null and 
                    src:samt_refc is not null) THEN
                    'samt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:samt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_rpc1), '0'), 38, 10) is null and 
                    src:samt_rpc1 is not null) THEN
                    'samt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:samt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_rpc2), '0'), 38, 10) is null and 
                    src:samt_rpc2 is not null) THEN
                    'samt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:samt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schd), '0'), 38, 10) is null and 
                    src:schd is not null) THEN
                    'schd contains a non-numeric value : \'' || AS_VARCHAR(src:schd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdsc), '0'), 38, 10) is null and 
                    src:sdsc is not null) THEN
                    'sdsc contains a non-numeric value : \'' || AS_VARCHAR(src:sdsc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) THEN
                    'seqn contains a non-numeric value : \'' || AS_VARCHAR(src:seqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) THEN
                    'sern contains a non-numeric value : \'' || AS_VARCHAR(src:sern) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serv_ref_compnr), '0'), 38, 10) is null and 
                    src:serv_ref_compnr is not null) THEN
                    'serv_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:serv_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shcn), '0'), 38, 10) is null and 
                    src:shcn is not null) THEN
                    'shcn contains a non-numeric value : \'' || AS_VARCHAR(src:shcn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sign), '0'), 38, 10) is null and 
                    src:sign is not null) THEN
                    'sign contains a non-numeric value : \'' || AS_VARCHAR(src:sign) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqan), '0'), 38, 10) is null and 
                    src:sqan is not null) THEN
                    'sqan contains a non-numeric value : \'' || AS_VARCHAR(src:sqan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srsl), '0'), 38, 10) is null and 
                    src:srsl is not null) THEN
                    'srsl contains a non-numeric value : \'' || AS_VARCHAR(src:srsl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_dlpt_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_dlpt_ref_compnr is not null) THEN
                    'stad_dlpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_dlpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) THEN
                    'stbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is null and 
                    src:stcn_ref_compnr is not null) THEN
                    'stcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:suni_ref_compnr), '0'), 38, 10) is null and 
                    src:suni_ref_compnr is not null) THEN
                    'suni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:suni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tatd_ref_compnr), '0'), 38, 10) is null and 
                    src:tatd_ref_compnr is not null) THEN
                    'tatd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tatd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcco_ref_compnr), '0'), 38, 10) is null and 
                    src:tcco_ref_compnr is not null) THEN
                    'tcco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tcco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcff), '0'), 38, 10) is null and 
                    src:tcff is not null) THEN
                    'tcff contains a non-numeric value : \'' || AS_VARCHAR(src:tcff) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcst), '0'), 38, 10) is null and 
                    src:tcst is not null) THEN
                    'tcst contains a non-numeric value : \'' || AS_VARCHAR(src:tcst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tdel), '0'), 38, 10) is null and 
                    src:tdel is not null) THEN
                    'tdel contains a non-numeric value : \'' || AS_VARCHAR(src:tdel) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprj_ref_compnr), '0'), 38, 10) is null and 
                    src:tprj_ref_compnr is not null) THEN
                    'tprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprj_tpla_ref_compnr), '0'), 38, 10) is null and 
                    src:tprj_tpla_ref_compnr is not null) THEN
                    'tprj_tpla_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tprj_tpla_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprj_tpla_tact_ref_compnr), '0'), 38, 10) is null and 
                    src:tprj_tpla_tact_ref_compnr is not null) THEN
                    'tprj_tpla_tact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tprj_tpla_tact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprj_tspa_ref_compnr), '0'), 38, 10) is null and 
                    src:tprj_tspa_ref_compnr is not null) THEN
                    'tprj_tspa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tprj_tspa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprj_tstl_ref_compnr), '0'), 38, 10) is null and 
                    src:tprj_tstl_ref_compnr is not null) THEN
                    'tprj_tstl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tprj_tstl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:twar_ref_compnr), '0'), 38, 10) is null and 
                    src:twar_ref_compnr is not null) THEN
                    'twar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:twar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unbd), '0'), 38, 10) is null and 
                    src:unbd is not null) THEN
                    'unbd contains a non-numeric value : \'' || AS_VARCHAR(src:unbd) || '\'' WHEN 
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
                src:cnln,
                src:sern,
                src:cprj,
                src:dlvr,
                src:cono,
                src:cpla,
                src:schd,
                src:cspa,
                src:cact  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM700 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acpn), '0'), 38, 10) is null and 
                    src:acpn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amts), '0'), 38, 10) is null and 
                    src:amts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amts_cntc), '0'), 38, 10) is null and 
                    src:amts_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amts_dwhc), '0'), 38, 10) is null and 
                    src:amts_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amts_homc), '0'), 38, 10) is null and 
                    src:amts_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amts_prjc), '0'), 38, 10) is null and 
                    src:amts_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amts_refc), '0'), 38, 10) is null and 
                    src:amts_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amts_rpc1), '0'), 38, 10) is null and 
                    src:amts_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amts_rpc2), '0'), 38, 10) is null and 
                    src:amts_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aqan), '0'), 38, 10) is null and 
                    src:aqan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:baor), '0'), 38, 10) is null and 
                    src:baor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrb), '0'), 38, 10) is null and 
                    src:bgrb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrq), '0'), 38, 10) is null and 
                    src:bgrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blck), '0'), 38, 10) is null and 
                    src:blck is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bqan), '0'), 38, 10) is null and 
                    src:bqan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:buni_ref_compnr), '0'), 38, 10) is null and 
                    src:buni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt), '0'), 38, 10) is null and 
                    src:camt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_cntc), '0'), 38, 10) is null and 
                    src:camt_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_dwhc), '0'), 38, 10) is null and 
                    src:camt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_homc), '0'), 38, 10) is null and 
                    src:camt_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_prjc), '0'), 38, 10) is null and 
                    src:camt_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_refc), '0'), 38, 10) is null and 
                    src:camt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_rpc1), '0'), 38, 10) is null and 
                    src:camt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_rpc2), '0'), 38, 10) is null and 
                    src:camt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:carr_ref_compnr), '0'), 38, 10) is null and 
                    src:carr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccfm), '0'), 38, 10) is null and 
                    src:ccfm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cddt), '1900-01-01')) is null and 
                    src:cddt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is null and 
                    src:citt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cnel_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cnel_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cnpl_cnac_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cnpl_cnac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocu_ref_compnr), '0'), 38, 10) is null and 
                    src:cocu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_milt_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_milt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpva), '0'), 38, 10) is null and 
                    src:cpva is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cqan), '0'), 38, 10) is null and 
                    src:cqan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crbn), '0'), 38, 10) is null and 
                    src:crbn is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspg), '0'), 38, 10) is null and 
                    src:cspg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv_cntc), '0'), 38, 10) is null and 
                    src:cstv_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv_dwhc), '0'), 38, 10) is null and 
                    src:cstv_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv_homc), '0'), 38, 10) is null and 
                    src:cstv_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv_prjc), '0'), 38, 10) is null and 
                    src:cstv_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv_refc), '0'), 38, 10) is null and 
                    src:cstv_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv_rpc1), '0'), 38, 10) is null and 
                    src:cstv_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv_rpc2), '0'), 38, 10) is null and 
                    src:cstv_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delt), '0'), 38, 10) is null and 
                    src:delt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc), '0'), 38, 10) is null and 
                    src:disc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlor), '0'), 38, 10) is null and 
                    src:dlor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlvr), '0'), 38, 10) is null and 
                    src:dlvr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg), '0'), 38, 10) is null and 
                    src:dorg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dspr), '0'), 38, 10) is null and 
                    src:dspr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is null and 
                    src:exrs_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:iddt), '1900-01-01')) is null and 
                    src:iddt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_site_ref_compnr), '0'), 38, 10) is null and 
                    src:item_site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcrq), '0'), 38, 10) is null and 
                    src:lcrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam), '0'), 38, 10) is null and 
                    src:ldam is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lssn_ref_compnr), '0'), 38, 10) is null and 
                    src:lssn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltsl), '0'), 38, 10) is null and 
                    src:ltsl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oqan), '0'), 38, 10) is null and 
                    src:oqan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osch), '0'), 38, 10) is null and 
                    src:osch is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ouni_ref_compnr), '0'), 38, 10) is null and 
                    src:ouni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pass), '0'), 38, 10) is null and 
                    src:pass is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pddt), '1900-01-01')) is null and 
                    src:pddt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:porg), '0'), 38, 10) is null and 
                    src:porg is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsg_ref_compnr), '0'), 38, 10) is null and 
                    src:prsg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rddt), '1900-01-01')) is null and 
                    src:rddt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfdl), '0'), 38, 10) is null and 
                    src:rfdl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfic), '0'), 38, 10) is null and 
                    src:rfic is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfid), '0'), 38, 10) is null and 
                    src:rfid is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfsc), '0'), 38, 10) is null and 
                    src:rfsc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown), '0'), 38, 10) is null and 
                    src:rown is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rqan), '0'), 38, 10) is null and 
                    src:rqan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrsn_ref_compnr), '0'), 38, 10) is null and 
                    src:rrsn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_1), '0'), 38, 10) is null and 
                    src:rtcc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_2), '0'), 38, 10) is null and 
                    src:rtcc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_3), '0'), 38, 10) is null and 
                    src:rtcc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_1), '0'), 38, 10) is null and 
                    src:rtcs_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_2), '0'), 38, 10) is null and 
                    src:rtcs_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_3), '0'), 38, 10) is null and 
                    src:rtcs_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_1), '0'), 38, 10) is null and 
                    src:rtfc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_2), '0'), 38, 10) is null and 
                    src:rtfc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_3), '0'), 38, 10) is null and 
                    src:rtfc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_1), '0'), 38, 10) is null and 
                    src:rtfs_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_2), '0'), 38, 10) is null and 
                    src:rtfs_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_3), '0'), 38, 10) is null and 
                    src:rtfs_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtor), '0'), 38, 10) is null and 
                    src:rtor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sacu_ref_compnr), '0'), 38, 10) is null and 
                    src:sacu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt), '0'), 38, 10) is null and 
                    src:samt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_cntc), '0'), 38, 10) is null and 
                    src:samt_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_dwhc), '0'), 38, 10) is null and 
                    src:samt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_homc), '0'), 38, 10) is null and 
                    src:samt_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_prjc), '0'), 38, 10) is null and 
                    src:samt_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_refc), '0'), 38, 10) is null and 
                    src:samt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_rpc1), '0'), 38, 10) is null and 
                    src:samt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_rpc2), '0'), 38, 10) is null and 
                    src:samt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schd), '0'), 38, 10) is null and 
                    src:schd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdsc), '0'), 38, 10) is null and 
                    src:sdsc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serv_ref_compnr), '0'), 38, 10) is null and 
                    src:serv_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shcn), '0'), 38, 10) is null and 
                    src:shcn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sign), '0'), 38, 10) is null and 
                    src:sign is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqan), '0'), 38, 10) is null and 
                    src:sqan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srsl), '0'), 38, 10) is null and 
                    src:srsl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_dlpt_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_dlpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is null and 
                    src:stcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:suni_ref_compnr), '0'), 38, 10) is null and 
                    src:suni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tatd_ref_compnr), '0'), 38, 10) is null and 
                    src:tatd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcco_ref_compnr), '0'), 38, 10) is null and 
                    src:tcco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcff), '0'), 38, 10) is null and 
                    src:tcff is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcst), '0'), 38, 10) is null and 
                    src:tcst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tdel), '0'), 38, 10) is null and 
                    src:tdel is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprj_ref_compnr), '0'), 38, 10) is null and 
                    src:tprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprj_tpla_ref_compnr), '0'), 38, 10) is null and 
                    src:tprj_tpla_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprj_tpla_tact_ref_compnr), '0'), 38, 10) is null and 
                    src:tprj_tpla_tact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprj_tspa_ref_compnr), '0'), 38, 10) is null and 
                    src:tprj_tspa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprj_tstl_ref_compnr), '0'), 38, 10) is null and 
                    src:tprj_tstl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:twar_ref_compnr), '0'), 38, 10) is null and 
                    src:twar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unbd), '0'), 38, 10) is null and 
                    src:unbd is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)