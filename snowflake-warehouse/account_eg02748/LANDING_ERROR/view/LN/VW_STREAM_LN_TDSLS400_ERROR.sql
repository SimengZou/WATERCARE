CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDSLS400_ERROR AS SELECT src, 'LN_TDSLS400' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:acdt), '1900-01-01')) is null and 
                    src:acdt is not null) THEN
                    'acdt contains a non-timestamp value : \'' || AS_VARCHAR(src:acdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:agen), '0'), 38, 10) is null and 
                    src:agen is not null) THEN
                    'agen contains a non-numeric value : \'' || AS_VARCHAR(src:agen) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:airp), '0'), 38, 10) is null and 
                    src:airp is not null) THEN
                    'airp contains a non-numeric value : \'' || AS_VARCHAR(src:airp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:akcd_ref_compnr), '0'), 38, 10) is null and 
                    src:akcd_ref_compnr is not null) THEN
                    'akcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:akcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrb), '0'), 38, 10) is null and 
                    src:bgrb is not null) THEN
                    'bgrb contains a non-numeric value : \'' || AS_VARCHAR(src:bgrb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrq), '0'), 38, 10) is null and 
                    src:bgrq is not null) THEN
                    'bgrq contains a non-numeric value : \'' || AS_VARCHAR(src:bgrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bkyn), '0'), 38, 10) is null and 
                    src:bkyn is not null) THEN
                    'bkyn contains a non-numeric value : \'' || AS_VARCHAR(src:bkyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bppr_ref_compnr), '0'), 38, 10) is null and 
                    src:bppr_ref_compnr is not null) THEN
                    'bppr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bppr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptx_ref_compnr), '0'), 38, 10) is null and 
                    src:bptx_ref_compnr is not null) THEN
                    'bptx_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptx_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) THEN
                    'cbrn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbrn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) THEN
                    'ccrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cfdt), '1900-01-01')) is null and 
                    src:cfdt is not null) THEN
                    'cfdt contains a non-timestamp value : \'' || AS_VARCHAR(src:cfdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) THEN
                    'cfrw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfrw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chrq), '0'), 38, 10) is null and 
                    src:chrq is not null) THEN
                    'chrq contains a non-numeric value : \'' || AS_VARCHAR(src:chrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clyn), '0'), 38, 10) is null and 
                    src:clyn is not null) THEN
                    'clyn contains a non-numeric value : \'' || AS_VARCHAR(src:clyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_rcmp), '0'), 38, 10) is null and 
                    src:cofc_rcmp is not null) THEN
                    'cofc_rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_rcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corg), '0'), 38, 10) is null and 
                    src:corg is not null) THEN
                    'corg contains a non-numeric value : \'' || AS_VARCHAR(src:corg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpls_ref_compnr), '0'), 38, 10) is null and 
                    src:cpls_ref_compnr is not null) THEN
                    'cpls_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpls_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crcd_ref_compnr), '0'), 38, 10) is null and 
                    src:crcd_ref_compnr is not null) THEN
                    'crcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) THEN
                    'creg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:creg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is null and 
                    src:crep_ref_compnr is not null) THEN
                    'crep_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crep_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crht), '0'), 38, 10) is null and 
                    src:crht is not null) THEN
                    'crht contains a non-numeric value : \'' || AS_VARCHAR(src:crht) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crht_ref_compnr), '0'), 38, 10) is null and 
                    src:crht_ref_compnr is not null) THEN
                    'crht_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crht_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crin), '0'), 38, 10) is null and 
                    src:crin is not null) THEN
                    'crin contains a non-numeric value : \'' || AS_VARCHAR(src:crin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cror), '0'), 38, 10) is null and 
                    src:cror is not null) THEN
                    'cror contains a non-numeric value : \'' || AS_VARCHAR(src:cror) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crrq), '0'), 38, 10) is null and 
                    src:crrq is not null) THEN
                    'crrq contains a non-numeric value : \'' || AS_VARCHAR(src:crrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crst), '0'), 38, 10) is null and 
                    src:crst is not null) THEN
                    'crst contains a non-numeric value : \'' || AS_VARCHAR(src:crst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) THEN
                    'crte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctcd_ref_compnr), '0'), 38, 10) is null and 
                    src:ctcd_ref_compnr is not null) THEN
                    'ctcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) THEN
                    'cvyn contains a non-numeric value : \'' || AS_VARCHAR(src:cvyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddat), '1900-01-01')) is null and 
                    src:ddat is not null) THEN
                    'ddat contains a non-timestamp value : \'' || AS_VARCHAR(src:ddat) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddtc), '1900-01-01')) is null and 
                    src:ddtc is not null) THEN
                    'ddtc contains a non-timestamp value : \'' || AS_VARCHAR(src:ddtc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delc_ref_compnr), '0'), 38, 10) is null and 
                    src:delc_ref_compnr is not null) THEN
                    'delc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:delc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ehis), '0'), 38, 10) is null and 
                    src:ehis is not null) THEN
                    'ehis contains a non-numeric value : \'' || AS_VARCHAR(src:ehis) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esin), '0'), 38, 10) is null and 
                    src:esin is not null) THEN
                    'esin contains a non-numeric value : \'' || AS_VARCHAR(src:esin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcrt), '0'), 38, 10) is null and 
                    src:fcrt is not null) THEN
                    'fcrt contains a non-numeric value : \'' || AS_VARCHAR(src:fcrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:fdpt_ref_compnr is not null) THEN
                    'fdpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fdpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frin), '0'), 38, 10) is null and 
                    src:frin is not null) THEN
                    'frin contains a non-numeric value : \'' || AS_VARCHAR(src:frin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:futo), '0'), 38, 10) is null and 
                    src:futo is not null) THEN
                    'futo contains a non-numeric value : \'' || AS_VARCHAR(src:futo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hdst), '0'), 38, 10) is null and 
                    src:hdst is not null) THEN
                    'hdst contains a non-numeric value : \'' || AS_VARCHAR(src:hdst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hism), '0'), 38, 10) is null and 
                    src:hism is not null) THEN
                    'hism contains a non-numeric value : \'' || AS_VARCHAR(src:hism) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiss), '0'), 38, 10) is null and 
                    src:hiss is not null) THEN
                    'hiss contains a non-numeric value : \'' || AS_VARCHAR(src:hiss) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:infr), '0'), 38, 10) is null and 
                    src:infr is not null) THEN
                    'infr contains a non-numeric value : \'' || AS_VARCHAR(src:infr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpl_ref_compnr), '0'), 38, 10) is null and 
                    src:inpl_ref_compnr is not null) THEN
                    'inpl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:inpl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itad_ref_compnr), '0'), 38, 10) is null and 
                    src:itad_ref_compnr is not null) THEN
                    'itad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) THEN
                    'itbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itcn_ref_compnr), '0'), 38, 10) is null and 
                    src:itcn_ref_compnr is not null) THEN
                    'itcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcrq), '0'), 38, 10) is null and 
                    src:lcrq is not null) THEN
                    'lcrq contains a non-numeric value : \'' || AS_VARCHAR(src:lcrq) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odis), '0'), 38, 10) is null and 
                    src:odis is not null) THEN
                    'odis contains a non-numeric value : \'' || AS_VARCHAR(src:odis) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odty), '0'), 38, 10) is null and 
                    src:odty is not null) THEN
                    'odty contains a non-numeric value : \'' || AS_VARCHAR(src:odty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) THEN
                    'ofad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) THEN
                    'ofcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_cosn_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_cosn_ref_compnr is not null) THEN
                    'orno_cosn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:orno_cosn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrp_ref_compnr), '0'), 38, 10) is null and 
                    src:osrp_ref_compnr is not null) THEN
                    'osrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:osrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcod_ref_compnr), '0'), 38, 10) is null and 
                    src:pcod_ref_compnr is not null) THEN
                    'pcod_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pcod_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is null and 
                    src:pfad_ref_compnr is not null) THEN
                    'pfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) THEN
                    'pfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:pfcn_ref_compnr is not null) THEN
                    'pfcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pldd_ref_compnr), '0'), 38, 10) is null and 
                    src:pldd_ref_compnr is not null) THEN
                    'pldd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pldd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) THEN
                    'prdt contains a non-timestamp value : \'' || AS_VARCHAR(src:prdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prno_ref_compnr), '0'), 38, 10) is null and 
                    src:prno_ref_compnr is not null) THEN
                    'prno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratt_ref_compnr), '0'), 38, 10) is null and 
                    src:ratt_ref_compnr is not null) THEN
                    'ratt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ratt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmp), '0'), 38, 10) is null and 
                    src:rcmp is not null) THEN
                    'rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:rcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:retr_ref_compnr), '0'), 38, 10) is null and 
                    src:retr_ref_compnr is not null) THEN
                    'retr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:retr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) THEN
                    'revn contains a non-numeric value : \'' || AS_VARCHAR(src:revn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ruso), '0'), 38, 10) is null and 
                    src:ruso is not null) THEN
                    'ruso contains a non-numeric value : \'' || AS_VARCHAR(src:ruso) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbim), '0'), 38, 10) is null and 
                    src:sbim is not null) THEN
                    'sbim contains a non-numeric value : \'' || AS_VARCHAR(src:sbim) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scon), '0'), 38, 10) is null and 
                    src:scon is not null) THEN
                    'scon contains a non-numeric value : \'' || AS_VARCHAR(src:scon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sotp_ref_compnr), '0'), 38, 10) is null and 
                    src:sotp_ref_compnr is not null) THEN
                    'sotp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sotp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) THEN
                    'stad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) THEN
                    'stbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is null and 
                    src:stcn_ref_compnr is not null) THEN
                    'stcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stcn_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) THEN
                    'txtb contains a non-numeric value : \'' || AS_VARCHAR(src:txtb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) THEN
                    'txtb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtb_ref_compnr) || '\'' WHEN 
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
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDSLS400 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:acdt), '1900-01-01')) is null and 
                    src:acdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:agen), '0'), 38, 10) is null and 
                    src:agen is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:airp), '0'), 38, 10) is null and 
                    src:airp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:akcd_ref_compnr), '0'), 38, 10) is null and 
                    src:akcd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrb), '0'), 38, 10) is null and 
                    src:bgrb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrq), '0'), 38, 10) is null and 
                    src:bgrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bkyn), '0'), 38, 10) is null and 
                    src:bkyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bppr_ref_compnr), '0'), 38, 10) is null and 
                    src:bppr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptx_ref_compnr), '0'), 38, 10) is null and 
                    src:bptx_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cfdt), '1900-01-01')) is null and 
                    src:cfdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chrq), '0'), 38, 10) is null and 
                    src:chrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clyn), '0'), 38, 10) is null and 
                    src:clyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_rcmp), '0'), 38, 10) is null and 
                    src:cofc_rcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corg), '0'), 38, 10) is null and 
                    src:corg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpls_ref_compnr), '0'), 38, 10) is null and 
                    src:cpls_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crcd_ref_compnr), '0'), 38, 10) is null and 
                    src:crcd_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is null and 
                    src:crep_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crht), '0'), 38, 10) is null and 
                    src:crht is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crht_ref_compnr), '0'), 38, 10) is null and 
                    src:crht_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crin), '0'), 38, 10) is null and 
                    src:crin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cror), '0'), 38, 10) is null and 
                    src:cror is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crrq), '0'), 38, 10) is null and 
                    src:crrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crst), '0'), 38, 10) is null and 
                    src:crst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctcd_ref_compnr), '0'), 38, 10) is null and 
                    src:ctcd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddat), '1900-01-01')) is null and 
                    src:ddat is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddtc), '1900-01-01')) is null and 
                    src:ddtc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delc_ref_compnr), '0'), 38, 10) is null and 
                    src:delc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ehis), '0'), 38, 10) is null and 
                    src:ehis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esin), '0'), 38, 10) is null and 
                    src:esin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcrt), '0'), 38, 10) is null and 
                    src:fcrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:fdpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frin), '0'), 38, 10) is null and 
                    src:frin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:futo), '0'), 38, 10) is null and 
                    src:futo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hdst), '0'), 38, 10) is null and 
                    src:hdst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hism), '0'), 38, 10) is null and 
                    src:hism is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiss), '0'), 38, 10) is null and 
                    src:hiss is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:infr), '0'), 38, 10) is null and 
                    src:infr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpl_ref_compnr), '0'), 38, 10) is null and 
                    src:inpl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itad_ref_compnr), '0'), 38, 10) is null and 
                    src:itad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itcn_ref_compnr), '0'), 38, 10) is null and 
                    src:itcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcrq), '0'), 38, 10) is null and 
                    src:lcrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:motv_ref_compnr), '0'), 38, 10) is null and 
                    src:motv_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mprm), '0'), 38, 10) is null and 
                    src:mprm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt), '0'), 38, 10) is null and 
                    src:oamt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:odat), '1900-01-01')) is null and 
                    src:odat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odis), '0'), 38, 10) is null and 
                    src:odis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odty), '0'), 38, 10) is null and 
                    src:odty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_cosn_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_cosn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrp_ref_compnr), '0'), 38, 10) is null and 
                    src:osrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcod_ref_compnr), '0'), 38, 10) is null and 
                    src:pcod_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is null and 
                    src:pfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:pfcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pldd_ref_compnr), '0'), 38, 10) is null and 
                    src:pldd_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prno_ref_compnr), '0'), 38, 10) is null and 
                    src:prno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratt_ref_compnr), '0'), 38, 10) is null and 
                    src:ratt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmp), '0'), 38, 10) is null and 
                    src:rcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:retr_ref_compnr), '0'), 38, 10) is null and 
                    src:retr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ruso), '0'), 38, 10) is null and 
                    src:ruso is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbim), '0'), 38, 10) is null and 
                    src:sbim is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scon), '0'), 38, 10) is null and 
                    src:scon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sotp_ref_compnr), '0'), 38, 10) is null and 
                    src:sotp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is null and 
                    src:stcn_ref_compnr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)