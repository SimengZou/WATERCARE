CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDPUR400_ERROR AS SELECT src, 'LN_TDPUR400' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:akcd_ref_compnr), '0'), 38, 10) is null and 
                    src:akcd_ref_compnr is not null) THEN
                    'akcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:akcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrb), '0'), 38, 10) is null and 
                    src:bgrb is not null) THEN
                    'bgrb contains a non-numeric value : \'' || AS_VARCHAR(src:bgrb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrq), '0'), 38, 10) is null and 
                    src:bgrq is not null) THEN
                    'bgrq contains a non-numeric value : \'' || AS_VARCHAR(src:bgrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bppr_ref_compnr), '0'), 38, 10) is null and 
                    src:bppr_ref_compnr is not null) THEN
                    'bppr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bppr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptx_ref_compnr), '0'), 38, 10) is null and 
                    src:bptx_ref_compnr is not null) THEN
                    'bptx_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptx_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) THEN
                    'cadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) THEN
                    'cbrn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbrn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccon_ref_compnr), '0'), 38, 10) is null and 
                    src:ccon_ref_compnr is not null) THEN
                    'ccon_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccon_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) THEN
                    'ccrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdf_adat), '1900-01-01')) is null and 
                    src:cdf_adat is not null) THEN
                    'cdf_adat contains a non-timestamp value : \'' || AS_VARCHAR(src:cdf_adat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cdf_cprj_ref_compnr is not null) THEN
                    'cdf_cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdf_crdt), '1900-01-01')) is null and 
                    src:cdf_crdt is not null) THEN
                    'cdf_crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:cdf_crdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdf_lmdt), '1900-01-01')) is null and 
                    src:cdf_lmdt is not null) THEN
                    'cdf_lmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:cdf_lmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) THEN
                    'cfrw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfrw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chrq), '0'), 38, 10) is null and 
                    src:chrq is not null) THEN
                    'chrq contains a non-numeric value : \'' || AS_VARCHAR(src:chrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comm), '0'), 38, 10) is null and 
                    src:comm is not null) THEN
                    'comm contains a non-numeric value : \'' || AS_VARCHAR(src:comm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corg), '0'), 38, 10) is null and 
                    src:corg is not null) THEN
                    'corg contains a non-numeric value : \'' || AS_VARCHAR(src:corg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp_ref_compnr), '0'), 38, 10) is null and 
                    src:cotp_ref_compnr is not null) THEN
                    'cotp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cotp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cplp_ref_compnr), '0'), 38, 10) is null and 
                    src:cplp_ref_compnr is not null) THEN
                    'cplp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cplp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crcd_ref_compnr), '0'), 38, 10) is null and 
                    src:crcd_ref_compnr is not null) THEN
                    'crcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crcl), '0'), 38, 10) is null and 
                    src:crcl is not null) THEN
                    'crcl contains a non-numeric value : \'' || AS_VARCHAR(src:crcl) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) THEN
                    'creg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:creg_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctcd_ref_compnr), '0'), 38, 10) is null and 
                    src:ctcd_ref_compnr is not null) THEN
                    'ctcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctrj_ref_compnr), '0'), 38, 10) is null and 
                    src:ctrj_ref_compnr is not null) THEN
                    'ctrj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctrj_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:egen), '0'), 38, 10) is null and 
                    src:egen is not null) THEN
                    'egen contains a non-numeric value : \'' || AS_VARCHAR(src:egen) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etpc), '0'), 38, 10) is null and 
                    src:etpc is not null) THEN
                    'etpc contains a non-numeric value : \'' || AS_VARCHAR(src:etpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:fdpt_ref_compnr is not null) THEN
                    'fdpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fdpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hdst), '0'), 38, 10) is null and 
                    src:hdst is not null) THEN
                    'hdst contains a non-numeric value : \'' || AS_VARCHAR(src:hdst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hism), '0'), 38, 10) is null and 
                    src:hism is not null) THEN
                    'hism contains a non-numeric value : \'' || AS_VARCHAR(src:hism) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hisp), '0'), 38, 10) is null and 
                    src:hisp is not null) THEN
                    'hisp contains a non-numeric value : \'' || AS_VARCHAR(src:hisp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iafc), '0'), 38, 10) is null and 
                    src:iafc is not null) THEN
                    'iafc contains a non-numeric value : \'' || AS_VARCHAR(src:iafc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iebp), '0'), 38, 10) is null and 
                    src:iebp is not null) THEN
                    'iebp contains a non-numeric value : \'' || AS_VARCHAR(src:iebp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifad_ref_compnr), '0'), 38, 10) is null and 
                    src:ifad_ref_compnr is not null) THEN
                    'ifad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ifad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) THEN
                    'ifbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ifbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ifcn_ref_compnr is not null) THEN
                    'ifcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ifcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issp), '0'), 38, 10) is null and 
                    src:issp is not null) THEN
                    'issp contains a non-numeric value : \'' || AS_VARCHAR(src:issp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lccl_ref_compnr), '0'), 38, 10) is null and 
                    src:lccl_ref_compnr is not null) THEN
                    'lccl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lccl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcrq), '0'), 38, 10) is null and 
                    src:lcrq is not null) THEN
                    'lcrq contains a non-numeric value : \'' || AS_VARCHAR(src:lcrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfr), '0'), 38, 10) is null and 
                    src:mcfr is not null) THEN
                    'mcfr contains a non-numeric value : \'' || AS_VARCHAR(src:mcfr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_cosn_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_cosn_ref_compnr is not null) THEN
                    'orno_cosn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:orno_cosn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otad_ref_compnr), '0'), 38, 10) is null and 
                    src:otad_ref_compnr is not null) THEN
                    'otad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otcn_ref_compnr), '0'), 38, 10) is null and 
                    src:otcn_ref_compnr is not null) THEN
                    'otcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paft), '0'), 38, 10) is null and 
                    src:paft is not null) THEN
                    'paft contains a non-numeric value : \'' || AS_VARCHAR(src:paft) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plnr_ref_compnr), '0'), 38, 10) is null and 
                    src:plnr_ref_compnr is not null) THEN
                    'plnr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:plnr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prno_ref_compnr), '0'), 38, 10) is null and 
                    src:prno_ref_compnr is not null) THEN
                    'prno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptad_ref_compnr), '0'), 38, 10) is null and 
                    src:ptad_ref_compnr is not null) THEN
                    'ptad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_ref_compnr is not null) THEN
                    'ptbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ptcn_ref_compnr is not null) THEN
                    'ptcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_dtwc), '0'), 38, 10) is null and 
                    src:ratc_dtwc is not null) THEN
                    'ratc_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_lclc), '0'), 38, 10) is null and 
                    src:ratc_lclc is not null) THEN
                    'ratc_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_rfrc), '0'), 38, 10) is null and 
                    src:ratc_rfrc is not null) THEN
                    'ratc_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_rpc1), '0'), 38, 10) is null and 
                    src:ratc_rpc1 is not null) THEN
                    'ratc_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_rpc2), '0'), 38, 10) is null and 
                    src:ratc_rpc2 is not null) THEN
                    'ratc_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_rpc2) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratp_1), '0'), 38, 10) is null and 
                    src:ratp_1 is not null) THEN
                    'ratp_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratp_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratp_2), '0'), 38, 10) is null and 
                    src:ratp_2 is not null) THEN
                    'ratp_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratp_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratp_3), '0'), 38, 10) is null and 
                    src:ratp_3 is not null) THEN
                    'ratp_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratp_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratt_ref_compnr), '0'), 38, 10) is null and 
                    src:ratt_ref_compnr is not null) THEN
                    'ratt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ratt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:raur), '0'), 38, 10) is null and 
                    src:raur is not null) THEN
                    'raur contains a non-numeric value : \'' || AS_VARCHAR(src:raur) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:retr_ref_compnr), '0'), 38, 10) is null and 
                    src:retr_ref_compnr is not null) THEN
                    'retr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:retr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) THEN
                    'revn contains a non-numeric value : \'' || AS_VARCHAR(src:revn) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfad_ref_compnr), '0'), 38, 10) is null and 
                    src:sfad_ref_compnr is not null) THEN
                    'sfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) THEN
                    'sfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:sfcn_ref_compnr is not null) THEN
                    'sfcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spss_ref_compnr), '0'), 38, 10) is null and 
                    src:spss_ref_compnr is not null) THEN
                    'spss_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:spss_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
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
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR400 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:akcd_ref_compnr), '0'), 38, 10) is null and 
                    src:akcd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrb), '0'), 38, 10) is null and 
                    src:bgrb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgrq), '0'), 38, 10) is null and 
                    src:bgrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bppr_ref_compnr), '0'), 38, 10) is null and 
                    src:bppr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptx_ref_compnr), '0'), 38, 10) is null and 
                    src:bptx_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccon_ref_compnr), '0'), 38, 10) is null and 
                    src:ccon_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdf_adat), '1900-01-01')) is null and 
                    src:cdf_adat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cdf_cprj_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdf_crdt), '1900-01-01')) is null and 
                    src:cdf_crdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdf_lmdt), '1900-01-01')) is null and 
                    src:cdf_lmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chrq), '0'), 38, 10) is null and 
                    src:chrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comm), '0'), 38, 10) is null and 
                    src:comm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corg), '0'), 38, 10) is null and 
                    src:corg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp_ref_compnr), '0'), 38, 10) is null and 
                    src:cotp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cplp_ref_compnr), '0'), 38, 10) is null and 
                    src:cplp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crcd_ref_compnr), '0'), 38, 10) is null and 
                    src:crcd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crcl), '0'), 38, 10) is null and 
                    src:crcl is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctcd_ref_compnr), '0'), 38, 10) is null and 
                    src:ctcd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctrj_ref_compnr), '0'), 38, 10) is null and 
                    src:ctrj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddat), '1900-01-01')) is null and 
                    src:ddat is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddtc), '1900-01-01')) is null and 
                    src:ddtc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:egen), '0'), 38, 10) is null and 
                    src:egen is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etpc), '0'), 38, 10) is null and 
                    src:etpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:fdpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hdst), '0'), 38, 10) is null and 
                    src:hdst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hism), '0'), 38, 10) is null and 
                    src:hism is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hisp), '0'), 38, 10) is null and 
                    src:hisp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iafc), '0'), 38, 10) is null and 
                    src:iafc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iebp), '0'), 38, 10) is null and 
                    src:iebp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifad_ref_compnr), '0'), 38, 10) is null and 
                    src:ifad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ifcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issp), '0'), 38, 10) is null and 
                    src:issp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lccl_ref_compnr), '0'), 38, 10) is null and 
                    src:lccl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcrq), '0'), 38, 10) is null and 
                    src:lcrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfr), '0'), 38, 10) is null and 
                    src:mcfr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt), '0'), 38, 10) is null and 
                    src:oamt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:odat), '1900-01-01')) is null and 
                    src:odat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odis), '0'), 38, 10) is null and 
                    src:odis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odty), '0'), 38, 10) is null and 
                    src:odty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_cosn_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_cosn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otad_ref_compnr), '0'), 38, 10) is null and 
                    src:otad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otcn_ref_compnr), '0'), 38, 10) is null and 
                    src:otcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paft), '0'), 38, 10) is null and 
                    src:paft is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plnr_ref_compnr), '0'), 38, 10) is null and 
                    src:plnr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prno_ref_compnr), '0'), 38, 10) is null and 
                    src:prno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptad_ref_compnr), '0'), 38, 10) is null and 
                    src:ptad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ptcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_dtwc), '0'), 38, 10) is null and 
                    src:ratc_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_lclc), '0'), 38, 10) is null and 
                    src:ratc_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_rfrc), '0'), 38, 10) is null and 
                    src:ratc_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_rpc1), '0'), 38, 10) is null and 
                    src:ratc_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_rpc2), '0'), 38, 10) is null and 
                    src:ratc_rpc2 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ratd), '1900-01-01')) is null and 
                    src:ratd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratp_1), '0'), 38, 10) is null and 
                    src:ratp_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratp_2), '0'), 38, 10) is null and 
                    src:ratp_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratp_3), '0'), 38, 10) is null and 
                    src:ratp_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratt_ref_compnr), '0'), 38, 10) is null and 
                    src:ratt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:raur), '0'), 38, 10) is null and 
                    src:raur is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:retr_ref_compnr), '0'), 38, 10) is null and 
                    src:retr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsta), '0'), 38, 10) is null and 
                    src:rsta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbim), '0'), 38, 10) is null and 
                    src:sbim is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbmt_ref_compnr), '0'), 38, 10) is null and 
                    src:sbmt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sclb), '0'), 38, 10) is null and 
                    src:sclb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfad_ref_compnr), '0'), 38, 10) is null and 
                    src:sfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:sfcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spss_ref_compnr), '0'), 38, 10) is null and 
                    src:spss_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
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