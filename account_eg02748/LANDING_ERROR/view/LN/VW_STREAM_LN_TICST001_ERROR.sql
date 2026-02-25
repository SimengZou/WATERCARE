CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TICST001_ERROR AS SELECT src, 'LN_TICST001' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_1), '0'), 38, 10) is null and 
                    src:aamt_1 is not null) THEN
                    'aamt_1 contains a non-numeric value : \'' || AS_VARCHAR(src:aamt_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_2), '0'), 38, 10) is null and 
                    src:aamt_2 is not null) THEN
                    'aamt_2 contains a non-numeric value : \'' || AS_VARCHAR(src:aamt_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_3), '0'), 38, 10) is null and 
                    src:aamt_3 is not null) THEN
                    'aamt_3 contains a non-numeric value : \'' || AS_VARCHAR(src:aamt_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_dwhc), '0'), 38, 10) is null and 
                    src:aamt_dwhc is not null) THEN
                    'aamt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:aamt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_lclc), '0'), 38, 10) is null and 
                    src:aamt_lclc is not null) THEN
                    'aamt_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:aamt_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_refc), '0'), 38, 10) is null and 
                    src:aamt_refc is not null) THEN
                    'aamt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:aamt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_rpc1), '0'), 38, 10) is null and 
                    src:aamt_rpc1 is not null) THEN
                    'aamt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:aamt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_rpc2), '0'), 38, 10) is null and 
                    src:aamt_rpc2 is not null) THEN
                    'aamt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:aamt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamu_1), '0'), 38, 10) is null and 
                    src:aamu_1 is not null) THEN
                    'aamu_1 contains a non-numeric value : \'' || AS_VARCHAR(src:aamu_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamu_2), '0'), 38, 10) is null and 
                    src:aamu_2 is not null) THEN
                    'aamu_2 contains a non-numeric value : \'' || AS_VARCHAR(src:aamu_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamu_3), '0'), 38, 10) is null and 
                    src:aamu_3 is not null) THEN
                    'aamu_3 contains a non-numeric value : \'' || AS_VARCHAR(src:aamu_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamu_dwhc), '0'), 38, 10) is null and 
                    src:aamu_dwhc is not null) THEN
                    'aamu_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:aamu_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamu_lclc), '0'), 38, 10) is null and 
                    src:aamu_lclc is not null) THEN
                    'aamu_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:aamu_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamu_refc), '0'), 38, 10) is null and 
                    src:aamu_refc is not null) THEN
                    'aamu_refc contains a non-numeric value : \'' || AS_VARCHAR(src:aamu_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamu_rpc1), '0'), 38, 10) is null and 
                    src:aamu_rpc1 is not null) THEN
                    'aamu_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:aamu_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamu_rpc2), '0'), 38, 10) is null and 
                    src:aamu_rpc2 is not null) THEN
                    'aamu_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:aamu_rpc2) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:aldt), '1900-01-01')) is null and 
                    src:aldt is not null) THEN
                    'aldt contains a non-timestamp value : \'' || AS_VARCHAR(src:aldt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alfp), '0'), 38, 10) is null and 
                    src:alfp is not null) THEN
                    'alfp contains a non-numeric value : \'' || AS_VARCHAR(src:alfp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:almi), '0'), 38, 10) is null and 
                    src:almi is not null) THEN
                    'almi contains a non-numeric value : \'' || AS_VARCHAR(src:almi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:altp), '0'), 38, 10) is null and 
                    src:altp is not null) THEN
                    'altp contains a non-numeric value : \'' || AS_VARCHAR(src:altp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alty), '0'), 38, 10) is null and 
                    src:alty is not null) THEN
                    'alty contains a non-numeric value : \'' || AS_VARCHAR(src:alty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_dwhc), '0'), 38, 10) is null and 
                    src:ames_dwhc is not null) THEN
                    'ames_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:ames_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_lclc), '0'), 38, 10) is null and 
                    src:ames_lclc is not null) THEN
                    'ames_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:ames_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_refc), '0'), 38, 10) is null and 
                    src:ames_refc is not null) THEN
                    'ames_refc contains a non-numeric value : \'' || AS_VARCHAR(src:ames_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_rpc1), '0'), 38, 10) is null and 
                    src:ames_rpc1 is not null) THEN
                    'ames_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:ames_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_rpc2), '0'), 38, 10) is null and 
                    src:ames_rpc2 is not null) THEN
                    'ames_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:ames_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aues_dwhc), '0'), 38, 10) is null and 
                    src:aues_dwhc is not null) THEN
                    'aues_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:aues_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aues_lclc), '0'), 38, 10) is null and 
                    src:aues_lclc is not null) THEN
                    'aues_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:aues_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aues_refc), '0'), 38, 10) is null and 
                    src:aues_refc is not null) THEN
                    'aues_refc contains a non-numeric value : \'' || AS_VARCHAR(src:aues_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aues_rpc1), '0'), 38, 10) is null and 
                    src:aues_rpc1 is not null) THEN
                    'aues_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:aues_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aues_rpc2), '0'), 38, 10) is null and 
                    src:aues_rpc2 is not null) THEN
                    'aues_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:aues_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfls), '0'), 38, 10) is null and 
                    src:bfls is not null) THEN
                    'bfls contains a non-numeric value : \'' || AS_VARCHAR(src:bfls) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:boun_ref_compnr), '0'), 38, 10) is null and 
                    src:boun_ref_compnr is not null) THEN
                    'boun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:boun_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpon), '0'), 38, 10) is null and 
                    src:bpon is not null) THEN
                    'bpon contains a non-numeric value : \'' || AS_VARCHAR(src:bpon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bseq), '0'), 38, 10) is null and 
                    src:bseq is not null) THEN
                    'bseq contains a non-numeric value : \'' || AS_VARCHAR(src:bseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bwar_ref_compnr), '0'), 38, 10) is null and 
                    src:bwar_ref_compnr is not null) THEN
                    'bwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctt), '0'), 38, 10) is null and 
                    src:cctt is not null) THEN
                    'cctt contains a non-numeric value : \'' || AS_VARCHAR(src:cctt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcs_1), '0'), 38, 10) is null and 
                    src:cpcs_1 is not null) THEN
                    'cpcs_1 contains a non-numeric value : \'' || AS_VARCHAR(src:cpcs_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcs_2), '0'), 38, 10) is null and 
                    src:cpcs_2 is not null) THEN
                    'cpcs_2 contains a non-numeric value : \'' || AS_VARCHAR(src:cpcs_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcs_3), '0'), 38, 10) is null and 
                    src:cpcs_3 is not null) THEN
                    'cpcs_3 contains a non-numeric value : \'' || AS_VARCHAR(src:cpcs_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcs_dwhc), '0'), 38, 10) is null and 
                    src:cpcs_dwhc is not null) THEN
                    'cpcs_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:cpcs_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcs_lclc), '0'), 38, 10) is null and 
                    src:cpcs_lclc is not null) THEN
                    'cpcs_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:cpcs_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcs_refc), '0'), 38, 10) is null and 
                    src:cpcs_refc is not null) THEN
                    'cpcs_refc contains a non-numeric value : \'' || AS_VARCHAR(src:cpcs_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcs_rpc1), '0'), 38, 10) is null and 
                    src:cpcs_rpc1 is not null) THEN
                    'cpcs_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:cpcs_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcs_rpc2), '0'), 38, 10) is null and 
                    src:cpcs_rpc2 is not null) THEN
                    'cpcs_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:cpcs_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_1), '0'), 38, 10) is null and 
                    src:cpcu_1 is not null) THEN
                    'cpcu_1 contains a non-numeric value : \'' || AS_VARCHAR(src:cpcu_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_2), '0'), 38, 10) is null and 
                    src:cpcu_2 is not null) THEN
                    'cpcu_2 contains a non-numeric value : \'' || AS_VARCHAR(src:cpcu_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_3), '0'), 38, 10) is null and 
                    src:cpcu_3 is not null) THEN
                    'cpcu_3 contains a non-numeric value : \'' || AS_VARCHAR(src:cpcu_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_dwhc), '0'), 38, 10) is null and 
                    src:cpcu_dwhc is not null) THEN
                    'cpcu_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:cpcu_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_lclc), '0'), 38, 10) is null and 
                    src:cpcu_lclc is not null) THEN
                    'cpcu_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:cpcu_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_refc), '0'), 38, 10) is null and 
                    src:cpcu_refc is not null) THEN
                    'cpcu_refc contains a non-numeric value : \'' || AS_VARCHAR(src:cpcu_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_rpc1), '0'), 38, 10) is null and 
                    src:cpcu_rpc1 is not null) THEN
                    'cpcu_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:cpcu_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_rpc2), '0'), 38, 10) is null and 
                    src:cpcu_rpc2 is not null) THEN
                    'cpcu_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:cpcu_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpes_1), '0'), 38, 10) is null and 
                    src:cpes_1 is not null) THEN
                    'cpes_1 contains a non-numeric value : \'' || AS_VARCHAR(src:cpes_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpes_2), '0'), 38, 10) is null and 
                    src:cpes_2 is not null) THEN
                    'cpes_2 contains a non-numeric value : \'' || AS_VARCHAR(src:cpes_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpes_3), '0'), 38, 10) is null and 
                    src:cpes_3 is not null) THEN
                    'cpes_3 contains a non-numeric value : \'' || AS_VARCHAR(src:cpes_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpes_dwhc), '0'), 38, 10) is null and 
                    src:cpes_dwhc is not null) THEN
                    'cpes_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:cpes_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpes_eam1), '0'), 38, 10) is null and 
                    src:cpes_eam1 is not null) THEN
                    'cpes_eam1 contains a non-numeric value : \'' || AS_VARCHAR(src:cpes_eam1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpes_eam2), '0'), 38, 10) is null and 
                    src:cpes_eam2 is not null) THEN
                    'cpes_eam2 contains a non-numeric value : \'' || AS_VARCHAR(src:cpes_eam2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpes_eam3), '0'), 38, 10) is null and 
                    src:cpes_eam3 is not null) THEN
                    'cpes_eam3 contains a non-numeric value : \'' || AS_VARCHAR(src:cpes_eam3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpes_refc), '0'), 38, 10) is null and 
                    src:cpes_refc is not null) THEN
                    'cpes_refc contains a non-numeric value : \'' || AS_VARCHAR(src:cpes_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cues_1), '0'), 38, 10) is null and 
                    src:cues_1 is not null) THEN
                    'cues_1 contains a non-numeric value : \'' || AS_VARCHAR(src:cues_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cues_2), '0'), 38, 10) is null and 
                    src:cues_2 is not null) THEN
                    'cues_2 contains a non-numeric value : \'' || AS_VARCHAR(src:cues_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cues_3), '0'), 38, 10) is null and 
                    src:cues_3 is not null) THEN
                    'cues_3 contains a non-numeric value : \'' || AS_VARCHAR(src:cues_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cues_dwhc), '0'), 38, 10) is null and 
                    src:cues_dwhc is not null) THEN
                    'cues_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:cues_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cues_eau1), '0'), 38, 10) is null and 
                    src:cues_eau1 is not null) THEN
                    'cues_eau1 contains a non-numeric value : \'' || AS_VARCHAR(src:cues_eau1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cues_eau2), '0'), 38, 10) is null and 
                    src:cues_eau2 is not null) THEN
                    'cues_eau2 contains a non-numeric value : \'' || AS_VARCHAR(src:cues_eau2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cues_eau3), '0'), 38, 10) is null and 
                    src:cues_eau3 is not null) THEN
                    'cues_eau3 contains a non-numeric value : \'' || AS_VARCHAR(src:cues_eau3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cues_refc), '0'), 38, 10) is null and 
                    src:cues_refc is not null) THEN
                    'cues_refc contains a non-numeric value : \'' || AS_VARCHAR(src:cues_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dptm_fcmp), '0'), 38, 10) is null and 
                    src:dptm_fcmp is not null) THEN
                    'dptm_fcmp contains a non-numeric value : \'' || AS_VARCHAR(src:dptm_fcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:drin), '0'), 38, 10) is null and 
                    src:drin is not null) THEN
                    'drin contains a non-numeric value : \'' || AS_VARCHAR(src:drin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dris), '0'), 38, 10) is null and 
                    src:dris is not null) THEN
                    'dris contains a non-numeric value : \'' || AS_VARCHAR(src:dris) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dstp), '0'), 38, 10) is null and 
                    src:dstp is not null) THEN
                    'dstp contains a non-numeric value : \'' || AS_VARCHAR(src:dstp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) THEN
                    'effn contains a non-numeric value : \'' || AS_VARCHAR(src:effn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icfm), '0'), 38, 10) is null and 
                    src:icfm is not null) THEN
                    'icfm contains a non-numeric value : \'' || AS_VARCHAR(src:icfm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ippg), '0'), 38, 10) is null and 
                    src:ippg is not null) THEN
                    'ippg contains a non-numeric value : \'' || AS_VARCHAR(src:ippg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issu), '0'), 38, 10) is null and 
                    src:issu is not null) THEN
                    'issu contains a non-numeric value : \'' || AS_VARCHAR(src:issu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issu_buar), '0'), 38, 10) is null and 
                    src:issu_buar is not null) THEN
                    'issu_buar contains a non-numeric value : \'' || AS_VARCHAR(src:issu_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issu_buln), '0'), 38, 10) is null and 
                    src:issu_buln is not null) THEN
                    'issu_buln contains a non-numeric value : \'' || AS_VARCHAR(src:issu_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issu_bupc), '0'), 38, 10) is null and 
                    src:issu_bupc is not null) THEN
                    'issu_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:issu_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issu_butm), '0'), 38, 10) is null and 
                    src:issu_butm is not null) THEN
                    'issu_butm contains a non-numeric value : \'' || AS_VARCHAR(src:issu_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issu_buvl), '0'), 38, 10) is null and 
                    src:issu_buvl is not null) THEN
                    'issu_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:issu_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issu_buwg), '0'), 38, 10) is null and 
                    src:issu_buwg is not null) THEN
                    'issu_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:issu_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iswh), '0'), 38, 10) is null and 
                    src:iswh is not null) THEN
                    'iswh contains a non-numeric value : \'' || AS_VARCHAR(src:iswh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iswh_buar), '0'), 38, 10) is null and 
                    src:iswh_buar is not null) THEN
                    'iswh_buar contains a non-numeric value : \'' || AS_VARCHAR(src:iswh_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iswh_buln), '0'), 38, 10) is null and 
                    src:iswh_buln is not null) THEN
                    'iswh_buln contains a non-numeric value : \'' || AS_VARCHAR(src:iswh_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iswh_bupc), '0'), 38, 10) is null and 
                    src:iswh_bupc is not null) THEN
                    'iswh_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:iswh_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iswh_butm), '0'), 38, 10) is null and 
                    src:iswh_butm is not null) THEN
                    'iswh_butm contains a non-numeric value : \'' || AS_VARCHAR(src:iswh_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iswh_buvl), '0'), 38, 10) is null and 
                    src:iswh_buvl is not null) THEN
                    'iswh_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:iswh_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iswh_buwg), '0'), 38, 10) is null and 
                    src:iswh_buwg is not null) THEN
                    'iswh_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:iswh_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_rcmp), '0'), 38, 10) is null and 
                    src:item_rcmp is not null) THEN
                    'item_rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:item_rcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leng), '0'), 38, 10) is null and 
                    src:leng is not null) THEN
                    'leng contains a non-numeric value : \'' || AS_VARCHAR(src:leng) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpno), '0'), 38, 10) is null and 
                    src:lpno is not null) THEN
                    'lpno contains a non-numeric value : \'' || AS_VARCHAR(src:lpno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsel), '0'), 38, 10) is null and 
                    src:lsel is not null) THEN
                    'lsel contains a non-numeric value : \'' || AS_VARCHAR(src:lsel) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcmd), '0'), 38, 10) is null and 
                    src:mcmd is not null) THEN
                    'mcmd contains a non-numeric value : \'' || AS_VARCHAR(src:mcmd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:noun), '0'), 38, 10) is null and 
                    src:noun is not null) THEN
                    'noun contains a non-numeric value : \'' || AS_VARCHAR(src:noun) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opno), '0'), 38, 10) is null and 
                    src:opno is not null) THEN
                    'opno contains a non-numeric value : \'' || AS_VARCHAR(src:opno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owns), '0'), 38, 10) is null and 
                    src:owns is not null) THEN
                    'owns contains a non-numeric value : \'' || AS_VARCHAR(src:owns) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdno_ref_compnr), '0'), 38, 10) is null and 
                    src:pdno_ref_compnr is not null) THEN
                    'pdno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pdno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:preq), '0'), 38, 10) is null and 
                    src:preq is not null) THEN
                    'preq contains a non-numeric value : \'' || AS_VARCHAR(src:preq) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqur), '0'), 38, 10) is null and 
                    src:qqur is not null) THEN
                    'qqur contains a non-numeric value : \'' || AS_VARCHAR(src:qqur) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscr), '0'), 38, 10) is null and 
                    src:qscr is not null) THEN
                    'qscr contains a non-numeric value : \'' || AS_VARCHAR(src:qscr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscr_buar), '0'), 38, 10) is null and 
                    src:qscr_buar is not null) THEN
                    'qscr_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qscr_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscr_buln), '0'), 38, 10) is null and 
                    src:qscr_buln is not null) THEN
                    'qscr_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qscr_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscr_bupc), '0'), 38, 10) is null and 
                    src:qscr_bupc is not null) THEN
                    'qscr_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qscr_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscr_butm), '0'), 38, 10) is null and 
                    src:qscr_butm is not null) THEN
                    'qscr_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qscr_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscr_buvl), '0'), 38, 10) is null and 
                    src:qscr_buvl is not null) THEN
                    'qscr_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qscr_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscr_buwg), '0'), 38, 10) is null and 
                    src:qscr_buwg is not null) THEN
                    'qscr_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qscr_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qspl), '0'), 38, 10) is null and 
                    src:qspl is not null) THEN
                    'qspl contains a non-numeric value : \'' || AS_VARCHAR(src:qspl) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qucs), '0'), 38, 10) is null and 
                    src:qucs is not null) THEN
                    'qucs contains a non-numeric value : \'' || AS_VARCHAR(src:qucs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qucs_buar), '0'), 38, 10) is null and 
                    src:qucs_buar is not null) THEN
                    'qucs_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qucs_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qucs_buln), '0'), 38, 10) is null and 
                    src:qucs_buln is not null) THEN
                    'qucs_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qucs_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qucs_bupc), '0'), 38, 10) is null and 
                    src:qucs_bupc is not null) THEN
                    'qucs_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qucs_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qucs_butm), '0'), 38, 10) is null and 
                    src:qucs_butm is not null) THEN
                    'qucs_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qucs_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qucs_buvl), '0'), 38, 10) is null and 
                    src:qucs_buvl is not null) THEN
                    'qucs_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qucs_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qucs_buwg), '0'), 38, 10) is null and 
                    src:qucs_buwg is not null) THEN
                    'qucs_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qucs_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ques), '0'), 38, 10) is null and 
                    src:ques is not null) THEN
                    'ques contains a non-numeric value : \'' || AS_VARCHAR(src:ques) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ques_buar), '0'), 38, 10) is null and 
                    src:ques_buar is not null) THEN
                    'ques_buar contains a non-numeric value : \'' || AS_VARCHAR(src:ques_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ques_buln), '0'), 38, 10) is null and 
                    src:ques_buln is not null) THEN
                    'ques_buln contains a non-numeric value : \'' || AS_VARCHAR(src:ques_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ques_bupc), '0'), 38, 10) is null and 
                    src:ques_bupc is not null) THEN
                    'ques_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:ques_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ques_butm), '0'), 38, 10) is null and 
                    src:ques_butm is not null) THEN
                    'ques_butm contains a non-numeric value : \'' || AS_VARCHAR(src:ques_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ques_buvl), '0'), 38, 10) is null and 
                    src:ques_buvl is not null) THEN
                    'ques_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:ques_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ques_buwg), '0'), 38, 10) is null and 
                    src:ques_buwg is not null) THEN
                    'ques_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:ques_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qune), '0'), 38, 10) is null and 
                    src:qune is not null) THEN
                    'qune contains a non-numeric value : \'' || AS_VARCHAR(src:qune) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qune_buar), '0'), 38, 10) is null and 
                    src:qune_buar is not null) THEN
                    'qune_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qune_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qune_buln), '0'), 38, 10) is null and 
                    src:qune_buln is not null) THEN
                    'qune_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qune_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qune_bupc), '0'), 38, 10) is null and 
                    src:qune_bupc is not null) THEN
                    'qune_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qune_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qune_butm), '0'), 38, 10) is null and 
                    src:qune_butm is not null) THEN
                    'qune_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qune_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qune_buvl), '0'), 38, 10) is null and 
                    src:qune_buvl is not null) THEN
                    'qune_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qune_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qune_buwg), '0'), 38, 10) is null and 
                    src:qune_buwg is not null) THEN
                    'qune_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qune_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdsp), '0'), 38, 10) is null and 
                    src:rdsp is not null) THEN
                    'rdsp contains a non-numeric value : \'' || AS_VARCHAR(src:rdsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) THEN
                    'revn contains a non-numeric value : \'' || AS_VARCHAR(src:revn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtas), '0'), 38, 10) is null and 
                    src:rtas is not null) THEN
                    'rtas contains a non-numeric value : \'' || AS_VARCHAR(src:rtas) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rwit), '0'), 38, 10) is null and 
                    src:rwit is not null) THEN
                    'rwit contains a non-numeric value : \'' || AS_VARCHAR(src:rwit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sayn), '0'), 38, 10) is null and 
                    src:sayn is not null) THEN
                    'sayn contains a non-numeric value : \'' || AS_VARCHAR(src:sayn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbsr), '0'), 38, 10) is null and 
                    src:sbsr is not null) THEN
                    'sbsr contains a non-numeric value : \'' || AS_VARCHAR(src:sbsr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scdl), '0'), 38, 10) is null and 
                    src:scdl is not null) THEN
                    'scdl contains a non-numeric value : \'' || AS_VARCHAR(src:scdl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpf), '0'), 38, 10) is null and 
                    src:scpf is not null) THEN
                    'scpf contains a non-numeric value : \'' || AS_VARCHAR(src:scpf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq), '0'), 38, 10) is null and 
                    src:scpq is not null) THEN
                    'scpq contains a non-numeric value : \'' || AS_VARCHAR(src:scpq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buar), '0'), 38, 10) is null and 
                    src:scpq_buar is not null) THEN
                    'scpq_buar contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buln), '0'), 38, 10) is null and 
                    src:scpq_buln is not null) THEN
                    'scpq_buln contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_bupc), '0'), 38, 10) is null and 
                    src:scpq_bupc is not null) THEN
                    'scpq_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_butm), '0'), 38, 10) is null and 
                    src:scpq_butm is not null) THEN
                    'scpq_butm contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buvl), '0'), 38, 10) is null and 
                    src:scpq_buvl is not null) THEN
                    'scpq_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buwg), '0'), 38, 10) is null and 
                    src:scpq_buwg is not null) THEN
                    'scpq_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sitm_ref_compnr), '0'), 38, 10) is null and 
                    src:sitm_ref_compnr is not null) THEN
                    'sitm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sitm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sizu_ref_compnr), '0'), 38, 10) is null and 
                    src:sizu_ref_compnr is not null) THEN
                    'sizu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sizu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) THEN
                    'subc contains a non-numeric value : \'' || AS_VARCHAR(src:subc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subd), '0'), 38, 10) is null and 
                    src:subd is not null) THEN
                    'subd contains a non-numeric value : \'' || AS_VARCHAR(src:subd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subd_buar), '0'), 38, 10) is null and 
                    src:subd_buar is not null) THEN
                    'subd_buar contains a non-numeric value : \'' || AS_VARCHAR(src:subd_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subd_buln), '0'), 38, 10) is null and 
                    src:subd_buln is not null) THEN
                    'subd_buln contains a non-numeric value : \'' || AS_VARCHAR(src:subd_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subd_bupc), '0'), 38, 10) is null and 
                    src:subd_bupc is not null) THEN
                    'subd_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:subd_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subd_butm), '0'), 38, 10) is null and 
                    src:subd_butm is not null) THEN
                    'subd_butm contains a non-numeric value : \'' || AS_VARCHAR(src:subd_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subd_buvl), '0'), 38, 10) is null and 
                    src:subd_buvl is not null) THEN
                    'subd_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:subd_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subd_buwg), '0'), 38, 10) is null and 
                    src:subd_buwg is not null) THEN
                    'subd_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:subd_buwg) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
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
                                    
                src:compnr,
                src:pdno,
                src:pono  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TICST001 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_1), '0'), 38, 10) is null and 
                    src:aamt_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_2), '0'), 38, 10) is null and 
                    src:aamt_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_3), '0'), 38, 10) is null and 
                    src:aamt_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_dwhc), '0'), 38, 10) is null and 
                    src:aamt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_lclc), '0'), 38, 10) is null and 
                    src:aamt_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_refc), '0'), 38, 10) is null and 
                    src:aamt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_rpc1), '0'), 38, 10) is null and 
                    src:aamt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_rpc2), '0'), 38, 10) is null and 
                    src:aamt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamu_1), '0'), 38, 10) is null and 
                    src:aamu_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamu_2), '0'), 38, 10) is null and 
                    src:aamu_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamu_3), '0'), 38, 10) is null and 
                    src:aamu_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamu_dwhc), '0'), 38, 10) is null and 
                    src:aamu_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamu_lclc), '0'), 38, 10) is null and 
                    src:aamu_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamu_refc), '0'), 38, 10) is null and 
                    src:aamu_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamu_rpc1), '0'), 38, 10) is null and 
                    src:aamu_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamu_rpc2), '0'), 38, 10) is null and 
                    src:aamu_rpc2 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:aldt), '1900-01-01')) is null and 
                    src:aldt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alfp), '0'), 38, 10) is null and 
                    src:alfp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:almi), '0'), 38, 10) is null and 
                    src:almi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:altp), '0'), 38, 10) is null and 
                    src:altp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alty), '0'), 38, 10) is null and 
                    src:alty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_dwhc), '0'), 38, 10) is null and 
                    src:ames_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_lclc), '0'), 38, 10) is null and 
                    src:ames_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_refc), '0'), 38, 10) is null and 
                    src:ames_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_rpc1), '0'), 38, 10) is null and 
                    src:ames_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_rpc2), '0'), 38, 10) is null and 
                    src:ames_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aues_dwhc), '0'), 38, 10) is null and 
                    src:aues_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aues_lclc), '0'), 38, 10) is null and 
                    src:aues_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aues_refc), '0'), 38, 10) is null and 
                    src:aues_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aues_rpc1), '0'), 38, 10) is null and 
                    src:aues_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aues_rpc2), '0'), 38, 10) is null and 
                    src:aues_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfls), '0'), 38, 10) is null and 
                    src:bfls is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:boun_ref_compnr), '0'), 38, 10) is null and 
                    src:boun_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpon), '0'), 38, 10) is null and 
                    src:bpon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bseq), '0'), 38, 10) is null and 
                    src:bseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bwar_ref_compnr), '0'), 38, 10) is null and 
                    src:bwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctt), '0'), 38, 10) is null and 
                    src:cctt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcs_1), '0'), 38, 10) is null and 
                    src:cpcs_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcs_2), '0'), 38, 10) is null and 
                    src:cpcs_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcs_3), '0'), 38, 10) is null and 
                    src:cpcs_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcs_dwhc), '0'), 38, 10) is null and 
                    src:cpcs_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcs_lclc), '0'), 38, 10) is null and 
                    src:cpcs_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcs_refc), '0'), 38, 10) is null and 
                    src:cpcs_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcs_rpc1), '0'), 38, 10) is null and 
                    src:cpcs_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcs_rpc2), '0'), 38, 10) is null and 
                    src:cpcs_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_1), '0'), 38, 10) is null and 
                    src:cpcu_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_2), '0'), 38, 10) is null and 
                    src:cpcu_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_3), '0'), 38, 10) is null and 
                    src:cpcu_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_dwhc), '0'), 38, 10) is null and 
                    src:cpcu_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_lclc), '0'), 38, 10) is null and 
                    src:cpcu_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_refc), '0'), 38, 10) is null and 
                    src:cpcu_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_rpc1), '0'), 38, 10) is null and 
                    src:cpcu_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcu_rpc2), '0'), 38, 10) is null and 
                    src:cpcu_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpes_1), '0'), 38, 10) is null and 
                    src:cpes_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpes_2), '0'), 38, 10) is null and 
                    src:cpes_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpes_3), '0'), 38, 10) is null and 
                    src:cpes_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpes_dwhc), '0'), 38, 10) is null and 
                    src:cpes_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpes_eam1), '0'), 38, 10) is null and 
                    src:cpes_eam1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpes_eam2), '0'), 38, 10) is null and 
                    src:cpes_eam2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpes_eam3), '0'), 38, 10) is null and 
                    src:cpes_eam3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpes_refc), '0'), 38, 10) is null and 
                    src:cpes_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cues_1), '0'), 38, 10) is null and 
                    src:cues_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cues_2), '0'), 38, 10) is null and 
                    src:cues_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cues_3), '0'), 38, 10) is null and 
                    src:cues_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cues_dwhc), '0'), 38, 10) is null and 
                    src:cues_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cues_eau1), '0'), 38, 10) is null and 
                    src:cues_eau1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cues_eau2), '0'), 38, 10) is null and 
                    src:cues_eau2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cues_eau3), '0'), 38, 10) is null and 
                    src:cues_eau3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cues_refc), '0'), 38, 10) is null and 
                    src:cues_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dptm_fcmp), '0'), 38, 10) is null and 
                    src:dptm_fcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:drin), '0'), 38, 10) is null and 
                    src:drin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dris), '0'), 38, 10) is null and 
                    src:dris is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dstp), '0'), 38, 10) is null and 
                    src:dstp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icfm), '0'), 38, 10) is null and 
                    src:icfm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ippg), '0'), 38, 10) is null and 
                    src:ippg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issu), '0'), 38, 10) is null and 
                    src:issu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issu_buar), '0'), 38, 10) is null and 
                    src:issu_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issu_buln), '0'), 38, 10) is null and 
                    src:issu_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issu_bupc), '0'), 38, 10) is null and 
                    src:issu_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issu_butm), '0'), 38, 10) is null and 
                    src:issu_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issu_buvl), '0'), 38, 10) is null and 
                    src:issu_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issu_buwg), '0'), 38, 10) is null and 
                    src:issu_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iswh), '0'), 38, 10) is null and 
                    src:iswh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iswh_buar), '0'), 38, 10) is null and 
                    src:iswh_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iswh_buln), '0'), 38, 10) is null and 
                    src:iswh_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iswh_bupc), '0'), 38, 10) is null and 
                    src:iswh_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iswh_butm), '0'), 38, 10) is null and 
                    src:iswh_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iswh_buvl), '0'), 38, 10) is null and 
                    src:iswh_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iswh_buwg), '0'), 38, 10) is null and 
                    src:iswh_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_rcmp), '0'), 38, 10) is null and 
                    src:item_rcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leng), '0'), 38, 10) is null and 
                    src:leng is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpno), '0'), 38, 10) is null and 
                    src:lpno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsel), '0'), 38, 10) is null and 
                    src:lsel is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcmd), '0'), 38, 10) is null and 
                    src:mcmd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:noun), '0'), 38, 10) is null and 
                    src:noun is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opno), '0'), 38, 10) is null and 
                    src:opno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owns), '0'), 38, 10) is null and 
                    src:owns is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdno_ref_compnr), '0'), 38, 10) is null and 
                    src:pdno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:preq), '0'), 38, 10) is null and 
                    src:preq is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqur), '0'), 38, 10) is null and 
                    src:qqur is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscr), '0'), 38, 10) is null and 
                    src:qscr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscr_buar), '0'), 38, 10) is null and 
                    src:qscr_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscr_buln), '0'), 38, 10) is null and 
                    src:qscr_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscr_bupc), '0'), 38, 10) is null and 
                    src:qscr_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscr_butm), '0'), 38, 10) is null and 
                    src:qscr_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscr_buvl), '0'), 38, 10) is null and 
                    src:qscr_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscr_buwg), '0'), 38, 10) is null and 
                    src:qscr_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qspl), '0'), 38, 10) is null and 
                    src:qspl is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qucs), '0'), 38, 10) is null and 
                    src:qucs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qucs_buar), '0'), 38, 10) is null and 
                    src:qucs_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qucs_buln), '0'), 38, 10) is null and 
                    src:qucs_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qucs_bupc), '0'), 38, 10) is null and 
                    src:qucs_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qucs_butm), '0'), 38, 10) is null and 
                    src:qucs_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qucs_buvl), '0'), 38, 10) is null and 
                    src:qucs_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qucs_buwg), '0'), 38, 10) is null and 
                    src:qucs_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ques), '0'), 38, 10) is null and 
                    src:ques is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ques_buar), '0'), 38, 10) is null and 
                    src:ques_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ques_buln), '0'), 38, 10) is null and 
                    src:ques_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ques_bupc), '0'), 38, 10) is null and 
                    src:ques_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ques_butm), '0'), 38, 10) is null and 
                    src:ques_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ques_buvl), '0'), 38, 10) is null and 
                    src:ques_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ques_buwg), '0'), 38, 10) is null and 
                    src:ques_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qune), '0'), 38, 10) is null and 
                    src:qune is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qune_buar), '0'), 38, 10) is null and 
                    src:qune_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qune_buln), '0'), 38, 10) is null and 
                    src:qune_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qune_bupc), '0'), 38, 10) is null and 
                    src:qune_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qune_butm), '0'), 38, 10) is null and 
                    src:qune_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qune_buvl), '0'), 38, 10) is null and 
                    src:qune_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qune_buwg), '0'), 38, 10) is null and 
                    src:qune_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdsp), '0'), 38, 10) is null and 
                    src:rdsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtas), '0'), 38, 10) is null and 
                    src:rtas is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rwit), '0'), 38, 10) is null and 
                    src:rwit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sayn), '0'), 38, 10) is null and 
                    src:sayn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbsr), '0'), 38, 10) is null and 
                    src:sbsr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scdl), '0'), 38, 10) is null and 
                    src:scdl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpf), '0'), 38, 10) is null and 
                    src:scpf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq), '0'), 38, 10) is null and 
                    src:scpq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buar), '0'), 38, 10) is null and 
                    src:scpq_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buln), '0'), 38, 10) is null and 
                    src:scpq_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_bupc), '0'), 38, 10) is null and 
                    src:scpq_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_butm), '0'), 38, 10) is null and 
                    src:scpq_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buvl), '0'), 38, 10) is null and 
                    src:scpq_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buwg), '0'), 38, 10) is null and 
                    src:scpq_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sitm_ref_compnr), '0'), 38, 10) is null and 
                    src:sitm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sizu_ref_compnr), '0'), 38, 10) is null and 
                    src:sizu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subd), '0'), 38, 10) is null and 
                    src:subd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subd_buar), '0'), 38, 10) is null and 
                    src:subd_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subd_buln), '0'), 38, 10) is null and 
                    src:subd_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subd_bupc), '0'), 38, 10) is null and 
                    src:subd_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subd_butm), '0'), 38, 10) is null and 
                    src:subd_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subd_buvl), '0'), 38, 10) is null and 
                    src:subd_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subd_buwg), '0'), 38, 10) is null and 
                    src:subd_buwg is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:widt), '0'), 38, 10) is null and 
                    src:widt is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)