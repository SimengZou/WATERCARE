CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSCFG200_ERROR AS SELECT src, 'LN_TSCFG200' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alty), '0'), 38, 10) is null and 
                    src:alty is not null) THEN
                    'alty contains a non-numeric value : \'' || AS_VARCHAR(src:alty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfad_ref_compnr), '0'), 38, 10) is null and 
                    src:bfad_ref_compnr is not null) THEN
                    'bfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:bfcn_ref_compnr is not null) THEN
                    'bfcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bfcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) THEN
                    'ccal_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccal_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccha), '0'), 38, 10) is null and 
                    src:ccha is not null) THEN
                    'ccha contains a non-numeric value : \'' || AS_VARCHAR(src:ccha) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccnt_ref_compnr), '0'), 38, 10) is null and 
                    src:ccnt_ref_compnr is not null) THEN
                    'ccnt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccnt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clst_ref_compnr), '0'), 38, 10) is null and 
                    src:clst_ref_compnr is not null) THEN
                    'clst_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clst_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) THEN
                    'cmnf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmnf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cndt), '1900-01-01')) is null and 
                    src:cndt is not null) THEN
                    'cndt contains a non-timestamp value : \'' || AS_VARCHAR(src:cndt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coun), '0'), 38, 10) is null and 
                    src:coun is not null) THEN
                    'coun contains a non-numeric value : \'' || AS_VARCHAR(src:coun) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crtm), '1900-01-01')) is null and 
                    src:crtm is not null) THEN
                    'crtm contains a non-timestamp value : \'' || AS_VARCHAR(src:crtm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csar_ref_compnr), '0'), 38, 10) is null and 
                    src:csar_ref_compnr is not null) THEN
                    'csar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cusc_ref_compnr), '0'), 38, 10) is null and 
                    src:cusc_ref_compnr is not null) THEN
                    'cusc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cusc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvco), '0'), 38, 10) is null and 
                    src:cvco is not null) THEN
                    'cvco contains a non-numeric value : \'' || AS_VARCHAR(src:cvco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwte_ref_compnr), '0'), 38, 10) is null and 
                    src:cwte_ref_compnr is not null) THEN
                    'cwte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:czon_ref_compnr), '0'), 38, 10) is null and 
                    src:czon_ref_compnr is not null) THEN
                    'czon_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:czon_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbpo), '0'), 38, 10) is null and 
                    src:dbpo is not null) THEN
                    'dbpo contains a non-numeric value : \'' || AS_VARCHAR(src:dbpo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlad_ref_compnr), '0'), 38, 10) is null and 
                    src:dlad_ref_compnr is not null) THEN
                    'dlad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dlad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlcn_ref_compnr), '0'), 38, 10) is null and 
                    src:dlcn_ref_compnr is not null) THEN
                    'dlcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dlcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dler_ref_compnr), '0'), 38, 10) is null and 
                    src:dler_ref_compnr is not null) THEN
                    'dler_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dler_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dltm), '1900-01-01')) is null and 
                    src:dltm is not null) THEN
                    'dltm contains a non-timestamp value : \'' || AS_VARCHAR(src:dltm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsnr), '0'), 38, 10) is null and 
                    src:dsnr is not null) THEN
                    'dsnr contains a non-numeric value : \'' || AS_VARCHAR(src:dsnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) THEN
                    'emno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:emno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gpac), '0'), 38, 10) is null and 
                    src:gpac is not null) THEN
                    'gpac contains a non-numeric value : \'' || AS_VARCHAR(src:gpac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gwte_ref_compnr), '0'), 38, 10) is null and 
                    src:gwte_ref_compnr is not null) THEN
                    'gwte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:gwte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_lste_ref_compnr), '0'), 38, 10) is null and 
                    src:item_lste_ref_compnr is not null) THEN
                    'item_lste_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_lste_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_rste_ref_compnr), '0'), 38, 10) is null and 
                    src:item_rste_ref_compnr is not null) THEN
                    'item_rste_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_rste_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_sern_ref_compnr), '0'), 38, 10) is null and 
                    src:item_sern_ref_compnr is not null) THEN
                    'item_sern_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_sern_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ladr_ref_compnr), '0'), 38, 10) is null and 
                    src:ladr_ref_compnr is not null) THEN
                    'ladr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ladr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lste_ref_compnr), '0'), 38, 10) is null and 
                    src:lste_ref_compnr is not null) THEN
                    'lste_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lste_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lutm), '1900-01-01')) is null and 
                    src:lutm is not null) THEN
                    'lutm contains a non-timestamp value : \'' || AS_VARCHAR(src:lutm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:mdpt_ref_compnr is not null) THEN
                    'mdpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mdpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:mftm), '1900-01-01')) is null and 
                    src:mftm is not null) THEN
                    'mftm contains a non-timestamp value : \'' || AS_VARCHAR(src:mftm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mobl), '0'), 38, 10) is null and 
                    src:mobl is not null) THEN
                    'mobl contains a non-numeric value : \'' || AS_VARCHAR(src:mobl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) THEN
                    'ofad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) THEN
                    'ofcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:optm), '1900-01-01')) is null and 
                    src:optm is not null) THEN
                    'optm contains a non-timestamp value : \'' || AS_VARCHAR(src:optm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ortp), '0'), 38, 10) is null and 
                    src:ortp is not null) THEN
                    'ortp contains a non-numeric value : \'' || AS_VARCHAR(src:ortp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owtp), '0'), 38, 10) is null and 
                    src:owtp is not null) THEN
                    'owtp contains a non-numeric value : \'' || AS_VARCHAR(src:owtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbsm), '0'), 38, 10) is null and 
                    src:pbsm is not null) THEN
                    'pbsm contains a non-numeric value : \'' || AS_VARCHAR(src:pbsm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phyt), '0'), 38, 10) is null and 
                    src:phyt is not null) THEN
                    'phyt contains a non-numeric value : \'' || AS_VARCHAR(src:phyt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pltm), '1900-01-01')) is null and 
                    src:pltm is not null) THEN
                    'pltm contains a non-timestamp value : \'' || AS_VARCHAR(src:pltm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmsc_ref_compnr), '0'), 38, 10) is null and 
                    src:pmsc_ref_compnr is not null) THEN
                    'pmsc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pmsc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prfb_ref_compnr), '0'), 38, 10) is null and 
                    src:prfb_ref_compnr is not null) THEN
                    'prfb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prfb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio), '0'), 38, 10) is null and 
                    src:prio is not null) THEN
                    'prio contains a non-numeric value : \'' || AS_VARCHAR(src:prio) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio_ref_compnr), '0'), 38, 10) is null and 
                    src:prio_ref_compnr is not null) THEN
                    'prio_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prio_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prip), '0'), 38, 10) is null and 
                    src:prip is not null) THEN
                    'prip contains a non-numeric value : \'' || AS_VARCHAR(src:prip) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prop_ref_compnr), '0'), 38, 10) is null and 
                    src:prop_ref_compnr is not null) THEN
                    'prop_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prop_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pste_ref_compnr), '0'), 38, 10) is null and 
                    src:pste_ref_compnr is not null) THEN
                    'pste_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pste_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ract_ref_compnr), '0'), 38, 10) is null and 
                    src:ract_ref_compnr is not null) THEN
                    'ract_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ract_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rnyn), '0'), 38, 10) is null and 
                    src:rnyn is not null) THEN
                    'rnyn contains a non-numeric value : \'' || AS_VARCHAR(src:rnyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc), '0'), 38, 10) is null and 
                    src:rowc is not null) THEN
                    'rowc contains a non-numeric value : \'' || AS_VARCHAR(src:rowc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is null and 
                    src:rowc_ref_compnr is not null) THEN
                    'rowc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rowc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is null and 
                    src:rown_ref_compnr is not null) THEN
                    'rown_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rown_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rste_ref_compnr), '0'), 38, 10) is null and 
                    src:rste_ref_compnr is not null) THEN
                    'rste_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rste_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rwdu), '0'), 38, 10) is null and 
                    src:rwdu is not null) THEN
                    'rwdu contains a non-numeric value : \'' || AS_VARCHAR(src:rwdu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rwun), '0'), 38, 10) is null and 
                    src:rwun is not null) THEN
                    'rwun contains a non-numeric value : \'' || AS_VARCHAR(src:rwun) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbct_ref_compnr), '0'), 38, 10) is null and 
                    src:sbct_ref_compnr is not null) THEN
                    'sbct_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sbct_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdno), '0'), 38, 10) is null and 
                    src:sdno is not null) THEN
                    'sdno contains a non-numeric value : \'' || AS_VARCHAR(src:sdno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) THEN
                    'sfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shad_ref_compnr), '0'), 38, 10) is null and 
                    src:shad_ref_compnr is not null) THEN
                    'shad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:shad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sigr_ref_compnr), '0'), 38, 10) is null and 
                    src:sigr_ref_compnr is not null) THEN
                    'sigr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sigr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:soff_ref_compnr), '0'), 38, 10) is null and 
                    src:soff_ref_compnr is not null) THEN
                    'soff_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:soff_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spno), '0'), 38, 10) is null and 
                    src:spno is not null) THEN
                    'spno contains a non-numeric value : \'' || AS_VARCHAR(src:spno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sswt), '0'), 38, 10) is null and 
                    src:sswt is not null) THEN
                    'sswt contains a non-numeric value : \'' || AS_VARCHAR(src:sswt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:strm), '0'), 38, 10) is null and 
                    src:strm is not null) THEN
                    'strm contains a non-numeric value : \'' || AS_VARCHAR(src:strm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sttm), '1900-01-01')) is null and 
                    src:sttm is not null) THEN
                    'sttm contains a non-timestamp value : \'' || AS_VARCHAR(src:sttm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sttp), '0'), 38, 10) is null and 
                    src:sttp is not null) THEN
                    'sttp contains a non-numeric value : \'' || AS_VARCHAR(src:sttp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:swte_ref_compnr), '0'), 38, 10) is null and 
                    src:swte_ref_compnr is not null) THEN
                    'swte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:swte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term), '0'), 38, 10) is null and 
                    src:term is not null) THEN
                    'term contains a non-numeric value : \'' || AS_VARCHAR(src:term) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tery_ref_compnr), '0'), 38, 10) is null and 
                    src:tery_ref_compnr is not null) THEN
                    'tery_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tery_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tgps), '0'), 38, 10) is null and 
                    src:tgps is not null) THEN
                    'tgps contains a non-numeric value : \'' || AS_VARCHAR(src:tgps) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:titm_ref_compnr), '0'), 38, 10) is null and 
                    src:titm_ref_compnr is not null) THEN
                    'titm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:titm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:titm_tser_ref_compnr), '0'), 38, 10) is null and 
                    src:titm_tser_ref_compnr is not null) THEN
                    'titm_tser_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:titm_tser_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdt), '0'), 38, 10) is null and 
                    src:trdt is not null) THEN
                    'trdt contains a non-numeric value : \'' || AS_VARCHAR(src:trdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trtm), '0'), 38, 10) is null and 
                    src:trtm is not null) THEN
                    'trtm contains a non-numeric value : \'' || AS_VARCHAR(src:trtm) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtc), '0'), 38, 10) is null and 
                    src:txtc is not null) THEN
                    'txtc contains a non-numeric value : \'' || AS_VARCHAR(src:txtc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtc_ref_compnr), '0'), 38, 10) is null and 
                    src:txtc_ref_compnr is not null) THEN
                    'txtc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubad_ref_compnr), '0'), 38, 10) is null and 
                    src:ubad_ref_compnr is not null) THEN
                    'ubad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ubad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ubbp_ref_compnr is not null) THEN
                    'ubbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ubbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ubcn_ref_compnr is not null) THEN
                    'ubcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ubcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubop_ref_compnr), '0'), 38, 10) is null and 
                    src:ubop_ref_compnr is not null) THEN
                    'ubop_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ubop_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubpc), '0'), 38, 10) is null and 
                    src:ubpc is not null) THEN
                    'ubpc contains a non-numeric value : \'' || AS_VARCHAR(src:ubpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wght), '0'), 38, 10) is null and 
                    src:wght is not null) THEN
                    'wght contains a non-numeric value : \'' || AS_VARCHAR(src:wght) || '\'' WHEN 
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
                                    
                src:sern,
                src:compnr,
                src:item  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSCFG200 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alty), '0'), 38, 10) is null and 
                    src:alty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfad_ref_compnr), '0'), 38, 10) is null and 
                    src:bfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:bfcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccha), '0'), 38, 10) is null and 
                    src:ccha is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccnt_ref_compnr), '0'), 38, 10) is null and 
                    src:ccnt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clst_ref_compnr), '0'), 38, 10) is null and 
                    src:clst_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cndt), '1900-01-01')) is null and 
                    src:cndt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coun), '0'), 38, 10) is null and 
                    src:coun is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crtm), '1900-01-01')) is null and 
                    src:crtm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csar_ref_compnr), '0'), 38, 10) is null and 
                    src:csar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cusc_ref_compnr), '0'), 38, 10) is null and 
                    src:cusc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvco), '0'), 38, 10) is null and 
                    src:cvco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwte_ref_compnr), '0'), 38, 10) is null and 
                    src:cwte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:czon_ref_compnr), '0'), 38, 10) is null and 
                    src:czon_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbpo), '0'), 38, 10) is null and 
                    src:dbpo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlad_ref_compnr), '0'), 38, 10) is null and 
                    src:dlad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlcn_ref_compnr), '0'), 38, 10) is null and 
                    src:dlcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dler_ref_compnr), '0'), 38, 10) is null and 
                    src:dler_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dltm), '1900-01-01')) is null and 
                    src:dltm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsnr), '0'), 38, 10) is null and 
                    src:dsnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gpac), '0'), 38, 10) is null and 
                    src:gpac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gwte_ref_compnr), '0'), 38, 10) is null and 
                    src:gwte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_lste_ref_compnr), '0'), 38, 10) is null and 
                    src:item_lste_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_rste_ref_compnr), '0'), 38, 10) is null and 
                    src:item_rste_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_sern_ref_compnr), '0'), 38, 10) is null and 
                    src:item_sern_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ladr_ref_compnr), '0'), 38, 10) is null and 
                    src:ladr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lste_ref_compnr), '0'), 38, 10) is null and 
                    src:lste_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lutm), '1900-01-01')) is null and 
                    src:lutm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:mdpt_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:mftm), '1900-01-01')) is null and 
                    src:mftm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mobl), '0'), 38, 10) is null and 
                    src:mobl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:optm), '1900-01-01')) is null and 
                    src:optm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ortp), '0'), 38, 10) is null and 
                    src:ortp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owtp), '0'), 38, 10) is null and 
                    src:owtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbsm), '0'), 38, 10) is null and 
                    src:pbsm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phyt), '0'), 38, 10) is null and 
                    src:phyt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pltm), '1900-01-01')) is null and 
                    src:pltm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmsc_ref_compnr), '0'), 38, 10) is null and 
                    src:pmsc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prfb_ref_compnr), '0'), 38, 10) is null and 
                    src:prfb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio), '0'), 38, 10) is null and 
                    src:prio is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio_ref_compnr), '0'), 38, 10) is null and 
                    src:prio_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prip), '0'), 38, 10) is null and 
                    src:prip is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prop_ref_compnr), '0'), 38, 10) is null and 
                    src:prop_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pste_ref_compnr), '0'), 38, 10) is null and 
                    src:pste_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ract_ref_compnr), '0'), 38, 10) is null and 
                    src:ract_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rnyn), '0'), 38, 10) is null and 
                    src:rnyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc), '0'), 38, 10) is null and 
                    src:rowc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is null and 
                    src:rowc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is null and 
                    src:rown_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rste_ref_compnr), '0'), 38, 10) is null and 
                    src:rste_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rwdu), '0'), 38, 10) is null and 
                    src:rwdu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rwun), '0'), 38, 10) is null and 
                    src:rwun is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbct_ref_compnr), '0'), 38, 10) is null and 
                    src:sbct_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdno), '0'), 38, 10) is null and 
                    src:sdno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shad_ref_compnr), '0'), 38, 10) is null and 
                    src:shad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sigr_ref_compnr), '0'), 38, 10) is null and 
                    src:sigr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:soff_ref_compnr), '0'), 38, 10) is null and 
                    src:soff_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spno), '0'), 38, 10) is null and 
                    src:spno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sswt), '0'), 38, 10) is null and 
                    src:sswt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:strm), '0'), 38, 10) is null and 
                    src:strm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sttm), '1900-01-01')) is null and 
                    src:sttm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sttp), '0'), 38, 10) is null and 
                    src:sttp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:swte_ref_compnr), '0'), 38, 10) is null and 
                    src:swte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term), '0'), 38, 10) is null and 
                    src:term is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tery_ref_compnr), '0'), 38, 10) is null and 
                    src:tery_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tgps), '0'), 38, 10) is null and 
                    src:tgps is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:titm_ref_compnr), '0'), 38, 10) is null and 
                    src:titm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:titm_tser_ref_compnr), '0'), 38, 10) is null and 
                    src:titm_tser_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdt), '0'), 38, 10) is null and 
                    src:trdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trtm), '0'), 38, 10) is null and 
                    src:trtm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtc), '0'), 38, 10) is null and 
                    src:txtc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtc_ref_compnr), '0'), 38, 10) is null and 
                    src:txtc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubad_ref_compnr), '0'), 38, 10) is null and 
                    src:ubad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ubbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ubcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubop_ref_compnr), '0'), 38, 10) is null and 
                    src:ubop_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubpc), '0'), 38, 10) is null and 
                    src:ubpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wght), '0'), 38, 10) is null and 
                    src:wght is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)