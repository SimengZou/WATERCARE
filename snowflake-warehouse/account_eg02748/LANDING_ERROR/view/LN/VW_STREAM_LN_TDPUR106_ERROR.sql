CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDPUR106_ERROR AS SELECT src, 'LN_TDPUR106' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amta), '0'), 38, 10) is null and 
                    src:amta is not null) THEN
                    'amta contains a non-numeric value : \'' || AS_VARCHAR(src:amta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ares), '0'), 38, 10) is null and 
                    src:ares is not null) THEN
                    'ares contains a non-numeric value : \'' || AS_VARCHAR(src:ares) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) THEN
                    'bptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) THEN
                    'cadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccon_ref_compnr), '0'), 38, 10) is null and 
                    src:ccon_ref_compnr is not null) THEN
                    'ccon_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccon_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) THEN
                    'ccty_cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) THEN
                    'cfrw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfrw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is null and 
                    src:citg_ref_compnr is not null) THEN
                    'citg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:citg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is null and 
                    src:citt_ref_compnr is not null) THEN
                    'citt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:citt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) THEN
                    'cmnf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmnf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnat), '0'), 38, 10) is null and 
                    src:cnat is not null) THEN
                    'cnat contains a non-numeric value : \'' || AS_VARCHAR(src:cnat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnty), '0'), 38, 10) is null and 
                    src:cnty is not null) THEN
                    'cnty contains a non-numeric value : \'' || AS_VARCHAR(src:cnty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copy), '0'), 38, 10) is null and 
                    src:copy is not null) THEN
                    'copy contains a non-numeric value : \'' || AS_VARCHAR(src:copy) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crbn), '0'), 38, 10) is null and 
                    src:crbn is not null) THEN
                    'crbn contains a non-numeric value : \'' || AS_VARCHAR(src:crbn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crcd_ref_compnr), '0'), 38, 10) is null and 
                    src:crcd_ref_compnr is not null) THEN
                    'crcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctcd_ref_compnr), '0'), 38, 10) is null and 
                    src:ctcd_ref_compnr is not null) THEN
                    'ctcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupp_ref_compnr), '0'), 38, 10) is null and 
                    src:cupp_ref_compnr is not null) THEN
                    'cupp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cupp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqp_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqp_ref_compnr is not null) THEN
                    'cuqp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuqp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvpp), '0'), 38, 10) is null and 
                    src:cvpp is not null) THEN
                    'cvpp contains a non-numeric value : \'' || AS_VARCHAR(src:cvpp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvqp), '0'), 38, 10) is null and 
                    src:cvqp is not null) THEN
                    'cvqp contains a non-numeric value : \'' || AS_VARCHAR(src:cvqp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddat), '1900-01-01')) is null and 
                    src:ddat is not null) THEN
                    'ddat contains a non-timestamp value : \'' || AS_VARCHAR(src:ddat) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsor), '0'), 38, 10) is null and 
                    src:dsor is not null) THEN
                    'dsor contains a non-numeric value : \'' || AS_VARCHAR(src:dsor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtrm), '0'), 38, 10) is null and 
                    src:dtrm is not null) THEN
                    'dtrm contains a non-numeric value : \'' || AS_VARCHAR(src:dtrm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edat), '1900-01-01')) is null and 
                    src:edat is not null) THEN
                    'edat contains a non-timestamp value : \'' || AS_VARCHAR(src:edat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) THEN
                    'effn contains a non-numeric value : \'' || AS_VARCHAR(src:effn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) THEN
                    'effn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:effn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elgb), '0'), 38, 10) is null and 
                    src:elgb is not null) THEN
                    'elgb contains a non-numeric value : \'' || AS_VARCHAR(src:elgb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eolf), '0'), 38, 10) is null and 
                    src:eolf is not null) THEN
                    'eolf contains a non-numeric value : \'' || AS_VARCHAR(src:eolf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etpc), '0'), 38, 10) is null and 
                    src:etpc is not null) THEN
                    'etpc contains a non-numeric value : \'' || AS_VARCHAR(src:etpc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) THEN
                    'exdt contains a non-timestamp value : \'' || AS_VARCHAR(src:exdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) THEN
                    'exmt contains a non-numeric value : \'' || AS_VARCHAR(src:exmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gefo), '0'), 38, 10) is null and 
                    src:gefo is not null) THEN
                    'gefo contains a non-numeric value : \'' || AS_VARCHAR(src:gefo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipla), '0'), 38, 10) is null and 
                    src:ipla is not null) THEN
                    'ipla contains a non-numeric value : \'' || AS_VARCHAR(src:ipla) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issp), '0'), 38, 10) is null and 
                    src:issp is not null) THEN
                    'issp contains a non-numeric value : \'' || AS_VARCHAR(src:issp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kofl), '0'), 38, 10) is null and 
                    src:kofl is not null) THEN
                    'kofl contains a non-numeric value : \'' || AS_VARCHAR(src:kofl) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lbdt), '1900-01-01')) is null and 
                    src:lbdt is not null) THEN
                    'lbdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lbdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lbuy), '0'), 38, 10) is null and 
                    src:lbuy is not null) THEN
                    'lbuy contains a non-numeric value : \'' || AS_VARCHAR(src:lbuy) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lccl_ref_compnr), '0'), 38, 10) is null and 
                    src:lccl_ref_compnr is not null) THEN
                    'lccl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lccl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) THEN
                    'lcmp contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) THEN
                    'lcmp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lead), '0'), 38, 10) is null and 
                    src:lead is not null) THEN
                    'lead contains a non-numeric value : \'' || AS_VARCHAR(src:lead) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leng), '0'), 38, 10) is null and 
                    src:leng is not null) THEN
                    'leng contains a non-numeric value : \'' || AS_VARCHAR(src:leng) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leun), '0'), 38, 10) is null and 
                    src:leun is not null) THEN
                    'leun contains a non-numeric value : \'' || AS_VARCHAR(src:leun) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltyp), '0'), 38, 10) is null and 
                    src:ltyp is not null) THEN
                    'ltyp contains a non-numeric value : \'' || AS_VARCHAR(src:ltyp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:mfdt), '1900-01-01')) is null and 
                    src:mfdt is not null) THEN
                    'mfdt contains a non-timestamp value : \'' || AS_VARCHAR(src:mfdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpnr_cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:mpnr_cmnf_ref_compnr is not null) THEN
                    'mpnr_cmnf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mpnr_cmnf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbor), '0'), 38, 10) is null and 
                    src:pbor is not null) THEN
                    'pbor contains a non-numeric value : \'' || AS_VARCHAR(src:pbor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pegd), '0'), 38, 10) is null and 
                    src:pegd is not null) THEN
                    'pegd contains a non-numeric value : \'' || AS_VARCHAR(src:pegd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plin), '0'), 38, 10) is null and 
                    src:plin is not null) THEN
                    'plin contains a non-numeric value : \'' || AS_VARCHAR(src:plin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmse), '0'), 38, 10) is null and 
                    src:pmse is not null) THEN
                    'pmse contains a non-numeric value : \'' || AS_VARCHAR(src:pmse) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:porg), '0'), 38, 10) is null and 
                    src:porg is not null) THEN
                    'porg contains a non-numeric value : \'' || AS_VARCHAR(src:porg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prbk_ref_compnr), '0'), 38, 10) is null and 
                    src:prbk_ref_compnr is not null) THEN
                    'prbk_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prbk_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) THEN
                    'pric contains a non-numeric value : \'' || AS_VARCHAR(src:pric) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prih_1), '0'), 38, 10) is null and 
                    src:prih_1 is not null) THEN
                    'prih_1 contains a non-numeric value : \'' || AS_VARCHAR(src:prih_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prih_2), '0'), 38, 10) is null and 
                    src:prih_2 is not null) THEN
                    'prih_2 contains a non-numeric value : \'' || AS_VARCHAR(src:prih_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prih_3), '0'), 38, 10) is null and 
                    src:prih_3 is not null) THEN
                    'prih_3 contains a non-numeric value : \'' || AS_VARCHAR(src:prih_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsg_ref_compnr), '0'), 38, 10) is null and 
                    src:prsg_ref_compnr is not null) THEN
                    'prsg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prsg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pseq), '0'), 38, 10) is null and 
                    src:pseq is not null) THEN
                    'pseq contains a non-numeric value : \'' || AS_VARCHAR(src:pseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:qdat), '1900-01-01')) is null and 
                    src:qdat is not null) THEN
                    'qdat contains a non-timestamp value : \'' || AS_VARCHAR(src:qdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qimf), '0'), 38, 10) is null and 
                    src:qimf is not null) THEN
                    'qimf contains a non-numeric value : \'' || AS_VARCHAR(src:qimf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qimi), '0'), 38, 10) is null and 
                    src:qimi is not null) THEN
                    'qimi contains a non-numeric value : \'' || AS_VARCHAR(src:qimi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qono_otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:qono_otbp_ref_compnr is not null) THEN
                    'qono_otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:qono_otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qono_ref_compnr), '0'), 38, 10) is null and 
                    src:qono_ref_compnr is not null) THEN
                    'qono_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:qono_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor), '0'), 38, 10) is null and 
                    src:qoor is not null) THEN
                    'qoor contains a non-numeric value : \'' || AS_VARCHAR(src:qoor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qspa), '0'), 38, 10) is null and 
                    src:qspa is not null) THEN
                    'qspa contains a non-numeric value : \'' || AS_VARCHAR(src:qspa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rank), '0'), 38, 10) is null and 
                    src:rank is not null) THEN
                    'rank contains a non-numeric value : \'' || AS_VARCHAR(src:rank) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) THEN
                    'rcod_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcod_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcsi_ref_compnr), '0'), 38, 10) is null and 
                    src:rcsi_ref_compnr is not null) THEN
                    'rcsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rtdt), '1900-01-01')) is null and 
                    src:rtdt is not null) THEN
                    'rtdt contains a non-timestamp value : \'' || AS_VARCHAR(src:rtdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) THEN
                    'sdat contains a non-timestamp value : \'' || AS_VARCHAR(src:sdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdsc), '0'), 38, 10) is null and 
                    src:sdsc is not null) THEN
                    'sdsc contains a non-numeric value : \'' || AS_VARCHAR(src:sdsc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:send), '0'), 38, 10) is null and 
                    src:send is not null) THEN
                    'send contains a non-numeric value : \'' || AS_VARCHAR(src:send) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srnb), '0'), 38, 10) is null and 
                    src:srnb is not null) THEN
                    'srnb contains a non-numeric value : \'' || AS_VARCHAR(src:srnb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stdc), '0'), 38, 10) is null and 
                    src:stdc is not null) THEN
                    'stdc contains a non-numeric value : \'' || AS_VARCHAR(src:stdc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:suti), '0'), 38, 10) is null and 
                    src:suti is not null) THEN
                    'suti contains a non-numeric value : \'' || AS_VARCHAR(src:suti) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sutu), '0'), 38, 10) is null and 
                    src:sutu is not null) THEN
                    'sutu contains a non-numeric value : \'' || AS_VARCHAR(src:sutu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tapr), '0'), 38, 10) is null and 
                    src:tapr is not null) THEN
                    'tapr contains a non-numeric value : \'' || AS_VARCHAR(src:tapr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) THEN
                    'txtb contains a non-numeric value : \'' || AS_VARCHAR(src:txtb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) THEN
                    'txtb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:warr), '0'), 38, 10) is null and 
                    src:warr is not null) THEN
                    'warr contains a non-numeric value : \'' || AS_VARCHAR(src:warr) || '\'' WHEN 
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
                src:qono,
                src:srnb,
                src:otbp,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR106 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amta), '0'), 38, 10) is null and 
                    src:amta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ares), '0'), 38, 10) is null and 
                    src:ares is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccon_ref_compnr), '0'), 38, 10) is null and 
                    src:ccon_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is null and 
                    src:citg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is null and 
                    src:citt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnat), '0'), 38, 10) is null and 
                    src:cnat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnty), '0'), 38, 10) is null and 
                    src:cnty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copy), '0'), 38, 10) is null and 
                    src:copy is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crbn), '0'), 38, 10) is null and 
                    src:crbn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crcd_ref_compnr), '0'), 38, 10) is null and 
                    src:crcd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctcd_ref_compnr), '0'), 38, 10) is null and 
                    src:ctcd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupp_ref_compnr), '0'), 38, 10) is null and 
                    src:cupp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqp_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvpp), '0'), 38, 10) is null and 
                    src:cvpp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvqp), '0'), 38, 10) is null and 
                    src:cvqp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddat), '1900-01-01')) is null and 
                    src:ddat is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsor), '0'), 38, 10) is null and 
                    src:dsor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtrm), '0'), 38, 10) is null and 
                    src:dtrm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edat), '1900-01-01')) is null and 
                    src:edat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elgb), '0'), 38, 10) is null and 
                    src:elgb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eolf), '0'), 38, 10) is null and 
                    src:eolf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etpc), '0'), 38, 10) is null and 
                    src:etpc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gefo), '0'), 38, 10) is null and 
                    src:gefo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipla), '0'), 38, 10) is null and 
                    src:ipla is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issp), '0'), 38, 10) is null and 
                    src:issp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kofl), '0'), 38, 10) is null and 
                    src:kofl is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lbdt), '1900-01-01')) is null and 
                    src:lbdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lbuy), '0'), 38, 10) is null and 
                    src:lbuy is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lccl_ref_compnr), '0'), 38, 10) is null and 
                    src:lccl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lead), '0'), 38, 10) is null and 
                    src:lead is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leng), '0'), 38, 10) is null and 
                    src:leng is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leun), '0'), 38, 10) is null and 
                    src:leun is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltyp), '0'), 38, 10) is null and 
                    src:ltyp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:mfdt), '1900-01-01')) is null and 
                    src:mfdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpnr_cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:mpnr_cmnf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbor), '0'), 38, 10) is null and 
                    src:pbor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pegd), '0'), 38, 10) is null and 
                    src:pegd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plin), '0'), 38, 10) is null and 
                    src:plin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmse), '0'), 38, 10) is null and 
                    src:pmse is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:porg), '0'), 38, 10) is null and 
                    src:porg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prbk_ref_compnr), '0'), 38, 10) is null and 
                    src:prbk_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prih_1), '0'), 38, 10) is null and 
                    src:prih_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prih_2), '0'), 38, 10) is null and 
                    src:prih_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prih_3), '0'), 38, 10) is null and 
                    src:prih_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsg_ref_compnr), '0'), 38, 10) is null and 
                    src:prsg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pseq), '0'), 38, 10) is null and 
                    src:pseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:qdat), '1900-01-01')) is null and 
                    src:qdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qimf), '0'), 38, 10) is null and 
                    src:qimf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qimi), '0'), 38, 10) is null and 
                    src:qimi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qono_otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:qono_otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qono_ref_compnr), '0'), 38, 10) is null and 
                    src:qono_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor), '0'), 38, 10) is null and 
                    src:qoor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qspa), '0'), 38, 10) is null and 
                    src:qspa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rank), '0'), 38, 10) is null and 
                    src:rank is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcsi_ref_compnr), '0'), 38, 10) is null and 
                    src:rcsi_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rtdt), '1900-01-01')) is null and 
                    src:rtdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdsc), '0'), 38, 10) is null and 
                    src:sdsc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:send), '0'), 38, 10) is null and 
                    src:send is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srnb), '0'), 38, 10) is null and 
                    src:srnb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stdc), '0'), 38, 10) is null and 
                    src:stdc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:suti), '0'), 38, 10) is null and 
                    src:suti is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sutu), '0'), 38, 10) is null and 
                    src:sutu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tapr), '0'), 38, 10) is null and 
                    src:tapr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:warr), '0'), 38, 10) is null and 
                    src:warr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:widt), '0'), 38, 10) is null and 
                    src:widt is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)