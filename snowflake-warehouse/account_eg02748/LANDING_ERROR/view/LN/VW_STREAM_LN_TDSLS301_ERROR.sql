CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDSLS301_ERROR AS SELECT src, 'LN_TDSLS301' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adco), '0'), 38, 10) is null and 
                    src:adco is not null) THEN
                    'adco contains a non-numeric value : \'' || AS_VARCHAR(src:adco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adcs_ref_compnr), '0'), 38, 10) is null and 
                    src:adcs_ref_compnr is not null) THEN
                    'adcs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:adcs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aeco), '0'), 38, 10) is null and 
                    src:aeco is not null) THEN
                    'aeco contains a non-numeric value : \'' || AS_VARCHAR(src:aeco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aeed), '0'), 38, 10) is null and 
                    src:aeed is not null) THEN
                    'aeed contains a non-numeric value : \'' || AS_VARCHAR(src:aeed) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aemq), '0'), 38, 10) is null and 
                    src:aemq is not null) THEN
                    'aemq contains a non-numeric value : \'' || AS_VARCHAR(src:aemq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afrb), '0'), 38, 10) is null and 
                    src:afrb is not null) THEN
                    'afrb contains a non-numeric value : \'' || AS_VARCHAR(src:afrb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bamt), '0'), 38, 10) is null and 
                    src:bamt is not null) THEN
                    'bamt contains a non-numeric value : \'' || AS_VARCHAR(src:bamt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bamt_dtwc), '0'), 38, 10) is null and 
                    src:bamt_dtwc is not null) THEN
                    'bamt_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:bamt_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bamt_lclc), '0'), 38, 10) is null and 
                    src:bamt_lclc is not null) THEN
                    'bamt_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:bamt_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bamt_rfrc), '0'), 38, 10) is null and 
                    src:bamt_rfrc is not null) THEN
                    'bamt_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:bamt_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bamt_rpc1), '0'), 38, 10) is null and 
                    src:bamt_rpc1 is not null) THEN
                    'bamt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:bamt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bamt_rpc2), '0'), 38, 10) is null and 
                    src:bamt_rpc2 is not null) THEN
                    'bamt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:bamt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:bfbp_ref_compnr is not null) THEN
                    'bfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) THEN
                    'bptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt), '0'), 38, 10) is null and 
                    src:camt is not null) THEN
                    'camt contains a non-numeric value : \'' || AS_VARCHAR(src:camt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_dtwc), '0'), 38, 10) is null and 
                    src:camt_dtwc is not null) THEN
                    'camt_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_lclc), '0'), 38, 10) is null and 
                    src:camt_lclc is not null) THEN
                    'camt_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_rfrc), '0'), 38, 10) is null and 
                    src:camt_rfrc is not null) THEN
                    'camt_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_rpc1), '0'), 38, 10) is null and 
                    src:camt_rpc1 is not null) THEN
                    'camt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_rpc2), '0'), 38, 10) is null and 
                    src:camt_rpc2 is not null) THEN
                    'camt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) THEN
                    'ccty_cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdat), '1900-01-01')) is null and 
                    src:cdat is not null) THEN
                    'cdat contains a non-timestamp value : \'' || AS_VARCHAR(src:cdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) THEN
                    'cono_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgs_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgs_ref_compnr is not null) THEN
                    'cpgs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpgs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpwc), '0'), 38, 10) is null and 
                    src:cpwc is not null) THEN
                    'cpwc contains a non-numeric value : \'' || AS_VARCHAR(src:cpwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is null and 
                    src:crep_ref_compnr is not null) THEN
                    'crep_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crep_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cups_ref_compnr), '0'), 38, 10) is null and 
                    src:cups_ref_compnr is not null) THEN
                    'cups_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cups_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqs_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqs_ref_compnr is not null) THEN
                    'cuqs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuqs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvps), '0'), 38, 10) is null and 
                    src:cvps is not null) THEN
                    'cvps contains a non-numeric value : \'' || AS_VARCHAR(src:cvps) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvqs), '0'), 38, 10) is null and 
                    src:cvqs is not null) THEN
                    'cvqs contains a non-numeric value : \'' || AS_VARCHAR(src:cvqs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dest), '0'), 38, 10) is null and 
                    src:dest is not null) THEN
                    'dest contains a non-numeric value : \'' || AS_VARCHAR(src:dest) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsor), '0'), 38, 10) is null and 
                    src:dsor is not null) THEN
                    'dsor contains a non-numeric value : \'' || AS_VARCHAR(src:dsor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtrm), '0'), 38, 10) is null and 
                    src:dtrm is not null) THEN
                    'dtrm contains a non-numeric value : \'' || AS_VARCHAR(src:dtrm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dttl), '0'), 38, 10) is null and 
                    src:dttl is not null) THEN
                    'dttl contains a non-numeric value : \'' || AS_VARCHAR(src:dttl) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) THEN
                    'exmt contains a non-numeric value : \'' || AS_VARCHAR(src:exmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gamt_dtwc), '0'), 38, 10) is null and 
                    src:gamt_dtwc is not null) THEN
                    'gamt_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:gamt_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gamt_lclc), '0'), 38, 10) is null and 
                    src:gamt_lclc is not null) THEN
                    'gamt_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:gamt_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gamt_rfrc), '0'), 38, 10) is null and 
                    src:gamt_rfrc is not null) THEN
                    'gamt_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:gamt_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gamt_rpc1), '0'), 38, 10) is null and 
                    src:gamt_rpc1 is not null) THEN
                    'gamt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:gamt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gamt_rpc2), '0'), 38, 10) is null and 
                    src:gamt_rpc2 is not null) THEN
                    'gamt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:gamt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gamt_trnc), '0'), 38, 10) is null and 
                    src:gamt_trnc is not null) THEN
                    'gamt_trnc contains a non-numeric value : \'' || AS_VARCHAR(src:gamt_trnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hqan), '0'), 38, 10) is null and 
                    src:hqan is not null) THEN
                    'hqan contains a non-numeric value : \'' || AS_VARCHAR(src:hqan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hval), '0'), 38, 10) is null and 
                    src:hval is not null) THEN
                    'hval contains a non-numeric value : \'' || AS_VARCHAR(src:hval) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icap), '0'), 38, 10) is null and 
                    src:icap is not null) THEN
                    'icap contains a non-numeric value : \'' || AS_VARCHAR(src:icap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icyp), '0'), 38, 10) is null and 
                    src:icyp is not null) THEN
                    'icyp contains a non-numeric value : \'' || AS_VARCHAR(src:icyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ieva), '0'), 38, 10) is null and 
                    src:ieva is not null) THEN
                    'ieva contains a non-numeric value : \'' || AS_VARCHAR(src:ieva) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iqab), '0'), 38, 10) is null and 
                    src:iqab is not null) THEN
                    'iqab contains a non-numeric value : \'' || AS_VARCHAR(src:iqab) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_site_ref_compnr), '0'), 38, 10) is null and 
                    src:item_site_ref_compnr is not null) THEN
                    'item_site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:item_stsi_ref_compnr is not null) THEN
                    'item_stsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_stsi_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maxv), '0'), 38, 10) is null and 
                    src:maxv is not null) THEN
                    'maxv contains a non-numeric value : \'' || AS_VARCHAR(src:maxv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:minv), '0'), 38, 10) is null and 
                    src:minv is not null) THEN
                    'minv contains a non-numeric value : \'' || AS_VARCHAR(src:minv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mqtl), '0'), 38, 10) is null and 
                    src:mqtl is not null) THEN
                    'mqtl contains a non-numeric value : \'' || AS_VARCHAR(src:mqtl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_dtwc), '0'), 38, 10) is null and 
                    src:namt_dtwc is not null) THEN
                    'namt_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:namt_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_lclc), '0'), 38, 10) is null and 
                    src:namt_lclc is not null) THEN
                    'namt_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:namt_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_rfrc), '0'), 38, 10) is null and 
                    src:namt_rfrc is not null) THEN
                    'namt_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:namt_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_rpc1), '0'), 38, 10) is null and 
                    src:namt_rpc1 is not null) THEN
                    'namt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:namt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_rpc2), '0'), 38, 10) is null and 
                    src:namt_rpc2 is not null) THEN
                    'namt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:namt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_trnc), '0'), 38, 10) is null and 
                    src:namt_trnc is not null) THEN
                    'namt_trnc contains a non-numeric value : \'' || AS_VARCHAR(src:namt_trnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrp_ref_compnr), '0'), 38, 10) is null and 
                    src:osrp_ref_compnr is not null) THEN
                    'osrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:osrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbor), '0'), 38, 10) is null and 
                    src:pbor is not null) THEN
                    'pbor contains a non-numeric value : \'' || AS_VARCHAR(src:pbor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcpr), '0'), 38, 10) is null and 
                    src:pcpr is not null) THEN
                    'pcpr contains a non-numeric value : \'' || AS_VARCHAR(src:pcpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prbo_ref_compnr), '0'), 38, 10) is null and 
                    src:prbo_ref_compnr is not null) THEN
                    'prbo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prbo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) THEN
                    'pric contains a non-numeric value : \'' || AS_VARCHAR(src:pric) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:priu), '0'), 38, 10) is null and 
                    src:priu is not null) THEN
                    'priu contains a non-numeric value : \'' || AS_VARCHAR(src:priu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicl), '0'), 38, 10) is null and 
                    src:qicl is not null) THEN
                    'qicl contains a non-numeric value : \'' || AS_VARCHAR(src:qicl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicl_buar), '0'), 38, 10) is null and 
                    src:qicl_buar is not null) THEN
                    'qicl_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qicl_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicl_buln), '0'), 38, 10) is null and 
                    src:qicl_buln is not null) THEN
                    'qicl_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qicl_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicl_bupc), '0'), 38, 10) is null and 
                    src:qicl_bupc is not null) THEN
                    'qicl_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qicl_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicl_butm), '0'), 38, 10) is null and 
                    src:qicl_butm is not null) THEN
                    'qicl_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qicl_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicl_buvl), '0'), 38, 10) is null and 
                    src:qicl_buvl is not null) THEN
                    'qicl_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qicl_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicl_buwg), '0'), 38, 10) is null and 
                    src:qicl_buwg is not null) THEN
                    'qicl_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qicl_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicl_trnu), '0'), 38, 10) is null and 
                    src:qicl_trnu is not null) THEN
                    'qicl_trnu contains a non-numeric value : \'' || AS_VARCHAR(src:qicl_trnu) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qimo), '0'), 38, 10) is null and 
                    src:qimo is not null) THEN
                    'qimo contains a non-numeric value : \'' || AS_VARCHAR(src:qimo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qomn), '0'), 38, 10) is null and 
                    src:qomn is not null) THEN
                    'qomn contains a non-numeric value : \'' || AS_VARCHAR(src:qomn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qomx), '0'), 38, 10) is null and 
                    src:qomx is not null) THEN
                    'qomx contains a non-numeric value : \'' || AS_VARCHAR(src:qomx) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) THEN
                    'rcod_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcod_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpds), '0'), 38, 10) is null and 
                    src:rpds is not null) THEN
                    'rpds contains a non-numeric value : \'' || AS_VARCHAR(src:rpds) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) THEN
                    'sdat contains a non-timestamp value : \'' || AS_VARCHAR(src:sdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdsc), '0'), 38, 10) is null and 
                    src:sdsc is not null) THEN
                    'sdsc contains a non-numeric value : \'' || AS_VARCHAR(src:sdsc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) THEN
                    'sfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:stsi_ref_compnr is not null) THEN
                    'stsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stwh_ref_compnr), '0'), 38, 10) is null and 
                    src:stwh_ref_compnr is not null) THEN
                    'stwh_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stwh_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:thic), '0'), 38, 10) is null and 
                    src:thic is not null) THEN
                    'thic contains a non-numeric value : \'' || AS_VARCHAR(src:thic) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucms), '0'), 38, 10) is null and 
                    src:ucms is not null) THEN
                    'ucms contains a non-numeric value : \'' || AS_VARCHAR(src:ucms) || '\'' WHEN 
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
                src:cono,
                src:pono,
                src:cofc  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDSLS301 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adco), '0'), 38, 10) is null and 
                    src:adco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adcs_ref_compnr), '0'), 38, 10) is null and 
                    src:adcs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aeco), '0'), 38, 10) is null and 
                    src:aeco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aeed), '0'), 38, 10) is null and 
                    src:aeed is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aemq), '0'), 38, 10) is null and 
                    src:aemq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afrb), '0'), 38, 10) is null and 
                    src:afrb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bamt), '0'), 38, 10) is null and 
                    src:bamt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bamt_dtwc), '0'), 38, 10) is null and 
                    src:bamt_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bamt_lclc), '0'), 38, 10) is null and 
                    src:bamt_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bamt_rfrc), '0'), 38, 10) is null and 
                    src:bamt_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bamt_rpc1), '0'), 38, 10) is null and 
                    src:bamt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bamt_rpc2), '0'), 38, 10) is null and 
                    src:bamt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:bfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt), '0'), 38, 10) is null and 
                    src:camt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_dtwc), '0'), 38, 10) is null and 
                    src:camt_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_lclc), '0'), 38, 10) is null and 
                    src:camt_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_rfrc), '0'), 38, 10) is null and 
                    src:camt_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_rpc1), '0'), 38, 10) is null and 
                    src:camt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_rpc2), '0'), 38, 10) is null and 
                    src:camt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdat), '1900-01-01')) is null and 
                    src:cdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgs_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpwc), '0'), 38, 10) is null and 
                    src:cpwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is null and 
                    src:crep_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cups_ref_compnr), '0'), 38, 10) is null and 
                    src:cups_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqs_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvps), '0'), 38, 10) is null and 
                    src:cvps is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvqs), '0'), 38, 10) is null and 
                    src:cvqs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dest), '0'), 38, 10) is null and 
                    src:dest is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsor), '0'), 38, 10) is null and 
                    src:dsor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtrm), '0'), 38, 10) is null and 
                    src:dtrm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dttl), '0'), 38, 10) is null and 
                    src:dttl is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edat), '1900-01-01')) is null and 
                    src:edat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elgb), '0'), 38, 10) is null and 
                    src:elgb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gamt_dtwc), '0'), 38, 10) is null and 
                    src:gamt_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gamt_lclc), '0'), 38, 10) is null and 
                    src:gamt_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gamt_rfrc), '0'), 38, 10) is null and 
                    src:gamt_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gamt_rpc1), '0'), 38, 10) is null and 
                    src:gamt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gamt_rpc2), '0'), 38, 10) is null and 
                    src:gamt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gamt_trnc), '0'), 38, 10) is null and 
                    src:gamt_trnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hqan), '0'), 38, 10) is null and 
                    src:hqan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hval), '0'), 38, 10) is null and 
                    src:hval is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icap), '0'), 38, 10) is null and 
                    src:icap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icyp), '0'), 38, 10) is null and 
                    src:icyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ieva), '0'), 38, 10) is null and 
                    src:ieva is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iqab), '0'), 38, 10) is null and 
                    src:iqab is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_site_ref_compnr), '0'), 38, 10) is null and 
                    src:item_site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:item_stsi_ref_compnr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maxv), '0'), 38, 10) is null and 
                    src:maxv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:minv), '0'), 38, 10) is null and 
                    src:minv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mqtl), '0'), 38, 10) is null and 
                    src:mqtl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_dtwc), '0'), 38, 10) is null and 
                    src:namt_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_lclc), '0'), 38, 10) is null and 
                    src:namt_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_rfrc), '0'), 38, 10) is null and 
                    src:namt_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_rpc1), '0'), 38, 10) is null and 
                    src:namt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_rpc2), '0'), 38, 10) is null and 
                    src:namt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_trnc), '0'), 38, 10) is null and 
                    src:namt_trnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrp_ref_compnr), '0'), 38, 10) is null and 
                    src:osrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbor), '0'), 38, 10) is null and 
                    src:pbor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcpr), '0'), 38, 10) is null and 
                    src:pcpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prbo_ref_compnr), '0'), 38, 10) is null and 
                    src:prbo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:priu), '0'), 38, 10) is null and 
                    src:priu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicl), '0'), 38, 10) is null and 
                    src:qicl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicl_buar), '0'), 38, 10) is null and 
                    src:qicl_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicl_buln), '0'), 38, 10) is null and 
                    src:qicl_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicl_bupc), '0'), 38, 10) is null and 
                    src:qicl_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicl_butm), '0'), 38, 10) is null and 
                    src:qicl_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicl_buvl), '0'), 38, 10) is null and 
                    src:qicl_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicl_buwg), '0'), 38, 10) is null and 
                    src:qicl_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicl_trnu), '0'), 38, 10) is null and 
                    src:qicl_trnu is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qimo), '0'), 38, 10) is null and 
                    src:qimo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qomn), '0'), 38, 10) is null and 
                    src:qomn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qomx), '0'), 38, 10) is null and 
                    src:qomx is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpds), '0'), 38, 10) is null and 
                    src:rpds is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdsc), '0'), 38, 10) is null and 
                    src:sdsc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_dlpt_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_dlpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is null and 
                    src:stcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:stsi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stwh_ref_compnr), '0'), 38, 10) is null and 
                    src:stwh_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:thic), '0'), 38, 10) is null and 
                    src:thic is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucms), '0'), 38, 10) is null and 
                    src:ucms is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:widt), '0'), 38, 10) is null and 
                    src:widt is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)