CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDSLS307_ERROR AS SELECT src, 'LN_TDSLS307' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) THEN
                    'bptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casi_ref_compnr), '0'), 38, 10) is null and 
                    src:casi_ref_compnr is not null) THEN
                    'casi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:casi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccof_ref_compnr), '0'), 38, 10) is null and 
                    src:ccof_ref_compnr is not null) THEN
                    'ccof_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccof_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) THEN
                    'ccty_cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cfdt), '1900-01-01')) is null and 
                    src:cfdt is not null) THEN
                    'cfdt contains a non-timestamp value : \'' || AS_VARCHAR(src:cfdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) THEN
                    'cfrw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfrw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chan_ref_compnr), '0'), 38, 10) is null and 
                    src:chan_ref_compnr is not null) THEN
                    'chan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:chan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_pono_ccof_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_pono_ccof_ref_compnr is not null) THEN
                    'cono_pono_ccof_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_pono_ccof_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) THEN
                    'cono_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyp), '0'), 38, 10) is null and 
                    src:ctyp is not null) THEN
                    'ctyp contains a non-numeric value : \'' || AS_VARCHAR(src:ctyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cucf_ref_compnr), '0'), 38, 10) is null and 
                    src:cucf_ref_compnr is not null) THEN
                    'cucf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cucf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqs_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqs_ref_compnr is not null) THEN
                    'cuqs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuqs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:curq_ref_compnr), '0'), 38, 10) is null and 
                    src:curq_ref_compnr is not null) THEN
                    'curq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:curq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuva), '0'), 38, 10) is null and 
                    src:cuva is not null) THEN
                    'cuva contains a non-numeric value : \'' || AS_VARCHAR(src:cuva) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvcf), '0'), 38, 10) is null and 
                    src:cvcf is not null) THEN
                    'cvcf contains a non-numeric value : \'' || AS_VARCHAR(src:cvcf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvps), '0'), 38, 10) is null and 
                    src:cvps is not null) THEN
                    'cvps contains a non-numeric value : \'' || AS_VARCHAR(src:cvps) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvqs), '0'), 38, 10) is null and 
                    src:cvqs is not null) THEN
                    'cvqs contains a non-numeric value : \'' || AS_VARCHAR(src:cvqs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvrq), '0'), 38, 10) is null and 
                    src:cvrq is not null) THEN
                    'cvrq contains a non-numeric value : \'' || AS_VARCHAR(src:cvrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddta), '1900-01-01')) is null and 
                    src:ddta is not null) THEN
                    'ddta contains a non-timestamp value : \'' || AS_VARCHAR(src:ddta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dest), '0'), 38, 10) is null and 
                    src:dest is not null) THEN
                    'dest contains a non-numeric value : \'' || AS_VARCHAR(src:dest) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edat), '1900-01-01')) is null and 
                    src:edat is not null) THEN
                    'edat contains a non-timestamp value : \'' || AS_VARCHAR(src:edat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) THEN
                    'effn contains a non-numeric value : \'' || AS_VARCHAR(src:effn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) THEN
                    'effn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:effn_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hold), '0'), 38, 10) is null and 
                    src:hold is not null) THEN
                    'hold contains a non-numeric value : \'' || AS_VARCHAR(src:hold) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:invd), '1900-01-01')) is null and 
                    src:invd is not null) THEN
                    'invd contains a non-timestamp value : \'' || AS_VARCHAR(src:invd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invn), '0'), 38, 10) is null and 
                    src:invn is not null) THEN
                    'invn contains a non-numeric value : \'' || AS_VARCHAR(src:invn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_site_ref_compnr), '0'), 38, 10) is null and 
                    src:item_site_ref_compnr is not null) THEN
                    'item_site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:item_stsi_ref_compnr is not null) THEN
                    'item_stsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_stsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leng), '0'), 38, 10) is null and 
                    src:leng is not null) THEN
                    'leng contains a non-numeric value : \'' || AS_VARCHAR(src:leng) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lnor), '0'), 38, 10) is null and 
                    src:lnor is not null) THEN
                    'lnor contains a non-numeric value : \'' || AS_VARCHAR(src:lnor) || '\'' WHEN 
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:odat), '1900-01-01')) is null and 
                    src:odat is not null) THEN
                    'odat contains a non-timestamp value : \'' || AS_VARCHAR(src:odat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opon), '0'), 38, 10) is null and 
                    src:opon is not null) THEN
                    'opon contains a non-numeric value : \'' || AS_VARCHAR(src:opon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmnt), '0'), 38, 10) is null and 
                    src:pmnt is not null) THEN
                    'pmnt contains a non-numeric value : \'' || AS_VARCHAR(src:pmnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) THEN
                    'prdt contains a non-timestamp value : \'' || AS_VARCHAR(src:prdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) THEN
                    'pric contains a non-numeric value : \'' || AS_VARCHAR(src:pric) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf), '0'), 38, 10) is null and 
                    src:qccf is not null) THEN
                    'qccf contains a non-numeric value : \'' || AS_VARCHAR(src:qccf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf_buar), '0'), 38, 10) is null and 
                    src:qccf_buar is not null) THEN
                    'qccf_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qccf_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf_buln), '0'), 38, 10) is null and 
                    src:qccf_buln is not null) THEN
                    'qccf_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qccf_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf_bupc), '0'), 38, 10) is null and 
                    src:qccf_bupc is not null) THEN
                    'qccf_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qccf_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf_butm), '0'), 38, 10) is null and 
                    src:qccf_butm is not null) THEN
                    'qccf_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qccf_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf_buvl), '0'), 38, 10) is null and 
                    src:qccf_buvl is not null) THEN
                    'qccf_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qccf_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf_buwg), '0'), 38, 10) is null and 
                    src:qccf_buwg is not null) THEN
                    'qccf_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qccf_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf_invu), '0'), 38, 10) is null and 
                    src:qccf_invu is not null) THEN
                    'qccf_invu contains a non-numeric value : \'' || AS_VARCHAR(src:qccf_invu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf_trnu), '0'), 38, 10) is null and 
                    src:qccf_trnu is not null) THEN
                    'qccf_trnu contains a non-numeric value : \'' || AS_VARCHAR(src:qccf_trnu) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoad), '0'), 38, 10) is null and 
                    src:qoad is not null) THEN
                    'qoad contains a non-numeric value : \'' || AS_VARCHAR(src:qoad) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoad_buar), '0'), 38, 10) is null and 
                    src:qoad_buar is not null) THEN
                    'qoad_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qoad_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoad_buln), '0'), 38, 10) is null and 
                    src:qoad_buln is not null) THEN
                    'qoad_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qoad_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoad_bupc), '0'), 38, 10) is null and 
                    src:qoad_bupc is not null) THEN
                    'qoad_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qoad_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoad_butm), '0'), 38, 10) is null and 
                    src:qoad_butm is not null) THEN
                    'qoad_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qoad_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoad_buvl), '0'), 38, 10) is null and 
                    src:qoad_buvl is not null) THEN
                    'qoad_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qoad_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoad_buwg), '0'), 38, 10) is null and 
                    src:qoad_buwg is not null) THEN
                    'qoad_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qoad_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoad_invu), '0'), 38, 10) is null and 
                    src:qoad_invu is not null) THEN
                    'qoad_invu contains a non-numeric value : \'' || AS_VARCHAR(src:qoad_invu) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq), '0'), 38, 10) is null and 
                    src:qrrq is not null) THEN
                    'qrrq contains a non-numeric value : \'' || AS_VARCHAR(src:qrrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq_buar), '0'), 38, 10) is null and 
                    src:qrrq_buar is not null) THEN
                    'qrrq_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qrrq_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq_buln), '0'), 38, 10) is null and 
                    src:qrrq_buln is not null) THEN
                    'qrrq_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qrrq_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq_bupc), '0'), 38, 10) is null and 
                    src:qrrq_bupc is not null) THEN
                    'qrrq_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qrrq_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq_butm), '0'), 38, 10) is null and 
                    src:qrrq_butm is not null) THEN
                    'qrrq_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qrrq_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq_buvl), '0'), 38, 10) is null and 
                    src:qrrq_buvl is not null) THEN
                    'qrrq_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qrrq_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq_buwg), '0'), 38, 10) is null and 
                    src:qrrq_buwg is not null) THEN
                    'qrrq_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qrrq_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq_invu), '0'), 38, 10) is null and 
                    src:qrrq_invu is not null) THEN
                    'qrrq_invu contains a non-numeric value : \'' || AS_VARCHAR(src:qrrq_invu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq_trnu), '0'), 38, 10) is null and 
                    src:qrrq_trnu is not null) THEN
                    'qrrq_trnu contains a non-numeric value : \'' || AS_VARCHAR(src:qrrq_trnu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) THEN
                    'rcod_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcod_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) THEN
                    'revn contains a non-numeric value : \'' || AS_VARCHAR(src:revn) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rrdt), '1900-01-01')) is null and 
                    src:rrdt is not null) THEN
                    'rrdt contains a non-timestamp value : \'' || AS_VARCHAR(src:rrdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp), '0'), 38, 10) is null and 
                    src:rtyp is not null) THEN
                    'rtyp contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt), '0'), 38, 10) is null and 
                    src:samt is not null) THEN
                    'samt contains a non-numeric value : \'' || AS_VARCHAR(src:samt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbim), '0'), 38, 10) is null and 
                    src:sbim is not null) THEN
                    'sbim contains a non-numeric value : \'' || AS_VARCHAR(src:sbim) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schn_sctp_revn_ref_compnr), '0'), 38, 10) is null and 
                    src:schn_sctp_revn_ref_compnr is not null) THEN
                    'schn_sctp_revn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:schn_sctp_revn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scmp), '0'), 38, 10) is null and 
                    src:scmp is not null) THEN
                    'scmp contains a non-numeric value : \'' || AS_VARCHAR(src:scmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sctp), '0'), 38, 10) is null and 
                    src:sctp is not null) THEN
                    'sctp contains a non-numeric value : \'' || AS_VARCHAR(src:sctp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) THEN
                    'sdat contains a non-timestamp value : \'' || AS_VARCHAR(src:sdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) THEN
                    'seqn contains a non-numeric value : \'' || AS_VARCHAR(src:seqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smsl), '0'), 38, 10) is null and 
                    src:smsl is not null) THEN
                    'smsl contains a non-numeric value : \'' || AS_VARCHAR(src:smsl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spon), '0'), 38, 10) is null and 
                    src:spon is not null) THEN
                    'spon contains a non-numeric value : \'' || AS_VARCHAR(src:spon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_dlpt_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_dlpt_ref_compnr is not null) THEN
                    'stad_dlpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_dlpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) THEN
                    'stad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) THEN
                    'stbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:stsi_ref_compnr is not null) THEN
                    'stsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stwh_ref_compnr), '0'), 38, 10) is null and 
                    src:stwh_ref_compnr is not null) THEN
                    'stwh_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stwh_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is null and 
                    src:styp_ref_compnr is not null) THEN
                    'styp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:styp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcpr_dtwc), '0'), 38, 10) is null and 
                    src:tcpr_dtwc is not null) THEN
                    'tcpr_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:tcpr_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcpr_lclc), '0'), 38, 10) is null and 
                    src:tcpr_lclc is not null) THEN
                    'tcpr_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:tcpr_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcpr_rfrc), '0'), 38, 10) is null and 
                    src:tcpr_rfrc is not null) THEN
                    'tcpr_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:tcpr_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcpr_rpc1), '0'), 38, 10) is null and 
                    src:tcpr_rpc1 is not null) THEN
                    'tcpr_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:tcpr_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcpr_rpc2), '0'), 38, 10) is null and 
                    src:tcpr_rpc2 is not null) THEN
                    'tcpr_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:tcpr_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcpr_trnc), '0'), 38, 10) is null and 
                    src:tcpr_trnc is not null) THEN
                    'tcpr_trnc contains a non-numeric value : \'' || AS_VARCHAR(src:tcpr_trnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:thic), '0'), 38, 10) is null and 
                    src:thic is not null) THEN
                    'thic contains a non-numeric value : \'' || AS_VARCHAR(src:thic) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiun), '0'), 38, 10) is null and 
                    src:tiun is not null) THEN
                    'tiun contains a non-numeric value : \'' || AS_VARCHAR(src:tiun) || '\'' WHEN 
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
                                    
                src:revn,
                src:compnr,
                src:schn,
                src:sctp,
                src:spon  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDSLS307 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casi_ref_compnr), '0'), 38, 10) is null and 
                    src:casi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccof_ref_compnr), '0'), 38, 10) is null and 
                    src:ccof_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cfdt), '1900-01-01')) is null and 
                    src:cfdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chan_ref_compnr), '0'), 38, 10) is null and 
                    src:chan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_pono_ccof_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_pono_ccof_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyp), '0'), 38, 10) is null and 
                    src:ctyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cucf_ref_compnr), '0'), 38, 10) is null and 
                    src:cucf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqs_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:curq_ref_compnr), '0'), 38, 10) is null and 
                    src:curq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuva), '0'), 38, 10) is null and 
                    src:cuva is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvcf), '0'), 38, 10) is null and 
                    src:cvcf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvps), '0'), 38, 10) is null and 
                    src:cvps is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvqs), '0'), 38, 10) is null and 
                    src:cvqs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvrq), '0'), 38, 10) is null and 
                    src:cvrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddta), '1900-01-01')) is null and 
                    src:ddta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dest), '0'), 38, 10) is null and 
                    src:dest is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edat), '1900-01-01')) is null and 
                    src:edat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hold), '0'), 38, 10) is null and 
                    src:hold is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:invd), '1900-01-01')) is null and 
                    src:invd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invn), '0'), 38, 10) is null and 
                    src:invn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_site_ref_compnr), '0'), 38, 10) is null and 
                    src:item_site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:item_stsi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leng), '0'), 38, 10) is null and 
                    src:leng is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lnor), '0'), 38, 10) is null and 
                    src:lnor is not null) or 
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:odat), '1900-01-01')) is null and 
                    src:odat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opon), '0'), 38, 10) is null and 
                    src:opon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmnt), '0'), 38, 10) is null and 
                    src:pmnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf), '0'), 38, 10) is null and 
                    src:qccf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf_buar), '0'), 38, 10) is null and 
                    src:qccf_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf_buln), '0'), 38, 10) is null and 
                    src:qccf_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf_bupc), '0'), 38, 10) is null and 
                    src:qccf_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf_butm), '0'), 38, 10) is null and 
                    src:qccf_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf_buvl), '0'), 38, 10) is null and 
                    src:qccf_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf_buwg), '0'), 38, 10) is null and 
                    src:qccf_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf_invu), '0'), 38, 10) is null and 
                    src:qccf_invu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qccf_trnu), '0'), 38, 10) is null and 
                    src:qccf_trnu is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoad), '0'), 38, 10) is null and 
                    src:qoad is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoad_buar), '0'), 38, 10) is null and 
                    src:qoad_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoad_buln), '0'), 38, 10) is null and 
                    src:qoad_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoad_bupc), '0'), 38, 10) is null and 
                    src:qoad_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoad_butm), '0'), 38, 10) is null and 
                    src:qoad_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoad_buvl), '0'), 38, 10) is null and 
                    src:qoad_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoad_buwg), '0'), 38, 10) is null and 
                    src:qoad_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoad_invu), '0'), 38, 10) is null and 
                    src:qoad_invu is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq), '0'), 38, 10) is null and 
                    src:qrrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq_buar), '0'), 38, 10) is null and 
                    src:qrrq_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq_buln), '0'), 38, 10) is null and 
                    src:qrrq_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq_bupc), '0'), 38, 10) is null and 
                    src:qrrq_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq_butm), '0'), 38, 10) is null and 
                    src:qrrq_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq_buvl), '0'), 38, 10) is null and 
                    src:qrrq_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq_buwg), '0'), 38, 10) is null and 
                    src:qrrq_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq_invu), '0'), 38, 10) is null and 
                    src:qrrq_invu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrrq_trnu), '0'), 38, 10) is null and 
                    src:qrrq_trnu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rrdt), '1900-01-01')) is null and 
                    src:rrdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp), '0'), 38, 10) is null and 
                    src:rtyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt), '0'), 38, 10) is null and 
                    src:samt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbim), '0'), 38, 10) is null and 
                    src:sbim is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schn_sctp_revn_ref_compnr), '0'), 38, 10) is null and 
                    src:schn_sctp_revn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scmp), '0'), 38, 10) is null and 
                    src:scmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sctp), '0'), 38, 10) is null and 
                    src:sctp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smsl), '0'), 38, 10) is null and 
                    src:smsl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spon), '0'), 38, 10) is null and 
                    src:spon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_dlpt_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_dlpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:stsi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stwh_ref_compnr), '0'), 38, 10) is null and 
                    src:stwh_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is null and 
                    src:styp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcpr_dtwc), '0'), 38, 10) is null and 
                    src:tcpr_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcpr_lclc), '0'), 38, 10) is null and 
                    src:tcpr_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcpr_rfrc), '0'), 38, 10) is null and 
                    src:tcpr_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcpr_rpc1), '0'), 38, 10) is null and 
                    src:tcpr_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcpr_rpc2), '0'), 38, 10) is null and 
                    src:tcpr_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcpr_trnc), '0'), 38, 10) is null and 
                    src:tcpr_trnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:thic), '0'), 38, 10) is null and 
                    src:thic is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiun), '0'), 38, 10) is null and 
                    src:tiun is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:widt), '0'), 38, 10) is null and 
                    src:widt is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)