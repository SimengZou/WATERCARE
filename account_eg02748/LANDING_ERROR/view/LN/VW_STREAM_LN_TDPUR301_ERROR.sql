CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDPUR301_ERROR AS SELECT src, 'LN_TDPUR301' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aemq), '0'), 38, 10) is null and 
                    src:aemq is not null) THEN
                    'aemq contains a non-numeric value : \'' || AS_VARCHAR(src:aemq) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) THEN
                    'bptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) THEN
                    'cadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:caga), '0'), 38, 10) is null and 
                    src:caga is not null) THEN
                    'caga contains a non-numeric value : \'' || AS_VARCHAR(src:caga) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:caga_dtwc), '0'), 38, 10) is null and 
                    src:caga_dtwc is not null) THEN
                    'caga_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:caga_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:caga_lclc), '0'), 38, 10) is null and 
                    src:caga_lclc is not null) THEN
                    'caga_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:caga_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:caga_rfrc), '0'), 38, 10) is null and 
                    src:caga_rfrc is not null) THEN
                    'caga_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:caga_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:caga_rpc1), '0'), 38, 10) is null and 
                    src:caga_rpc1 is not null) THEN
                    'caga_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:caga_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:caga_rpc2), '0'), 38, 10) is null and 
                    src:caga_rpc2 is not null) THEN
                    'caga_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:caga_rpc2) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccon_ref_compnr), '0'), 38, 10) is null and 
                    src:ccon_ref_compnr is not null) THEN
                    'ccon_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccon_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_clmt), '0'), 38, 10) is null and 
                    src:cdf_clmt is not null) THEN
                    'cdf_clmt contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_clmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_comt), '0'), 38, 10) is null and 
                    src:cdf_comt is not null) THEN
                    'cdf_comt contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_comt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_ramt), '0'), 38, 10) is null and 
                    src:cdf_ramt is not null) THEN
                    'cdf_ramt contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_ramt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is null and 
                    src:citt_ref_compnr is not null) THEN
                    'citt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:citt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cltp), '0'), 38, 10) is null and 
                    src:cltp is not null) THEN
                    'cltp contains a non-numeric value : \'' || AS_VARCHAR(src:cltp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) THEN
                    'cmnf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmnf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) THEN
                    'cono_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cons), '0'), 38, 10) is null and 
                    src:cons is not null) THEN
                    'cons contains a non-numeric value : \'' || AS_VARCHAR(src:cons) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgp_ref_compnr is not null) THEN
                    'cpgp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpgp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crrf), '0'), 38, 10) is null and 
                    src:crrf is not null) THEN
                    'crrf contains a non-numeric value : \'' || AS_VARCHAR(src:crrf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csqn), '0'), 38, 10) is null and 
                    src:csqn is not null) THEN
                    'csqn contains a non-numeric value : \'' || AS_VARCHAR(src:csqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csts), '0'), 38, 10) is null and 
                    src:csts is not null) THEN
                    'csts contains a non-numeric value : \'' || AS_VARCHAR(src:csts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqp_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqp_ref_compnr is not null) THEN
                    'cuqp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuqp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvqp), '0'), 38, 10) is null and 
                    src:cvqp is not null) THEN
                    'cvqp contains a non-numeric value : \'' || AS_VARCHAR(src:cvqp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsch), '0'), 38, 10) is null and 
                    src:dsch is not null) THEN
                    'dsch contains a non-numeric value : \'' || AS_VARCHAR(src:dsch) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iaga), '0'), 38, 10) is null and 
                    src:iaga is not null) THEN
                    'iaga contains a non-numeric value : \'' || AS_VARCHAR(src:iaga) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iaga_dtwc), '0'), 38, 10) is null and 
                    src:iaga_dtwc is not null) THEN
                    'iaga_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:iaga_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iaga_lclc), '0'), 38, 10) is null and 
                    src:iaga_lclc is not null) THEN
                    'iaga_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:iaga_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iaga_rfrc), '0'), 38, 10) is null and 
                    src:iaga_rfrc is not null) THEN
                    'iaga_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:iaga_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iaga_rpc1), '0'), 38, 10) is null and 
                    src:iaga_rpc1 is not null) THEN
                    'iaga_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:iaga_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iaga_rpc2), '0'), 38, 10) is null and 
                    src:iaga_rpc2 is not null) THEN
                    'iaga_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:iaga_rpc2) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leng), '0'), 38, 10) is null and 
                    src:leng is not null) THEN
                    'leng contains a non-numeric value : \'' || AS_VARCHAR(src:leng) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpnr_cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:mpnr_cmnf_ref_compnr is not null) THEN
                    'mpnr_cmnf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mpnr_cmnf_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncmp), '0'), 38, 10) is null and 
                    src:ncmp is not null) THEN
                    'ncmp contains a non-numeric value : \'' || AS_VARCHAR(src:ncmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncmp_ref_compnr), '0'), 38, 10) is null and 
                    src:ncmp_ref_compnr is not null) THEN
                    'ncmp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ncmp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obpr), '0'), 38, 10) is null and 
                    src:obpr is not null) THEN
                    'obpr contains a non-numeric value : \'' || AS_VARCHAR(src:obpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paft), '0'), 38, 10) is null and 
                    src:paft is not null) THEN
                    'paft contains a non-numeric value : \'' || AS_VARCHAR(src:paft) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:poff_ref_compnr), '0'), 38, 10) is null and 
                    src:poff_ref_compnr is not null) THEN
                    'poff_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:poff_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap), '0'), 38, 10) is null and 
                    src:qiap is not null) THEN
                    'qiap contains a non-numeric value : \'' || AS_VARCHAR(src:qiap) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiec), '0'), 38, 10) is null and 
                    src:qiec is not null) THEN
                    'qiec contains a non-numeric value : \'' || AS_VARCHAR(src:qiec) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qifi), '0'), 38, 10) is null and 
                    src:qifi is not null) THEN
                    'qifi contains a non-numeric value : \'' || AS_VARCHAR(src:qifi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigc), '0'), 38, 10) is null and 
                    src:qigc is not null) THEN
                    'qigc contains a non-numeric value : \'' || AS_VARCHAR(src:qigc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigc_buar), '0'), 38, 10) is null and 
                    src:qigc_buar is not null) THEN
                    'qigc_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qigc_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigc_buln), '0'), 38, 10) is null and 
                    src:qigc_buln is not null) THEN
                    'qigc_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qigc_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigc_bupc), '0'), 38, 10) is null and 
                    src:qigc_bupc is not null) THEN
                    'qigc_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qigc_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigc_butm), '0'), 38, 10) is null and 
                    src:qigc_butm is not null) THEN
                    'qigc_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qigc_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigc_buvl), '0'), 38, 10) is null and 
                    src:qigc_buvl is not null) THEN
                    'qigc_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qigc_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigc_buwg), '0'), 38, 10) is null and 
                    src:qigc_buwg is not null) THEN
                    'qigc_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qigc_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigc_trnu), '0'), 38, 10) is null and 
                    src:qigc_trnu is not null) THEN
                    'qigc_trnu contains a non-numeric value : \'' || AS_VARCHAR(src:qigc_trnu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigi), '0'), 38, 10) is null and 
                    src:qigi is not null) THEN
                    'qigi contains a non-numeric value : \'' || AS_VARCHAR(src:qigi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigi_buar), '0'), 38, 10) is null and 
                    src:qigi_buar is not null) THEN
                    'qigi_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qigi_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigi_buln), '0'), 38, 10) is null and 
                    src:qigi_buln is not null) THEN
                    'qigi_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qigi_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigi_bupc), '0'), 38, 10) is null and 
                    src:qigi_bupc is not null) THEN
                    'qigi_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qigi_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigi_butm), '0'), 38, 10) is null and 
                    src:qigi_butm is not null) THEN
                    'qigi_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qigi_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigi_buvl), '0'), 38, 10) is null and 
                    src:qigi_buvl is not null) THEN
                    'qigi_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qigi_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigi_buwg), '0'), 38, 10) is null and 
                    src:qigi_buwg is not null) THEN
                    'qigi_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qigi_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigi_trnu), '0'), 38, 10) is null and 
                    src:qigi_trnu is not null) THEN
                    'qigi_trnu contains a non-numeric value : \'' || AS_VARCHAR(src:qigi_trnu) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qima), '0'), 38, 10) is null and 
                    src:qima is not null) THEN
                    'qima contains a non-numeric value : \'' || AS_VARCHAR(src:qima) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qimf), '0'), 38, 10) is null and 
                    src:qimf is not null) THEN
                    'qimf contains a non-numeric value : \'' || AS_VARCHAR(src:qimf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qimi), '0'), 38, 10) is null and 
                    src:qimi is not null) THEN
                    'qimi contains a non-numeric value : \'' || AS_VARCHAR(src:qimi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qogo), '0'), 38, 10) is null and 
                    src:qogo is not null) THEN
                    'qogo contains a non-numeric value : \'' || AS_VARCHAR(src:qogo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qogo_buar), '0'), 38, 10) is null and 
                    src:qogo_buar is not null) THEN
                    'qogo_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qogo_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qogo_buln), '0'), 38, 10) is null and 
                    src:qogo_buln is not null) THEN
                    'qogo_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qogo_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qogo_bupc), '0'), 38, 10) is null and 
                    src:qogo_bupc is not null) THEN
                    'qogo_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qogo_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qogo_butm), '0'), 38, 10) is null and 
                    src:qogo_butm is not null) THEN
                    'qogo_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qogo_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qogo_buvl), '0'), 38, 10) is null and 
                    src:qogo_buvl is not null) THEN
                    'qogo_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qogo_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qogo_buwg), '0'), 38, 10) is null and 
                    src:qogo_buwg is not null) THEN
                    'qogo_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qogo_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qogo_invu), '0'), 38, 10) is null and 
                    src:qogo_invu is not null) THEN
                    'qogo_invu contains a non-numeric value : \'' || AS_VARCHAR(src:qogo_invu) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qual), '0'), 38, 10) is null and 
                    src:qual is not null) THEN
                    'qual contains a non-numeric value : \'' || AS_VARCHAR(src:qual) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) THEN
                    'rcod_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcod_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcsi_ref_compnr), '0'), 38, 10) is null and 
                    src:rcsi_ref_compnr is not null) THEN
                    'rcsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpdr), '0'), 38, 10) is null and 
                    src:rpdr is not null) THEN
                    'rpdr contains a non-numeric value : \'' || AS_VARCHAR(src:rpdr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbdt), '0'), 38, 10) is null and 
                    src:sbdt is not null) THEN
                    'sbdt contains a non-numeric value : \'' || AS_VARCHAR(src:sbdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbim), '0'), 38, 10) is null and 
                    src:sbim is not null) THEN
                    'sbim contains a non-numeric value : \'' || AS_VARCHAR(src:sbim) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbmt_ref_compnr), '0'), 38, 10) is null and 
                    src:sbmt_ref_compnr is not null) THEN
                    'sbmt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sbmt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scus), '0'), 38, 10) is null and 
                    src:scus is not null) THEN
                    'scus contains a non-numeric value : \'' || AS_VARCHAR(src:scus) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) THEN
                    'sdat contains a non-timestamp value : \'' || AS_VARCHAR(src:sdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdsc), '0'), 38, 10) is null and 
                    src:sdsc is not null) THEN
                    'sdsc contains a non-numeric value : \'' || AS_VARCHAR(src:sdsc) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) THEN
                    'subc contains a non-numeric value : \'' || AS_VARCHAR(src:subc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subs_ref_compnr), '0'), 38, 10) is null and 
                    src:subs_ref_compnr is not null) THEN
                    'subs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:subs_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtt), '0'), 38, 10) is null and 
                    src:txtt is not null) THEN
                    'txtt contains a non-numeric value : \'' || AS_VARCHAR(src:txtt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtt_ref_compnr), '0'), 38, 10) is null and 
                    src:txtt_ref_compnr is not null) THEN
                    'txtt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtt_ref_compnr) || '\'' WHEN 
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
                                    
                src:cofc,
                src:cono,
                src:pono,
                src:csqn,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR301 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aemq), '0'), 38, 10) is null and 
                    src:aemq is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:caga), '0'), 38, 10) is null and 
                    src:caga is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:caga_dtwc), '0'), 38, 10) is null and 
                    src:caga_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:caga_lclc), '0'), 38, 10) is null and 
                    src:caga_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:caga_rfrc), '0'), 38, 10) is null and 
                    src:caga_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:caga_rpc1), '0'), 38, 10) is null and 
                    src:caga_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:caga_rpc2), '0'), 38, 10) is null and 
                    src:caga_rpc2 is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccon_ref_compnr), '0'), 38, 10) is null and 
                    src:ccon_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdat), '1900-01-01')) is null and 
                    src:cdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_clmt), '0'), 38, 10) is null and 
                    src:cdf_clmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_comt), '0'), 38, 10) is null and 
                    src:cdf_comt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_ramt), '0'), 38, 10) is null and 
                    src:cdf_ramt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is null and 
                    src:citt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cltp), '0'), 38, 10) is null and 
                    src:cltp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cons), '0'), 38, 10) is null and 
                    src:cons is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crrf), '0'), 38, 10) is null and 
                    src:crrf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csqn), '0'), 38, 10) is null and 
                    src:csqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csts), '0'), 38, 10) is null and 
                    src:csts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqp_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvqp), '0'), 38, 10) is null and 
                    src:cvqp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsch), '0'), 38, 10) is null and 
                    src:dsch is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iaga), '0'), 38, 10) is null and 
                    src:iaga is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iaga_dtwc), '0'), 38, 10) is null and 
                    src:iaga_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iaga_lclc), '0'), 38, 10) is null and 
                    src:iaga_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iaga_rfrc), '0'), 38, 10) is null and 
                    src:iaga_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iaga_rpc1), '0'), 38, 10) is null and 
                    src:iaga_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iaga_rpc2), '0'), 38, 10) is null and 
                    src:iaga_rpc2 is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leng), '0'), 38, 10) is null and 
                    src:leng is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpnr_cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:mpnr_cmnf_ref_compnr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncmp), '0'), 38, 10) is null and 
                    src:ncmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncmp_ref_compnr), '0'), 38, 10) is null and 
                    src:ncmp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obpr), '0'), 38, 10) is null and 
                    src:obpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paft), '0'), 38, 10) is null and 
                    src:paft is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:poff_ref_compnr), '0'), 38, 10) is null and 
                    src:poff_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap), '0'), 38, 10) is null and 
                    src:qiap is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiec), '0'), 38, 10) is null and 
                    src:qiec is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qifi), '0'), 38, 10) is null and 
                    src:qifi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigc), '0'), 38, 10) is null and 
                    src:qigc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigc_buar), '0'), 38, 10) is null and 
                    src:qigc_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigc_buln), '0'), 38, 10) is null and 
                    src:qigc_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigc_bupc), '0'), 38, 10) is null and 
                    src:qigc_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigc_butm), '0'), 38, 10) is null and 
                    src:qigc_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigc_buvl), '0'), 38, 10) is null and 
                    src:qigc_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigc_buwg), '0'), 38, 10) is null and 
                    src:qigc_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigc_trnu), '0'), 38, 10) is null and 
                    src:qigc_trnu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigi), '0'), 38, 10) is null and 
                    src:qigi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigi_buar), '0'), 38, 10) is null and 
                    src:qigi_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigi_buln), '0'), 38, 10) is null and 
                    src:qigi_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigi_bupc), '0'), 38, 10) is null and 
                    src:qigi_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigi_butm), '0'), 38, 10) is null and 
                    src:qigi_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigi_buvl), '0'), 38, 10) is null and 
                    src:qigi_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigi_buwg), '0'), 38, 10) is null and 
                    src:qigi_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qigi_trnu), '0'), 38, 10) is null and 
                    src:qigi_trnu is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qima), '0'), 38, 10) is null and 
                    src:qima is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qimf), '0'), 38, 10) is null and 
                    src:qimf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qimi), '0'), 38, 10) is null and 
                    src:qimi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qogo), '0'), 38, 10) is null and 
                    src:qogo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qogo_buar), '0'), 38, 10) is null and 
                    src:qogo_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qogo_buln), '0'), 38, 10) is null and 
                    src:qogo_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qogo_bupc), '0'), 38, 10) is null and 
                    src:qogo_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qogo_butm), '0'), 38, 10) is null and 
                    src:qogo_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qogo_buvl), '0'), 38, 10) is null and 
                    src:qogo_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qogo_buwg), '0'), 38, 10) is null and 
                    src:qogo_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qogo_invu), '0'), 38, 10) is null and 
                    src:qogo_invu is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qual), '0'), 38, 10) is null and 
                    src:qual is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcsi_ref_compnr), '0'), 38, 10) is null and 
                    src:rcsi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpdr), '0'), 38, 10) is null and 
                    src:rpdr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbdt), '0'), 38, 10) is null and 
                    src:sbdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbim), '0'), 38, 10) is null and 
                    src:sbim is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbmt_ref_compnr), '0'), 38, 10) is null and 
                    src:sbmt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scus), '0'), 38, 10) is null and 
                    src:scus is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdsc), '0'), 38, 10) is null and 
                    src:sdsc is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subs_ref_compnr), '0'), 38, 10) is null and 
                    src:subs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:thic), '0'), 38, 10) is null and 
                    src:thic is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtt), '0'), 38, 10) is null and 
                    src:txtt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtt_ref_compnr), '0'), 38, 10) is null and 
                    src:txtt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:widt), '0'), 38, 10) is null and 
                    src:widt is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)