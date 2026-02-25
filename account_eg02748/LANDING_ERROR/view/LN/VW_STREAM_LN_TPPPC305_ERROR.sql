CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPPC305_ERROR AS SELECT src, 'LN_TPPPC305' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amhc_1), '0'), 38, 10) is null and 
                    src:amhc_1 is not null) THEN
                    'amhc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amhc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amhc_2), '0'), 38, 10) is null and 
                    src:amhc_2 is not null) THEN
                    'amhc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amhc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amhc_3), '0'), 38, 10) is null and 
                    src:amhc_3 is not null) THEN
                    'amhc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amhc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor), '0'), 38, 10) is null and 
                    src:amor is not null) THEN
                    'amor contains a non-numeric value : \'' || AS_VARCHAR(src:amor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor_cntc), '0'), 38, 10) is null and 
                    src:amor_cntc is not null) THEN
                    'amor_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:amor_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor_dwhc), '0'), 38, 10) is null and 
                    src:amor_dwhc is not null) THEN
                    'amor_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:amor_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor_homc), '0'), 38, 10) is null and 
                    src:amor_homc is not null) THEN
                    'amor_homc contains a non-numeric value : \'' || AS_VARCHAR(src:amor_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor_prjc), '0'), 38, 10) is null and 
                    src:amor_prjc is not null) THEN
                    'amor_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:amor_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor_refc), '0'), 38, 10) is null and 
                    src:amor_refc is not null) THEN
                    'amor_refc contains a non-numeric value : \'' || AS_VARCHAR(src:amor_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor_rpc1), '0'), 38, 10) is null and 
                    src:amor_rpc1 is not null) THEN
                    'amor_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:amor_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor_rpc2), '0'), 38, 10) is null and 
                    src:amor_rpc2 is not null) THEN
                    'amor_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:amor_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) THEN
                    'bptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) THEN
                    'ccco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) THEN
                    'ccty_cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdru), '1900-01-01')) is null and 
                    src:cdru is not null) THEN
                    'cdru contains a non-timestamp value : \'' || AS_VARCHAR(src:cdru) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdru_cser_ref_compnr), '0'), 38, 10) is null and 
                    src:cdru_cser_ref_compnr is not null) THEN
                    'cdru_cser_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdru_cser_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cntp), '0'), 38, 10) is null and 
                    src:cntp is not null) THEN
                    'cntp contains a non-numeric value : \'' || AS_VARCHAR(src:cntp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comf), '0'), 38, 10) is null and 
                    src:comf is not null) THEN
                    'comf contains a non-numeric value : \'' || AS_VARCHAR(src:comf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_ref_compnr is not null) THEN
                    'cono_cnln_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_cnln_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) THEN
                    'cono_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) THEN
                    'cprj_cpla_cact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_cact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) THEN
                    'cprj_cspa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cspa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) THEN
                    'cprj_cstl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cstl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptc_ref_compnr), '0'), 38, 10) is null and 
                    src:cptc_ref_compnr is not null) THEN
                    'cptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptc_year_peri_ref_compnr), '0'), 38, 10) is null and 
                    src:cptc_year_peri_ref_compnr is not null) THEN
                    'cptc_year_peri_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cptc_year_peri_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptf_fyea_fper_ref_compnr), '0'), 38, 10) is null and 
                    src:cptf_fyea_fper_ref_compnr is not null) THEN
                    'cptf_fyea_fper_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cptf_fyea_fper_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptf_ref_compnr), '0'), 38, 10) is null and 
                    src:cptf_ref_compnr is not null) THEN
                    'cptf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cptf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crdf), '0'), 38, 10) is null and 
                    src:crdf is not null) THEN
                    'crdf contains a non-numeric value : \'' || AS_VARCHAR(src:crdf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cser), '0'), 38, 10) is null and 
                    src:cser is not null) THEN
                    'cser contains a non-numeric value : \'' || AS_VARCHAR(src:cser) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) THEN
                    'cstv contains a non-numeric value : \'' || AS_VARCHAR(src:cstv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) THEN
                    'cuvc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuvc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) THEN
                    'cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:drun), '1900-01-01')) is null and 
                    src:drun is not null) THEN
                    'drun contains a non-timestamp value : \'' || AS_VARCHAR(src:drun) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:drun_serb_ref_compnr), '0'), 38, 10) is null and 
                    src:drun_serb_ref_compnr is not null) THEN
                    'drun_serb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:drun_serb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) THEN
                    'emno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:emno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) THEN
                    'exmt contains a non-numeric value : \'' || AS_VARCHAR(src:exmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is null and 
                    src:exrs_ref_compnr is not null) THEN
                    'exrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:exrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmp), '0'), 38, 10) is null and 
                    src:fcmp is not null) THEN
                    'fcmp contains a non-numeric value : \'' || AS_VARCHAR(src:fcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcrt), '0'), 38, 10) is null and 
                    src:fcrt is not null) THEN
                    'fcrt contains a non-numeric value : \'' || AS_VARCHAR(src:fcrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdoc), '0'), 38, 10) is null and 
                    src:fdoc is not null) THEN
                    'fdoc contains a non-numeric value : \'' || AS_VARCHAR(src:fdoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flin), '0'), 38, 10) is null and 
                    src:flin is not null) THEN
                    'flin contains a non-numeric value : \'' || AS_VARCHAR(src:flin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fper), '0'), 38, 10) is null and 
                    src:fper is not null) THEN
                    'fper contains a non-numeric value : \'' || AS_VARCHAR(src:fper) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:frgd), '1900-01-01')) is null and 
                    src:frgd is not null) THEN
                    'frgd contains a non-timestamp value : \'' || AS_VARCHAR(src:frgd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fsrl), '0'), 38, 10) is null and 
                    src:fsrl is not null) THEN
                    'fsrl contains a non-numeric value : \'' || AS_VARCHAR(src:fsrl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ftln), '0'), 38, 10) is null and 
                    src:ftln is not null) THEN
                    'ftln contains a non-numeric value : \'' || AS_VARCHAR(src:ftln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fyea), '0'), 38, 10) is null and 
                    src:fyea is not null) THEN
                    'fyea contains a non-numeric value : \'' || AS_VARCHAR(src:fyea) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hccr_1), '0'), 38, 10) is null and 
                    src:hccr_1 is not null) THEN
                    'hccr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:hccr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hccr_2), '0'), 38, 10) is null and 
                    src:hccr_2 is not null) THEN
                    'hccr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:hccr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hccr_3), '0'), 38, 10) is null and 
                    src:hccr_3 is not null) THEN
                    'hccr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:hccr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) THEN
                    'idoc contains a non-numeric value : \'' || AS_VARCHAR(src:idoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iseq), '0'), 38, 10) is null and 
                    src:iseq is not null) THEN
                    'iseq contains a non-numeric value : \'' || AS_VARCHAR(src:iseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) THEN
                    'itbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:koor), '0'), 38, 10) is null and 
                    src:koor is not null) THEN
                    'koor contains a non-numeric value : \'' || AS_VARCHAR(src:koor) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltdt), '1900-01-01')) is null and 
                    src:ltdt is not null) THEN
                    'ltdt contains a non-timestamp value : \'' || AS_VARCHAR(src:ltdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nadv), '0'), 38, 10) is null and 
                    src:nadv is not null) THEN
                    'nadv contains a non-numeric value : \'' || AS_VARCHAR(src:nadv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncmp), '0'), 38, 10) is null and 
                    src:ncmp is not null) THEN
                    'ncmp contains a non-numeric value : \'' || AS_VARCHAR(src:ncmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nins), '0'), 38, 10) is null and 
                    src:nins is not null) THEN
                    'nins contains a non-numeric value : \'' || AS_VARCHAR(src:nins) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp), '0'), 38, 10) is null and 
                    src:ocmp is not null) THEN
                    'ocmp contains a non-numeric value : \'' || AS_VARCHAR(src:ocmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peri), '0'), 38, 10) is null and 
                    src:peri is not null) THEN
                    'peri contains a non-numeric value : \'' || AS_VARCHAR(src:peri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:potf), '0'), 38, 10) is null and 
                    src:potf is not null) THEN
                    'potf contains a non-numeric value : \'' || AS_VARCHAR(src:potf) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) THEN
                    'prdt contains a non-timestamp value : \'' || AS_VARCHAR(src:prdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_1), '0'), 38, 10) is null and 
                    src:rate_1 is not null) THEN
                    'rate_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rate_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_2), '0'), 38, 10) is null and 
                    src:rate_2 is not null) THEN
                    'rate_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rate_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_3), '0'), 38, 10) is null and 
                    src:rate_3 is not null) THEN
                    'rate_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rate_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) THEN
                    'ratf_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) THEN
                    'ratf_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) THEN
                    'ratf_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) THEN
                    'rdat contains a non-timestamp value : \'' || AS_VARCHAR(src:rdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rest), '0'), 38, 10) is null and 
                    src:rest is not null) THEN
                    'rest contains a non-numeric value : \'' || AS_VARCHAR(src:rest) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfcr_1), '0'), 38, 10) is null and 
                    src:rfcr_1 is not null) THEN
                    'rfcr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rfcr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfcr_2), '0'), 38, 10) is null and 
                    src:rfcr_2 is not null) THEN
                    'rfcr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rfcr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfcr_3), '0'), 38, 10) is null and 
                    src:rfcr_3 is not null) THEN
                    'rfcr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rfcr_3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rgdt), '1900-01-01')) is null and 
                    src:rgdt is not null) THEN
                    'rgdt contains a non-timestamp value : \'' || AS_VARCHAR(src:rgdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsst), '0'), 38, 10) is null and 
                    src:rsst is not null) THEN
                    'rsst contains a non-numeric value : \'' || AS_VARCHAR(src:rsst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcr_1), '0'), 38, 10) is null and 
                    src:rtcr_1 is not null) THEN
                    'rtcr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcr_2), '0'), 38, 10) is null and 
                    src:rtcr_2 is not null) THEN
                    'rtcr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcr_3), '0'), 38, 10) is null and 
                    src:rtcr_3 is not null) THEN
                    'rtcr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) THEN
                    'rtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schd), '0'), 38, 10) is null and 
                    src:schd is not null) THEN
                    'schd contains a non-numeric value : \'' || AS_VARCHAR(src:schd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serb), '0'), 38, 10) is null and 
                    src:serb is not null) THEN
                    'serb contains a non-numeric value : \'' || AS_VARCHAR(src:serb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) THEN
                    'sern contains a non-numeric value : \'' || AS_VARCHAR(src:sern) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tetl), '0'), 38, 10) is null and 
                    src:tetl is not null) THEN
                    'tetl contains a non-numeric value : \'' || AS_VARCHAR(src:tetl) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) THEN
                    'year contains a non-numeric value : \'' || AS_VARCHAR(src:year) || '\'' WHEN 
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
                src:cpro,
                src:cprj,
                src:cspa,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPPC305 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amhc_1), '0'), 38, 10) is null and 
                    src:amhc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amhc_2), '0'), 38, 10) is null and 
                    src:amhc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amhc_3), '0'), 38, 10) is null and 
                    src:amhc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor), '0'), 38, 10) is null and 
                    src:amor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor_cntc), '0'), 38, 10) is null and 
                    src:amor_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor_dwhc), '0'), 38, 10) is null and 
                    src:amor_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor_homc), '0'), 38, 10) is null and 
                    src:amor_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor_prjc), '0'), 38, 10) is null and 
                    src:amor_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor_refc), '0'), 38, 10) is null and 
                    src:amor_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor_rpc1), '0'), 38, 10) is null and 
                    src:amor_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor_rpc2), '0'), 38, 10) is null and 
                    src:amor_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdru), '1900-01-01')) is null and 
                    src:cdru is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdru_cser_ref_compnr), '0'), 38, 10) is null and 
                    src:cdru_cser_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cntp), '0'), 38, 10) is null and 
                    src:cntp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comf), '0'), 38, 10) is null and 
                    src:comf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptc_ref_compnr), '0'), 38, 10) is null and 
                    src:cptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptc_year_peri_ref_compnr), '0'), 38, 10) is null and 
                    src:cptc_year_peri_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptf_fyea_fper_ref_compnr), '0'), 38, 10) is null and 
                    src:cptf_fyea_fper_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptf_ref_compnr), '0'), 38, 10) is null and 
                    src:cptf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crdf), '0'), 38, 10) is null and 
                    src:crdf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cser), '0'), 38, 10) is null and 
                    src:cser is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:drun), '1900-01-01')) is null and 
                    src:drun is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:drun_serb_ref_compnr), '0'), 38, 10) is null and 
                    src:drun_serb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is null and 
                    src:exrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmp), '0'), 38, 10) is null and 
                    src:fcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcrt), '0'), 38, 10) is null and 
                    src:fcrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdoc), '0'), 38, 10) is null and 
                    src:fdoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flin), '0'), 38, 10) is null and 
                    src:flin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fper), '0'), 38, 10) is null and 
                    src:fper is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:frgd), '1900-01-01')) is null and 
                    src:frgd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fsrl), '0'), 38, 10) is null and 
                    src:fsrl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ftln), '0'), 38, 10) is null and 
                    src:ftln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fyea), '0'), 38, 10) is null and 
                    src:fyea is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hccr_1), '0'), 38, 10) is null and 
                    src:hccr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hccr_2), '0'), 38, 10) is null and 
                    src:hccr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hccr_3), '0'), 38, 10) is null and 
                    src:hccr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iseq), '0'), 38, 10) is null and 
                    src:iseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:koor), '0'), 38, 10) is null and 
                    src:koor is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltdt), '1900-01-01')) is null and 
                    src:ltdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nadv), '0'), 38, 10) is null and 
                    src:nadv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncmp), '0'), 38, 10) is null and 
                    src:ncmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nins), '0'), 38, 10) is null and 
                    src:nins is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp), '0'), 38, 10) is null and 
                    src:ocmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peri), '0'), 38, 10) is null and 
                    src:peri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:potf), '0'), 38, 10) is null and 
                    src:potf is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_1), '0'), 38, 10) is null and 
                    src:rate_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_2), '0'), 38, 10) is null and 
                    src:rate_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_3), '0'), 38, 10) is null and 
                    src:rate_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rest), '0'), 38, 10) is null and 
                    src:rest is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfcr_1), '0'), 38, 10) is null and 
                    src:rfcr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfcr_2), '0'), 38, 10) is null and 
                    src:rfcr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfcr_3), '0'), 38, 10) is null and 
                    src:rfcr_3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rgdt), '1900-01-01')) is null and 
                    src:rgdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsst), '0'), 38, 10) is null and 
                    src:rsst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcr_1), '0'), 38, 10) is null and 
                    src:rtcr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcr_2), '0'), 38, 10) is null and 
                    src:rtcr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcr_3), '0'), 38, 10) is null and 
                    src:rtcr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schd), '0'), 38, 10) is null and 
                    src:schd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serb), '0'), 38, 10) is null and 
                    src:serb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tetl), '0'), 38, 10) is null and 
                    src:tetl is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)