CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPPC350_ERROR AS SELECT src, 'LN_TPPPC350' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afin), '0'), 38, 10) is null and 
                    src:afin is not null) THEN
                    'afin contains a non-numeric value : \'' || AS_VARCHAR(src:afin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amhc_1), '0'), 38, 10) is null and 
                    src:amhc_1 is not null) THEN
                    'amhc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amhc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amhc_2), '0'), 38, 10) is null and 
                    src:amhc_2 is not null) THEN
                    'amhc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amhc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amhc_3), '0'), 38, 10) is null and 
                    src:amhc_3 is not null) THEN
                    'amhc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amhc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) THEN
                    'amnt contains a non-numeric value : \'' || AS_VARCHAR(src:amnt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:apdt), '1900-01-01')) is null and 
                    src:apdt is not null) THEN
                    'apdt contains a non-timestamp value : \'' || AS_VARCHAR(src:apdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfpp), '0'), 38, 10) is null and 
                    src:bfpp is not null) THEN
                    'bfpp contains a non-numeric value : \'' || AS_VARCHAR(src:bfpp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfrf), '0'), 38, 10) is null and 
                    src:bfrf is not null) THEN
                    'bfrf contains a non-numeric value : \'' || AS_VARCHAR(src:bfrf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgam), '0'), 38, 10) is null and 
                    src:bgam is not null) THEN
                    'bgam contains a non-numeric value : \'' || AS_VARCHAR(src:bgam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blnc), '0'), 38, 10) is null and 
                    src:blnc is not null) THEN
                    'blnc contains a non-numeric value : \'' || AS_VARCHAR(src:blnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blnc_cntc), '0'), 38, 10) is null and 
                    src:blnc_cntc is not null) THEN
                    'blnc_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:blnc_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blnc_dwhc), '0'), 38, 10) is null and 
                    src:blnc_dwhc is not null) THEN
                    'blnc_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:blnc_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blnc_homc), '0'), 38, 10) is null and 
                    src:blnc_homc is not null) THEN
                    'blnc_homc contains a non-numeric value : \'' || AS_VARCHAR(src:blnc_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blnc_prjc), '0'), 38, 10) is null and 
                    src:blnc_prjc is not null) THEN
                    'blnc_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:blnc_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blnc_refc), '0'), 38, 10) is null and 
                    src:blnc_refc is not null) THEN
                    'blnc_refc contains a non-numeric value : \'' || AS_VARCHAR(src:blnc_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blnc_rpc1), '0'), 38, 10) is null and 
                    src:blnc_rpc1 is not null) THEN
                    'blnc_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:blnc_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blnc_rpc2), '0'), 38, 10) is null and 
                    src:blnc_rpc2 is not null) THEN
                    'blnc_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:blnc_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btcs), '0'), 38, 10) is null and 
                    src:btcs is not null) THEN
                    'btcs contains a non-numeric value : \'' || AS_VARCHAR(src:btcs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camc), '0'), 38, 10) is null and 
                    src:camc is not null) THEN
                    'camc contains a non-numeric value : \'' || AS_VARCHAR(src:camc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camc_cntc), '0'), 38, 10) is null and 
                    src:camc_cntc is not null) THEN
                    'camc_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:camc_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camc_dwhc), '0'), 38, 10) is null and 
                    src:camc_dwhc is not null) THEN
                    'camc_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:camc_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camc_prjc), '0'), 38, 10) is null and 
                    src:camc_prjc is not null) THEN
                    'camc_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:camc_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camc_refc), '0'), 38, 10) is null and 
                    src:camc_refc is not null) THEN
                    'camc_refc contains a non-numeric value : \'' || AS_VARCHAR(src:camc_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr), '0'), 38, 10) is null and 
                    src:camr is not null) THEN
                    'camr contains a non-numeric value : \'' || AS_VARCHAR(src:camr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr_cntc), '0'), 38, 10) is null and 
                    src:camr_cntc is not null) THEN
                    'camr_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:camr_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr_dwhc), '0'), 38, 10) is null and 
                    src:camr_dwhc is not null) THEN
                    'camr_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:camr_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr_prjc), '0'), 38, 10) is null and 
                    src:camr_prjc is not null) THEN
                    'camr_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:camr_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr_refc), '0'), 38, 10) is null and 
                    src:camr_refc is not null) THEN
                    'camr_refc contains a non-numeric value : \'' || AS_VARCHAR(src:camr_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdru), '1900-01-01')) is null and 
                    src:cdru is not null) THEN
                    'cdru contains a non-timestamp value : \'' || AS_VARCHAR(src:cdru) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cert_ref_compnr), '0'), 38, 10) is null and 
                    src:cert_ref_compnr is not null) THEN
                    'cert_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cert_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfpo), '0'), 38, 10) is null and 
                    src:cfpo is not null) THEN
                    'cfpo contains a non-numeric value : \'' || AS_VARCHAR(src:cfpo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chcc_1), '0'), 38, 10) is null and 
                    src:chcc_1 is not null) THEN
                    'chcc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:chcc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chcc_2), '0'), 38, 10) is null and 
                    src:chcc_2 is not null) THEN
                    'chcc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:chcc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chcc_3), '0'), 38, 10) is null and 
                    src:chcc_3 is not null) THEN
                    'chcc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:chcc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chcr_1), '0'), 38, 10) is null and 
                    src:chcr_1 is not null) THEN
                    'chcr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:chcr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chcr_2), '0'), 38, 10) is null and 
                    src:chcr_2 is not null) THEN
                    'chcr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:chcr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chcr_3), '0'), 38, 10) is null and 
                    src:chcr_3 is not null) THEN
                    'chcr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:chcr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clmg), '0'), 38, 10) is null and 
                    src:clmg is not null) THEN
                    'clmg contains a non-numeric value : \'' || AS_VARCHAR(src:clmg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmfb), '0'), 38, 10) is null and 
                    src:cmfb is not null) THEN
                    'cmfb contains a non-numeric value : \'' || AS_VARCHAR(src:cmfb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmpc), '0'), 38, 10) is null and 
                    src:cmpc is not null) THEN
                    'cmpc contains a non-numeric value : \'' || AS_VARCHAR(src:cmpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cogs), '0'), 38, 10) is null and 
                    src:cogs is not null) THEN
                    'cogs contains a non-numeric value : \'' || AS_VARCHAR(src:cogs) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr), '0'), 38, 10) is null and 
                    src:copr is not null) THEN
                    'copr contains a non-numeric value : \'' || AS_VARCHAR(src:copr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprv_ref_compnr), '0'), 38, 10) is null and 
                    src:cprv_ref_compnr is not null) THEN
                    'cprv_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprv_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptc_year_peri_ref_compnr), '0'), 38, 10) is null and 
                    src:cptc_year_peri_ref_compnr is not null) THEN
                    'cptc_year_peri_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cptc_year_peri_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptf_fyea_fper_ref_compnr), '0'), 38, 10) is null and 
                    src:cptf_fyea_fper_ref_compnr is not null) THEN
                    'cptf_fyea_fper_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cptf_fyea_fper_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdc), '1900-01-01')) is null and 
                    src:crdc is not null) THEN
                    'crdc contains a non-timestamp value : \'' || AS_VARCHAR(src:crdc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdr), '1900-01-01')) is null and 
                    src:crdr is not null) THEN
                    'crdr contains a non-timestamp value : \'' || AS_VARCHAR(src:crdr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crfc_1), '0'), 38, 10) is null and 
                    src:crfc_1 is not null) THEN
                    'crfc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:crfc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crfc_2), '0'), 38, 10) is null and 
                    src:crfc_2 is not null) THEN
                    'crfc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:crfc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crfc_3), '0'), 38, 10) is null and 
                    src:crfc_3 is not null) THEN
                    'crfc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:crfc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crfr_1), '0'), 38, 10) is null and 
                    src:crfr_1 is not null) THEN
                    'crfr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:crfr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crfr_2), '0'), 38, 10) is null and 
                    src:crfr_2 is not null) THEN
                    'crfr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:crfr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crfr_3), '0'), 38, 10) is null and 
                    src:crfr_3 is not null) THEN
                    'crfr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:crfr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtc_1), '0'), 38, 10) is null and 
                    src:crtc_1 is not null) THEN
                    'crtc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:crtc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtc_2), '0'), 38, 10) is null and 
                    src:crtc_2 is not null) THEN
                    'crtc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:crtc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtc_3), '0'), 38, 10) is null and 
                    src:crtc_3 is not null) THEN
                    'crtc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:crtc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtr_1), '0'), 38, 10) is null and 
                    src:crtr_1 is not null) THEN
                    'crtr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:crtr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtr_2), '0'), 38, 10) is null and 
                    src:crtr_2 is not null) THEN
                    'crtr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:crtr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtr_3), '0'), 38, 10) is null and 
                    src:crtr_3 is not null) THEN
                    'crtr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:crtr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cser), '0'), 38, 10) is null and 
                    src:cser is not null) THEN
                    'cser contains a non-numeric value : \'' || AS_VARCHAR(src:cser) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctic), '0'), 38, 10) is null and 
                    src:ctic is not null) THEN
                    'ctic contains a non-numeric value : \'' || AS_VARCHAR(src:ctic) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:drun), '1900-01-01')) is null and 
                    src:drun is not null) THEN
                    'drun contains a non-timestamp value : \'' || AS_VARCHAR(src:drun) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:earf), '0'), 38, 10) is null and 
                    src:earf is not null) THEN
                    'earf contains a non-numeric value : \'' || AS_VARCHAR(src:earf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:estc), '0'), 38, 10) is null and 
                    src:estc is not null) THEN
                    'estc contains a non-numeric value : \'' || AS_VARCHAR(src:estc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etcm), '0'), 38, 10) is null and 
                    src:etcm is not null) THEN
                    'etcm contains a non-numeric value : \'' || AS_VARCHAR(src:etcm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exrv), '0'), 38, 10) is null and 
                    src:exrv is not null) THEN
                    'exrv contains a non-numeric value : \'' || AS_VARCHAR(src:exrv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:farv), '0'), 38, 10) is null and 
                    src:farv is not null) THEN
                    'farv contains a non-numeric value : \'' || AS_VARCHAR(src:farv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fper), '0'), 38, 10) is null and 
                    src:fper is not null) THEN
                    'fper contains a non-numeric value : \'' || AS_VARCHAR(src:fper) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:frgd), '1900-01-01')) is null and 
                    src:frgd is not null) THEN
                    'frgd contains a non-timestamp value : \'' || AS_VARCHAR(src:frgd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fyea), '0'), 38, 10) is null and 
                    src:fyea is not null) THEN
                    'fyea contains a non-numeric value : \'' || AS_VARCHAR(src:fyea) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:heac), '0'), 38, 10) is null and 
                    src:heac is not null) THEN
                    'heac contains a non-numeric value : \'' || AS_VARCHAR(src:heac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hetc), '0'), 38, 10) is null and 
                    src:hetc is not null) THEN
                    'hetc contains a non-numeric value : \'' || AS_VARCHAR(src:hetc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hrbg), '0'), 38, 10) is null and 
                    src:hrbg is not null) THEN
                    'hrbg contains a non-numeric value : \'' || AS_VARCHAR(src:hrbg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hrct), '0'), 38, 10) is null and 
                    src:hrct is not null) THEN
                    'hrct contains a non-numeric value : \'' || AS_VARCHAR(src:hrct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:irby), '0'), 38, 10) is null and 
                    src:irby is not null) THEN
                    'irby contains a non-numeric value : \'' || AS_VARCHAR(src:irby) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iror), '0'), 38, 10) is null and 
                    src:iror is not null) THEN
                    'iror contains a non-numeric value : \'' || AS_VARCHAR(src:iror) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:irsc), '0'), 38, 10) is null and 
                    src:irsc is not null) THEN
                    'irsc contains a non-numeric value : \'' || AS_VARCHAR(src:irsc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltdt), '1900-01-01')) is null and 
                    src:ltdt is not null) THEN
                    'ltdt contains a non-timestamp value : \'' || AS_VARCHAR(src:ltdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmp), '0'), 38, 10) is null and 
                    src:pcmp is not null) THEN
                    'pcmp contains a non-numeric value : \'' || AS_VARCHAR(src:pcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peri), '0'), 38, 10) is null and 
                    src:peri is not null) THEN
                    'peri contains a non-numeric value : \'' || AS_VARCHAR(src:peri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfob), '0'), 38, 10) is null and 
                    src:pfob is not null) THEN
                    'pfob contains a non-numeric value : \'' || AS_VARCHAR(src:pfob) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:post), '0'), 38, 10) is null and 
                    src:post is not null) THEN
                    'post contains a non-numeric value : \'' || AS_VARCHAR(src:post) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prpc), '0'), 38, 10) is null and 
                    src:prpc is not null) THEN
                    'prpc contains a non-numeric value : \'' || AS_VARCHAR(src:prpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prvt), '0'), 38, 10) is null and 
                    src:prvt is not null) THEN
                    'prvt contains a non-numeric value : \'' || AS_VARCHAR(src:prvt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pstb), '0'), 38, 10) is null and 
                    src:pstb is not null) THEN
                    'pstb contains a non-numeric value : \'' || AS_VARCHAR(src:pstb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramc), '0'), 38, 10) is null and 
                    src:ramc is not null) THEN
                    'ramc contains a non-numeric value : \'' || AS_VARCHAR(src:ramc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramc_cntc), '0'), 38, 10) is null and 
                    src:ramc_cntc is not null) THEN
                    'ramc_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:ramc_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramc_dwhc), '0'), 38, 10) is null and 
                    src:ramc_dwhc is not null) THEN
                    'ramc_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:ramc_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramc_prjc), '0'), 38, 10) is null and 
                    src:ramc_prjc is not null) THEN
                    'ramc_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:ramc_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramc_refc), '0'), 38, 10) is null and 
                    src:ramc_refc is not null) THEN
                    'ramc_refc contains a non-numeric value : \'' || AS_VARCHAR(src:ramc_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramr), '0'), 38, 10) is null and 
                    src:ramr is not null) THEN
                    'ramr contains a non-numeric value : \'' || AS_VARCHAR(src:ramr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramr_cntc), '0'), 38, 10) is null and 
                    src:ramr_cntc is not null) THEN
                    'ramr_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:ramr_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramr_dwhc), '0'), 38, 10) is null and 
                    src:ramr_dwhc is not null) THEN
                    'ramr_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:ramr_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramr_prjc), '0'), 38, 10) is null and 
                    src:ramr_prjc is not null) THEN
                    'ramr_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:ramr_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramr_refc), '0'), 38, 10) is null and 
                    src:ramr_refc is not null) THEN
                    'ramr_refc contains a non-numeric value : \'' || AS_VARCHAR(src:ramr_refc) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rert_ref_compnr), '0'), 38, 10) is null and 
                    src:rert_ref_compnr is not null) THEN
                    'rert_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rert_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revl), '0'), 38, 10) is null and 
                    src:revl is not null) THEN
                    'revl contains a non-numeric value : \'' || AS_VARCHAR(src:revl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revr), '0'), 38, 10) is null and 
                    src:revr is not null) THEN
                    'revr contains a non-numeric value : \'' || AS_VARCHAR(src:revr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revt), '0'), 38, 10) is null and 
                    src:revt is not null) THEN
                    'revt contains a non-numeric value : \'' || AS_VARCHAR(src:revt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rgdt), '1900-01-01')) is null and 
                    src:rgdt is not null) THEN
                    'rgdt contains a non-timestamp value : \'' || AS_VARCHAR(src:rgdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rhcc_1), '0'), 38, 10) is null and 
                    src:rhcc_1 is not null) THEN
                    'rhcc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rhcc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rhcc_2), '0'), 38, 10) is null and 
                    src:rhcc_2 is not null) THEN
                    'rhcc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rhcc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rhcc_3), '0'), 38, 10) is null and 
                    src:rhcc_3 is not null) THEN
                    'rhcc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rhcc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rhcr_1), '0'), 38, 10) is null and 
                    src:rhcr_1 is not null) THEN
                    'rhcr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rhcr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rhcr_2), '0'), 38, 10) is null and 
                    src:rhcr_2 is not null) THEN
                    'rhcr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rhcr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rhcr_3), '0'), 38, 10) is null and 
                    src:rhcr_3 is not null) THEN
                    'rhcr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rhcr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrby), '0'), 38, 10) is null and 
                    src:rrby is not null) THEN
                    'rrby contains a non-numeric value : \'' || AS_VARCHAR(src:rrby) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rrdc), '1900-01-01')) is null and 
                    src:rrdc is not null) THEN
                    'rrdc contains a non-timestamp value : \'' || AS_VARCHAR(src:rrdc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rrdr), '1900-01-01')) is null and 
                    src:rrdr is not null) THEN
                    'rrdr contains a non-timestamp value : \'' || AS_VARCHAR(src:rrdr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrfc_1), '0'), 38, 10) is null and 
                    src:rrfc_1 is not null) THEN
                    'rrfc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rrfc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrfc_2), '0'), 38, 10) is null and 
                    src:rrfc_2 is not null) THEN
                    'rrfc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rrfc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrfc_3), '0'), 38, 10) is null and 
                    src:rrfc_3 is not null) THEN
                    'rrfc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rrfc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrfr_1), '0'), 38, 10) is null and 
                    src:rrfr_1 is not null) THEN
                    'rrfr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rrfr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrfr_2), '0'), 38, 10) is null and 
                    src:rrfr_2 is not null) THEN
                    'rrfr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rrfr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrfr_3), '0'), 38, 10) is null and 
                    src:rrfr_3 is not null) THEN
                    'rrfr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rrfr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtc_1), '0'), 38, 10) is null and 
                    src:rrtc_1 is not null) THEN
                    'rrtc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rrtc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtc_2), '0'), 38, 10) is null and 
                    src:rrtc_2 is not null) THEN
                    'rrtc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rrtc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtc_3), '0'), 38, 10) is null and 
                    src:rrtc_3 is not null) THEN
                    'rrtc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rrtc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtr_1), '0'), 38, 10) is null and 
                    src:rrtr_1 is not null) THEN
                    'rrtr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rrtr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtr_2), '0'), 38, 10) is null and 
                    src:rrtr_2 is not null) THEN
                    'rrtr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rrtr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtr_3), '0'), 38, 10) is null and 
                    src:rrtr_3 is not null) THEN
                    'rrtr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rrtr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtco_ref_compnr), '0'), 38, 10) is null and 
                    src:rtco_ref_compnr is not null) THEN
                    'rtco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtre_ref_compnr), '0'), 38, 10) is null and 
                    src:rtre_ref_compnr is not null) THEN
                    'rtre_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtre_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) THEN
                    'rtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serb), '0'), 38, 10) is null and 
                    src:serb is not null) THEN
                    'serb contains a non-numeric value : \'' || AS_VARCHAR(src:serb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) THEN
                    'sern contains a non-numeric value : \'' || AS_VARCHAR(src:sern) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trpr), '0'), 38, 10) is null and 
                    src:trpr is not null) THEN
                    'trpr contains a non-numeric value : \'' || AS_VARCHAR(src:trpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtn), '0'), 38, 10) is null and 
                    src:txtn is not null) THEN
                    'txtn contains a non-numeric value : \'' || AS_VARCHAR(src:txtn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtn_ref_compnr), '0'), 38, 10) is null and 
                    src:txtn_ref_compnr is not null) THEN
                    'txtn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtn_ref_compnr) || '\'' WHEN 
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
                                    
                src:compnr,
                src:cspa,
                src:sern,
                src:cact,
                src:cprj,
                src:cstl  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPPC350 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afin), '0'), 38, 10) is null and 
                    src:afin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amhc_1), '0'), 38, 10) is null and 
                    src:amhc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amhc_2), '0'), 38, 10) is null and 
                    src:amhc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amhc_3), '0'), 38, 10) is null and 
                    src:amhc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:apdt), '1900-01-01')) is null and 
                    src:apdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfpp), '0'), 38, 10) is null and 
                    src:bfpp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfrf), '0'), 38, 10) is null and 
                    src:bfrf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgam), '0'), 38, 10) is null and 
                    src:bgam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blnc), '0'), 38, 10) is null and 
                    src:blnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blnc_cntc), '0'), 38, 10) is null and 
                    src:blnc_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blnc_dwhc), '0'), 38, 10) is null and 
                    src:blnc_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blnc_homc), '0'), 38, 10) is null and 
                    src:blnc_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blnc_prjc), '0'), 38, 10) is null and 
                    src:blnc_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blnc_refc), '0'), 38, 10) is null and 
                    src:blnc_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blnc_rpc1), '0'), 38, 10) is null and 
                    src:blnc_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blnc_rpc2), '0'), 38, 10) is null and 
                    src:blnc_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btcs), '0'), 38, 10) is null and 
                    src:btcs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camc), '0'), 38, 10) is null and 
                    src:camc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camc_cntc), '0'), 38, 10) is null and 
                    src:camc_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camc_dwhc), '0'), 38, 10) is null and 
                    src:camc_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camc_prjc), '0'), 38, 10) is null and 
                    src:camc_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camc_refc), '0'), 38, 10) is null and 
                    src:camc_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr), '0'), 38, 10) is null and 
                    src:camr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr_cntc), '0'), 38, 10) is null and 
                    src:camr_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr_dwhc), '0'), 38, 10) is null and 
                    src:camr_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr_prjc), '0'), 38, 10) is null and 
                    src:camr_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr_refc), '0'), 38, 10) is null and 
                    src:camr_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdru), '1900-01-01')) is null and 
                    src:cdru is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cert_ref_compnr), '0'), 38, 10) is null and 
                    src:cert_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfpo), '0'), 38, 10) is null and 
                    src:cfpo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chcc_1), '0'), 38, 10) is null and 
                    src:chcc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chcc_2), '0'), 38, 10) is null and 
                    src:chcc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chcc_3), '0'), 38, 10) is null and 
                    src:chcc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chcr_1), '0'), 38, 10) is null and 
                    src:chcr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chcr_2), '0'), 38, 10) is null and 
                    src:chcr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chcr_3), '0'), 38, 10) is null and 
                    src:chcr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clmg), '0'), 38, 10) is null and 
                    src:clmg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmfb), '0'), 38, 10) is null and 
                    src:cmfb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmpc), '0'), 38, 10) is null and 
                    src:cmpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cogs), '0'), 38, 10) is null and 
                    src:cogs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comf), '0'), 38, 10) is null and 
                    src:comf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr), '0'), 38, 10) is null and 
                    src:copr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprv_ref_compnr), '0'), 38, 10) is null and 
                    src:cprv_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptc_year_peri_ref_compnr), '0'), 38, 10) is null and 
                    src:cptc_year_peri_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptf_fyea_fper_ref_compnr), '0'), 38, 10) is null and 
                    src:cptf_fyea_fper_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdc), '1900-01-01')) is null and 
                    src:crdc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdr), '1900-01-01')) is null and 
                    src:crdr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crfc_1), '0'), 38, 10) is null and 
                    src:crfc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crfc_2), '0'), 38, 10) is null and 
                    src:crfc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crfc_3), '0'), 38, 10) is null and 
                    src:crfc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crfr_1), '0'), 38, 10) is null and 
                    src:crfr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crfr_2), '0'), 38, 10) is null and 
                    src:crfr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crfr_3), '0'), 38, 10) is null and 
                    src:crfr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtc_1), '0'), 38, 10) is null and 
                    src:crtc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtc_2), '0'), 38, 10) is null and 
                    src:crtc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtc_3), '0'), 38, 10) is null and 
                    src:crtc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtr_1), '0'), 38, 10) is null and 
                    src:crtr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtr_2), '0'), 38, 10) is null and 
                    src:crtr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtr_3), '0'), 38, 10) is null and 
                    src:crtr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cser), '0'), 38, 10) is null and 
                    src:cser is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctic), '0'), 38, 10) is null and 
                    src:ctic is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:drun), '1900-01-01')) is null and 
                    src:drun is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:earf), '0'), 38, 10) is null and 
                    src:earf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:estc), '0'), 38, 10) is null and 
                    src:estc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etcm), '0'), 38, 10) is null and 
                    src:etcm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exrv), '0'), 38, 10) is null and 
                    src:exrv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:farv), '0'), 38, 10) is null and 
                    src:farv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fper), '0'), 38, 10) is null and 
                    src:fper is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:frgd), '1900-01-01')) is null and 
                    src:frgd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fyea), '0'), 38, 10) is null and 
                    src:fyea is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:heac), '0'), 38, 10) is null and 
                    src:heac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hetc), '0'), 38, 10) is null and 
                    src:hetc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hrbg), '0'), 38, 10) is null and 
                    src:hrbg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hrct), '0'), 38, 10) is null and 
                    src:hrct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:irby), '0'), 38, 10) is null and 
                    src:irby is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iror), '0'), 38, 10) is null and 
                    src:iror is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:irsc), '0'), 38, 10) is null and 
                    src:irsc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltdt), '1900-01-01')) is null and 
                    src:ltdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmp), '0'), 38, 10) is null and 
                    src:pcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peri), '0'), 38, 10) is null and 
                    src:peri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfob), '0'), 38, 10) is null and 
                    src:pfob is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:post), '0'), 38, 10) is null and 
                    src:post is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prpc), '0'), 38, 10) is null and 
                    src:prpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prvt), '0'), 38, 10) is null and 
                    src:prvt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pstb), '0'), 38, 10) is null and 
                    src:pstb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramc), '0'), 38, 10) is null and 
                    src:ramc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramc_cntc), '0'), 38, 10) is null and 
                    src:ramc_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramc_dwhc), '0'), 38, 10) is null and 
                    src:ramc_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramc_prjc), '0'), 38, 10) is null and 
                    src:ramc_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramc_refc), '0'), 38, 10) is null and 
                    src:ramc_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramr), '0'), 38, 10) is null and 
                    src:ramr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramr_cntc), '0'), 38, 10) is null and 
                    src:ramr_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramr_dwhc), '0'), 38, 10) is null and 
                    src:ramr_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramr_prjc), '0'), 38, 10) is null and 
                    src:ramr_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramr_refc), '0'), 38, 10) is null and 
                    src:ramr_refc is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rert_ref_compnr), '0'), 38, 10) is null and 
                    src:rert_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revl), '0'), 38, 10) is null and 
                    src:revl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revr), '0'), 38, 10) is null and 
                    src:revr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revt), '0'), 38, 10) is null and 
                    src:revt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rgdt), '1900-01-01')) is null and 
                    src:rgdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rhcc_1), '0'), 38, 10) is null and 
                    src:rhcc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rhcc_2), '0'), 38, 10) is null and 
                    src:rhcc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rhcc_3), '0'), 38, 10) is null and 
                    src:rhcc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rhcr_1), '0'), 38, 10) is null and 
                    src:rhcr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rhcr_2), '0'), 38, 10) is null and 
                    src:rhcr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rhcr_3), '0'), 38, 10) is null and 
                    src:rhcr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrby), '0'), 38, 10) is null and 
                    src:rrby is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rrdc), '1900-01-01')) is null and 
                    src:rrdc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rrdr), '1900-01-01')) is null and 
                    src:rrdr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrfc_1), '0'), 38, 10) is null and 
                    src:rrfc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrfc_2), '0'), 38, 10) is null and 
                    src:rrfc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrfc_3), '0'), 38, 10) is null and 
                    src:rrfc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrfr_1), '0'), 38, 10) is null and 
                    src:rrfr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrfr_2), '0'), 38, 10) is null and 
                    src:rrfr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrfr_3), '0'), 38, 10) is null and 
                    src:rrfr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtc_1), '0'), 38, 10) is null and 
                    src:rrtc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtc_2), '0'), 38, 10) is null and 
                    src:rrtc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtc_3), '0'), 38, 10) is null and 
                    src:rrtc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtr_1), '0'), 38, 10) is null and 
                    src:rrtr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtr_2), '0'), 38, 10) is null and 
                    src:rrtr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtr_3), '0'), 38, 10) is null and 
                    src:rrtr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtco_ref_compnr), '0'), 38, 10) is null and 
                    src:rtco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtre_ref_compnr), '0'), 38, 10) is null and 
                    src:rtre_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serb), '0'), 38, 10) is null and 
                    src:serb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trpr), '0'), 38, 10) is null and 
                    src:trpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtn), '0'), 38, 10) is null and 
                    src:txtn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtn_ref_compnr), '0'), 38, 10) is null and 
                    src:txtn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)