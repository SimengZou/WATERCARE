CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPTC311_ERROR AS SELECT src, 'LN_TPPTC311' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaoc_1), '0'), 38, 10) is null and 
                    src:aaoc_1 is not null) THEN
                    'aaoc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:aaoc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaoc_2), '0'), 38, 10) is null and 
                    src:aaoc_2 is not null) THEN
                    'aaoc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:aaoc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaoc_3), '0'), 38, 10) is null and 
                    src:aaoc_3 is not null) THEN
                    'aaoc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:aaoc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaoc_4), '0'), 38, 10) is null and 
                    src:aaoc_4 is not null) THEN
                    'aaoc_4 contains a non-numeric value : \'' || AS_VARCHAR(src:aaoc_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaos_1), '0'), 38, 10) is null and 
                    src:aaos_1 is not null) THEN
                    'aaos_1 contains a non-numeric value : \'' || AS_VARCHAR(src:aaos_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaos_2), '0'), 38, 10) is null and 
                    src:aaos_2 is not null) THEN
                    'aaos_2 contains a non-numeric value : \'' || AS_VARCHAR(src:aaos_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaos_3), '0'), 38, 10) is null and 
                    src:aaos_3 is not null) THEN
                    'aaos_3 contains a non-numeric value : \'' || AS_VARCHAR(src:aaos_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaos_4), '0'), 38, 10) is null and 
                    src:aaos_4 is not null) THEN
                    'aaos_4 contains a non-numeric value : \'' || AS_VARCHAR(src:aaos_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adjm), '0'), 38, 10) is null and 
                    src:adjm is not null) THEN
                    'adjm contains a non-numeric value : \'' || AS_VARCHAR(src:adjm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc), '0'), 38, 10) is null and 
                    src:amoc is not null) THEN
                    'amoc contains a non-numeric value : \'' || AS_VARCHAR(src:amoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_cntc), '0'), 38, 10) is null and 
                    src:amoc_cntc is not null) THEN
                    'amoc_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:amoc_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_dwhc), '0'), 38, 10) is null and 
                    src:amoc_dwhc is not null) THEN
                    'amoc_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:amoc_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_prjc), '0'), 38, 10) is null and 
                    src:amoc_prjc is not null) THEN
                    'amoc_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:amoc_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_refc), '0'), 38, 10) is null and 
                    src:amoc_refc is not null) THEN
                    'amoc_refc contains a non-numeric value : \'' || AS_VARCHAR(src:amoc_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos), '0'), 38, 10) is null and 
                    src:amos is not null) THEN
                    'amos contains a non-numeric value : \'' || AS_VARCHAR(src:amos) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_cntc), '0'), 38, 10) is null and 
                    src:amos_cntc is not null) THEN
                    'amos_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:amos_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_dwhc), '0'), 38, 10) is null and 
                    src:amos_dwhc is not null) THEN
                    'amos_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:amos_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_prjc), '0'), 38, 10) is null and 
                    src:amos_prjc is not null) THEN
                    'amos_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:amos_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_refc), '0'), 38, 10) is null and 
                    src:amos_refc is not null) THEN
                    'amos_refc contains a non-numeric value : \'' || AS_VARCHAR(src:amos_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:basn), '0'), 38, 10) is null and 
                    src:basn is not null) THEN
                    'basn contains a non-numeric value : \'' || AS_VARCHAR(src:basn) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:btdt), '1900-01-01')) is null and 
                    src:btdt is not null) THEN
                    'btdt contains a non-timestamp value : \'' || AS_VARCHAR(src:btdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) THEN
                    'ccco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cico_rcmp), '0'), 38, 10) is null and 
                    src:cico_rcmp is not null) THEN
                    'cico_rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:cico_rcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clas_ref_compnr), '0'), 38, 10) is null and 
                    src:clas_ref_compnr is not null) THEN
                    'clas_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clas_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocu_ref_compnr), '0'), 38, 10) is null and 
                    src:cocu_ref_compnr is not null) THEN
                    'cocu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cocu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohc_1), '0'), 38, 10) is null and 
                    src:cohc_1 is not null) THEN
                    'cohc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:cohc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohc_2), '0'), 38, 10) is null and 
                    src:cohc_2 is not null) THEN
                    'cohc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:cohc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohc_3), '0'), 38, 10) is null and 
                    src:cohc_3 is not null) THEN
                    'cohc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:cohc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohc_4), '0'), 38, 10) is null and 
                    src:cohc_4 is not null) THEN
                    'cohc_4 contains a non-numeric value : \'' || AS_VARCHAR(src:cohc_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cona), '0'), 38, 10) is null and 
                    src:cona is not null) THEN
                    'cona contains a non-numeric value : \'' || AS_VARCHAR(src:cona) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cona_cntc), '0'), 38, 10) is null and 
                    src:cona_cntc is not null) THEN
                    'cona_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:cona_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cona_dwhc), '0'), 38, 10) is null and 
                    src:cona_dwhc is not null) THEN
                    'cona_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:cona_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cona_prjc), '0'), 38, 10) is null and 
                    src:cona_prjc is not null) THEN
                    'cona_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:cona_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cona_refc), '0'), 38, 10) is null and 
                    src:cona_refc is not null) THEN
                    'cona_refc contains a non-numeric value : \'' || AS_VARCHAR(src:cona_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp), '0'), 38, 10) is null and 
                    src:cotp is not null) THEN
                    'cotp contains a non-numeric value : \'' || AS_VARCHAR(src:cotp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_basn_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_basn_ref_compnr is not null) THEN
                    'cprj_basn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_basn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ccal_ref_compnr is not null) THEN
                    'cprj_ccal_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ccal_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) THEN
                    'cprj_cpla_cact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_cact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) THEN
                    'cprj_cpla_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) THEN
                    'cprj_cspa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cspa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_fcmp), '0'), 38, 10) is null and 
                    src:cprj_fcmp is not null) THEN
                    'cprj_fcmp contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_fcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptc_ref_compnr), '0'), 38, 10) is null and 
                    src:cptc_ref_compnr is not null) THEN
                    'cptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptc_year_peri_ref_compnr), '0'), 38, 10) is null and 
                    src:cptc_year_peri_ref_compnr is not null) THEN
                    'cptc_year_peri_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cptc_year_peri_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is null and 
                    src:cuti_ref_compnr is not null) THEN
                    'cuti_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuti_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dura), '0'), 38, 10) is null and 
                    src:dura is not null) THEN
                    'dura contains a non-numeric value : \'' || AS_VARCHAR(src:dura) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) THEN
                    'emno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:emno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exeq), '0'), 38, 10) is null and 
                    src:exeq is not null) THEN
                    'exeq contains a non-numeric value : \'' || AS_VARCHAR(src:exeq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcaa_1), '0'), 38, 10) is null and 
                    src:lcaa_1 is not null) THEN
                    'lcaa_1 contains a non-numeric value : \'' || AS_VARCHAR(src:lcaa_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcaa_2), '0'), 38, 10) is null and 
                    src:lcaa_2 is not null) THEN
                    'lcaa_2 contains a non-numeric value : \'' || AS_VARCHAR(src:lcaa_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcaa_3), '0'), 38, 10) is null and 
                    src:lcaa_3 is not null) THEN
                    'lcaa_3 contains a non-numeric value : \'' || AS_VARCHAR(src:lcaa_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcaa_4), '0'), 38, 10) is null and 
                    src:lcaa_4 is not null) THEN
                    'lcaa_4 contains a non-numeric value : \'' || AS_VARCHAR(src:lcaa_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcos_cntc), '0'), 38, 10) is null and 
                    src:lcos_cntc is not null) THEN
                    'lcos_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:lcos_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcos_dwhc), '0'), 38, 10) is null and 
                    src:lcos_dwhc is not null) THEN
                    'lcos_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:lcos_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcos_prjc), '0'), 38, 10) is null and 
                    src:lcos_prjc is not null) THEN
                    'lcos_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:lcos_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcos_refc), '0'), 38, 10) is null and 
                    src:lcos_refc is not null) THEN
                    'lcos_refc contains a non-numeric value : \'' || AS_VARCHAR(src:lcos_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcta), '0'), 38, 10) is null and 
                    src:lcta is not null) THEN
                    'lcta contains a non-numeric value : \'' || AS_VARCHAR(src:lcta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:norm), '0'), 38, 10) is null and 
                    src:norm is not null) THEN
                    'norm contains a non-numeric value : \'' || AS_VARCHAR(src:norm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peri), '0'), 38, 10) is null and 
                    src:peri is not null) THEN
                    'peri contains a non-numeric value : \'' || AS_VARCHAR(src:peri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prca_1), '0'), 38, 10) is null and 
                    src:prca_1 is not null) THEN
                    'prca_1 contains a non-numeric value : \'' || AS_VARCHAR(src:prca_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prca_2), '0'), 38, 10) is null and 
                    src:prca_2 is not null) THEN
                    'prca_2 contains a non-numeric value : \'' || AS_VARCHAR(src:prca_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prca_3), '0'), 38, 10) is null and 
                    src:prca_3 is not null) THEN
                    'prca_3 contains a non-numeric value : \'' || AS_VARCHAR(src:prca_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prca_4), '0'), 38, 10) is null and 
                    src:prca_4 is not null) THEN
                    'prca_4 contains a non-numeric value : \'' || AS_VARCHAR(src:prca_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) THEN
                    'pric contains a non-numeric value : \'' || AS_VARCHAR(src:pric) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_cntc), '0'), 38, 10) is null and 
                    src:pric_cntc is not null) THEN
                    'pric_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:pric_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_dwhc), '0'), 38, 10) is null and 
                    src:pric_dwhc is not null) THEN
                    'pric_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:pric_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_prjc), '0'), 38, 10) is null and 
                    src:pric_prjc is not null) THEN
                    'pric_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:pric_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_refc), '0'), 38, 10) is null and 
                    src:pric_refc is not null) THEN
                    'pric_refc contains a non-numeric value : \'' || AS_VARCHAR(src:pric_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) THEN
                    'pris contains a non-numeric value : \'' || AS_VARCHAR(src:pris) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_cntc), '0'), 38, 10) is null and 
                    src:pris_cntc is not null) THEN
                    'pris_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:pris_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_dwhc), '0'), 38, 10) is null and 
                    src:pris_dwhc is not null) THEN
                    'pris_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:pris_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_prjc), '0'), 38, 10) is null and 
                    src:pris_prjc is not null) THEN
                    'pris_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:pris_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_refc), '0'), 38, 10) is null and 
                    src:pris_refc is not null) THEN
                    'pris_refc contains a non-numeric value : \'' || AS_VARCHAR(src:pris_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsa_1), '0'), 38, 10) is null and 
                    src:prsa_1 is not null) THEN
                    'prsa_1 contains a non-numeric value : \'' || AS_VARCHAR(src:prsa_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsa_2), '0'), 38, 10) is null and 
                    src:prsa_2 is not null) THEN
                    'prsa_2 contains a non-numeric value : \'' || AS_VARCHAR(src:prsa_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsa_3), '0'), 38, 10) is null and 
                    src:prsa_3 is not null) THEN
                    'prsa_3 contains a non-numeric value : \'' || AS_VARCHAR(src:prsa_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsa_4), '0'), 38, 10) is null and 
                    src:prsa_4 is not null) THEN
                    'prsa_4 contains a non-numeric value : \'' || AS_VARCHAR(src:prsa_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan), '0'), 38, 10) is null and 
                    src:quan is not null) THEN
                    'quan contains a non-numeric value : \'' || AS_VARCHAR(src:quan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_base_time), '0'), 38, 10) is null and 
                    src:quan_base_time is not null) THEN
                    'quan_base_time contains a non-numeric value : \'' || AS_VARCHAR(src:quan_base_time) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buar), '0'), 38, 10) is null and 
                    src:quan_buar is not null) THEN
                    'quan_buar contains a non-numeric value : \'' || AS_VARCHAR(src:quan_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buln), '0'), 38, 10) is null and 
                    src:quan_buln is not null) THEN
                    'quan_buln contains a non-numeric value : \'' || AS_VARCHAR(src:quan_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_bupc), '0'), 38, 10) is null and 
                    src:quan_bupc is not null) THEN
                    'quan_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:quan_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buvl), '0'), 38, 10) is null and 
                    src:quan_buvl is not null) THEN
                    'quan_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:quan_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buwg), '0'), 38, 10) is null and 
                    src:quan_buwg is not null) THEN
                    'quan_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:quan_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_ctut), '0'), 38, 10) is null and 
                    src:quan_ctut is not null) THEN
                    'quan_ctut contains a non-numeric value : \'' || AS_VARCHAR(src:quan_ctut) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qutm), '0'), 38, 10) is null and 
                    src:qutm is not null) THEN
                    'qutm contains a non-numeric value : \'' || AS_VARCHAR(src:qutm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc), '0'), 38, 10) is null and 
                    src:ratc is not null) THEN
                    'ratc contains a non-numeric value : \'' || AS_VARCHAR(src:ratc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats), '0'), 38, 10) is null and 
                    src:rats is not null) THEN
                    'rats contains a non-numeric value : \'' || AS_VARCHAR(src:rats) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rfin), '1900-01-01')) is null and 
                    src:rfin is not null) THEN
                    'rfin contains a non-timestamp value : \'' || AS_VARCHAR(src:rfin) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rsta), '1900-01-01')) is null and 
                    src:rsta is not null) THEN
                    'rsta contains a non-timestamp value : \'' || AS_VARCHAR(src:rsta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtca_1), '0'), 38, 10) is null and 
                    src:rtca_1 is not null) THEN
                    'rtca_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtca_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtca_2), '0'), 38, 10) is null and 
                    src:rtca_2 is not null) THEN
                    'rtca_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtca_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtca_3), '0'), 38, 10) is null and 
                    src:rtca_3 is not null) THEN
                    'rtca_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtca_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtca_4), '0'), 38, 10) is null and 
                    src:rtca_4 is not null) THEN
                    'rtca_4 contains a non-numeric value : \'' || AS_VARCHAR(src:rtca_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_1), '0'), 38, 10) is null and 
                    src:rtsa_1 is not null) THEN
                    'rtsa_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtsa_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_2), '0'), 38, 10) is null and 
                    src:rtsa_2 is not null) THEN
                    'rtsa_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtsa_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_3), '0'), 38, 10) is null and 
                    src:rtsa_3 is not null) THEN
                    'rtsa_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtsa_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_4), '0'), 38, 10) is null and 
                    src:rtsa_4 is not null) THEN
                    'rtsa_4 contains a non-numeric value : \'' || AS_VARCHAR(src:rtsa_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sacu_ref_compnr), '0'), 38, 10) is null and 
                    src:sacu_ref_compnr is not null) THEN
                    'sacu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sacu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpf), '0'), 38, 10) is null and 
                    src:scpf is not null) THEN
                    'scpf contains a non-numeric value : \'' || AS_VARCHAR(src:scpf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) THEN
                    'sern contains a non-numeric value : \'' || AS_VARCHAR(src:sern) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:task_rcmp), '0'), 38, 10) is null and 
                    src:task_rcmp is not null) THEN
                    'task_rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:task_rcmp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmud_ref_compnr), '0'), 38, 10) is null and 
                    src:tmud_ref_compnr is not null) THEN
                    'tmud_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tmud_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unrt_ref_compnr), '0'), 38, 10) is null and 
                    src:unrt_ref_compnr is not null) THEN
                    'unrt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:unrt_ref_compnr) || '\'' WHEN 
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
                                    
                src:cptc,
                src:ccal,
                src:sern,
                src:compnr,
                src:cprj,
                src:year,
                src:peri  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPTC311 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaoc_1), '0'), 38, 10) is null and 
                    src:aaoc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaoc_2), '0'), 38, 10) is null and 
                    src:aaoc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaoc_3), '0'), 38, 10) is null and 
                    src:aaoc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaoc_4), '0'), 38, 10) is null and 
                    src:aaoc_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaos_1), '0'), 38, 10) is null and 
                    src:aaos_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaos_2), '0'), 38, 10) is null and 
                    src:aaos_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaos_3), '0'), 38, 10) is null and 
                    src:aaos_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaos_4), '0'), 38, 10) is null and 
                    src:aaos_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adjm), '0'), 38, 10) is null and 
                    src:adjm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc), '0'), 38, 10) is null and 
                    src:amoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_cntc), '0'), 38, 10) is null and 
                    src:amoc_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_dwhc), '0'), 38, 10) is null and 
                    src:amoc_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_prjc), '0'), 38, 10) is null and 
                    src:amoc_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_refc), '0'), 38, 10) is null and 
                    src:amoc_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos), '0'), 38, 10) is null and 
                    src:amos is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_cntc), '0'), 38, 10) is null and 
                    src:amos_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_dwhc), '0'), 38, 10) is null and 
                    src:amos_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_prjc), '0'), 38, 10) is null and 
                    src:amos_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_refc), '0'), 38, 10) is null and 
                    src:amos_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:basn), '0'), 38, 10) is null and 
                    src:basn is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:btdt), '1900-01-01')) is null and 
                    src:btdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cico_rcmp), '0'), 38, 10) is null and 
                    src:cico_rcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clas_ref_compnr), '0'), 38, 10) is null and 
                    src:clas_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocu_ref_compnr), '0'), 38, 10) is null and 
                    src:cocu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohc_1), '0'), 38, 10) is null and 
                    src:cohc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohc_2), '0'), 38, 10) is null and 
                    src:cohc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohc_3), '0'), 38, 10) is null and 
                    src:cohc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohc_4), '0'), 38, 10) is null and 
                    src:cohc_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cona), '0'), 38, 10) is null and 
                    src:cona is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cona_cntc), '0'), 38, 10) is null and 
                    src:cona_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cona_dwhc), '0'), 38, 10) is null and 
                    src:cona_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cona_prjc), '0'), 38, 10) is null and 
                    src:cona_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cona_refc), '0'), 38, 10) is null and 
                    src:cona_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp), '0'), 38, 10) is null and 
                    src:cotp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_basn_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_basn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ccal_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_fcmp), '0'), 38, 10) is null and 
                    src:cprj_fcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptc_ref_compnr), '0'), 38, 10) is null and 
                    src:cptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptc_year_peri_ref_compnr), '0'), 38, 10) is null and 
                    src:cptc_year_peri_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is null and 
                    src:cuti_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dura), '0'), 38, 10) is null and 
                    src:dura is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exeq), '0'), 38, 10) is null and 
                    src:exeq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcaa_1), '0'), 38, 10) is null and 
                    src:lcaa_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcaa_2), '0'), 38, 10) is null and 
                    src:lcaa_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcaa_3), '0'), 38, 10) is null and 
                    src:lcaa_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcaa_4), '0'), 38, 10) is null and 
                    src:lcaa_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcos_cntc), '0'), 38, 10) is null and 
                    src:lcos_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcos_dwhc), '0'), 38, 10) is null and 
                    src:lcos_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcos_prjc), '0'), 38, 10) is null and 
                    src:lcos_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcos_refc), '0'), 38, 10) is null and 
                    src:lcos_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcta), '0'), 38, 10) is null and 
                    src:lcta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:norm), '0'), 38, 10) is null and 
                    src:norm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peri), '0'), 38, 10) is null and 
                    src:peri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prca_1), '0'), 38, 10) is null and 
                    src:prca_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prca_2), '0'), 38, 10) is null and 
                    src:prca_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prca_3), '0'), 38, 10) is null and 
                    src:prca_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prca_4), '0'), 38, 10) is null and 
                    src:prca_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_cntc), '0'), 38, 10) is null and 
                    src:pric_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_dwhc), '0'), 38, 10) is null and 
                    src:pric_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_prjc), '0'), 38, 10) is null and 
                    src:pric_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_refc), '0'), 38, 10) is null and 
                    src:pric_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_cntc), '0'), 38, 10) is null and 
                    src:pris_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_dwhc), '0'), 38, 10) is null and 
                    src:pris_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_prjc), '0'), 38, 10) is null and 
                    src:pris_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_refc), '0'), 38, 10) is null and 
                    src:pris_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsa_1), '0'), 38, 10) is null and 
                    src:prsa_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsa_2), '0'), 38, 10) is null and 
                    src:prsa_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsa_3), '0'), 38, 10) is null and 
                    src:prsa_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsa_4), '0'), 38, 10) is null and 
                    src:prsa_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan), '0'), 38, 10) is null and 
                    src:quan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_base_time), '0'), 38, 10) is null and 
                    src:quan_base_time is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buar), '0'), 38, 10) is null and 
                    src:quan_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buln), '0'), 38, 10) is null and 
                    src:quan_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_bupc), '0'), 38, 10) is null and 
                    src:quan_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buvl), '0'), 38, 10) is null and 
                    src:quan_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buwg), '0'), 38, 10) is null and 
                    src:quan_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_ctut), '0'), 38, 10) is null and 
                    src:quan_ctut is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qutm), '0'), 38, 10) is null and 
                    src:qutm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc), '0'), 38, 10) is null and 
                    src:ratc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats), '0'), 38, 10) is null and 
                    src:rats is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rfin), '1900-01-01')) is null and 
                    src:rfin is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rsta), '1900-01-01')) is null and 
                    src:rsta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtca_1), '0'), 38, 10) is null and 
                    src:rtca_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtca_2), '0'), 38, 10) is null and 
                    src:rtca_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtca_3), '0'), 38, 10) is null and 
                    src:rtca_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtca_4), '0'), 38, 10) is null and 
                    src:rtca_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_1), '0'), 38, 10) is null and 
                    src:rtsa_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_2), '0'), 38, 10) is null and 
                    src:rtsa_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_3), '0'), 38, 10) is null and 
                    src:rtsa_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_4), '0'), 38, 10) is null and 
                    src:rtsa_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sacu_ref_compnr), '0'), 38, 10) is null and 
                    src:sacu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpf), '0'), 38, 10) is null and 
                    src:scpf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:task_rcmp), '0'), 38, 10) is null and 
                    src:task_rcmp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmud_ref_compnr), '0'), 38, 10) is null and 
                    src:tmud_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unrt_ref_compnr), '0'), 38, 10) is null and 
                    src:unrt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)