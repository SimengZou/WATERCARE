CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPPC200_ERROR AS SELECT src, 'LN_TPPPC200' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alin), '0'), 38, 10) is null and 
                    src:alin is not null) THEN
                    'alin contains a non-numeric value : \'' || AS_VARCHAR(src:alin) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_homc), '0'), 38, 10) is null and 
                    src:amos_homc is not null) THEN
                    'amos_homc contains a non-numeric value : \'' || AS_VARCHAR(src:amos_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_prjc), '0'), 38, 10) is null and 
                    src:amos_prjc is not null) THEN
                    'amos_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:amos_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_refc), '0'), 38, 10) is null and 
                    src:amos_refc is not null) THEN
                    'amos_refc contains a non-numeric value : \'' || AS_VARCHAR(src:amos_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_rpc1), '0'), 38, 10) is null and 
                    src:amos_rpc1 is not null) THEN
                    'amos_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:amos_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_rpc2), '0'), 38, 10) is null and 
                    src:amos_rpc2 is not null) THEN
                    'amos_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:amos_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:base_vrsn_ref_compnr), '0'), 38, 10) is null and 
                    src:base_vrsn_ref_compnr is not null) THEN
                    'base_vrsn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:base_vrsn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) THEN
                    'blbl contains a non-numeric value : \'' || AS_VARCHAR(src:blbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpct_ref_compnr), '0'), 38, 10) is null and 
                    src:bpct_ref_compnr is not null) THEN
                    'bpct_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpct_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bprc), '0'), 38, 10) is null and 
                    src:bprc is not null) THEN
                    'bprc contains a non-numeric value : \'' || AS_VARCHAR(src:bprc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btxt), '0'), 38, 10) is null and 
                    src:btxt is not null) THEN
                    'btxt contains a non-numeric value : \'' || AS_VARCHAR(src:btxt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btxt_ref_compnr), '0'), 38, 10) is null and 
                    src:btxt_ref_compnr is not null) THEN
                    'btxt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:btxt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bwln), '0'), 38, 10) is null and 
                    src:bwln is not null) THEN
                    'bwln contains a non-numeric value : \'' || AS_VARCHAR(src:bwln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) THEN
                    'ccco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccma_ref_compnr), '0'), 38, 10) is null and 
                    src:ccma_ref_compnr is not null) THEN
                    'ccma_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccma_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cdoc_ref_compnr is not null) THEN
                    'cdoc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdoc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdru), '1900-01-01')) is null and 
                    src:cdru is not null) THEN
                    'cdru contains a non-timestamp value : \'' || AS_VARCHAR(src:cdru) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdru_cser_ref_compnr), '0'), 38, 10) is null and 
                    src:cdru_cser_ref_compnr is not null) THEN
                    'cdru_cser_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdru_cser_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chpr), '0'), 38, 10) is null and 
                    src:chpr is not null) THEN
                    'chpr contains a non-numeric value : \'' || AS_VARCHAR(src:chpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clin), '0'), 38, 10) is null and 
                    src:clin is not null) THEN
                    'clin contains a non-numeric value : \'' || AS_VARCHAR(src:clin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohc_1), '0'), 38, 10) is null and 
                    src:cohc_1 is not null) THEN
                    'cohc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:cohc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohc_2), '0'), 38, 10) is null and 
                    src:cohc_2 is not null) THEN
                    'cohc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:cohc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohc_3), '0'), 38, 10) is null and 
                    src:cohc_3 is not null) THEN
                    'cohc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:cohc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohd_1), '0'), 38, 10) is null and 
                    src:cohd_1 is not null) THEN
                    'cohd_1 contains a non-numeric value : \'' || AS_VARCHAR(src:cohd_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohd_2), '0'), 38, 10) is null and 
                    src:cohd_2 is not null) THEN
                    'cohd_2 contains a non-numeric value : \'' || AS_VARCHAR(src:cohd_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohd_3), '0'), 38, 10) is null and 
                    src:cohd_3 is not null) THEN
                    'cohd_3 contains a non-numeric value : \'' || AS_VARCHAR(src:cohd_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comf), '0'), 38, 10) is null and 
                    src:comf is not null) THEN
                    'comf contains a non-numeric value : \'' || AS_VARCHAR(src:comf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_ref_compnr is not null) THEN
                    'cono_cnln_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_cnln_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp), '0'), 38, 10) is null and 
                    src:cotp is not null) THEN
                    'cotp contains a non-numeric value : \'' || AS_VARCHAR(src:cotp) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cser), '0'), 38, 10) is null and 
                    src:cser is not null) THEN
                    'cser contains a non-numeric value : \'' || AS_VARCHAR(src:cser) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csts), '0'), 38, 10) is null and 
                    src:csts is not null) THEN
                    'csts contains a non-numeric value : \'' || AS_VARCHAR(src:csts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) THEN
                    'cstv contains a non-numeric value : \'' || AS_VARCHAR(src:cstv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:curc_ref_compnr), '0'), 38, 10) is null and 
                    src:curc_ref_compnr is not null) THEN
                    'curc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:curc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:curs_ref_compnr), '0'), 38, 10) is null and 
                    src:curs_ref_compnr is not null) THEN
                    'curs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:curs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is null and 
                    src:cuti_ref_compnr is not null) THEN
                    'cuti_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuti_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) THEN
                    'cuvc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuvc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) THEN
                    'cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwgt_ref_compnr), '0'), 38, 10) is null and 
                    src:cwgt_ref_compnr is not null) THEN
                    'cwgt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwgt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:drun), '1900-01-01')) is null and 
                    src:drun is not null) THEN
                    'drun contains a non-timestamp value : \'' || AS_VARCHAR(src:drun) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:drun_serb_ref_compnr), '0'), 38, 10) is null and 
                    src:drun_serb_ref_compnr is not null) THEN
                    'drun_serb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:drun_serb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ecom), '0'), 38, 10) is null and 
                    src:ecom is not null) THEN
                    'ecom contains a non-numeric value : \'' || AS_VARCHAR(src:ecom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) THEN
                    'effn contains a non-numeric value : \'' || AS_VARCHAR(src:effn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) THEN
                    'effn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:effn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:efph), '0'), 38, 10) is null and 
                    src:efph is not null) THEN
                    'efph contains a non-numeric value : \'' || AS_VARCHAR(src:efph) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:efph_ref_compnr), '0'), 38, 10) is null and 
                    src:efph_ref_compnr is not null) THEN
                    'efph_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:efph_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) THEN
                    'emno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:emno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:enty), '0'), 38, 10) is null and 
                    src:enty is not null) THEN
                    'enty contains a non-numeric value : \'' || AS_VARCHAR(src:enty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) THEN
                    'exmt contains a non-numeric value : \'' || AS_VARCHAR(src:exmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmp), '0'), 38, 10) is null and 
                    src:fcmp is not null) THEN
                    'fcmp contains a non-numeric value : \'' || AS_VARCHAR(src:fcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcor), '0'), 38, 10) is null and 
                    src:fcor is not null) THEN
                    'fcor contains a non-numeric value : \'' || AS_VARCHAR(src:fcor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcrt), '0'), 38, 10) is null and 
                    src:fcrt is not null) THEN
                    'fcrt contains a non-numeric value : \'' || AS_VARCHAR(src:fcrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdoc), '0'), 38, 10) is null and 
                    src:fdoc is not null) THEN
                    'fdoc contains a non-numeric value : \'' || AS_VARCHAR(src:fdoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fitr), '0'), 38, 10) is null and 
                    src:fitr is not null) THEN
                    'fitr contains a non-numeric value : \'' || AS_VARCHAR(src:fitr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flin), '0'), 38, 10) is null and 
                    src:flin is not null) THEN
                    'flin contains a non-numeric value : \'' || AS_VARCHAR(src:flin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fper), '0'), 38, 10) is null and 
                    src:fper is not null) THEN
                    'fper contains a non-numeric value : \'' || AS_VARCHAR(src:fper) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:frdc), '1900-01-01')) is null and 
                    src:frdc is not null) THEN
                    'frdc contains a non-timestamp value : \'' || AS_VARCHAR(src:frdc) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbnr), '0'), 38, 10) is null and 
                    src:hbnr is not null) THEN
                    'hbnr contains a non-numeric value : \'' || AS_VARCHAR(src:hbnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbyn), '0'), 38, 10) is null and 
                    src:hbyn is not null) THEN
                    'hbyn contains a non-numeric value : \'' || AS_VARCHAR(src:hbyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hper), '0'), 38, 10) is null and 
                    src:hper is not null) THEN
                    'hper contains a non-numeric value : \'' || AS_VARCHAR(src:hper) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hyea), '0'), 38, 10) is null and 
                    src:hyea is not null) THEN
                    'hyea contains a non-numeric value : \'' || AS_VARCHAR(src:hyea) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) THEN
                    'idoc contains a non-numeric value : \'' || AS_VARCHAR(src:idoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) THEN
                    'ifbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ifbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intc), '0'), 38, 10) is null and 
                    src:intc is not null) THEN
                    'intc contains a non-numeric value : \'' || AS_VARCHAR(src:intc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inti), '0'), 38, 10) is null and 
                    src:inti is not null) THEN
                    'inti contains a non-numeric value : \'' || AS_VARCHAR(src:inti) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invo), '0'), 38, 10) is null and 
                    src:invo is not null) THEN
                    'invo contains a non-numeric value : \'' || AS_VARCHAR(src:invo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:isln), '0'), 38, 10) is null and 
                    src:isln is not null) THEN
                    'isln contains a non-numeric value : \'' || AS_VARCHAR(src:isln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itco), '0'), 38, 10) is null and 
                    src:itco is not null) THEN
                    'itco contains a non-numeric value : \'' || AS_VARCHAR(src:itco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itln), '0'), 38, 10) is null and 
                    src:itln is not null) THEN
                    'itln contains a non-numeric value : \'' || AS_VARCHAR(src:itln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:koor), '0'), 38, 10) is null and 
                    src:koor is not null) THEN
                    'koor contains a non-numeric value : \'' || AS_VARCHAR(src:koor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcln), '0'), 38, 10) is null and 
                    src:lcln is not null) THEN
                    'lcln contains a non-numeric value : \'' || AS_VARCHAR(src:lcln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcor), '0'), 38, 10) is null and 
                    src:lcor is not null) THEN
                    'lcor contains a non-numeric value : \'' || AS_VARCHAR(src:lcor) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltdt), '1900-01-01')) is null and 
                    src:ltdt is not null) THEN
                    'ltdt contains a non-timestamp value : \'' || AS_VARCHAR(src:ltdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncmp), '0'), 38, 10) is null and 
                    src:ncmp is not null) THEN
                    'ncmp contains a non-numeric value : \'' || AS_VARCHAR(src:ncmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ntbl), '0'), 38, 10) is null and 
                    src:ntbl is not null) THEN
                    'ntbl contains a non-numeric value : \'' || AS_VARCHAR(src:ntbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp), '0'), 38, 10) is null and 
                    src:ocmp is not null) THEN
                    'ocmp contains a non-numeric value : \'' || AS_VARCHAR(src:ocmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohpr), '0'), 38, 10) is null and 
                    src:ohpr is not null) THEN
                    'ohpr contains a non-numeric value : \'' || AS_VARCHAR(src:ohpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oitm_ref_compnr), '0'), 38, 10) is null and 
                    src:oitm_ref_compnr is not null) THEN
                    'oitm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:oitm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oldf), '0'), 38, 10) is null and 
                    src:oldf is not null) THEN
                    'oldf contains a non-numeric value : \'' || AS_VARCHAR(src:oldf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ovhd_ref_compnr), '0'), 38, 10) is null and 
                    src:ovhd_ref_compnr is not null) THEN
                    'ovhd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ovhd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peri), '0'), 38, 10) is null and 
                    src:peri is not null) THEN
                    'peri contains a non-numeric value : \'' || AS_VARCHAR(src:peri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phit_ref_compnr), '0'), 38, 10) is null and 
                    src:phit_ref_compnr is not null) THEN
                    'phit_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:phit_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:potf), '0'), 38, 10) is null and 
                    src:potf is not null) THEN
                    'potf contains a non-numeric value : \'' || AS_VARCHAR(src:potf) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) THEN
                    'prdt contains a non-timestamp value : \'' || AS_VARCHAR(src:prdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) THEN
                    'pric contains a non-numeric value : \'' || AS_VARCHAR(src:pric) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) THEN
                    'pris contains a non-numeric value : \'' || AS_VARCHAR(src:pris) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pstt), '0'), 38, 10) is null and 
                    src:pstt is not null) THEN
                    'pstt contains a non-numeric value : \'' || AS_VARCHAR(src:pstt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptyc), '0'), 38, 10) is null and 
                    src:ptyc is not null) THEN
                    'ptyc contains a non-numeric value : \'' || AS_VARCHAR(src:ptyc) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcln), '0'), 38, 10) is null and 
                    src:rcln is not null) THEN
                    'rcln contains a non-numeric value : \'' || AS_VARCHAR(src:rcln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) THEN
                    'rcod_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcod_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdas), '1900-01-01')) is null and 
                    src:rdas is not null) THEN
                    'rdas contains a non-timestamp value : \'' || AS_VARCHAR(src:rdas) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) THEN
                    'rdat contains a non-timestamp value : \'' || AS_VARCHAR(src:rdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reas_ref_compnr), '0'), 38, 10) is null and 
                    src:reas_ref_compnr is not null) THEN
                    'reas_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:reas_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:retx), '0'), 38, 10) is null and 
                    src:retx is not null) THEN
                    'retx contains a non-numeric value : \'' || AS_VARCHAR(src:retx) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rgdt), '1900-01-01')) is null and 
                    src:rgdt is not null) THEN
                    'rgdt contains a non-timestamp value : \'' || AS_VARCHAR(src:rgdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rlin), '0'), 38, 10) is null and 
                    src:rlin is not null) THEN
                    'rlin contains a non-numeric value : \'' || AS_VARCHAR(src:rlin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsqn), '0'), 38, 10) is null and 
                    src:rsqn is not null) THEN
                    'rsqn contains a non-numeric value : \'' || AS_VARCHAR(src:rsqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_1), '0'), 38, 10) is null and 
                    src:rtcc_1 is not null) THEN
                    'rtcc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_2), '0'), 38, 10) is null and 
                    src:rtcc_2 is not null) THEN
                    'rtcc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_3), '0'), 38, 10) is null and 
                    src:rtcc_3 is not null) THEN
                    'rtcc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_1), '0'), 38, 10) is null and 
                    src:rtcs_1 is not null) THEN
                    'rtcs_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcs_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_2), '0'), 38, 10) is null and 
                    src:rtcs_2 is not null) THEN
                    'rtcs_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcs_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_3), '0'), 38, 10) is null and 
                    src:rtcs_3 is not null) THEN
                    'rtcs_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcs_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_1), '0'), 38, 10) is null and 
                    src:rtfc_1 is not null) THEN
                    'rtfc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_2), '0'), 38, 10) is null and 
                    src:rtfc_2 is not null) THEN
                    'rtfc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_3), '0'), 38, 10) is null and 
                    src:rtfc_3 is not null) THEN
                    'rtfc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_1), '0'), 38, 10) is null and 
                    src:rtfs_1 is not null) THEN
                    'rtfs_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfs_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_2), '0'), 38, 10) is null and 
                    src:rtfs_2 is not null) THEN
                    'rtfs_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfs_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_3), '0'), 38, 10) is null and 
                    src:rtfs_3 is not null) THEN
                    'rtfs_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfs_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rttp), '0'), 38, 10) is null and 
                    src:rttp is not null) THEN
                    'rttp contains a non-numeric value : \'' || AS_VARCHAR(src:rttp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) THEN
                    'rtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serb), '0'), 38, 10) is null and 
                    src:serb is not null) THEN
                    'serb contains a non-numeric value : \'' || AS_VARCHAR(src:serb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serh), '0'), 38, 10) is null and 
                    src:serh is not null) THEN
                    'serh contains a non-numeric value : \'' || AS_VARCHAR(src:serh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) THEN
                    'sern contains a non-numeric value : \'' || AS_VARCHAR(src:sern) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:snec), '0'), 38, 10) is null and 
                    src:snec is not null) THEN
                    'snec contains a non-numeric value : \'' || AS_VARCHAR(src:snec) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spln), '0'), 38, 10) is null and 
                    src:spln is not null) THEN
                    'spln contains a non-numeric value : \'' || AS_VARCHAR(src:spln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srnb), '0'), 38, 10) is null and 
                    src:srnb is not null) THEN
                    'srnb contains a non-numeric value : \'' || AS_VARCHAR(src:srnb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srni), '0'), 38, 10) is null and 
                    src:srni is not null) THEN
                    'srni contains a non-numeric value : \'' || AS_VARCHAR(src:srni) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sttl), '0'), 38, 10) is null and 
                    src:sttl is not null) THEN
                    'sttl contains a non-numeric value : \'' || AS_VARCHAR(src:sttl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:time), '0'), 38, 10) is null and 
                    src:time is not null) THEN
                    'time contains a non-numeric value : \'' || AS_VARCHAR(src:time) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tror), '0'), 38, 10) is null and 
                    src:tror is not null) THEN
                    'tror contains a non-numeric value : \'' || AS_VARCHAR(src:tror) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trsl), '0'), 38, 10) is null and 
                    src:trsl is not null) THEN
                    'trsl contains a non-numeric value : \'' || AS_VARCHAR(src:trsl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txct_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:txct_cvat_ref_compnr is not null) THEN
                    'txct_cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txct_cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txct_ref_compnr), '0'), 38, 10) is null and 
                    src:txct_ref_compnr is not null) THEN
                    'txct_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txct_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vrln), '0'), 38, 10) is null and 
                    src:vrln is not null) THEN
                    'vrln contains a non-numeric value : \'' || AS_VARCHAR(src:vrln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vrsn), '0'), 38, 10) is null and 
                    src:vrsn is not null) THEN
                    'vrsn contains a non-numeric value : \'' || AS_VARCHAR(src:vrsn) || '\'' WHEN 
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
                                    
                src:coob,
                src:cspa,
                src:cprj,
                src:cotp,
                src:compnr,
                src:sern  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPPC200 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alin), '0'), 38, 10) is null and 
                    src:alin is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_homc), '0'), 38, 10) is null and 
                    src:amos_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_prjc), '0'), 38, 10) is null and 
                    src:amos_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_refc), '0'), 38, 10) is null and 
                    src:amos_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_rpc1), '0'), 38, 10) is null and 
                    src:amos_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos_rpc2), '0'), 38, 10) is null and 
                    src:amos_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:base_vrsn_ref_compnr), '0'), 38, 10) is null and 
                    src:base_vrsn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpct_ref_compnr), '0'), 38, 10) is null and 
                    src:bpct_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bprc), '0'), 38, 10) is null and 
                    src:bprc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btxt), '0'), 38, 10) is null and 
                    src:btxt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btxt_ref_compnr), '0'), 38, 10) is null and 
                    src:btxt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bwln), '0'), 38, 10) is null and 
                    src:bwln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccma_ref_compnr), '0'), 38, 10) is null and 
                    src:ccma_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cdoc_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdru), '1900-01-01')) is null and 
                    src:cdru is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdru_cser_ref_compnr), '0'), 38, 10) is null and 
                    src:cdru_cser_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chpr), '0'), 38, 10) is null and 
                    src:chpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clin), '0'), 38, 10) is null and 
                    src:clin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohc_1), '0'), 38, 10) is null and 
                    src:cohc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohc_2), '0'), 38, 10) is null and 
                    src:cohc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohc_3), '0'), 38, 10) is null and 
                    src:cohc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohd_1), '0'), 38, 10) is null and 
                    src:cohd_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohd_2), '0'), 38, 10) is null and 
                    src:cohd_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cohd_3), '0'), 38, 10) is null and 
                    src:cohd_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comf), '0'), 38, 10) is null and 
                    src:comf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp), '0'), 38, 10) is null and 
                    src:cotp is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cser), '0'), 38, 10) is null and 
                    src:cser is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csts), '0'), 38, 10) is null and 
                    src:csts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:curc_ref_compnr), '0'), 38, 10) is null and 
                    src:curc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:curs_ref_compnr), '0'), 38, 10) is null and 
                    src:curs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is null and 
                    src:cuti_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwgt_ref_compnr), '0'), 38, 10) is null and 
                    src:cwgt_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:drun), '1900-01-01')) is null and 
                    src:drun is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:drun_serb_ref_compnr), '0'), 38, 10) is null and 
                    src:drun_serb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ecom), '0'), 38, 10) is null and 
                    src:ecom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:efph), '0'), 38, 10) is null and 
                    src:efph is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:efph_ref_compnr), '0'), 38, 10) is null and 
                    src:efph_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:enty), '0'), 38, 10) is null and 
                    src:enty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmp), '0'), 38, 10) is null and 
                    src:fcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcor), '0'), 38, 10) is null and 
                    src:fcor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcrt), '0'), 38, 10) is null and 
                    src:fcrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdoc), '0'), 38, 10) is null and 
                    src:fdoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fitr), '0'), 38, 10) is null and 
                    src:fitr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flin), '0'), 38, 10) is null and 
                    src:flin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fper), '0'), 38, 10) is null and 
                    src:fper is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:frdc), '1900-01-01')) is null and 
                    src:frdc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:frgd), '1900-01-01')) is null and 
                    src:frgd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fsrl), '0'), 38, 10) is null and 
                    src:fsrl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ftln), '0'), 38, 10) is null and 
                    src:ftln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fyea), '0'), 38, 10) is null and 
                    src:fyea is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbnr), '0'), 38, 10) is null and 
                    src:hbnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbyn), '0'), 38, 10) is null and 
                    src:hbyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hper), '0'), 38, 10) is null and 
                    src:hper is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hyea), '0'), 38, 10) is null and 
                    src:hyea is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intc), '0'), 38, 10) is null and 
                    src:intc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inti), '0'), 38, 10) is null and 
                    src:inti is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invo), '0'), 38, 10) is null and 
                    src:invo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:isln), '0'), 38, 10) is null and 
                    src:isln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itco), '0'), 38, 10) is null and 
                    src:itco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itln), '0'), 38, 10) is null and 
                    src:itln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:koor), '0'), 38, 10) is null and 
                    src:koor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcln), '0'), 38, 10) is null and 
                    src:lcln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcor), '0'), 38, 10) is null and 
                    src:lcor is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltdt), '1900-01-01')) is null and 
                    src:ltdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncmp), '0'), 38, 10) is null and 
                    src:ncmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ntbl), '0'), 38, 10) is null and 
                    src:ntbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp), '0'), 38, 10) is null and 
                    src:ocmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohpr), '0'), 38, 10) is null and 
                    src:ohpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oitm_ref_compnr), '0'), 38, 10) is null and 
                    src:oitm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oldf), '0'), 38, 10) is null and 
                    src:oldf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ovhd_ref_compnr), '0'), 38, 10) is null and 
                    src:ovhd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peri), '0'), 38, 10) is null and 
                    src:peri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phit_ref_compnr), '0'), 38, 10) is null and 
                    src:phit_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:potf), '0'), 38, 10) is null and 
                    src:potf is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pstt), '0'), 38, 10) is null and 
                    src:pstt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptyc), '0'), 38, 10) is null and 
                    src:ptyc is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcln), '0'), 38, 10) is null and 
                    src:rcln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdas), '1900-01-01')) is null and 
                    src:rdas is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reas_ref_compnr), '0'), 38, 10) is null and 
                    src:reas_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:retx), '0'), 38, 10) is null and 
                    src:retx is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rgdt), '1900-01-01')) is null and 
                    src:rgdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rlin), '0'), 38, 10) is null and 
                    src:rlin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsqn), '0'), 38, 10) is null and 
                    src:rsqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_1), '0'), 38, 10) is null and 
                    src:rtcc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_2), '0'), 38, 10) is null and 
                    src:rtcc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_3), '0'), 38, 10) is null and 
                    src:rtcc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_1), '0'), 38, 10) is null and 
                    src:rtcs_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_2), '0'), 38, 10) is null and 
                    src:rtcs_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_3), '0'), 38, 10) is null and 
                    src:rtcs_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_1), '0'), 38, 10) is null and 
                    src:rtfc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_2), '0'), 38, 10) is null and 
                    src:rtfc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_3), '0'), 38, 10) is null and 
                    src:rtfc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_1), '0'), 38, 10) is null and 
                    src:rtfs_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_2), '0'), 38, 10) is null and 
                    src:rtfs_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_3), '0'), 38, 10) is null and 
                    src:rtfs_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rttp), '0'), 38, 10) is null and 
                    src:rttp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serb), '0'), 38, 10) is null and 
                    src:serb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serh), '0'), 38, 10) is null and 
                    src:serh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:snec), '0'), 38, 10) is null and 
                    src:snec is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spln), '0'), 38, 10) is null and 
                    src:spln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srnb), '0'), 38, 10) is null and 
                    src:srnb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srni), '0'), 38, 10) is null and 
                    src:srni is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sttl), '0'), 38, 10) is null and 
                    src:sttl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:time), '0'), 38, 10) is null and 
                    src:time is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tror), '0'), 38, 10) is null and 
                    src:tror is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trsl), '0'), 38, 10) is null and 
                    src:trsl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txct_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:txct_cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txct_ref_compnr), '0'), 38, 10) is null and 
                    src:txct_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vrln), '0'), 38, 10) is null and 
                    src:vrln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vrsn), '0'), 38, 10) is null and 
                    src:vrsn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)