CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPCTM100_ERROR AS SELECT src, 'LN_TPCTM100' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acpn), '0'), 38, 10) is null and 
                    src:acpn is not null) THEN
                    'acpn contains a non-numeric value : \'' || AS_VARCHAR(src:acpn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adtp), '0'), 38, 10) is null and 
                    src:adtp is not null) THEN
                    'adtp contains a non-numeric value : \'' || AS_VARCHAR(src:adtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:advn), '0'), 38, 10) is null and 
                    src:advn is not null) THEN
                    'advn contains a non-numeric value : \'' || AS_VARCHAR(src:advn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bcrp), '0'), 38, 10) is null and 
                    src:bcrp is not null) THEN
                    'bcrp contains a non-numeric value : \'' || AS_VARCHAR(src:bcrp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blcl_ref_compnr), '0'), 38, 10) is null and 
                    src:blcl_ref_compnr is not null) THEN
                    'blcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:blcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) THEN
                    'cbrn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbrn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccam_ref_compnr), '0'), 38, 10) is null and 
                    src:ccam_ref_compnr is not null) THEN
                    'ccam_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccam_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccat_ref_compnr is not null) THEN
                    'ccat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccot_ref_compnr), '0'), 38, 10) is null and 
                    src:ccot_ref_compnr is not null) THEN
                    'ccot_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccot_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) THEN
                    'ccrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfac_ref_compnr), '0'), 38, 10) is null and 
                    src:cfac_ref_compnr is not null) THEN
                    'cfac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cidm_ref_compnr), '0'), 38, 10) is null and 
                    src:cidm_ref_compnr is not null) THEN
                    'cidm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cidm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinm_ref_compnr), '0'), 38, 10) is null and 
                    src:cinm_ref_compnr is not null) THEN
                    'cinm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cinm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmng_ref_compnr), '0'), 38, 10) is null and 
                    src:cmng_ref_compnr is not null) THEN
                    'cmng_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmng_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:codt), '1900-01-01')) is null and 
                    src:codt is not null) THEN
                    'codt contains a non-timestamp value : \'' || AS_VARCHAR(src:codt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_fcmp), '0'), 38, 10) is null and 
                    src:cono_fcmp is not null) THEN
                    'cono_fcmp contains a non-numeric value : \'' || AS_VARCHAR(src:cono_fcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr), '0'), 38, 10) is null and 
                    src:copr is not null) THEN
                    'copr contains a non-numeric value : \'' || AS_VARCHAR(src:copr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_dwhc), '0'), 38, 10) is null and 
                    src:copr_dwhc is not null) THEN
                    'copr_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:copr_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_homc), '0'), 38, 10) is null and 
                    src:copr_homc is not null) THEN
                    'copr_homc contains a non-numeric value : \'' || AS_VARCHAR(src:copr_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_refc), '0'), 38, 10) is null and 
                    src:copr_refc is not null) THEN
                    'copr_refc contains a non-numeric value : \'' || AS_VARCHAR(src:copr_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_rpc1), '0'), 38, 10) is null and 
                    src:copr_rpc1 is not null) THEN
                    'copr_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:copr_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_rpc2), '0'), 38, 10) is null and 
                    src:copr_rpc2 is not null) THEN
                    'copr_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:copr_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coty), '0'), 38, 10) is null and 
                    src:coty is not null) THEN
                    'coty contains a non-numeric value : \'' || AS_VARCHAR(src:coty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_bver_bnum_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_bver_bnum_ref_compnr is not null) THEN
                    'cprj_bver_bnum_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_bver_bnum_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) THEN
                    'creg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:creg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is null and 
                    src:crep_ref_compnr is not null) THEN
                    'crep_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crep_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is null and 
                    src:csec_ref_compnr is not null) THEN
                    'csec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstg_ref_compnr), '0'), 38, 10) is null and 
                    src:cstg_ref_compnr is not null) THEN
                    'cstg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cstg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csts), '0'), 38, 10) is null and 
                    src:csts is not null) THEN
                    'csts contains a non-numeric value : \'' || AS_VARCHAR(src:csts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyp), '0'), 38, 10) is null and 
                    src:ctyp is not null) THEN
                    'ctyp contains a non-numeric value : \'' || AS_VARCHAR(src:ctyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfis_ref_compnr), '0'), 38, 10) is null and 
                    src:dfis_ref_compnr is not null) THEN
                    'dfis_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dfis_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc), '0'), 38, 10) is null and 
                    src:disc is not null) THEN
                    'disc contains a non-numeric value : \'' || AS_VARCHAR(src:disc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) THEN
                    'efdt contains a non-timestamp value : \'' || AS_VARCHAR(src:efdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:enip), '0'), 38, 10) is null and 
                    src:enip is not null) THEN
                    'enip contains a non-numeric value : \'' || AS_VARCHAR(src:enip) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) THEN
                    'exdt contains a non-timestamp value : \'' || AS_VARCHAR(src:exdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcrt), '0'), 38, 10) is null and 
                    src:fcrt is not null) THEN
                    'fcrt contains a non-numeric value : \'' || AS_VARCHAR(src:fcrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdis), '0'), 38, 10) is null and 
                    src:fdis is not null) THEN
                    'fdis contains a non-numeric value : \'' || AS_VARCHAR(src:fdis) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frvt), '0'), 38, 10) is null and 
                    src:frvt is not null) THEN
                    'frvt contains a non-numeric value : \'' || AS_VARCHAR(src:frvt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbpc), '0'), 38, 10) is null and 
                    src:hbpc is not null) THEN
                    'hbpc contains a non-numeric value : \'' || AS_VARCHAR(src:hbpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hldb), '0'), 38, 10) is null and 
                    src:hldb is not null) THEN
                    'hldb contains a non-numeric value : \'' || AS_VARCHAR(src:hldb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hrsn_ref_compnr), '0'), 38, 10) is null and 
                    src:hrsn_ref_compnr is not null) THEN
                    'hrsn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:hrsn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invm), '0'), 38, 10) is null and 
                    src:invm is not null) THEN
                    'invm contains a non-numeric value : \'' || AS_VARCHAR(src:invm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itad_ref_compnr), '0'), 38, 10) is null and 
                    src:itad_ref_compnr is not null) THEN
                    'itad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) THEN
                    'itbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itcn_ref_compnr), '0'), 38, 10) is null and 
                    src:itcn_ref_compnr is not null) THEN
                    'itcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod), '0'), 38, 10) is null and 
                    src:itod is not null) THEN
                    'itod contains a non-numeric value : \'' || AS_VARCHAR(src:itod) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_dwhc), '0'), 38, 10) is null and 
                    src:itod_dwhc is not null) THEN
                    'itod_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:itod_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_homc), '0'), 38, 10) is null and 
                    src:itod_homc is not null) THEN
                    'itod_homc contains a non-numeric value : \'' || AS_VARCHAR(src:itod_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_refc), '0'), 38, 10) is null and 
                    src:itod_refc is not null) THEN
                    'itod_refc contains a non-numeric value : \'' || AS_VARCHAR(src:itod_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_rpc1), '0'), 38, 10) is null and 
                    src:itod_rpc1 is not null) THEN
                    'itod_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:itod_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_rpc2), '0'), 38, 10) is null and 
                    src:itod_rpc2 is not null) THEN
                    'itod_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:itod_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam), '0'), 38, 10) is null and 
                    src:ivam is not null) THEN
                    'ivam contains a non-numeric value : \'' || AS_VARCHAR(src:ivam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_dwhc), '0'), 38, 10) is null and 
                    src:ivam_dwhc is not null) THEN
                    'ivam_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:ivam_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_homc), '0'), 38, 10) is null and 
                    src:ivam_homc is not null) THEN
                    'ivam_homc contains a non-numeric value : \'' || AS_VARCHAR(src:ivam_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_refc), '0'), 38, 10) is null and 
                    src:ivam_refc is not null) THEN
                    'ivam_refc contains a non-numeric value : \'' || AS_VARCHAR(src:ivam_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_rpc1), '0'), 38, 10) is null and 
                    src:ivam_rpc1 is not null) THEN
                    'ivam_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:ivam_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_rpc2), '0'), 38, 10) is null and 
                    src:ivam_rpc2 is not null) THEN
                    'ivam_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:ivam_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcrs_ref_compnr), '0'), 38, 10) is null and 
                    src:lcrs_ref_compnr is not null) THEN
                    'lcrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) THEN
                    'lmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpad), '0'), 38, 10) is null and 
                    src:lpad is not null) THEN
                    'lpad contains a non-numeric value : \'' || AS_VARCHAR(src:lpad) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpin), '0'), 38, 10) is null and 
                    src:lpin is not null) THEN
                    'lpin contains a non-numeric value : \'' || AS_VARCHAR(src:lpin) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltti), '1900-01-01')) is null and 
                    src:ltti is not null) THEN
                    'ltti contains a non-timestamp value : \'' || AS_VARCHAR(src:ltti) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) THEN
                    'ofad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) THEN
                    'ofcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrp_ref_compnr), '0'), 38, 10) is null and 
                    src:osrp_ref_compnr is not null) THEN
                    'osrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:osrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcba), '0'), 38, 10) is null and 
                    src:pcba is not null) THEN
                    'pcba contains a non-numeric value : \'' || AS_VARCHAR(src:pcba) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcwa), '0'), 38, 10) is null and 
                    src:pcwa is not null) THEN
                    'pcwa contains a non-numeric value : \'' || AS_VARCHAR(src:pcwa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is null and 
                    src:pfad_ref_compnr is not null) THEN
                    'pfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) THEN
                    'pfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:pfcn_ref_compnr is not null) THEN
                    'pfcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgpm), '0'), 38, 10) is null and 
                    src:pgpm is not null) THEN
                    'pgpm contains a non-numeric value : \'' || AS_VARCHAR(src:pgpm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plpc), '0'), 38, 10) is null and 
                    src:plpc is not null) THEN
                    'plpc contains a non-numeric value : \'' || AS_VARCHAR(src:plpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmng_ref_compnr), '0'), 38, 10) is null and 
                    src:pmng_ref_compnr is not null) THEN
                    'pmng_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pmng_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:poin), '0'), 38, 10) is null and 
                    src:poin is not null) THEN
                    'poin contains a non-numeric value : \'' || AS_VARCHAR(src:poin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppim_ref_compnr), '0'), 38, 10) is null and 
                    src:ppim_ref_compnr is not null) THEN
                    'ppim_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ppim_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pppc), '0'), 38, 10) is null and 
                    src:pppc is not null) THEN
                    'pppc contains a non-numeric value : \'' || AS_VARCHAR(src:pppc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prgm_ref_compnr), '0'), 38, 10) is null and 
                    src:prgm_ref_compnr is not null) THEN
                    'prgm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prgm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prst), '0'), 38, 10) is null and 
                    src:prst is not null) THEN
                    'prst contains a non-numeric value : \'' || AS_VARCHAR(src:prst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prtx), '0'), 38, 10) is null and 
                    src:prtx is not null) THEN
                    'prtx contains a non-numeric value : \'' || AS_VARCHAR(src:prtx) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpp_ref_compnr is not null) THEN
                    'ptpp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptsh), '0'), 38, 10) is null and 
                    src:ptsh is not null) THEN
                    'ptsh contains a non-numeric value : \'' || AS_VARCHAR(src:ptsh) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) THEN
                    'rtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is null and 
                    src:styp_ref_compnr is not null) THEN
                    'styp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:styp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) THEN
                    'text contains a non-numeric value : \'' || AS_VARCHAR(src:text) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) THEN
                    'text_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:text_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tins), '0'), 38, 10) is null and 
                    src:tins is not null) THEN
                    'tins contains a non-numeric value : \'' || AS_VARCHAR(src:tins) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ulpd), '0'), 38, 10) is null and 
                    src:ulpd is not null) THEN
                    'ulpd contains a non-numeric value : \'' || AS_VARCHAR(src:ulpd) || '\'' WHEN 
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
                src:cono  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPCTM100 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acpn), '0'), 38, 10) is null and 
                    src:acpn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adtp), '0'), 38, 10) is null and 
                    src:adtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:advn), '0'), 38, 10) is null and 
                    src:advn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bcrp), '0'), 38, 10) is null and 
                    src:bcrp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blcl_ref_compnr), '0'), 38, 10) is null and 
                    src:blcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccam_ref_compnr), '0'), 38, 10) is null and 
                    src:ccam_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccot_ref_compnr), '0'), 38, 10) is null and 
                    src:ccot_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfac_ref_compnr), '0'), 38, 10) is null and 
                    src:cfac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cidm_ref_compnr), '0'), 38, 10) is null and 
                    src:cidm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinm_ref_compnr), '0'), 38, 10) is null and 
                    src:cinm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmng_ref_compnr), '0'), 38, 10) is null and 
                    src:cmng_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:codt), '1900-01-01')) is null and 
                    src:codt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_fcmp), '0'), 38, 10) is null and 
                    src:cono_fcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr), '0'), 38, 10) is null and 
                    src:copr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_dwhc), '0'), 38, 10) is null and 
                    src:copr_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_homc), '0'), 38, 10) is null and 
                    src:copr_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_refc), '0'), 38, 10) is null and 
                    src:copr_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_rpc1), '0'), 38, 10) is null and 
                    src:copr_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_rpc2), '0'), 38, 10) is null and 
                    src:copr_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coty), '0'), 38, 10) is null and 
                    src:coty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_bver_bnum_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_bver_bnum_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is null and 
                    src:crep_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is null and 
                    src:csec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstg_ref_compnr), '0'), 38, 10) is null and 
                    src:cstg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csts), '0'), 38, 10) is null and 
                    src:csts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyp), '0'), 38, 10) is null and 
                    src:ctyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfis_ref_compnr), '0'), 38, 10) is null and 
                    src:dfis_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc), '0'), 38, 10) is null and 
                    src:disc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:enip), '0'), 38, 10) is null and 
                    src:enip is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcrt), '0'), 38, 10) is null and 
                    src:fcrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdis), '0'), 38, 10) is null and 
                    src:fdis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frvt), '0'), 38, 10) is null and 
                    src:frvt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbpc), '0'), 38, 10) is null and 
                    src:hbpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hldb), '0'), 38, 10) is null and 
                    src:hldb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hrsn_ref_compnr), '0'), 38, 10) is null and 
                    src:hrsn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invm), '0'), 38, 10) is null and 
                    src:invm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itad_ref_compnr), '0'), 38, 10) is null and 
                    src:itad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itcn_ref_compnr), '0'), 38, 10) is null and 
                    src:itcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod), '0'), 38, 10) is null and 
                    src:itod is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_dwhc), '0'), 38, 10) is null and 
                    src:itod_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_homc), '0'), 38, 10) is null and 
                    src:itod_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_refc), '0'), 38, 10) is null and 
                    src:itod_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_rpc1), '0'), 38, 10) is null and 
                    src:itod_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itod_rpc2), '0'), 38, 10) is null and 
                    src:itod_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam), '0'), 38, 10) is null and 
                    src:ivam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_dwhc), '0'), 38, 10) is null and 
                    src:ivam_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_homc), '0'), 38, 10) is null and 
                    src:ivam_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_refc), '0'), 38, 10) is null and 
                    src:ivam_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_rpc1), '0'), 38, 10) is null and 
                    src:ivam_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivam_rpc2), '0'), 38, 10) is null and 
                    src:ivam_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcrs_ref_compnr), '0'), 38, 10) is null and 
                    src:lcrs_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpad), '0'), 38, 10) is null and 
                    src:lpad is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpin), '0'), 38, 10) is null and 
                    src:lpin is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltti), '1900-01-01')) is null and 
                    src:ltti is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrp_ref_compnr), '0'), 38, 10) is null and 
                    src:osrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcba), '0'), 38, 10) is null and 
                    src:pcba is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcwa), '0'), 38, 10) is null and 
                    src:pcwa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is null and 
                    src:pfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:pfcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgpm), '0'), 38, 10) is null and 
                    src:pgpm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plpc), '0'), 38, 10) is null and 
                    src:plpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmng_ref_compnr), '0'), 38, 10) is null and 
                    src:pmng_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:poin), '0'), 38, 10) is null and 
                    src:poin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppim_ref_compnr), '0'), 38, 10) is null and 
                    src:ppim_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pppc), '0'), 38, 10) is null and 
                    src:pppc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prgm_ref_compnr), '0'), 38, 10) is null and 
                    src:prgm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prst), '0'), 38, 10) is null and 
                    src:prst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prtx), '0'), 38, 10) is null and 
                    src:prtx is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptsh), '0'), 38, 10) is null and 
                    src:ptsh is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is null and 
                    src:styp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tins), '0'), 38, 10) is null and 
                    src:tins is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ulpd), '0'), 38, 10) is null and 
                    src:ulpd is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)