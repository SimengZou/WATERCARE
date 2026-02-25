CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPIN020_ERROR AS SELECT src, 'LN_TPPIN020' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) THEN
                    'bptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:catv), '0'), 38, 10) is null and 
                    src:catv is not null) THEN
                    'catv contains a non-numeric value : \'' || AS_VARCHAR(src:catv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) THEN
                    'ccrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) THEN
                    'ccty_cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clos), '0'), 38, 10) is null and 
                    src:clos is not null) THEN
                    'clos contains a non-numeric value : \'' || AS_VARCHAR(src:clos) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cpla_cact_ref_compnr is not null) THEN
                    'cnpr_cpla_cact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cnpr_cpla_cact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cpla_ref_compnr is not null) THEN
                    'cnpr_cpla_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cnpr_cpla_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cpla_tact_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cpla_tact_ref_compnr is not null) THEN
                    'cnpr_cpla_tact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cnpr_cpla_tact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cspa_ref_compnr is not null) THEN
                    'cnpr_cspa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cnpr_cspa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_ref_compnr is not null) THEN
                    'cnpr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cnpr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_hbnr_cprj_ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_hbnr_cprj_ofbp_ref_compnr is not null) THEN
                    'cono_cnln_hbnr_cprj_ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_cnln_hbnr_cprj_ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_ref_compnr is not null) THEN
                    'cono_cnln_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_cnln_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) THEN
                    'cono_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) THEN
                    'cstv contains a non-numeric value : \'' || AS_VARCHAR(src:cstv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) THEN
                    'cuvc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuvc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) THEN
                    'cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) THEN
                    'cvyn contains a non-numeric value : \'' || AS_VARCHAR(src:cvyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc), '0'), 38, 10) is null and 
                    src:disc is not null) THEN
                    'disc contains a non-numeric value : \'' || AS_VARCHAR(src:disc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) THEN
                    'exmt contains a non-numeric value : \'' || AS_VARCHAR(src:exmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is null and 
                    src:exrs_ref_compnr is not null) THEN
                    'exrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:exrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcma), '0'), 38, 10) is null and 
                    src:fcma is not null) THEN
                    'fcma contains a non-numeric value : \'' || AS_VARCHAR(src:fcma) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmp), '0'), 38, 10) is null and 
                    src:fcmp is not null) THEN
                    'fcmp contains a non-numeric value : \'' || AS_VARCHAR(src:fcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fins), '0'), 38, 10) is null and 
                    src:fins is not null) THEN
                    'fins contains a non-numeric value : \'' || AS_VARCHAR(src:fins) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai), '0'), 38, 10) is null and 
                    src:hbai is not null) THEN
                    'hbai contains a non-numeric value : \'' || AS_VARCHAR(src:hbai) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbnr), '0'), 38, 10) is null and 
                    src:hbnr is not null) THEN
                    'hbnr contains a non-numeric value : \'' || AS_VARCHAR(src:hbnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbpc), '0'), 38, 10) is null and 
                    src:hbpc is not null) THEN
                    'hbpc contains a non-numeric value : \'' || AS_VARCHAR(src:hbpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbyn), '0'), 38, 10) is null and 
                    src:hbyn is not null) THEN
                    'hbyn contains a non-numeric value : \'' || AS_VARCHAR(src:hbyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idca), '0'), 38, 10) is null and 
                    src:idca is not null) THEN
                    'idca contains a non-numeric value : \'' || AS_VARCHAR(src:idca) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) THEN
                    'idoc contains a non-numeric value : \'' || AS_VARCHAR(src:idoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iiaa), '0'), 38, 10) is null and 
                    src:iiaa is not null) THEN
                    'iiaa contains a non-numeric value : \'' || AS_VARCHAR(src:iiaa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iiaa_cntc), '0'), 38, 10) is null and 
                    src:iiaa_cntc is not null) THEN
                    'iiaa_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:iiaa_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iiaa_dwhc), '0'), 38, 10) is null and 
                    src:iiaa_dwhc is not null) THEN
                    'iiaa_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:iiaa_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iiaa_homc), '0'), 38, 10) is null and 
                    src:iiaa_homc is not null) THEN
                    'iiaa_homc contains a non-numeric value : \'' || AS_VARCHAR(src:iiaa_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iiaa_prjc), '0'), 38, 10) is null and 
                    src:iiaa_prjc is not null) THEN
                    'iiaa_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:iiaa_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iiaa_refc), '0'), 38, 10) is null and 
                    src:iiaa_refc is not null) THEN
                    'iiaa_refc contains a non-numeric value : \'' || AS_VARCHAR(src:iiaa_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iiaa_rpc1), '0'), 38, 10) is null and 
                    src:iiaa_rpc1 is not null) THEN
                    'iiaa_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:iiaa_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iiaa_rpc2), '0'), 38, 10) is null and 
                    src:iiaa_rpc2 is not null) THEN
                    'iiaa_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:iiaa_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inaa), '0'), 38, 10) is null and 
                    src:inaa is not null) THEN
                    'inaa contains a non-numeric value : \'' || AS_VARCHAR(src:inaa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inaa_cntc), '0'), 38, 10) is null and 
                    src:inaa_cntc is not null) THEN
                    'inaa_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:inaa_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inaa_dwhc), '0'), 38, 10) is null and 
                    src:inaa_dwhc is not null) THEN
                    'inaa_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:inaa_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inaa_homc), '0'), 38, 10) is null and 
                    src:inaa_homc is not null) THEN
                    'inaa_homc contains a non-numeric value : \'' || AS_VARCHAR(src:inaa_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inaa_prjc), '0'), 38, 10) is null and 
                    src:inaa_prjc is not null) THEN
                    'inaa_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:inaa_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inaa_refc), '0'), 38, 10) is null and 
                    src:inaa_refc is not null) THEN
                    'inaa_refc contains a non-numeric value : \'' || AS_VARCHAR(src:inaa_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inaa_rpc1), '0'), 38, 10) is null and 
                    src:inaa_rpc1 is not null) THEN
                    'inaa_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:inaa_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inaa_rpc2), '0'), 38, 10) is null and 
                    src:inaa_rpc2 is not null) THEN
                    'inaa_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:inaa_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inia), '0'), 38, 10) is null and 
                    src:inia is not null) THEN
                    'inia contains a non-numeric value : \'' || AS_VARCHAR(src:inia) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inia_cntc), '0'), 38, 10) is null and 
                    src:inia_cntc is not null) THEN
                    'inia_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:inia_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inia_dwhc), '0'), 38, 10) is null and 
                    src:inia_dwhc is not null) THEN
                    'inia_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:inia_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inia_homc), '0'), 38, 10) is null and 
                    src:inia_homc is not null) THEN
                    'inia_homc contains a non-numeric value : \'' || AS_VARCHAR(src:inia_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inia_prjc), '0'), 38, 10) is null and 
                    src:inia_prjc is not null) THEN
                    'inia_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:inia_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inia_refc), '0'), 38, 10) is null and 
                    src:inia_refc is not null) THEN
                    'inia_refc contains a non-numeric value : \'' || AS_VARCHAR(src:inia_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inia_rpc1), '0'), 38, 10) is null and 
                    src:inia_rpc1 is not null) THEN
                    'inia_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:inia_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inia_rpc2), '0'), 38, 10) is null and 
                    src:inia_rpc2 is not null) THEN
                    'inia_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:inia_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpo), '0'), 38, 10) is null and 
                    src:inpo is not null) THEN
                    'inpo contains a non-numeric value : \'' || AS_VARCHAR(src:inpo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpr), '0'), 38, 10) is null and 
                    src:inpr is not null) THEN
                    'inpr contains a non-numeric value : \'' || AS_VARCHAR(src:inpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insa), '0'), 38, 10) is null and 
                    src:insa is not null) THEN
                    'insa contains a non-numeric value : \'' || AS_VARCHAR(src:insa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insa_cntc), '0'), 38, 10) is null and 
                    src:insa_cntc is not null) THEN
                    'insa_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:insa_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insa_dwhc), '0'), 38, 10) is null and 
                    src:insa_dwhc is not null) THEN
                    'insa_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:insa_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insa_homc), '0'), 38, 10) is null and 
                    src:insa_homc is not null) THEN
                    'insa_homc contains a non-numeric value : \'' || AS_VARCHAR(src:insa_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insa_prjc), '0'), 38, 10) is null and 
                    src:insa_prjc is not null) THEN
                    'insa_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:insa_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insa_refc), '0'), 38, 10) is null and 
                    src:insa_refc is not null) THEN
                    'insa_refc contains a non-numeric value : \'' || AS_VARCHAR(src:insa_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insa_rpc1), '0'), 38, 10) is null and 
                    src:insa_rpc1 is not null) THEN
                    'insa_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:insa_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insa_rpc2), '0'), 38, 10) is null and 
                    src:insa_rpc2 is not null) THEN
                    'insa_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:insa_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invo), '0'), 38, 10) is null and 
                    src:invo is not null) THEN
                    'invo contains a non-numeric value : \'' || AS_VARCHAR(src:invo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nins), '0'), 38, 10) is null and 
                    src:nins is not null) THEN
                    'nins contains a non-numeric value : \'' || AS_VARCHAR(src:nins) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nper), '0'), 38, 10) is null and 
                    src:nper is not null) THEN
                    'nper contains a non-numeric value : \'' || AS_VARCHAR(src:nper) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:npoi), '0'), 38, 10) is null and 
                    src:npoi is not null) THEN
                    'npoi contains a non-numeric value : \'' || AS_VARCHAR(src:npoi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:peru), '1900-01-01')) is null and 
                    src:peru is not null) THEN
                    'peru contains a non-timestamp value : \'' || AS_VARCHAR(src:peru) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pfdt), '1900-01-01')) is null and 
                    src:pfdt is not null) THEN
                    'pfdt contains a non-timestamp value : \'' || AS_VARCHAR(src:pfdt) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sidt), '1900-01-01')) is null and 
                    src:sidt is not null) THEN
                    'sidt contains a non-timestamp value : \'' || AS_VARCHAR(src:sidt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcst), '0'), 38, 10) is null and 
                    src:tcst is not null) THEN
                    'tcst contains a non-numeric value : \'' || AS_VARCHAR(src:tcst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiaa), '0'), 38, 10) is null and 
                    src:tiaa is not null) THEN
                    'tiaa contains a non-numeric value : \'' || AS_VARCHAR(src:tiaa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiaa_cntc), '0'), 38, 10) is null and 
                    src:tiaa_cntc is not null) THEN
                    'tiaa_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:tiaa_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiaa_dwhc), '0'), 38, 10) is null and 
                    src:tiaa_dwhc is not null) THEN
                    'tiaa_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:tiaa_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiaa_homc), '0'), 38, 10) is null and 
                    src:tiaa_homc is not null) THEN
                    'tiaa_homc contains a non-numeric value : \'' || AS_VARCHAR(src:tiaa_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiaa_prjc), '0'), 38, 10) is null and 
                    src:tiaa_prjc is not null) THEN
                    'tiaa_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:tiaa_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiaa_refc), '0'), 38, 10) is null and 
                    src:tiaa_refc is not null) THEN
                    'tiaa_refc contains a non-numeric value : \'' || AS_VARCHAR(src:tiaa_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiaa_rpc1), '0'), 38, 10) is null and 
                    src:tiaa_rpc1 is not null) THEN
                    'tiaa_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:tiaa_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiaa_rpc2), '0'), 38, 10) is null and 
                    src:tiaa_rpc2 is not null) THEN
                    'tiaa_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:tiaa_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiia), '0'), 38, 10) is null and 
                    src:tiia is not null) THEN
                    'tiia contains a non-numeric value : \'' || AS_VARCHAR(src:tiia) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiia_cntc), '0'), 38, 10) is null and 
                    src:tiia_cntc is not null) THEN
                    'tiia_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:tiia_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiia_dwhc), '0'), 38, 10) is null and 
                    src:tiia_dwhc is not null) THEN
                    'tiia_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:tiia_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiia_homc), '0'), 38, 10) is null and 
                    src:tiia_homc is not null) THEN
                    'tiia_homc contains a non-numeric value : \'' || AS_VARCHAR(src:tiia_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiia_prjc), '0'), 38, 10) is null and 
                    src:tiia_prjc is not null) THEN
                    'tiia_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:tiia_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiia_refc), '0'), 38, 10) is null and 
                    src:tiia_refc is not null) THEN
                    'tiia_refc contains a non-numeric value : \'' || AS_VARCHAR(src:tiia_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiia_rpc1), '0'), 38, 10) is null and 
                    src:tiia_rpc1 is not null) THEN
                    'tiia_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:tiia_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiia_rpc2), '0'), 38, 10) is null and 
                    src:tiia_rpc2 is not null) THEN
                    'tiia_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:tiia_rpc2) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tipo), '0'), 38, 10) is null and 
                    src:tipo is not null) THEN
                    'tipo contains a non-numeric value : \'' || AS_VARCHAR(src:tipo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tipr), '0'), 38, 10) is null and 
                    src:tipr is not null) THEN
                    'tipr contains a non-numeric value : \'' || AS_VARCHAR(src:tipr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:toin), '0'), 38, 10) is null and 
                    src:toin is not null) THEN
                    'toin contains a non-numeric value : \'' || AS_VARCHAR(src:toin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trsl), '0'), 38, 10) is null and 
                    src:trsl is not null) THEN
                    'trsl contains a non-numeric value : \'' || AS_VARCHAR(src:trsl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
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
                                    
                src:nins,
                src:ofbp,
                src:cono,
                src:cprj,
                src:cnln,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPIN020 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:catv), '0'), 38, 10) is null and 
                    src:catv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clos), '0'), 38, 10) is null and 
                    src:clos is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cpla_cact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cpla_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cpla_tact_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cpla_tact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cspa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_hbnr_cprj_ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_hbnr_cprj_ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc), '0'), 38, 10) is null and 
                    src:disc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is null and 
                    src:exrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcma), '0'), 38, 10) is null and 
                    src:fcma is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmp), '0'), 38, 10) is null and 
                    src:fcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fins), '0'), 38, 10) is null and 
                    src:fins is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai), '0'), 38, 10) is null and 
                    src:hbai is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbnr), '0'), 38, 10) is null and 
                    src:hbnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbpc), '0'), 38, 10) is null and 
                    src:hbpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbyn), '0'), 38, 10) is null and 
                    src:hbyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idca), '0'), 38, 10) is null and 
                    src:idca is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iiaa), '0'), 38, 10) is null and 
                    src:iiaa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iiaa_cntc), '0'), 38, 10) is null and 
                    src:iiaa_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iiaa_dwhc), '0'), 38, 10) is null and 
                    src:iiaa_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iiaa_homc), '0'), 38, 10) is null and 
                    src:iiaa_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iiaa_prjc), '0'), 38, 10) is null and 
                    src:iiaa_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iiaa_refc), '0'), 38, 10) is null and 
                    src:iiaa_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iiaa_rpc1), '0'), 38, 10) is null and 
                    src:iiaa_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iiaa_rpc2), '0'), 38, 10) is null and 
                    src:iiaa_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inaa), '0'), 38, 10) is null and 
                    src:inaa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inaa_cntc), '0'), 38, 10) is null and 
                    src:inaa_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inaa_dwhc), '0'), 38, 10) is null and 
                    src:inaa_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inaa_homc), '0'), 38, 10) is null and 
                    src:inaa_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inaa_prjc), '0'), 38, 10) is null and 
                    src:inaa_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inaa_refc), '0'), 38, 10) is null and 
                    src:inaa_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inaa_rpc1), '0'), 38, 10) is null and 
                    src:inaa_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inaa_rpc2), '0'), 38, 10) is null and 
                    src:inaa_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inia), '0'), 38, 10) is null and 
                    src:inia is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inia_cntc), '0'), 38, 10) is null and 
                    src:inia_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inia_dwhc), '0'), 38, 10) is null and 
                    src:inia_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inia_homc), '0'), 38, 10) is null and 
                    src:inia_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inia_prjc), '0'), 38, 10) is null and 
                    src:inia_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inia_refc), '0'), 38, 10) is null and 
                    src:inia_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inia_rpc1), '0'), 38, 10) is null and 
                    src:inia_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inia_rpc2), '0'), 38, 10) is null and 
                    src:inia_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpo), '0'), 38, 10) is null and 
                    src:inpo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpr), '0'), 38, 10) is null and 
                    src:inpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insa), '0'), 38, 10) is null and 
                    src:insa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insa_cntc), '0'), 38, 10) is null and 
                    src:insa_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insa_dwhc), '0'), 38, 10) is null and 
                    src:insa_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insa_homc), '0'), 38, 10) is null and 
                    src:insa_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insa_prjc), '0'), 38, 10) is null and 
                    src:insa_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insa_refc), '0'), 38, 10) is null and 
                    src:insa_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insa_rpc1), '0'), 38, 10) is null and 
                    src:insa_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insa_rpc2), '0'), 38, 10) is null and 
                    src:insa_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invo), '0'), 38, 10) is null and 
                    src:invo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nins), '0'), 38, 10) is null and 
                    src:nins is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nper), '0'), 38, 10) is null and 
                    src:nper is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:npoi), '0'), 38, 10) is null and 
                    src:npoi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:peru), '1900-01-01')) is null and 
                    src:peru is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pfdt), '1900-01-01')) is null and 
                    src:pfdt is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sidt), '1900-01-01')) is null and 
                    src:sidt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcst), '0'), 38, 10) is null and 
                    src:tcst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiaa), '0'), 38, 10) is null and 
                    src:tiaa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiaa_cntc), '0'), 38, 10) is null and 
                    src:tiaa_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiaa_dwhc), '0'), 38, 10) is null and 
                    src:tiaa_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiaa_homc), '0'), 38, 10) is null and 
                    src:tiaa_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiaa_prjc), '0'), 38, 10) is null and 
                    src:tiaa_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiaa_refc), '0'), 38, 10) is null and 
                    src:tiaa_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiaa_rpc1), '0'), 38, 10) is null and 
                    src:tiaa_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiaa_rpc2), '0'), 38, 10) is null and 
                    src:tiaa_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiia), '0'), 38, 10) is null and 
                    src:tiia is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiia_cntc), '0'), 38, 10) is null and 
                    src:tiia_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiia_dwhc), '0'), 38, 10) is null and 
                    src:tiia_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiia_homc), '0'), 38, 10) is null and 
                    src:tiia_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiia_prjc), '0'), 38, 10) is null and 
                    src:tiia_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiia_refc), '0'), 38, 10) is null and 
                    src:tiia_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiia_rpc1), '0'), 38, 10) is null and 
                    src:tiia_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tiia_rpc2), '0'), 38, 10) is null and 
                    src:tiia_rpc2 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tipo), '0'), 38, 10) is null and 
                    src:tipo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tipr), '0'), 38, 10) is null and 
                    src:tipr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:toin), '0'), 38, 10) is null and 
                    src:toin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trsl), '0'), 38, 10) is null and 
                    src:trsl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)