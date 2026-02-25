CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFGLD004_ERROR AS SELECT src, 'LN_TFGLD004' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmn), '0'), 38, 10) is null and 
                    src:acmn is not null) THEN
                    'acmn contains a non-numeric value : \'' || AS_VARCHAR(src:acmn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:addr_ref_compnr), '0'), 38, 10) is null and 
                    src:addr_ref_compnr is not null) THEN
                    'addr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:addr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amfn), '0'), 38, 10) is null and 
                    src:amfn is not null) THEN
                    'amfn contains a non-numeric value : \'' || AS_VARCHAR(src:amfn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atol), '0'), 38, 10) is null and 
                    src:atol is not null) THEN
                    'atol contains a non-numeric value : \'' || AS_VARCHAR(src:atol) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:badm), '0'), 38, 10) is null and 
                    src:badm is not null) THEN
                    'badm contains a non-numeric value : \'' || AS_VARCHAR(src:badm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blac_ref_compnr), '0'), 38, 10) is null and 
                    src:blac_ref_compnr is not null) THEN
                    'blac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:blac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blca_ref_compnr), '0'), 38, 10) is null and 
                    src:blca_ref_compnr is not null) THEN
                    'blca_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:blca_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blpl), '0'), 38, 10) is null and 
                    src:blpl is not null) THEN
                    'blpl contains a non-numeric value : \'' || AS_VARCHAR(src:blpl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bref), '0'), 38, 10) is null and 
                    src:bref is not null) THEN
                    'bref contains a non-numeric value : \'' || AS_VARCHAR(src:bref) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bsjv), '0'), 38, 10) is null and 
                    src:bsjv is not null) THEN
                    'bsjv contains a non-numeric value : \'' || AS_VARCHAR(src:bsjv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btyp), '0'), 38, 10) is null and 
                    src:btyp is not null) THEN
                    'btyp contains a non-numeric value : \'' || AS_VARCHAR(src:btyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdif), '0'), 38, 10) is null and 
                    src:cdif is not null) THEN
                    'cdif contains a non-numeric value : \'' || AS_VARCHAR(src:cdif) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfac_ref_compnr), '0'), 38, 10) is null and 
                    src:cfac_ref_compnr is not null) THEN
                    'cfac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfcl_ref_compnr), '0'), 38, 10) is null and 
                    src:cfcl_ref_compnr is not null) THEN
                    'cfcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cftp_ref_compnr), '0'), 38, 10) is null and 
                    src:cftp_ref_compnr is not null) THEN
                    'cftp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cftp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clac_ref_compnr), '0'), 38, 10) is null and 
                    src:clac_ref_compnr is not null) THEN
                    'clac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clcl_ref_compnr), '0'), 38, 10) is null and 
                    src:clcl_ref_compnr is not null) THEN
                    'clcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmac_ref_compnr), '0'), 38, 10) is null and 
                    src:cmac_ref_compnr is not null) THEN
                    'cmac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crvr), '0'), 38, 10) is null and 
                    src:crvr is not null) THEN
                    'crvr contains a non-numeric value : \'' || AS_VARCHAR(src:crvr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyp_ref_compnr), '0'), 38, 10) is null and 
                    src:ctyp_ref_compnr is not null) THEN
                    'ctyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfdm), '0'), 38, 10) is null and 
                    src:dfdm is not null) THEN
                    'dfdm contains a non-numeric value : \'' || AS_VARCHAR(src:dfdm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dga1_ref_compnr), '0'), 38, 10) is null and 
                    src:dga1_ref_compnr is not null) THEN
                    'dga1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dga1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dga2_ref_compnr), '0'), 38, 10) is null and 
                    src:dga2_ref_compnr is not null) THEN
                    'dga2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dga2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dgla_ref_compnr), '0'), 38, 10) is null and 
                    src:dgla_ref_compnr is not null) THEN
                    'dgla_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dgla_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dglc_ref_compnr), '0'), 38, 10) is null and 
                    src:dglc_ref_compnr is not null) THEN
                    'dglc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dglc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dla1_ref_compnr), '0'), 38, 10) is null and 
                    src:dla1_ref_compnr is not null) THEN
                    'dla1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dla1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dla2_ref_compnr), '0'), 38, 10) is null and 
                    src:dla2_ref_compnr is not null) THEN
                    'dla2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dla2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:drpf), '0'), 38, 10) is null and 
                    src:drpf is not null) THEN
                    'drpf contains a non-numeric value : \'' || AS_VARCHAR(src:drpf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dssq_1), '0'), 38, 10) is null and 
                    src:dssq_1 is not null) THEN
                    'dssq_1 contains a non-numeric value : \'' || AS_VARCHAR(src:dssq_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dssq_2), '0'), 38, 10) is null and 
                    src:dssq_2 is not null) THEN
                    'dssq_2 contains a non-numeric value : \'' || AS_VARCHAR(src:dssq_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dssq_3), '0'), 38, 10) is null and 
                    src:dssq_3 is not null) THEN
                    'dssq_3 contains a non-numeric value : \'' || AS_VARCHAR(src:dssq_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dssq_4), '0'), 38, 10) is null and 
                    src:dssq_4 is not null) THEN
                    'dssq_4 contains a non-numeric value : \'' || AS_VARCHAR(src:dssq_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esrn), '0'), 38, 10) is null and 
                    src:esrn is not null) THEN
                    'esrn contains a non-numeric value : \'' || AS_VARCHAR(src:esrn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etyp_ref_compnr), '0'), 38, 10) is null and 
                    src:etyp_ref_compnr is not null) THEN
                    'etyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:etyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fbya), '0'), 38, 10) is null and 
                    src:fbya is not null) THEN
                    'fbya contains a non-numeric value : \'' || AS_VARCHAR(src:fbya) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fpnw_ref_compnr), '0'), 38, 10) is null and 
                    src:fpnw_ref_compnr is not null) THEN
                    'fpnw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fpnw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fpor_ref_compnr), '0'), 38, 10) is null and 
                    src:fpor_ref_compnr is not null) THEN
                    'fpor_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fpor_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gcmp), '0'), 38, 10) is null and 
                    src:gcmp is not null) THEN
                    'gcmp contains a non-numeric value : \'' || AS_VARCHAR(src:gcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gcmp_rcmp), '0'), 38, 10) is null and 
                    src:gcmp_rcmp is not null) THEN
                    'gcmp_rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:gcmp_rcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:glac), '0'), 38, 10) is null and 
                    src:glac is not null) THEN
                    'glac contains a non-numeric value : \'' || AS_VARCHAR(src:glac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:glty_ref_compnr), '0'), 38, 10) is null and 
                    src:glty_ref_compnr is not null) THEN
                    'glty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:glty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hcmp), '0'), 38, 10) is null and 
                    src:hcmp is not null) THEN
                    'hcmp contains a non-numeric value : \'' || AS_VARCHAR(src:hcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ilac_ref_compnr), '0'), 38, 10) is null and 
                    src:ilac_ref_compnr is not null) THEN
                    'ilac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ilac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ilcc_ref_compnr), '0'), 38, 10) is null and 
                    src:ilcc_ref_compnr is not null) THEN
                    'ilcc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ilcc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inac_ref_compnr), '0'), 38, 10) is null and 
                    src:inac_ref_compnr is not null) THEN
                    'inac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:inac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incc_ref_compnr), '0'), 38, 10) is null and 
                    src:incc_ref_compnr is not null) THEN
                    'incc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:incc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:indm), '0'), 38, 10) is null and 
                    src:indm is not null) THEN
                    'indm contains a non-numeric value : \'' || AS_VARCHAR(src:indm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) THEN
                    'indt contains a non-timestamp value : \'' || AS_VARCHAR(src:indt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:infa), '0'), 38, 10) is null and 
                    src:infa is not null) THEN
                    'infa contains a non-numeric value : \'' || AS_VARCHAR(src:infa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:isgc_ref_compnr), '0'), 38, 10) is null and 
                    src:isgc_ref_compnr is not null) THEN
                    'isgc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:isgc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:isgm_ref_compnr), '0'), 38, 10) is null and 
                    src:isgm_ref_compnr is not null) THEN
                    'isgm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:isgm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:jprt), '0'), 38, 10) is null and 
                    src:jprt is not null) THEN
                    'jprt contains a non-numeric value : \'' || AS_VARCHAR(src:jprt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:jrbk), '0'), 38, 10) is null and 
                    src:jrbk is not null) THEN
                    'jrbk contains a non-numeric value : \'' || AS_VARCHAR(src:jrbk) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:latg), '0'), 38, 10) is null and 
                    src:latg is not null) THEN
                    'latg contains a non-numeric value : \'' || AS_VARCHAR(src:latg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:made), '0'), 38, 10) is null and 
                    src:made is not null) THEN
                    'made contains a non-numeric value : \'' || AS_VARCHAR(src:made) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:madu), '0'), 38, 10) is null and 
                    src:madu is not null) THEN
                    'madu contains a non-numeric value : \'' || AS_VARCHAR(src:madu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mauc), '0'), 38, 10) is null and 
                    src:mauc is not null) THEN
                    'mauc contains a non-numeric value : \'' || AS_VARCHAR(src:mauc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfa_ref_compnr), '0'), 38, 10) is null and 
                    src:mcfa_ref_compnr is not null) THEN
                    'mcfa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mcfa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfb_ref_compnr), '0'), 38, 10) is null and 
                    src:mcfb_ref_compnr is not null) THEN
                    'mcfb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mcfb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfc_ref_compnr), '0'), 38, 10) is null and 
                    src:mcfc_ref_compnr is not null) THEN
                    'mcfc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mcfc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfd_ref_compnr), '0'), 38, 10) is null and 
                    src:mcfd_ref_compnr is not null) THEN
                    'mcfd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mcfd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfn_ref_compnr), '0'), 38, 10) is null and 
                    src:mcfn_ref_compnr is not null) THEN
                    'mcfn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mcfn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfo_ref_compnr), '0'), 38, 10) is null and 
                    src:mcfo_ref_compnr is not null) THEN
                    'mcfo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mcfo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcra_ref_compnr), '0'), 38, 10) is null and 
                    src:mcra_ref_compnr is not null) THEN
                    'mcra_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mcra_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcrb_ref_compnr), '0'), 38, 10) is null and 
                    src:mcrb_ref_compnr is not null) THEN
                    'mcrb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mcrb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcrc_ref_compnr), '0'), 38, 10) is null and 
                    src:mcrc_ref_compnr is not null) THEN
                    'mcrc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mcrc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcrd_ref_compnr), '0'), 38, 10) is null and 
                    src:mcrd_ref_compnr is not null) THEN
                    'mcrd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mcrd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcrn_ref_compnr), '0'), 38, 10) is null and 
                    src:mcrn_ref_compnr is not null) THEN
                    'mcrn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mcrn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcro_ref_compnr), '0'), 38, 10) is null and 
                    src:mcro_ref_compnr is not null) THEN
                    'mcro_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mcro_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpie), '0'), 38, 10) is null and 
                    src:mpie is not null) THEN
                    'mpie contains a non-numeric value : \'' || AS_VARCHAR(src:mpie) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpiu), '0'), 38, 10) is null and 
                    src:mpiu is not null) THEN
                    'mpiu contains a non-numeric value : \'' || AS_VARCHAR(src:mpiu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtol), '0'), 38, 10) is null and 
                    src:mtol is not null) THEN
                    'mtol contains a non-numeric value : \'' || AS_VARCHAR(src:mtol) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:mtyp_ref_compnr is not null) THEN
                    'mtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nftr), '0'), 38, 10) is null and 
                    src:nftr is not null) THEN
                    'nftr contains a non-numeric value : \'' || AS_VARCHAR(src:nftr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obcp_ref_compnr), '0'), 38, 10) is null and 
                    src:obcp_ref_compnr is not null) THEN
                    'obcp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:obcp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obst_ref_compnr), '0'), 38, 10) is null and 
                    src:obst_ref_compnr is not null) THEN
                    'obst_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:obst_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paym), '0'), 38, 10) is null and 
                    src:paym is not null) THEN
                    'paym contains a non-numeric value : \'' || AS_VARCHAR(src:paym) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfdm), '0'), 38, 10) is null and 
                    src:pfdm is not null) THEN
                    'pfdm contains a non-numeric value : \'' || AS_VARCHAR(src:pfdm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plat), '0'), 38, 10) is null and 
                    src:plat is not null) THEN
                    'plat contains a non-numeric value : \'' || AS_VARCHAR(src:plat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcac), '0'), 38, 10) is null and 
                    src:rcac is not null) THEN
                    'rcac contains a non-numeric value : \'' || AS_VARCHAR(src:rcac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcbp), '0'), 38, 10) is null and 
                    src:rcbp is not null) THEN
                    'rcbp contains a non-numeric value : \'' || AS_VARCHAR(src:rcbp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:recp_ref_compnr), '0'), 38, 10) is null and 
                    src:recp_ref_compnr is not null) THEN
                    'recp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:recp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rest_ref_compnr), '0'), 38, 10) is null and 
                    src:rest_ref_compnr is not null) THEN
                    'rest_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rest_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rjic), '0'), 38, 10) is null and 
                    src:rjic is not null) THEN
                    'rjic contains a non-numeric value : \'' || AS_VARCHAR(src:rjic) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:roca_ref_compnr), '0'), 38, 10) is null and 
                    src:roca_ref_compnr is not null) THEN
                    'roca_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:roca_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:roda_ref_compnr), '0'), 38, 10) is null and 
                    src:roda_ref_compnr is not null) THEN
                    'roda_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:roda_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpnw_ref_compnr), '0'), 38, 10) is null and 
                    src:rpnw_ref_compnr is not null) THEN
                    'rpnw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rpnw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpor_ref_compnr), '0'), 38, 10) is null and 
                    src:rpor_ref_compnr is not null) THEN
                    'rpor_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rpor_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rvse), '0'), 38, 10) is null and 
                    src:rvse is not null) THEN
                    'rvse contains a non-numeric value : \'' || AS_VARCHAR(src:rvse) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rvtt_ref_compnr), '0'), 38, 10) is null and 
                    src:rvtt_ref_compnr is not null) THEN
                    'rvtt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rvtt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sapi), '0'), 38, 10) is null and 
                    src:sapi is not null) THEN
                    'sapi contains a non-numeric value : \'' || AS_VARCHAR(src:sapi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbct_ref_compnr), '0'), 38, 10) is null and 
                    src:sbct_ref_compnr is not null) THEN
                    'sbct_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sbct_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schm), '0'), 38, 10) is null and 
                    src:schm is not null) THEN
                    'schm contains a non-numeric value : \'' || AS_VARCHAR(src:schm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smac_ref_compnr), '0'), 38, 10) is null and 
                    src:smac_ref_compnr is not null) THEN
                    'smac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:smac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spac_ref_compnr), '0'), 38, 10) is null and 
                    src:spac_ref_compnr is not null) THEN
                    'spac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:spac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sprt), '0'), 38, 10) is null and 
                    src:sprt is not null) THEN
                    'sprt contains a non-numeric value : \'' || AS_VARCHAR(src:sprt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srac), '0'), 38, 10) is null and 
                    src:srac is not null) THEN
                    'srac contains a non-numeric value : \'' || AS_VARCHAR(src:srac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:taut_ref_compnr), '0'), 38, 10) is null and 
                    src:taut_ref_compnr is not null) THEN
                    'taut_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:taut_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tbat), '0'), 38, 10) is null and 
                    src:tbat is not null) THEN
                    'tbat contains a non-numeric value : \'' || AS_VARCHAR(src:tbat) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmfc_ref_compnr), '0'), 38, 10) is null and 
                    src:tmfc_ref_compnr is not null) THEN
                    'tmfc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tmfc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10), '0'), 38, 10) is null and 
                    src:tp10 is not null) THEN
                    'tp10 contains a non-numeric value : \'' || AS_VARCHAR(src:tp10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_bc10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_bc10_ref_compnr is not null) THEN
                    'tp10_bc10_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp10_bc10_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_bd10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_bd10_ref_compnr is not null) THEN
                    'tp10_bd10_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp10_bd10_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_ca10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_ca10_ref_compnr is not null) THEN
                    'tp10_ca10_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp10_ca10_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_cc10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_cc10_ref_compnr is not null) THEN
                    'tp10_cc10_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp10_cc10_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_dc10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_dc10_ref_compnr is not null) THEN
                    'tp10_dc10_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp10_dc10_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_ds10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_ds10_ref_compnr is not null) THEN
                    'tp10_ds10_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp10_ds10_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_fa10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_fa10_ref_compnr is not null) THEN
                    'tp10_fa10_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp10_fa10_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_fc10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_fc10_ref_compnr is not null) THEN
                    'tp10_fc10_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp10_fc10_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_ic10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_ic10_ref_compnr is not null) THEN
                    'tp10_ic10_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp10_ic10_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_id10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_id10_ref_compnr is not null) THEN
                    'tp10_id10_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp10_id10_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_rc10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_rc10_ref_compnr is not null) THEN
                    'tp10_rc10_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp10_rc10_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_rd10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_rd10_ref_compnr is not null) THEN
                    'tp10_rd10_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp10_rd10_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_ro10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_ro10_ref_compnr is not null) THEN
                    'tp10_ro10_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp10_ro10_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_rs10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_rs10_ref_compnr is not null) THEN
                    'tp10_rs10_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp10_rs10_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11), '0'), 38, 10) is null and 
                    src:tp11 is not null) THEN
                    'tp11 contains a non-numeric value : \'' || AS_VARCHAR(src:tp11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_bc11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_bc11_ref_compnr is not null) THEN
                    'tp11_bc11_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp11_bc11_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_bd11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_bd11_ref_compnr is not null) THEN
                    'tp11_bd11_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp11_bd11_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_ca11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_ca11_ref_compnr is not null) THEN
                    'tp11_ca11_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp11_ca11_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_cc11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_cc11_ref_compnr is not null) THEN
                    'tp11_cc11_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp11_cc11_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_dc11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_dc11_ref_compnr is not null) THEN
                    'tp11_dc11_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp11_dc11_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_ds11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_ds11_ref_compnr is not null) THEN
                    'tp11_ds11_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp11_ds11_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_fa11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_fa11_ref_compnr is not null) THEN
                    'tp11_fa11_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp11_fa11_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_fc11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_fc11_ref_compnr is not null) THEN
                    'tp11_fc11_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp11_fc11_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_ic11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_ic11_ref_compnr is not null) THEN
                    'tp11_ic11_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp11_ic11_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_id11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_id11_ref_compnr is not null) THEN
                    'tp11_id11_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp11_id11_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_rc11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_rc11_ref_compnr is not null) THEN
                    'tp11_rc11_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp11_rc11_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_rd11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_rd11_ref_compnr is not null) THEN
                    'tp11_rd11_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp11_rd11_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_ro11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_ro11_ref_compnr is not null) THEN
                    'tp11_ro11_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp11_ro11_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_rs11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_rs11_ref_compnr is not null) THEN
                    'tp11_rs11_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp11_rs11_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12), '0'), 38, 10) is null and 
                    src:tp12 is not null) THEN
                    'tp12 contains a non-numeric value : \'' || AS_VARCHAR(src:tp12) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_bc12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_bc12_ref_compnr is not null) THEN
                    'tp12_bc12_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp12_bc12_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_bd12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_bd12_ref_compnr is not null) THEN
                    'tp12_bd12_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp12_bd12_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_ca12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_ca12_ref_compnr is not null) THEN
                    'tp12_ca12_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp12_ca12_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_cc12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_cc12_ref_compnr is not null) THEN
                    'tp12_cc12_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp12_cc12_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_dc12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_dc12_ref_compnr is not null) THEN
                    'tp12_dc12_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp12_dc12_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_ds12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_ds12_ref_compnr is not null) THEN
                    'tp12_ds12_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp12_ds12_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_fa12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_fa12_ref_compnr is not null) THEN
                    'tp12_fa12_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp12_fa12_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_fc12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_fc12_ref_compnr is not null) THEN
                    'tp12_fc12_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp12_fc12_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_ic12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_ic12_ref_compnr is not null) THEN
                    'tp12_ic12_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp12_ic12_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_id12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_id12_ref_compnr is not null) THEN
                    'tp12_id12_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp12_id12_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_rc12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_rc12_ref_compnr is not null) THEN
                    'tp12_rc12_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp12_rc12_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_rd12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_rd12_ref_compnr is not null) THEN
                    'tp12_rd12_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp12_rd12_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_ro12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_ro12_ref_compnr is not null) THEN
                    'tp12_ro12_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp12_ro12_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_rs12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_rs12_ref_compnr is not null) THEN
                    'tp12_rs12_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tp12_rs12_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ttcj_ref_compnr), '0'), 38, 10) is null and 
                    src:ttcj_ref_compnr is not null) THEN
                    'ttcj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ttcj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1), '0'), 38, 10) is null and 
                    src:typ1 is not null) THEN
                    'typ1 contains a non-numeric value : \'' || AS_VARCHAR(src:typ1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_blc1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_blc1_ref_compnr is not null) THEN
                    'typ1_blc1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ1_blc1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_bld1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_bld1_ref_compnr is not null) THEN
                    'typ1_bld1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ1_bld1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_cfa1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_cfa1_ref_compnr is not null) THEN
                    'typ1_cfa1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ1_cfa1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_cfc1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_cfc1_ref_compnr is not null) THEN
                    'typ1_cfc1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ1_cfc1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_cla1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_cla1_ref_compnr is not null) THEN
                    'typ1_cla1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ1_cla1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_clc1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_clc1_ref_compnr is not null) THEN
                    'typ1_clc1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ1_clc1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_dcm1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_dcm1_ref_compnr is not null) THEN
                    'typ1_dcm1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ1_dcm1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_dst1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_dst1_ref_compnr is not null) THEN
                    'typ1_dst1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ1_dst1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_inc1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_inc1_ref_compnr is not null) THEN
                    'typ1_inc1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ1_inc1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_ind1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_ind1_ref_compnr is not null) THEN
                    'typ1_ind1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ1_ind1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_rcd1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_rcd1_ref_compnr is not null) THEN
                    'typ1_rcd1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ1_rcd1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_roc1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_roc1_ref_compnr is not null) THEN
                    'typ1_roc1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ1_roc1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_rod1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_rod1_ref_compnr is not null) THEN
                    'typ1_rod1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ1_rod1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_rsd1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_rsd1_ref_compnr is not null) THEN
                    'typ1_rsd1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ1_rsd1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2), '0'), 38, 10) is null and 
                    src:typ2 is not null) THEN
                    'typ2 contains a non-numeric value : \'' || AS_VARCHAR(src:typ2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_blc2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_blc2_ref_compnr is not null) THEN
                    'typ2_blc2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ2_blc2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_bld2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_bld2_ref_compnr is not null) THEN
                    'typ2_bld2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ2_bld2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_cfa2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_cfa2_ref_compnr is not null) THEN
                    'typ2_cfa2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ2_cfa2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_cfc2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_cfc2_ref_compnr is not null) THEN
                    'typ2_cfc2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ2_cfc2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_cla2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_cla2_ref_compnr is not null) THEN
                    'typ2_cla2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ2_cla2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_clc2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_clc2_ref_compnr is not null) THEN
                    'typ2_clc2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ2_clc2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_dcm2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_dcm2_ref_compnr is not null) THEN
                    'typ2_dcm2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ2_dcm2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_dst2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_dst2_ref_compnr is not null) THEN
                    'typ2_dst2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ2_dst2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_inc2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_inc2_ref_compnr is not null) THEN
                    'typ2_inc2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ2_inc2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_ind2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_ind2_ref_compnr is not null) THEN
                    'typ2_ind2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ2_ind2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_rcd2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_rcd2_ref_compnr is not null) THEN
                    'typ2_rcd2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ2_rcd2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_roc2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_roc2_ref_compnr is not null) THEN
                    'typ2_roc2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ2_roc2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_rod2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_rod2_ref_compnr is not null) THEN
                    'typ2_rod2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ2_rod2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_rsd2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_rsd2_ref_compnr is not null) THEN
                    'typ2_rsd2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ2_rsd2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3), '0'), 38, 10) is null and 
                    src:typ3 is not null) THEN
                    'typ3 contains a non-numeric value : \'' || AS_VARCHAR(src:typ3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_blc3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_blc3_ref_compnr is not null) THEN
                    'typ3_blc3_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ3_blc3_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_bld3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_bld3_ref_compnr is not null) THEN
                    'typ3_bld3_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ3_bld3_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_cfa3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_cfa3_ref_compnr is not null) THEN
                    'typ3_cfa3_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ3_cfa3_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_cfc3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_cfc3_ref_compnr is not null) THEN
                    'typ3_cfc3_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ3_cfc3_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_cla3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_cla3_ref_compnr is not null) THEN
                    'typ3_cla3_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ3_cla3_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_clc3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_clc3_ref_compnr is not null) THEN
                    'typ3_clc3_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ3_clc3_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_dcm3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_dcm3_ref_compnr is not null) THEN
                    'typ3_dcm3_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ3_dcm3_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_dst3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_dst3_ref_compnr is not null) THEN
                    'typ3_dst3_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ3_dst3_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_inc3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_inc3_ref_compnr is not null) THEN
                    'typ3_inc3_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ3_inc3_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_ind3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_ind3_ref_compnr is not null) THEN
                    'typ3_ind3_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ3_ind3_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_rcd3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_rcd3_ref_compnr is not null) THEN
                    'typ3_rcd3_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ3_rcd3_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_roc3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_roc3_ref_compnr is not null) THEN
                    'typ3_roc3_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ3_roc3_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_rod3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_rod3_ref_compnr is not null) THEN
                    'typ3_rod3_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ3_rod3_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_rsd3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_rsd3_ref_compnr is not null) THEN
                    'typ3_rsd3_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ3_rsd3_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4), '0'), 38, 10) is null and 
                    src:typ4 is not null) THEN
                    'typ4 contains a non-numeric value : \'' || AS_VARCHAR(src:typ4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_blc4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_blc4_ref_compnr is not null) THEN
                    'typ4_blc4_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ4_blc4_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_bld4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_bld4_ref_compnr is not null) THEN
                    'typ4_bld4_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ4_bld4_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_cfa4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_cfa4_ref_compnr is not null) THEN
                    'typ4_cfa4_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ4_cfa4_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_cfc4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_cfc4_ref_compnr is not null) THEN
                    'typ4_cfc4_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ4_cfc4_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_cla4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_cla4_ref_compnr is not null) THEN
                    'typ4_cla4_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ4_cla4_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_clc4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_clc4_ref_compnr is not null) THEN
                    'typ4_clc4_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ4_clc4_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_dcm4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_dcm4_ref_compnr is not null) THEN
                    'typ4_dcm4_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ4_dcm4_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_dst4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_dst4_ref_compnr is not null) THEN
                    'typ4_dst4_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ4_dst4_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_inc4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_inc4_ref_compnr is not null) THEN
                    'typ4_inc4_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ4_inc4_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_ind4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_ind4_ref_compnr is not null) THEN
                    'typ4_ind4_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ4_ind4_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_rcd4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_rcd4_ref_compnr is not null) THEN
                    'typ4_rcd4_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ4_rcd4_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_roc4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_roc4_ref_compnr is not null) THEN
                    'typ4_roc4_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ4_roc4_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_rod4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_rod4_ref_compnr is not null) THEN
                    'typ4_rod4_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ4_rod4_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_rsd4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_rsd4_ref_compnr is not null) THEN
                    'typ4_rsd4_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ4_rsd4_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5), '0'), 38, 10) is null and 
                    src:typ5 is not null) THEN
                    'typ5 contains a non-numeric value : \'' || AS_VARCHAR(src:typ5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_blc5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_blc5_ref_compnr is not null) THEN
                    'typ5_blc5_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ5_blc5_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_bld5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_bld5_ref_compnr is not null) THEN
                    'typ5_bld5_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ5_bld5_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_cfa5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_cfa5_ref_compnr is not null) THEN
                    'typ5_cfa5_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ5_cfa5_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_cfc5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_cfc5_ref_compnr is not null) THEN
                    'typ5_cfc5_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ5_cfc5_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_cla5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_cla5_ref_compnr is not null) THEN
                    'typ5_cla5_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ5_cla5_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_clc5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_clc5_ref_compnr is not null) THEN
                    'typ5_clc5_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ5_clc5_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_dcm5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_dcm5_ref_compnr is not null) THEN
                    'typ5_dcm5_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ5_dcm5_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_dst5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_dst5_ref_compnr is not null) THEN
                    'typ5_dst5_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ5_dst5_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_inc5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_inc5_ref_compnr is not null) THEN
                    'typ5_inc5_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ5_inc5_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_ind5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_ind5_ref_compnr is not null) THEN
                    'typ5_ind5_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ5_ind5_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_rcd5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_rcd5_ref_compnr is not null) THEN
                    'typ5_rcd5_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ5_rcd5_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_roc5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_roc5_ref_compnr is not null) THEN
                    'typ5_roc5_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ5_roc5_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_rod5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_rod5_ref_compnr is not null) THEN
                    'typ5_rod5_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ5_rod5_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_rsd5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_rsd5_ref_compnr is not null) THEN
                    'typ5_rsd5_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ5_rsd5_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6), '0'), 38, 10) is null and 
                    src:typ6 is not null) THEN
                    'typ6 contains a non-numeric value : \'' || AS_VARCHAR(src:typ6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_blc6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_blc6_ref_compnr is not null) THEN
                    'typ6_blc6_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ6_blc6_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_bld6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_bld6_ref_compnr is not null) THEN
                    'typ6_bld6_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ6_bld6_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_cfa6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_cfa6_ref_compnr is not null) THEN
                    'typ6_cfa6_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ6_cfa6_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_cfc6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_cfc6_ref_compnr is not null) THEN
                    'typ6_cfc6_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ6_cfc6_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_cla6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_cla6_ref_compnr is not null) THEN
                    'typ6_cla6_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ6_cla6_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_clc6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_clc6_ref_compnr is not null) THEN
                    'typ6_clc6_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ6_clc6_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_dcm6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_dcm6_ref_compnr is not null) THEN
                    'typ6_dcm6_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ6_dcm6_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_dst6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_dst6_ref_compnr is not null) THEN
                    'typ6_dst6_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ6_dst6_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_inc6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_inc6_ref_compnr is not null) THEN
                    'typ6_inc6_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ6_inc6_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_ind6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_ind6_ref_compnr is not null) THEN
                    'typ6_ind6_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ6_ind6_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_rcd6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_rcd6_ref_compnr is not null) THEN
                    'typ6_rcd6_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ6_rcd6_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_roc6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_roc6_ref_compnr is not null) THEN
                    'typ6_roc6_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ6_roc6_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_rod6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_rod6_ref_compnr is not null) THEN
                    'typ6_rod6_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ6_rod6_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_rsd6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_rsd6_ref_compnr is not null) THEN
                    'typ6_rsd6_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ6_rsd6_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7), '0'), 38, 10) is null and 
                    src:typ7 is not null) THEN
                    'typ7 contains a non-numeric value : \'' || AS_VARCHAR(src:typ7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_blc7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_blc7_ref_compnr is not null) THEN
                    'typ7_blc7_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ7_blc7_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_bld7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_bld7_ref_compnr is not null) THEN
                    'typ7_bld7_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ7_bld7_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_cfa7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_cfa7_ref_compnr is not null) THEN
                    'typ7_cfa7_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ7_cfa7_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_cfc7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_cfc7_ref_compnr is not null) THEN
                    'typ7_cfc7_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ7_cfc7_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_cla7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_cla7_ref_compnr is not null) THEN
                    'typ7_cla7_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ7_cla7_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_clc7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_clc7_ref_compnr is not null) THEN
                    'typ7_clc7_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ7_clc7_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_dcm7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_dcm7_ref_compnr is not null) THEN
                    'typ7_dcm7_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ7_dcm7_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_dst7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_dst7_ref_compnr is not null) THEN
                    'typ7_dst7_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ7_dst7_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_inc7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_inc7_ref_compnr is not null) THEN
                    'typ7_inc7_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ7_inc7_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_ind7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_ind7_ref_compnr is not null) THEN
                    'typ7_ind7_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ7_ind7_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_rcd7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_rcd7_ref_compnr is not null) THEN
                    'typ7_rcd7_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ7_rcd7_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_roc7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_roc7_ref_compnr is not null) THEN
                    'typ7_roc7_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ7_roc7_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_rod7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_rod7_ref_compnr is not null) THEN
                    'typ7_rod7_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ7_rod7_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_rsd7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_rsd7_ref_compnr is not null) THEN
                    'typ7_rsd7_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ7_rsd7_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8), '0'), 38, 10) is null and 
                    src:typ8 is not null) THEN
                    'typ8 contains a non-numeric value : \'' || AS_VARCHAR(src:typ8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_blc8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_blc8_ref_compnr is not null) THEN
                    'typ8_blc8_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ8_blc8_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_bld8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_bld8_ref_compnr is not null) THEN
                    'typ8_bld8_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ8_bld8_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_cfa8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_cfa8_ref_compnr is not null) THEN
                    'typ8_cfa8_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ8_cfa8_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_cfc8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_cfc8_ref_compnr is not null) THEN
                    'typ8_cfc8_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ8_cfc8_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_cla8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_cla8_ref_compnr is not null) THEN
                    'typ8_cla8_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ8_cla8_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_clc8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_clc8_ref_compnr is not null) THEN
                    'typ8_clc8_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ8_clc8_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_dcm8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_dcm8_ref_compnr is not null) THEN
                    'typ8_dcm8_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ8_dcm8_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_dst8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_dst8_ref_compnr is not null) THEN
                    'typ8_dst8_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ8_dst8_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_inc8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_inc8_ref_compnr is not null) THEN
                    'typ8_inc8_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ8_inc8_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_ind8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_ind8_ref_compnr is not null) THEN
                    'typ8_ind8_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ8_ind8_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_rcd8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_rcd8_ref_compnr is not null) THEN
                    'typ8_rcd8_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ8_rcd8_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_roc8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_roc8_ref_compnr is not null) THEN
                    'typ8_roc8_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ8_roc8_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_rod8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_rod8_ref_compnr is not null) THEN
                    'typ8_rod8_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ8_rod8_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_rsd8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_rsd8_ref_compnr is not null) THEN
                    'typ8_rsd8_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ8_rsd8_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9), '0'), 38, 10) is null and 
                    src:typ9 is not null) THEN
                    'typ9 contains a non-numeric value : \'' || AS_VARCHAR(src:typ9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_blc9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_blc9_ref_compnr is not null) THEN
                    'typ9_blc9_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ9_blc9_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_bld9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_bld9_ref_compnr is not null) THEN
                    'typ9_bld9_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ9_bld9_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_cfa9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_cfa9_ref_compnr is not null) THEN
                    'typ9_cfa9_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ9_cfa9_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_cfc9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_cfc9_ref_compnr is not null) THEN
                    'typ9_cfc9_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ9_cfc9_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_cla9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_cla9_ref_compnr is not null) THEN
                    'typ9_cla9_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ9_cla9_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_clc9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_clc9_ref_compnr is not null) THEN
                    'typ9_clc9_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ9_clc9_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_dcm9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_dcm9_ref_compnr is not null) THEN
                    'typ9_dcm9_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ9_dcm9_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_dst9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_dst9_ref_compnr is not null) THEN
                    'typ9_dst9_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ9_dst9_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_inc9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_inc9_ref_compnr is not null) THEN
                    'typ9_inc9_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ9_inc9_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_ind9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_ind9_ref_compnr is not null) THEN
                    'typ9_ind9_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ9_ind9_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_rcd9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_rcd9_ref_compnr is not null) THEN
                    'typ9_rcd9_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ9_rcd9_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_roc9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_roc9_ref_compnr is not null) THEN
                    'typ9_roc9_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ9_roc9_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_rod9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_rod9_ref_compnr is not null) THEN
                    'typ9_rod9_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ9_rod9_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_rsd9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_rsd9_ref_compnr is not null) THEN
                    'typ9_rsd9_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typ9_rsd9_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usac), '0'), 38, 10) is null and 
                    src:usac is not null) THEN
                    'usac contains a non-numeric value : \'' || AS_VARCHAR(src:usac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vabk), '0'), 38, 10) is null and 
                    src:vabk is not null) THEN
                    'vabk contains a non-numeric value : \'' || AS_VARCHAR(src:vabk) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) THEN
                    'year contains a non-numeric value : \'' || AS_VARCHAR(src:year) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year_ref_compnr), '0'), 38, 10) is null and 
                    src:year_ref_compnr is not null) THEN
                    'year_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:year_ref_compnr) || '\'' WHEN 
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
                                    
                src:indt,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFGLD004 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmn), '0'), 38, 10) is null and 
                    src:acmn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:addr_ref_compnr), '0'), 38, 10) is null and 
                    src:addr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amfn), '0'), 38, 10) is null and 
                    src:amfn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atol), '0'), 38, 10) is null and 
                    src:atol is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:badm), '0'), 38, 10) is null and 
                    src:badm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blac_ref_compnr), '0'), 38, 10) is null and 
                    src:blac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blca_ref_compnr), '0'), 38, 10) is null and 
                    src:blca_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blpl), '0'), 38, 10) is null and 
                    src:blpl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bref), '0'), 38, 10) is null and 
                    src:bref is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bsjv), '0'), 38, 10) is null and 
                    src:bsjv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btyp), '0'), 38, 10) is null and 
                    src:btyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdif), '0'), 38, 10) is null and 
                    src:cdif is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfac_ref_compnr), '0'), 38, 10) is null and 
                    src:cfac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfcl_ref_compnr), '0'), 38, 10) is null and 
                    src:cfcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cftp_ref_compnr), '0'), 38, 10) is null and 
                    src:cftp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clac_ref_compnr), '0'), 38, 10) is null and 
                    src:clac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clcl_ref_compnr), '0'), 38, 10) is null and 
                    src:clcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmac_ref_compnr), '0'), 38, 10) is null and 
                    src:cmac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crvr), '0'), 38, 10) is null and 
                    src:crvr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyp_ref_compnr), '0'), 38, 10) is null and 
                    src:ctyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfdm), '0'), 38, 10) is null and 
                    src:dfdm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dga1_ref_compnr), '0'), 38, 10) is null and 
                    src:dga1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dga2_ref_compnr), '0'), 38, 10) is null and 
                    src:dga2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dgla_ref_compnr), '0'), 38, 10) is null and 
                    src:dgla_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dglc_ref_compnr), '0'), 38, 10) is null and 
                    src:dglc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dla1_ref_compnr), '0'), 38, 10) is null and 
                    src:dla1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dla2_ref_compnr), '0'), 38, 10) is null and 
                    src:dla2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:drpf), '0'), 38, 10) is null and 
                    src:drpf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dssq_1), '0'), 38, 10) is null and 
                    src:dssq_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dssq_2), '0'), 38, 10) is null and 
                    src:dssq_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dssq_3), '0'), 38, 10) is null and 
                    src:dssq_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dssq_4), '0'), 38, 10) is null and 
                    src:dssq_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esrn), '0'), 38, 10) is null and 
                    src:esrn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etyp_ref_compnr), '0'), 38, 10) is null and 
                    src:etyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fbya), '0'), 38, 10) is null and 
                    src:fbya is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fpnw_ref_compnr), '0'), 38, 10) is null and 
                    src:fpnw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fpor_ref_compnr), '0'), 38, 10) is null and 
                    src:fpor_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gcmp), '0'), 38, 10) is null and 
                    src:gcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gcmp_rcmp), '0'), 38, 10) is null and 
                    src:gcmp_rcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:glac), '0'), 38, 10) is null and 
                    src:glac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:glty_ref_compnr), '0'), 38, 10) is null and 
                    src:glty_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hcmp), '0'), 38, 10) is null and 
                    src:hcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ilac_ref_compnr), '0'), 38, 10) is null and 
                    src:ilac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ilcc_ref_compnr), '0'), 38, 10) is null and 
                    src:ilcc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inac_ref_compnr), '0'), 38, 10) is null and 
                    src:inac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incc_ref_compnr), '0'), 38, 10) is null and 
                    src:incc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:indm), '0'), 38, 10) is null and 
                    src:indm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:infa), '0'), 38, 10) is null and 
                    src:infa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:isgc_ref_compnr), '0'), 38, 10) is null and 
                    src:isgc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:isgm_ref_compnr), '0'), 38, 10) is null and 
                    src:isgm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:jprt), '0'), 38, 10) is null and 
                    src:jprt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:jrbk), '0'), 38, 10) is null and 
                    src:jrbk is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:latg), '0'), 38, 10) is null and 
                    src:latg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:made), '0'), 38, 10) is null and 
                    src:made is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:madu), '0'), 38, 10) is null and 
                    src:madu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mauc), '0'), 38, 10) is null and 
                    src:mauc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfa_ref_compnr), '0'), 38, 10) is null and 
                    src:mcfa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfb_ref_compnr), '0'), 38, 10) is null and 
                    src:mcfb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfc_ref_compnr), '0'), 38, 10) is null and 
                    src:mcfc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfd_ref_compnr), '0'), 38, 10) is null and 
                    src:mcfd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfn_ref_compnr), '0'), 38, 10) is null and 
                    src:mcfn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfo_ref_compnr), '0'), 38, 10) is null and 
                    src:mcfo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcra_ref_compnr), '0'), 38, 10) is null and 
                    src:mcra_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcrb_ref_compnr), '0'), 38, 10) is null and 
                    src:mcrb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcrc_ref_compnr), '0'), 38, 10) is null and 
                    src:mcrc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcrd_ref_compnr), '0'), 38, 10) is null and 
                    src:mcrd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcrn_ref_compnr), '0'), 38, 10) is null and 
                    src:mcrn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcro_ref_compnr), '0'), 38, 10) is null and 
                    src:mcro_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpie), '0'), 38, 10) is null and 
                    src:mpie is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpiu), '0'), 38, 10) is null and 
                    src:mpiu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtol), '0'), 38, 10) is null and 
                    src:mtol is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:mtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nftr), '0'), 38, 10) is null and 
                    src:nftr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obcp_ref_compnr), '0'), 38, 10) is null and 
                    src:obcp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obst_ref_compnr), '0'), 38, 10) is null and 
                    src:obst_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paym), '0'), 38, 10) is null and 
                    src:paym is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfdm), '0'), 38, 10) is null and 
                    src:pfdm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plat), '0'), 38, 10) is null and 
                    src:plat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcac), '0'), 38, 10) is null and 
                    src:rcac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcbp), '0'), 38, 10) is null and 
                    src:rcbp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:recp_ref_compnr), '0'), 38, 10) is null and 
                    src:recp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rest_ref_compnr), '0'), 38, 10) is null and 
                    src:rest_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rjic), '0'), 38, 10) is null and 
                    src:rjic is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:roca_ref_compnr), '0'), 38, 10) is null and 
                    src:roca_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:roda_ref_compnr), '0'), 38, 10) is null and 
                    src:roda_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpnw_ref_compnr), '0'), 38, 10) is null and 
                    src:rpnw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpor_ref_compnr), '0'), 38, 10) is null and 
                    src:rpor_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rvse), '0'), 38, 10) is null and 
                    src:rvse is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rvtt_ref_compnr), '0'), 38, 10) is null and 
                    src:rvtt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sapi), '0'), 38, 10) is null and 
                    src:sapi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbct_ref_compnr), '0'), 38, 10) is null and 
                    src:sbct_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schm), '0'), 38, 10) is null and 
                    src:schm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smac_ref_compnr), '0'), 38, 10) is null and 
                    src:smac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spac_ref_compnr), '0'), 38, 10) is null and 
                    src:spac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sprt), '0'), 38, 10) is null and 
                    src:sprt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srac), '0'), 38, 10) is null and 
                    src:srac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:taut_ref_compnr), '0'), 38, 10) is null and 
                    src:taut_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tbat), '0'), 38, 10) is null and 
                    src:tbat is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmfc_ref_compnr), '0'), 38, 10) is null and 
                    src:tmfc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10), '0'), 38, 10) is null and 
                    src:tp10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_bc10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_bc10_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_bd10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_bd10_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_ca10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_ca10_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_cc10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_cc10_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_dc10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_dc10_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_ds10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_ds10_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_fa10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_fa10_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_fc10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_fc10_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_ic10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_ic10_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_id10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_id10_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_rc10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_rc10_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_rd10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_rd10_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_ro10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_ro10_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp10_rs10_ref_compnr), '0'), 38, 10) is null and 
                    src:tp10_rs10_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11), '0'), 38, 10) is null and 
                    src:tp11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_bc11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_bc11_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_bd11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_bd11_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_ca11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_ca11_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_cc11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_cc11_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_dc11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_dc11_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_ds11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_ds11_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_fa11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_fa11_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_fc11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_fc11_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_ic11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_ic11_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_id11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_id11_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_rc11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_rc11_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_rd11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_rd11_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_ro11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_ro11_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp11_rs11_ref_compnr), '0'), 38, 10) is null and 
                    src:tp11_rs11_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12), '0'), 38, 10) is null and 
                    src:tp12 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_bc12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_bc12_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_bd12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_bd12_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_ca12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_ca12_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_cc12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_cc12_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_dc12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_dc12_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_ds12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_ds12_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_fa12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_fa12_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_fc12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_fc12_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_ic12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_ic12_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_id12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_id12_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_rc12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_rc12_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_rd12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_rd12_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_ro12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_ro12_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tp12_rs12_ref_compnr), '0'), 38, 10) is null and 
                    src:tp12_rs12_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ttcj_ref_compnr), '0'), 38, 10) is null and 
                    src:ttcj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1), '0'), 38, 10) is null and 
                    src:typ1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_blc1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_blc1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_bld1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_bld1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_cfa1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_cfa1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_cfc1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_cfc1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_cla1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_cla1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_clc1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_clc1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_dcm1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_dcm1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_dst1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_dst1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_inc1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_inc1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_ind1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_ind1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_rcd1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_rcd1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_roc1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_roc1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_rod1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_rod1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1_rsd1_ref_compnr), '0'), 38, 10) is null and 
                    src:typ1_rsd1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2), '0'), 38, 10) is null and 
                    src:typ2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_blc2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_blc2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_bld2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_bld2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_cfa2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_cfa2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_cfc2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_cfc2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_cla2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_cla2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_clc2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_clc2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_dcm2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_dcm2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_dst2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_dst2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_inc2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_inc2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_ind2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_ind2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_rcd2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_rcd2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_roc2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_roc2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_rod2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_rod2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2_rsd2_ref_compnr), '0'), 38, 10) is null and 
                    src:typ2_rsd2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3), '0'), 38, 10) is null and 
                    src:typ3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_blc3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_blc3_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_bld3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_bld3_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_cfa3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_cfa3_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_cfc3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_cfc3_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_cla3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_cla3_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_clc3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_clc3_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_dcm3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_dcm3_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_dst3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_dst3_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_inc3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_inc3_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_ind3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_ind3_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_rcd3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_rcd3_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_roc3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_roc3_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_rod3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_rod3_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3_rsd3_ref_compnr), '0'), 38, 10) is null and 
                    src:typ3_rsd3_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4), '0'), 38, 10) is null and 
                    src:typ4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_blc4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_blc4_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_bld4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_bld4_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_cfa4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_cfa4_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_cfc4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_cfc4_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_cla4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_cla4_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_clc4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_clc4_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_dcm4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_dcm4_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_dst4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_dst4_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_inc4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_inc4_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_ind4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_ind4_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_rcd4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_rcd4_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_roc4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_roc4_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_rod4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_rod4_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4_rsd4_ref_compnr), '0'), 38, 10) is null and 
                    src:typ4_rsd4_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5), '0'), 38, 10) is null and 
                    src:typ5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_blc5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_blc5_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_bld5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_bld5_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_cfa5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_cfa5_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_cfc5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_cfc5_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_cla5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_cla5_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_clc5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_clc5_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_dcm5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_dcm5_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_dst5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_dst5_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_inc5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_inc5_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_ind5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_ind5_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_rcd5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_rcd5_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_roc5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_roc5_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_rod5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_rod5_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5_rsd5_ref_compnr), '0'), 38, 10) is null and 
                    src:typ5_rsd5_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6), '0'), 38, 10) is null and 
                    src:typ6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_blc6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_blc6_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_bld6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_bld6_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_cfa6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_cfa6_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_cfc6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_cfc6_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_cla6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_cla6_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_clc6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_clc6_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_dcm6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_dcm6_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_dst6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_dst6_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_inc6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_inc6_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_ind6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_ind6_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_rcd6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_rcd6_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_roc6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_roc6_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_rod6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_rod6_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ6_rsd6_ref_compnr), '0'), 38, 10) is null and 
                    src:typ6_rsd6_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7), '0'), 38, 10) is null and 
                    src:typ7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_blc7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_blc7_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_bld7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_bld7_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_cfa7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_cfa7_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_cfc7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_cfc7_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_cla7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_cla7_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_clc7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_clc7_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_dcm7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_dcm7_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_dst7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_dst7_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_inc7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_inc7_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_ind7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_ind7_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_rcd7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_rcd7_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_roc7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_roc7_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_rod7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_rod7_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ7_rsd7_ref_compnr), '0'), 38, 10) is null and 
                    src:typ7_rsd7_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8), '0'), 38, 10) is null and 
                    src:typ8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_blc8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_blc8_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_bld8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_bld8_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_cfa8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_cfa8_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_cfc8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_cfc8_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_cla8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_cla8_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_clc8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_clc8_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_dcm8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_dcm8_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_dst8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_dst8_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_inc8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_inc8_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_ind8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_ind8_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_rcd8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_rcd8_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_roc8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_roc8_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_rod8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_rod8_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ8_rsd8_ref_compnr), '0'), 38, 10) is null and 
                    src:typ8_rsd8_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9), '0'), 38, 10) is null and 
                    src:typ9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_blc9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_blc9_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_bld9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_bld9_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_cfa9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_cfa9_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_cfc9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_cfc9_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_cla9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_cla9_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_clc9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_clc9_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_dcm9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_dcm9_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_dst9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_dst9_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_inc9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_inc9_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_ind9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_ind9_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_rcd9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_rcd9_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_roc9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_roc9_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_rod9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_rod9_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ9_rsd9_ref_compnr), '0'), 38, 10) is null and 
                    src:typ9_rsd9_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usac), '0'), 38, 10) is null and 
                    src:usac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vabk), '0'), 38, 10) is null and 
                    src:vabk is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year_ref_compnr), '0'), 38, 10) is null and 
                    src:year_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)