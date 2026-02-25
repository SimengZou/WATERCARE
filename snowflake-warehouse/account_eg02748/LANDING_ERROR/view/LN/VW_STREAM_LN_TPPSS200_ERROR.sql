CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPSS200_ERROR AS SELECT src, 'LN_TPPSS200' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmn_ref_compnr), '0'), 38, 10) is null and 
                    src:acmn_ref_compnr is not null) THEN
                    'acmn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:acmn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:actp), '0'), 38, 10) is null and 
                    src:actp is not null) THEN
                    'actp contains a non-numeric value : \'' || AS_VARCHAR(src:actp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:addr_ref_compnr), '0'), 38, 10) is null and 
                    src:addr_ref_compnr is not null) THEN
                    'addr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:addr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:afdt), '1900-01-01')) is null and 
                    src:afdt is not null) THEN
                    'afdt contains a non-timestamp value : \'' || AS_VARCHAR(src:afdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afrt), '0'), 38, 10) is null and 
                    src:afrt is not null) THEN
                    'afrt contains a non-numeric value : \'' || AS_VARCHAR(src:afrt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:asdt), '1900-01-01')) is null and 
                    src:asdt is not null) THEN
                    'asdt contains a non-timestamp value : \'' || AS_VARCHAR(src:asdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:avtp_ref_compnr), '0'), 38, 10) is null and 
                    src:avtp_ref_compnr is not null) THEN
                    'avtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:avtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) THEN
                    'blbl contains a non-numeric value : \'' || AS_VARCHAR(src:blbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capa), '0'), 38, 10) is null and 
                    src:capa is not null) THEN
                    'capa contains a non-numeric value : \'' || AS_VARCHAR(src:capa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capt), '0'), 38, 10) is null and 
                    src:capt is not null) THEN
                    'capt contains a non-numeric value : \'' || AS_VARCHAR(src:capt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccat_ref_compnr is not null) THEN
                    'ccat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctr), '0'), 38, 10) is null and 
                    src:cctr is not null) THEN
                    'cctr contains a non-numeric value : \'' || AS_VARCHAR(src:cctr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_awip), '0'), 38, 10) is null and 
                    src:cdf_awip is not null) THEN
                    'cdf_awip contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_awip) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_camt), '0'), 38, 10) is null and 
                    src:cdf_camt is not null) THEN
                    'cdf_camt contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_camt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_depm), '0'), 38, 10) is null and 
                    src:cdf_depm is not null) THEN
                    'cdf_depm contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_depm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_uamt), '0'), 38, 10) is null and 
                    src:cdf_uamt is not null) THEN
                    'cdf_uamt contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_uamt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cnsd), '1900-01-01')) is null and 
                    src:cnsd is not null) THEN
                    'cnsd contains a non-timestamp value : \'' || AS_VARCHAR(src:cnsd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnst), '0'), 38, 10) is null and 
                    src:cnst is not null) THEN
                    'cnst contains a non-numeric value : \'' || AS_VARCHAR(src:cnst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coel_rcmp), '0'), 38, 10) is null and 
                    src:coel_rcmp is not null) THEN
                    'coel_rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:coel_rcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cloc_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cloc_ref_compnr is not null) THEN
                    'cprj_cloc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cloc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_lact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_lact_ref_compnr is not null) THEN
                    'cprj_cpla_lact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_lact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_pact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_pact_ref_compnr is not null) THEN
                    'cprj_cpla_pact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_pact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) THEN
                    'cprj_cpla_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) THEN
                    'cprj_cstl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cstl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) THEN
                    'creg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:creg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is null and 
                    src:csec_ref_compnr is not null) THEN
                    'csec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspt), '0'), 38, 10) is null and 
                    src:cspt is not null) THEN
                    'cspt contains a non-numeric value : \'' || AS_VARCHAR(src:cspt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstg_ref_compnr), '0'), 38, 10) is null and 
                    src:cstg_ref_compnr is not null) THEN
                    'cstg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cstg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) THEN
                    'cstv contains a non-numeric value : \'' || AS_VARCHAR(src:cstv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is null and 
                    src:cuti_ref_compnr is not null) THEN
                    'cuti_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuti_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) THEN
                    'cuvc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuvc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddln), '1900-01-01')) is null and 
                    src:ddln is not null) THEN
                    'ddln contains a non-timestamp value : \'' || AS_VARCHAR(src:ddln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfac_ref_compnr), '0'), 38, 10) is null and 
                    src:dfac_ref_compnr is not null) THEN
                    'dfac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dfac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dura), '0'), 38, 10) is null and 
                    src:dura is not null) THEN
                    'dura contains a non-numeric value : \'' || AS_VARCHAR(src:dura) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dwar_ref_compnr), '0'), 38, 10) is null and 
                    src:dwar_ref_compnr is not null) THEN
                    'dwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) THEN
                    'efdt contains a non-timestamp value : \'' || AS_VARCHAR(src:efdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eoty), '0'), 38, 10) is null and 
                    src:eoty is not null) THEN
                    'eoty contains a non-numeric value : \'' || AS_VARCHAR(src:eoty) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:esdt), '1900-01-01')) is null and 
                    src:esdt is not null) THEN
                    'esdt contains a non-timestamp value : \'' || AS_VARCHAR(src:esdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ffhr), '0'), 38, 10) is null and 
                    src:ffhr is not null) THEN
                    'ffhr contains a non-numeric value : \'' || AS_VARCHAR(src:ffhr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fref), '0'), 38, 10) is null and 
                    src:fref is not null) THEN
                    'fref contains a non-numeric value : \'' || AS_VARCHAR(src:fref) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:freq), '0'), 38, 10) is null and 
                    src:freq is not null) THEN
                    'freq contains a non-numeric value : \'' || AS_VARCHAR(src:freq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:glat), '0'), 38, 10) is null and 
                    src:glat is not null) THEN
                    'glat contains a non-numeric value : \'' || AS_VARCHAR(src:glat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:glon), '0'), 38, 10) is null and 
                    src:glon is not null) THEN
                    'glon contains a non-numeric value : \'' || AS_VARCHAR(src:glon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gris), '0'), 38, 10) is null and 
                    src:gris is not null) THEN
                    'gris contains a non-numeric value : \'' || AS_VARCHAR(src:gris) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbyn), '0'), 38, 10) is null and 
                    src:hbyn is not null) THEN
                    'hbyn contains a non-numeric value : \'' || AS_VARCHAR(src:hbyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:levl), '0'), 38, 10) is null and 
                    src:levl is not null) THEN
                    'levl contains a non-numeric value : \'' || AS_VARCHAR(src:levl) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lfdt), '1900-01-01')) is null and 
                    src:lfdt is not null) THEN
                    'lfdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lfdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:litm_ref_compnr), '0'), 38, 10) is null and 
                    src:litm_ref_compnr is not null) THEN
                    'litm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:litm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:llnr), '0'), 38, 10) is null and 
                    src:llnr is not null) THEN
                    'llnr contains a non-numeric value : \'' || AS_VARCHAR(src:llnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lsdt), '1900-01-01')) is null and 
                    src:lsdt is not null) THEN
                    'lsdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lsdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lvps), '0'), 38, 10) is null and 
                    src:lvps is not null) THEN
                    'lvps contains a non-numeric value : \'' || AS_VARCHAR(src:lvps) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mamt), '0'), 38, 10) is null and 
                    src:mamt is not null) THEN
                    'mamt contains a non-numeric value : \'' || AS_VARCHAR(src:mamt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mevl), '0'), 38, 10) is null and 
                    src:mevl is not null) THEN
                    'mevl contains a non-numeric value : \'' || AS_VARCHAR(src:mevl) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ohdt), '1900-01-01')) is null and 
                    src:ohdt is not null) THEN
                    'ohdt contains a non-timestamp value : \'' || AS_VARCHAR(src:ohdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcom), '0'), 38, 10) is null and 
                    src:pcom is not null) THEN
                    'pcom contains a non-numeric value : \'' || AS_VARCHAR(src:pcom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plmc), '0'), 38, 10) is null and 
                    src:plmc is not null) THEN
                    'plmc contains a non-numeric value : \'' || AS_VARCHAR(src:plmc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prin), '0'), 38, 10) is null and 
                    src:prin is not null) THEN
                    'prin contains a non-numeric value : \'' || AS_VARCHAR(src:prin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) THEN
                    'pris contains a non-numeric value : \'' || AS_VARCHAR(src:pris) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prms), '0'), 38, 10) is null and 
                    src:prms is not null) THEN
                    'prms contains a non-numeric value : \'' || AS_VARCHAR(src:prms) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prnd), '0'), 38, 10) is null and 
                    src:prnd is not null) THEN
                    'prnd contains a non-numeric value : \'' || AS_VARCHAR(src:prnd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsp), '0'), 38, 10) is null and 
                    src:prsp is not null) THEN
                    'prsp contains a non-numeric value : \'' || AS_VARCHAR(src:prsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prss), '0'), 38, 10) is null and 
                    src:prss is not null) THEN
                    'prss contains a non-numeric value : \'' || AS_VARCHAR(src:prss) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prst), '0'), 38, 10) is null and 
                    src:prst is not null) THEN
                    'prst contains a non-numeric value : \'' || AS_VARCHAR(src:prst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pwar_ref_compnr), '0'), 38, 10) is null and 
                    src:pwar_ref_compnr is not null) THEN
                    'pwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qans), '0'), 38, 10) is null and 
                    src:qans is not null) THEN
                    'qans contains a non-numeric value : \'' || AS_VARCHAR(src:qans) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan), '0'), 38, 10) is null and 
                    src:quan is not null) THEN
                    'quan contains a non-numeric value : \'' || AS_VARCHAR(src:quan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qutm), '0'), 38, 10) is null and 
                    src:qutm is not null) THEN
                    'qutm contains a non-numeric value : \'' || AS_VARCHAR(src:qutm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) THEN
                    'rcod_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcod_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdas), '1900-01-01')) is null and 
                    src:rdas is not null) THEN
                    'rdas contains a non-timestamp value : \'' || AS_VARCHAR(src:rdas) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdse), '1900-01-01')) is null and 
                    src:rdse is not null) THEN
                    'rdse contains a non-timestamp value : \'' || AS_VARCHAR(src:rdse) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rent), '0'), 38, 10) is null and 
                    src:rent is not null) THEN
                    'rent contains a non-numeric value : \'' || AS_VARCHAR(src:rent) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfsa_1), '0'), 38, 10) is null and 
                    src:rfsa_1 is not null) THEN
                    'rfsa_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rfsa_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfsa_2), '0'), 38, 10) is null and 
                    src:rfsa_2 is not null) THEN
                    'rfsa_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rfsa_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfsa_3), '0'), 38, 10) is null and 
                    src:rfsa_3 is not null) THEN
                    'rfsa_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rfsa_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfse_1), '0'), 38, 10) is null and 
                    src:rfse_1 is not null) THEN
                    'rfse_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rfse_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfse_2), '0'), 38, 10) is null and 
                    src:rfse_2 is not null) THEN
                    'rfse_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rfse_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfse_3), '0'), 38, 10) is null and 
                    src:rfse_3 is not null) THEN
                    'rfse_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rfse_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_1), '0'), 38, 10) is null and 
                    src:rtsa_1 is not null) THEN
                    'rtsa_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtsa_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_2), '0'), 38, 10) is null and 
                    src:rtsa_2 is not null) THEN
                    'rtsa_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtsa_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_3), '0'), 38, 10) is null and 
                    src:rtsa_3 is not null) THEN
                    'rtsa_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtsa_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtse_1), '0'), 38, 10) is null and 
                    src:rtse_1 is not null) THEN
                    'rtse_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtse_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtse_2), '0'), 38, 10) is null and 
                    src:rtse_2 is not null) THEN
                    'rtse_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtse_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtse_3), '0'), 38, 10) is null and 
                    src:rtse_3 is not null) THEN
                    'rtse_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtse_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sacu_ref_compnr), '0'), 38, 10) is null and 
                    src:sacu_ref_compnr is not null) THEN
                    'sacu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sacu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scmd), '0'), 38, 10) is null and 
                    src:scmd is not null) THEN
                    'scmd contains a non-numeric value : \'' || AS_VARCHAR(src:scmd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdch), '0'), 38, 10) is null and 
                    src:sdch is not null) THEN
                    'sdch contains a non-numeric value : \'' || AS_VARCHAR(src:sdch) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:secu_ref_compnr), '0'), 38, 10) is null and 
                    src:secu_ref_compnr is not null) THEN
                    'secu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:secu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:setl), '0'), 38, 10) is null and 
                    src:setl is not null) THEN
                    'setl contains a non-numeric value : \'' || AS_VARCHAR(src:setl) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sfdt), '1900-01-01')) is null and 
                    src:sfdt is not null) THEN
                    'sfdt contains a non-timestamp value : \'' || AS_VARCHAR(src:sfdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ssdt), '1900-01-01')) is null and 
                    src:ssdt is not null) THEN
                    'ssdt contains a non-timestamp value : \'' || AS_VARCHAR(src:ssdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tact), '0'), 38, 10) is null and 
                    src:tact is not null) THEN
                    'tact contains a non-numeric value : \'' || AS_VARCHAR(src:tact) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tfhr), '0'), 38, 10) is null and 
                    src:tfhr is not null) THEN
                    'tfhr contains a non-numeric value : \'' || AS_VARCHAR(src:tfhr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmud_ref_compnr), '0'), 38, 10) is null and 
                    src:tmud_ref_compnr is not null) THEN
                    'tmud_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tmud_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:totf), '0'), 38, 10) is null and 
                    src:totf is not null) THEN
                    'totf contains a non-numeric value : \'' || AS_VARCHAR(src:totf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wast), '0'), 38, 10) is null and 
                    src:wast is not null) THEN
                    'wast contains a non-numeric value : \'' || AS_VARCHAR(src:wast) || '\'' WHEN 
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
                src:cprj,
                src:cpla,
                src:cact  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPSS200 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmn_ref_compnr), '0'), 38, 10) is null and 
                    src:acmn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:actp), '0'), 38, 10) is null and 
                    src:actp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:addr_ref_compnr), '0'), 38, 10) is null and 
                    src:addr_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:afdt), '1900-01-01')) is null and 
                    src:afdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afrt), '0'), 38, 10) is null and 
                    src:afrt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:asdt), '1900-01-01')) is null and 
                    src:asdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:avtp_ref_compnr), '0'), 38, 10) is null and 
                    src:avtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capa), '0'), 38, 10) is null and 
                    src:capa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capt), '0'), 38, 10) is null and 
                    src:capt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctr), '0'), 38, 10) is null and 
                    src:cctr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_awip), '0'), 38, 10) is null and 
                    src:cdf_awip is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_camt), '0'), 38, 10) is null and 
                    src:cdf_camt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_depm), '0'), 38, 10) is null and 
                    src:cdf_depm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_uamt), '0'), 38, 10) is null and 
                    src:cdf_uamt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cnsd), '1900-01-01')) is null and 
                    src:cnsd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnst), '0'), 38, 10) is null and 
                    src:cnst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coel_rcmp), '0'), 38, 10) is null and 
                    src:coel_rcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cloc_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cloc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_lact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_lact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_pact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_pact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is null and 
                    src:csec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspt), '0'), 38, 10) is null and 
                    src:cspt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstg_ref_compnr), '0'), 38, 10) is null and 
                    src:cstg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is null and 
                    src:cuti_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddln), '1900-01-01')) is null and 
                    src:ddln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfac_ref_compnr), '0'), 38, 10) is null and 
                    src:dfac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dura), '0'), 38, 10) is null and 
                    src:dura is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dwar_ref_compnr), '0'), 38, 10) is null and 
                    src:dwar_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eoty), '0'), 38, 10) is null and 
                    src:eoty is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:esdt), '1900-01-01')) is null and 
                    src:esdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ffhr), '0'), 38, 10) is null and 
                    src:ffhr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fref), '0'), 38, 10) is null and 
                    src:fref is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:freq), '0'), 38, 10) is null and 
                    src:freq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:glat), '0'), 38, 10) is null and 
                    src:glat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:glon), '0'), 38, 10) is null and 
                    src:glon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gris), '0'), 38, 10) is null and 
                    src:gris is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbyn), '0'), 38, 10) is null and 
                    src:hbyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:levl), '0'), 38, 10) is null and 
                    src:levl is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lfdt), '1900-01-01')) is null and 
                    src:lfdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:litm_ref_compnr), '0'), 38, 10) is null and 
                    src:litm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:llnr), '0'), 38, 10) is null and 
                    src:llnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lsdt), '1900-01-01')) is null and 
                    src:lsdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lvps), '0'), 38, 10) is null and 
                    src:lvps is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mamt), '0'), 38, 10) is null and 
                    src:mamt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mevl), '0'), 38, 10) is null and 
                    src:mevl is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ohdt), '1900-01-01')) is null and 
                    src:ohdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcom), '0'), 38, 10) is null and 
                    src:pcom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plmc), '0'), 38, 10) is null and 
                    src:plmc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prin), '0'), 38, 10) is null and 
                    src:prin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prms), '0'), 38, 10) is null and 
                    src:prms is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prnd), '0'), 38, 10) is null and 
                    src:prnd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsp), '0'), 38, 10) is null and 
                    src:prsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prss), '0'), 38, 10) is null and 
                    src:prss is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prst), '0'), 38, 10) is null and 
                    src:prst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pwar_ref_compnr), '0'), 38, 10) is null and 
                    src:pwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qans), '0'), 38, 10) is null and 
                    src:qans is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan), '0'), 38, 10) is null and 
                    src:quan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qutm), '0'), 38, 10) is null and 
                    src:qutm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdas), '1900-01-01')) is null and 
                    src:rdas is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdse), '1900-01-01')) is null and 
                    src:rdse is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rent), '0'), 38, 10) is null and 
                    src:rent is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfsa_1), '0'), 38, 10) is null and 
                    src:rfsa_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfsa_2), '0'), 38, 10) is null and 
                    src:rfsa_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfsa_3), '0'), 38, 10) is null and 
                    src:rfsa_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfse_1), '0'), 38, 10) is null and 
                    src:rfse_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfse_2), '0'), 38, 10) is null and 
                    src:rfse_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfse_3), '0'), 38, 10) is null and 
                    src:rfse_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_1), '0'), 38, 10) is null and 
                    src:rtsa_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_2), '0'), 38, 10) is null and 
                    src:rtsa_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_3), '0'), 38, 10) is null and 
                    src:rtsa_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtse_1), '0'), 38, 10) is null and 
                    src:rtse_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtse_2), '0'), 38, 10) is null and 
                    src:rtse_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtse_3), '0'), 38, 10) is null and 
                    src:rtse_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sacu_ref_compnr), '0'), 38, 10) is null and 
                    src:sacu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scmd), '0'), 38, 10) is null and 
                    src:scmd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdch), '0'), 38, 10) is null and 
                    src:sdch is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:secu_ref_compnr), '0'), 38, 10) is null and 
                    src:secu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:setl), '0'), 38, 10) is null and 
                    src:setl is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sfdt), '1900-01-01')) is null and 
                    src:sfdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ssdt), '1900-01-01')) is null and 
                    src:ssdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tact), '0'), 38, 10) is null and 
                    src:tact is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tfhr), '0'), 38, 10) is null and 
                    src:tfhr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmud_ref_compnr), '0'), 38, 10) is null and 
                    src:tmud_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:totf), '0'), 38, 10) is null and 
                    src:totf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wast), '0'), 38, 10) is null and 
                    src:wast is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)