CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSSOC210_ERROR AS SELECT src, 'LN_TSSOC210' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:acdt), '1900-01-01')) is null and 
                    src:acdt is not null) THEN
                    'acdt contains a non-timestamp value : \'' || AS_VARCHAR(src:acdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acdu), '0'), 38, 10) is null and 
                    src:acdu is not null) THEN
                    'acdu contains a non-numeric value : \'' || AS_VARCHAR(src:acdu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acln), '0'), 38, 10) is null and 
                    src:acln is not null) THEN
                    'acln contains a non-numeric value : \'' || AS_VARCHAR(src:acln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:actg_ref_compnr), '0'), 38, 10) is null and 
                    src:actg_ref_compnr is not null) THEN
                    'actg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:actg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adfa), '0'), 38, 10) is null and 
                    src:adfa is not null) THEN
                    'adfa contains a non-numeric value : \'' || AS_VARCHAR(src:adfa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adtm), '0'), 38, 10) is null and 
                    src:adtm is not null) THEN
                    'adtm contains a non-numeric value : \'' || AS_VARCHAR(src:adtm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:advl), '0'), 38, 10) is null and 
                    src:advl is not null) THEN
                    'advl contains a non-numeric value : \'' || AS_VARCHAR(src:advl) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:aftm), '1900-01-01')) is null and 
                    src:aftm is not null) THEN
                    'aftm contains a non-timestamp value : \'' || AS_VARCHAR(src:aftm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ains), '0'), 38, 10) is null and 
                    src:ains is not null) THEN
                    'ains contains a non-numeric value : \'' || AS_VARCHAR(src:ains) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aivl), '0'), 38, 10) is null and 
                    src:aivl is not null) THEN
                    'aivl contains a non-numeric value : \'' || AS_VARCHAR(src:aivl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:altn), '0'), 38, 10) is null and 
                    src:altn is not null) THEN
                    'altn contains a non-numeric value : \'' || AS_VARCHAR(src:altn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod), '0'), 38, 10) is null and 
                    src:amod is not null) THEN
                    'amod contains a non-numeric value : \'' || AS_VARCHAR(src:amod) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appo), '0'), 38, 10) is null and 
                    src:appo is not null) THEN
                    'appo contains a non-numeric value : \'' || AS_VARCHAR(src:appo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asta), '0'), 38, 10) is null and 
                    src:asta is not null) THEN
                    'asta contains a non-numeric value : \'' || AS_VARCHAR(src:asta) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:astm), '1900-01-01')) is null and 
                    src:astm is not null) THEN
                    'astm contains a non-timestamp value : \'' || AS_VARCHAR(src:astm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atbl), '0'), 38, 10) is null and 
                    src:atbl is not null) THEN
                    'atbl contains a non-numeric value : \'' || AS_VARCHAR(src:atbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atpc), '0'), 38, 10) is null and 
                    src:atpc is not null) THEN
                    'atpc contains a non-numeric value : \'' || AS_VARCHAR(src:atpc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:atpd), '1900-01-01')) is null and 
                    src:atpd is not null) THEN
                    'atpd contains a non-timestamp value : \'' || AS_VARCHAR(src:atpd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:bfbp_ref_compnr is not null) THEN
                    'bfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bsch_ref_compnr), '0'), 38, 10) is null and 
                    src:bsch_ref_compnr is not null) THEN
                    'bsch_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bsch_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cadt), '1900-01-01')) is null and 
                    src:cadt is not null) THEN
                    'cadt contains a non-timestamp value : \'' || AS_VARCHAR(src:cadt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr_ref_compnr), '0'), 38, 10) is null and 
                    src:camr_ref_compnr is not null) THEN
                    'camr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:camr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccar_ref_compnr), '0'), 38, 10) is null and 
                    src:ccar_ref_compnr is not null) THEN
                    'ccar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cchl), '0'), 38, 10) is null and 
                    src:cchl is not null) THEN
                    'cchl contains a non-numeric value : \'' || AS_VARCHAR(src:cchl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cchl_ref_compnr), '0'), 38, 10) is null and 
                    src:cchl_ref_compnr is not null) THEN
                    'cchl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cchl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccll_ref_compnr), '0'), 38, 10) is null and 
                    src:ccll_ref_compnr is not null) THEN
                    'ccll_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccll_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctp_ref_compnr), '0'), 38, 10) is null and 
                    src:cctp_ref_compnr is not null) THEN
                    'cctp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cctp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdis_ref_compnr), '0'), 38, 10) is null and 
                    src:cdis_ref_compnr is not null) THEN
                    'cdis_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdis_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clst_ref_compnr), '0'), 38, 10) is null and 
                    src:clst_ref_compnr is not null) THEN
                    'clst_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clst_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmea_ref_compnr), '0'), 38, 10) is null and 
                    src:cmea_ref_compnr is not null) THEN
                    'cmea_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmea_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:come_ref_compnr), '0'), 38, 10) is null and 
                    src:come_ref_compnr is not null) THEN
                    'come_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:come_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cova), '0'), 38, 10) is null and 
                    src:cova is not null) THEN
                    'cova contains a non-numeric value : \'' || AS_VARCHAR(src:cova) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpor), '0'), 38, 10) is null and 
                    src:cpor is not null) THEN
                    'cpor contains a non-numeric value : \'' || AS_VARCHAR(src:cpor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprl_ref_compnr is not null) THEN
                    'cprl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crac_ref_compnr), '0'), 38, 10) is null and 
                    src:crac_ref_compnr is not null) THEN
                    'crac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crpu), '0'), 38, 10) is null and 
                    src:crpu is not null) THEN
                    'crpu contains a non-numeric value : \'' || AS_VARCHAR(src:crpu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtc_ref_compnr), '0'), 38, 10) is null and 
                    src:crtc_ref_compnr is not null) THEN
                    'crtc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crtc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csar_ref_compnr), '0'), 38, 10) is null and 
                    src:csar_ref_compnr is not null) THEN
                    'csar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) THEN
                    'csco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cskt_ref_compnr), '0'), 38, 10) is null and 
                    src:cskt_ref_compnr is not null) THEN
                    'cskt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cskt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstn_ref_compnr), '0'), 38, 10) is null and 
                    src:cstn_ref_compnr is not null) THEN
                    'cstn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cstn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstp_ref_compnr), '0'), 38, 10) is null and 
                    src:cstp_ref_compnr is not null) THEN
                    'cstp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cstp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupr), '0'), 38, 10) is null and 
                    src:cupr is not null) THEN
                    'cupr contains a non-numeric value : \'' || AS_VARCHAR(src:cupr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupr_ref_compnr), '0'), 38, 10) is null and 
                    src:cupr_ref_compnr is not null) THEN
                    'cupr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cupr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuxr_ref_compnr), '0'), 38, 10) is null and 
                    src:cuxr_ref_compnr is not null) THEN
                    'cuxr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuxr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cvtm), '1900-01-01')) is null and 
                    src:cvtm is not null) THEN
                    'cvtm contains a non-timestamp value : \'' || AS_VARCHAR(src:cvtm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbfc), '0'), 38, 10) is null and 
                    src:dbfc is not null) THEN
                    'dbfc contains a non-numeric value : \'' || AS_VARCHAR(src:dbfc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:docn_ref_compnr), '0'), 38, 10) is null and 
                    src:docn_ref_compnr is not null) THEN
                    'docn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:docn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eaod), '0'), 38, 10) is null and 
                    src:eaod is not null) THEN
                    'eaod contains a non-numeric value : \'' || AS_VARCHAR(src:eaod) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) THEN
                    'emno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:emno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:erdt), '1900-01-01')) is null and 
                    src:erdt is not null) THEN
                    'erdt contains a non-timestamp value : \'' || AS_VARCHAR(src:erdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:espr), '0'), 38, 10) is null and 
                    src:espr is not null) THEN
                    'espr contains a non-numeric value : \'' || AS_VARCHAR(src:espr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:estm), '1900-01-01')) is null and 
                    src:estm is not null) THEN
                    'estm contains a non-timestamp value : \'' || AS_VARCHAR(src:estm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcon_ref_compnr), '0'), 38, 10) is null and 
                    src:fcon_ref_compnr is not null) THEN
                    'fcon_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fcon_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inby), '0'), 38, 10) is null and 
                    src:inby is not null) THEN
                    'inby contains a non-numeric value : \'' || AS_VARCHAR(src:inby) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpl_ref_compnr), '0'), 38, 10) is null and 
                    src:inpl_ref_compnr is not null) THEN
                    'inpl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:inpl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_sern_ref_compnr), '0'), 38, 10) is null and 
                    src:item_sern_ref_compnr is not null) THEN
                    'item_sern_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_sern_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_xrsr_ref_compnr), '0'), 38, 10) is null and 
                    src:item_xrsr_ref_compnr is not null) THEN
                    'item_xrsr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_xrsr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcad_ref_compnr), '0'), 38, 10) is null and 
                    src:lcad_ref_compnr is not null) THEN
                    'lcad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lftm), '1900-01-01')) is null and 
                    src:lftm is not null) THEN
                    'lftm contains a non-timestamp value : \'' || AS_VARCHAR(src:lftm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mepo_ref_compnr), '0'), 38, 10) is null and 
                    src:mepo_ref_compnr is not null) THEN
                    'mepo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mepo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:micn), '0'), 38, 10) is null and 
                    src:micn is not null) THEN
                    'micn contains a non-numeric value : \'' || AS_VARCHAR(src:micn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mvla), '0'), 38, 10) is null and 
                    src:mvla is not null) THEN
                    'mvla contains a non-numeric value : \'' || AS_VARCHAR(src:mvla) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mvln), '0'), 38, 10) is null and 
                    src:mvln is not null) THEN
                    'mvln contains a non-numeric value : \'' || AS_VARCHAR(src:mvln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nivl), '0'), 38, 10) is null and 
                    src:nivl is not null) THEN
                    'nivl contains a non-numeric value : \'' || AS_VARCHAR(src:nivl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:noun), '0'), 38, 10) is null and 
                    src:noun is not null) THEN
                    'noun contains a non-numeric value : \'' || AS_VARCHAR(src:noun) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obpr), '0'), 38, 10) is null and 
                    src:obpr is not null) THEN
                    'obpr contains a non-numeric value : \'' || AS_VARCHAR(src:obpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obpr_ref_compnr), '0'), 38, 10) is null and 
                    src:obpr_ref_compnr is not null) THEN
                    'obpr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:obpr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:oftm), '1900-01-01')) is null and 
                    src:oftm is not null) THEN
                    'oftm contains a non-timestamp value : \'' || AS_VARCHAR(src:oftm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oras), '0'), 38, 10) is null and 
                    src:oras is not null) THEN
                    'oras contains a non-numeric value : \'' || AS_VARCHAR(src:oras) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_ref_compnr is not null) THEN
                    'orno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:orno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orps), '0'), 38, 10) is null and 
                    src:orps is not null) THEN
                    'orps contains a non-numeric value : \'' || AS_VARCHAR(src:orps) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ostm), '1900-01-01')) is null and 
                    src:ostm is not null) THEN
                    'ostm contains a non-timestamp value : \'' || AS_VARCHAR(src:ostm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbsm), '0'), 38, 10) is null and 
                    src:pbsm is not null) THEN
                    'pbsm contains a non-numeric value : \'' || AS_VARCHAR(src:pbsm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcch), '0'), 38, 10) is null and 
                    src:pcch is not null) THEN
                    'pcch contains a non-numeric value : \'' || AS_VARCHAR(src:pcch) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcln), '0'), 38, 10) is null and 
                    src:pcln is not null) THEN
                    'pcln contains a non-numeric value : \'' || AS_VARCHAR(src:pcln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcon_pcch_ref_compnr), '0'), 38, 10) is null and 
                    src:pcon_pcch_ref_compnr is not null) THEN
                    'pcon_pcch_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pcon_pcch_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcon_ref_compnr), '0'), 38, 10) is null and 
                    src:pcon_ref_compnr is not null) THEN
                    'pcon_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pcon_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdvl), '0'), 38, 10) is null and 
                    src:pdvl is not null) THEN
                    'pdvl contains a non-numeric value : \'' || AS_VARCHAR(src:pdvl) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pftm), '1900-01-01')) is null and 
                    src:pftm is not null) THEN
                    'pftm contains a non-timestamp value : \'' || AS_VARCHAR(src:pftm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmtd), '0'), 38, 10) is null and 
                    src:pmtd is not null) THEN
                    'pmtd contains a non-numeric value : \'' || AS_VARCHAR(src:pmtd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:porg), '0'), 38, 10) is null and 
                    src:porg is not null) THEN
                    'porg contains a non-numeric value : \'' || AS_VARCHAR(src:porg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prac), '0'), 38, 10) is null and 
                    src:prac is not null) THEN
                    'prac contains a non-numeric value : \'' || AS_VARCHAR(src:prac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) THEN
                    'pris contains a non-numeric value : \'' || AS_VARCHAR(src:pris) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prpr), '0'), 38, 10) is null and 
                    src:prpr is not null) THEN
                    'prpr contains a non-numeric value : \'' || AS_VARCHAR(src:prpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prpr_ref_compnr), '0'), 38, 10) is null and 
                    src:prpr_ref_compnr is not null) THEN
                    'prpr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prpr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pstm), '1900-01-01')) is null and 
                    src:pstm is not null) THEN
                    'pstm contains a non-timestamp value : \'' || AS_VARCHAR(src:pstm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ptft), '1900-01-01')) is null and 
                    src:ptft is not null) THEN
                    'ptft contains a non-timestamp value : \'' || AS_VARCHAR(src:ptft) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ptst), '1900-01-01')) is null and 
                    src:ptst is not null) THEN
                    'ptst contains a non-timestamp value : \'' || AS_VARCHAR(src:ptst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtnl), '0'), 38, 10) is null and 
                    src:qtnl is not null) THEN
                    'qtnl contains a non-numeric value : \'' || AS_VARCHAR(src:qtnl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtno_revn_qtnl_altn_ref_compnr), '0'), 38, 10) is null and 
                    src:qtno_revn_qtnl_altn_ref_compnr is not null) THEN
                    'qtno_revn_qtnl_altn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:qtno_revn_qtnl_altn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) THEN
                    'revn contains a non-numeric value : \'' || AS_VARCHAR(src:revn) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rltm), '1900-01-01')) is null and 
                    src:rltm is not null) THEN
                    'rltm contains a non-timestamp value : \'' || AS_VARCHAR(src:rltm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rnyn), '0'), 38, 10) is null and 
                    src:rnyn is not null) THEN
                    'rnyn contains a non-numeric value : \'' || AS_VARCHAR(src:rnyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is null and 
                    src:rown_ref_compnr is not null) THEN
                    'rown_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rown_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrqt), '0'), 38, 10) is null and 
                    src:rrqt is not null) THEN
                    'rrqt contains a non-numeric value : \'' || AS_VARCHAR(src:rrqt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbit_ref_compnr), '0'), 38, 10) is null and 
                    src:sbit_ref_compnr is not null) THEN
                    'sbit_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sbit_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfad_ref_compnr), '0'), 38, 10) is null and 
                    src:sfad_ref_compnr is not null) THEN
                    'sfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smto), '0'), 38, 10) is null and 
                    src:smto is not null) THEN
                    'smto contains a non-numeric value : \'' || AS_VARCHAR(src:smto) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssta), '0'), 38, 10) is null and 
                    src:ssta is not null) THEN
                    'ssta contains a non-numeric value : \'' || AS_VARCHAR(src:ssta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) THEN
                    'stad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) THEN
                    'stbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is null and 
                    src:stcn_ref_compnr is not null) THEN
                    'stcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) THEN
                    'subc contains a non-numeric value : \'' || AS_VARCHAR(src:subc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:supm), '0'), 38, 10) is null and 
                    src:supm is not null) THEN
                    'supm contains a non-numeric value : \'' || AS_VARCHAR(src:supm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmun), '0'), 38, 10) is null and 
                    src:tmun is not null) THEN
                    'tmun contains a non-numeric value : \'' || AS_VARCHAR(src:tmun) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdi), '0'), 38, 10) is null and 
                    src:trdi is not null) THEN
                    'trdi contains a non-numeric value : \'' || AS_VARCHAR(src:trdi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdi_buln), '0'), 38, 10) is null and 
                    src:trdi_buln is not null) THEN
                    'trdi_buln contains a non-numeric value : \'' || AS_VARCHAR(src:trdi_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdu), '0'), 38, 10) is null and 
                    src:trdu is not null) THEN
                    'trdu contains a non-numeric value : \'' || AS_VARCHAR(src:trdu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) THEN
                    'txtb contains a non-numeric value : \'' || AS_VARCHAR(src:txtb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) THEN
                    'txtb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtc), '0'), 38, 10) is null and 
                    src:txtc is not null) THEN
                    'txtc contains a non-numeric value : \'' || AS_VARCHAR(src:txtc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtc_ref_compnr), '0'), 38, 10) is null and 
                    src:txtc_ref_compnr is not null) THEN
                    'txtc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtd), '0'), 38, 10) is null and 
                    src:txtd is not null) THEN
                    'txtd contains a non-numeric value : \'' || AS_VARCHAR(src:txtd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtd_ref_compnr), '0'), 38, 10) is null and 
                    src:txtd_ref_compnr is not null) THEN
                    'txtd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vtbo), '0'), 38, 10) is null and 
                    src:vtbo is not null) THEN
                    'vtbo contains a non-numeric value : \'' || AS_VARCHAR(src:vtbo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wrtp), '0'), 38, 10) is null and 
                    src:wrtp is not null) THEN
                    'wrtp contains a non-numeric value : \'' || AS_VARCHAR(src:wrtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xrcu_ref_compnr), '0'), 38, 10) is null and 
                    src:xrcu_ref_compnr is not null) THEN
                    'xrcu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:xrcu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xrnh), '0'), 38, 10) is null and 
                    src:xrnh is not null) THEN
                    'xrnh contains a non-numeric value : \'' || AS_VARCHAR(src:xrnh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xrrt), '0'), 38, 10) is null and 
                    src:xrrt is not null) THEN
                    'xrrt contains a non-numeric value : \'' || AS_VARCHAR(src:xrrt) || '\'' WHEN 
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
                                    
                src:acln,
                src:compnr,
                src:orno  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSSOC210 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:acdt), '1900-01-01')) is null and 
                    src:acdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acdu), '0'), 38, 10) is null and 
                    src:acdu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acln), '0'), 38, 10) is null and 
                    src:acln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:actg_ref_compnr), '0'), 38, 10) is null and 
                    src:actg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adfa), '0'), 38, 10) is null and 
                    src:adfa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adtm), '0'), 38, 10) is null and 
                    src:adtm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:advl), '0'), 38, 10) is null and 
                    src:advl is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:aftm), '1900-01-01')) is null and 
                    src:aftm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ains), '0'), 38, 10) is null and 
                    src:ains is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aivl), '0'), 38, 10) is null and 
                    src:aivl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:altn), '0'), 38, 10) is null and 
                    src:altn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod), '0'), 38, 10) is null and 
                    src:amod is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appo), '0'), 38, 10) is null and 
                    src:appo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asta), '0'), 38, 10) is null and 
                    src:asta is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:astm), '1900-01-01')) is null and 
                    src:astm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atbl), '0'), 38, 10) is null and 
                    src:atbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atpc), '0'), 38, 10) is null and 
                    src:atpc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:atpd), '1900-01-01')) is null and 
                    src:atpd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:bfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bsch_ref_compnr), '0'), 38, 10) is null and 
                    src:bsch_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cadt), '1900-01-01')) is null and 
                    src:cadt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr_ref_compnr), '0'), 38, 10) is null and 
                    src:camr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccar_ref_compnr), '0'), 38, 10) is null and 
                    src:ccar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cchl), '0'), 38, 10) is null and 
                    src:cchl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cchl_ref_compnr), '0'), 38, 10) is null and 
                    src:cchl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccll_ref_compnr), '0'), 38, 10) is null and 
                    src:ccll_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctp_ref_compnr), '0'), 38, 10) is null and 
                    src:cctp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdis_ref_compnr), '0'), 38, 10) is null and 
                    src:cdis_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clst_ref_compnr), '0'), 38, 10) is null and 
                    src:clst_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmea_ref_compnr), '0'), 38, 10) is null and 
                    src:cmea_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:come_ref_compnr), '0'), 38, 10) is null and 
                    src:come_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cova), '0'), 38, 10) is null and 
                    src:cova is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpor), '0'), 38, 10) is null and 
                    src:cpor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crac_ref_compnr), '0'), 38, 10) is null and 
                    src:crac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crpu), '0'), 38, 10) is null and 
                    src:crpu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtc_ref_compnr), '0'), 38, 10) is null and 
                    src:crtc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csar_ref_compnr), '0'), 38, 10) is null and 
                    src:csar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cskt_ref_compnr), '0'), 38, 10) is null and 
                    src:cskt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstn_ref_compnr), '0'), 38, 10) is null and 
                    src:cstn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstp_ref_compnr), '0'), 38, 10) is null and 
                    src:cstp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupr), '0'), 38, 10) is null and 
                    src:cupr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupr_ref_compnr), '0'), 38, 10) is null and 
                    src:cupr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuxr_ref_compnr), '0'), 38, 10) is null and 
                    src:cuxr_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cvtm), '1900-01-01')) is null and 
                    src:cvtm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbfc), '0'), 38, 10) is null and 
                    src:dbfc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:docn_ref_compnr), '0'), 38, 10) is null and 
                    src:docn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eaod), '0'), 38, 10) is null and 
                    src:eaod is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:erdt), '1900-01-01')) is null and 
                    src:erdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:espr), '0'), 38, 10) is null and 
                    src:espr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:estm), '1900-01-01')) is null and 
                    src:estm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcon_ref_compnr), '0'), 38, 10) is null and 
                    src:fcon_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inby), '0'), 38, 10) is null and 
                    src:inby is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpl_ref_compnr), '0'), 38, 10) is null and 
                    src:inpl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_sern_ref_compnr), '0'), 38, 10) is null and 
                    src:item_sern_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_xrsr_ref_compnr), '0'), 38, 10) is null and 
                    src:item_xrsr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcad_ref_compnr), '0'), 38, 10) is null and 
                    src:lcad_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lftm), '1900-01-01')) is null and 
                    src:lftm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mepo_ref_compnr), '0'), 38, 10) is null and 
                    src:mepo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:micn), '0'), 38, 10) is null and 
                    src:micn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mvla), '0'), 38, 10) is null and 
                    src:mvla is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mvln), '0'), 38, 10) is null and 
                    src:mvln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nivl), '0'), 38, 10) is null and 
                    src:nivl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:noun), '0'), 38, 10) is null and 
                    src:noun is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obpr), '0'), 38, 10) is null and 
                    src:obpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obpr_ref_compnr), '0'), 38, 10) is null and 
                    src:obpr_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:oftm), '1900-01-01')) is null and 
                    src:oftm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oras), '0'), 38, 10) is null and 
                    src:oras is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orps), '0'), 38, 10) is null and 
                    src:orps is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ostm), '1900-01-01')) is null and 
                    src:ostm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbsm), '0'), 38, 10) is null and 
                    src:pbsm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcch), '0'), 38, 10) is null and 
                    src:pcch is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcln), '0'), 38, 10) is null and 
                    src:pcln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcon_pcch_ref_compnr), '0'), 38, 10) is null and 
                    src:pcon_pcch_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcon_ref_compnr), '0'), 38, 10) is null and 
                    src:pcon_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdvl), '0'), 38, 10) is null and 
                    src:pdvl is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pftm), '1900-01-01')) is null and 
                    src:pftm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmtd), '0'), 38, 10) is null and 
                    src:pmtd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:porg), '0'), 38, 10) is null and 
                    src:porg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prac), '0'), 38, 10) is null and 
                    src:prac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prpr), '0'), 38, 10) is null and 
                    src:prpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prpr_ref_compnr), '0'), 38, 10) is null and 
                    src:prpr_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pstm), '1900-01-01')) is null and 
                    src:pstm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ptft), '1900-01-01')) is null and 
                    src:ptft is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ptst), '1900-01-01')) is null and 
                    src:ptst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtnl), '0'), 38, 10) is null and 
                    src:qtnl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtno_revn_qtnl_altn_ref_compnr), '0'), 38, 10) is null and 
                    src:qtno_revn_qtnl_altn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rltm), '1900-01-01')) is null and 
                    src:rltm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rnyn), '0'), 38, 10) is null and 
                    src:rnyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is null and 
                    src:rown_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrqt), '0'), 38, 10) is null and 
                    src:rrqt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbit_ref_compnr), '0'), 38, 10) is null and 
                    src:sbit_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfad_ref_compnr), '0'), 38, 10) is null and 
                    src:sfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smto), '0'), 38, 10) is null and 
                    src:smto is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssta), '0'), 38, 10) is null and 
                    src:ssta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is null and 
                    src:stcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:supm), '0'), 38, 10) is null and 
                    src:supm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmun), '0'), 38, 10) is null and 
                    src:tmun is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdi), '0'), 38, 10) is null and 
                    src:trdi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdi_buln), '0'), 38, 10) is null and 
                    src:trdi_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdu), '0'), 38, 10) is null and 
                    src:trdu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtc), '0'), 38, 10) is null and 
                    src:txtc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtc_ref_compnr), '0'), 38, 10) is null and 
                    src:txtc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtd), '0'), 38, 10) is null and 
                    src:txtd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtd_ref_compnr), '0'), 38, 10) is null and 
                    src:txtd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vtbo), '0'), 38, 10) is null and 
                    src:vtbo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wrtp), '0'), 38, 10) is null and 
                    src:wrtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xrcu_ref_compnr), '0'), 38, 10) is null and 
                    src:xrcu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xrnh), '0'), 38, 10) is null and 
                    src:xrnh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xrrt), '0'), 38, 10) is null and 
                    src:xrrt is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)