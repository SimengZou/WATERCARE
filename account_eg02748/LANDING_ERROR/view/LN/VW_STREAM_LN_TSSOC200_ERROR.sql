CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSSOC200_ERROR AS SELECT src, 'LN_TSSOC200' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:acdt), '1900-01-01')) is null and 
                    src:acdt is not null) THEN
                    'acdt contains a non-timestamp value : \'' || AS_VARCHAR(src:acdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:aftm), '1900-01-01')) is null and 
                    src:aftm is not null) THEN
                    'aftm contains a non-timestamp value : \'' || AS_VARCHAR(src:aftm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ahpt), '0'), 38, 10) is null and 
                    src:ahpt is not null) THEN
                    'ahpt contains a non-numeric value : \'' || AS_VARCHAR(src:ahpt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) THEN
                    'amnt contains a non-numeric value : \'' || AS_VARCHAR(src:amnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_dwhc), '0'), 38, 10) is null and 
                    src:amnt_dwhc is not null) THEN
                    'amnt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_homc), '0'), 38, 10) is null and 
                    src:amnt_homc is not null) THEN
                    'amnt_homc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_refc), '0'), 38, 10) is null and 
                    src:amnt_refc is not null) THEN
                    'amnt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc1), '0'), 38, 10) is null and 
                    src:amnt_rpc1 is not null) THEN
                    'amnt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc2), '0'), 38, 10) is null and 
                    src:amnt_rpc2 is not null) THEN
                    'amnt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appo), '0'), 38, 10) is null and 
                    src:appo is not null) THEN
                    'appo contains a non-numeric value : \'' || AS_VARCHAR(src:appo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aril), '0'), 38, 10) is null and 
                    src:aril is not null) THEN
                    'aril contains a non-numeric value : \'' || AS_VARCHAR(src:aril) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:astm), '1900-01-01')) is null and 
                    src:astm is not null) THEN
                    'astm contains a non-timestamp value : \'' || AS_VARCHAR(src:astm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atpc), '0'), 38, 10) is null and 
                    src:atpc is not null) THEN
                    'atpc contains a non-numeric value : \'' || AS_VARCHAR(src:atpc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:atpd), '1900-01-01')) is null and 
                    src:atpd is not null) THEN
                    'atpd contains a non-timestamp value : \'' || AS_VARCHAR(src:atpd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blyn), '0'), 38, 10) is null and 
                    src:blyn is not null) THEN
                    'blyn contains a non-numeric value : \'' || AS_VARCHAR(src:blyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bppr_ref_compnr), '0'), 38, 10) is null and 
                    src:bppr_ref_compnr is not null) THEN
                    'bppr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bppr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cadt), '1900-01-01')) is null and 
                    src:cadt is not null) THEN
                    'cadt contains a non-timestamp value : \'' || AS_VARCHAR(src:cadt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr_ref_compnr), '0'), 38, 10) is null and 
                    src:camr_ref_compnr is not null) THEN
                    'camr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:camr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:caro_ref_compnr), '0'), 38, 10) is null and 
                    src:caro_ref_compnr is not null) THEN
                    'caro_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:caro_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) THEN
                    'cbrn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbrn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccar_ref_compnr), '0'), 38, 10) is null and 
                    src:ccar_ref_compnr is not null) THEN
                    'ccar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) THEN
                    'ccrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctp_ref_compnr), '0'), 38, 10) is null and 
                    src:cctp_ref_compnr is not null) THEN
                    'cctp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cctp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdis_ref_compnr), '0'), 38, 10) is null and 
                    src:cdis_ref_compnr is not null) THEN
                    'cdis_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdis_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) THEN
                    'cfrw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfrw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clmt), '0'), 38, 10) is null and 
                    src:clmt is not null) THEN
                    'clmt contains a non-numeric value : \'' || AS_VARCHAR(src:clmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clst_ref_compnr), '0'), 38, 10) is null and 
                    src:clst_ref_compnr is not null) THEN
                    'clst_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clst_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cplt_ref_compnr), '0'), 38, 10) is null and 
                    src:cplt_ref_compnr is not null) THEN
                    'cplt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cplt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpor), '0'), 38, 10) is null and 
                    src:cpor is not null) THEN
                    'cpor contains a non-numeric value : \'' || AS_VARCHAR(src:cpor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) THEN
                    'creg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:creg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is null and 
                    src:crep_ref_compnr is not null) THEN
                    'crep_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crep_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crit_ref_compnr), '0'), 38, 10) is null and 
                    src:crit_ref_compnr is not null) THEN
                    'crit_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crit_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) THEN
                    'crte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csar_ref_compnr), '0'), 38, 10) is null and 
                    src:csar_ref_compnr is not null) THEN
                    'csar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) THEN
                    'csco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csqu_ref_compnr), '0'), 38, 10) is null and 
                    src:csqu_ref_compnr is not null) THEN
                    'csqu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csqu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstp_ref_compnr), '0'), 38, 10) is null and 
                    src:cstp_ref_compnr is not null) THEN
                    'cstp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cstp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctcs), '0'), 38, 10) is null and 
                    src:ctcs is not null) THEN
                    'ctcs contains a non-numeric value : \'' || AS_VARCHAR(src:ctcs) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cvtm), '1900-01-01')) is null and 
                    src:cvtm is not null) THEN
                    'cvtm contains a non-timestamp value : \'' || AS_VARCHAR(src:cvtm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) THEN
                    'cwoc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwoc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfpb), '0'), 38, 10) is null and 
                    src:dfpb is not null) THEN
                    'dfpb contains a non-numeric value : \'' || AS_VARCHAR(src:dfpb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dipy), '0'), 38, 10) is null and 
                    src:dipy is not null) THEN
                    'dipy contains a non-numeric value : \'' || AS_VARCHAR(src:dipy) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) THEN
                    'emno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:emno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:espr), '0'), 38, 10) is null and 
                    src:espr is not null) THEN
                    'espr contains a non-numeric value : \'' || AS_VARCHAR(src:espr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:estm), '1900-01-01')) is null and 
                    src:estm is not null) THEN
                    'estm contains a non-timestamp value : \'' || AS_VARCHAR(src:estm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcrt), '0'), 38, 10) is null and 
                    src:fcrt is not null) THEN
                    'fcrt contains a non-numeric value : \'' || AS_VARCHAR(src:fcrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:fdpt_ref_compnr is not null) THEN
                    'fdpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fdpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inby), '0'), 38, 10) is null and 
                    src:inby is not null) THEN
                    'inby contains a non-numeric value : \'' || AS_VARCHAR(src:inby) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpl_ref_compnr), '0'), 38, 10) is null and 
                    src:inpl_ref_compnr is not null) THEN
                    'inpl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:inpl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insm), '0'), 38, 10) is null and 
                    src:insm is not null) THEN
                    'insm contains a non-numeric value : \'' || AS_VARCHAR(src:insm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itad_ref_compnr), '0'), 38, 10) is null and 
                    src:itad_ref_compnr is not null) THEN
                    'itad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) THEN
                    'itbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itcn_ref_compnr), '0'), 38, 10) is null and 
                    src:itcn_ref_compnr is not null) THEN
                    'itcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_sern_ref_compnr), '0'), 38, 10) is null and 
                    src:item_sern_ref_compnr is not null) THEN
                    'item_sern_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_sern_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_site_ref_compnr), '0'), 38, 10) is null and 
                    src:item_site_ref_compnr is not null) THEN
                    'item_site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ityn), '0'), 38, 10) is null and 
                    src:ityn is not null) THEN
                    'ityn contains a non-numeric value : \'' || AS_VARCHAR(src:ityn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcad_ref_compnr), '0'), 38, 10) is null and 
                    src:lcad_ref_compnr is not null) THEN
                    'lcad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lftm), '1900-01-01')) is null and 
                    src:lftm is not null) THEN
                    'lftm contains a non-timestamp value : \'' || AS_VARCHAR(src:lftm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lutd), '1900-01-01')) is null and 
                    src:lutd is not null) THEN
                    'lutd contains a non-timestamp value : \'' || AS_VARCHAR(src:lutd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:micn), '0'), 38, 10) is null and 
                    src:micn is not null) THEN
                    'micn contains a non-numeric value : \'' || AS_VARCHAR(src:micn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:npeg), '0'), 38, 10) is null and 
                    src:npeg is not null) THEN
                    'npeg contains a non-numeric value : \'' || AS_VARCHAR(src:npeg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt), '0'), 38, 10) is null and 
                    src:oamt is not null) THEN
                    'oamt contains a non-numeric value : \'' || AS_VARCHAR(src:oamt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt_dwhc), '0'), 38, 10) is null and 
                    src:oamt_dwhc is not null) THEN
                    'oamt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:oamt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt_homc), '0'), 38, 10) is null and 
                    src:oamt_homc is not null) THEN
                    'oamt_homc contains a non-numeric value : \'' || AS_VARCHAR(src:oamt_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt_refc), '0'), 38, 10) is null and 
                    src:oamt_refc is not null) THEN
                    'oamt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:oamt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt_rpc1), '0'), 38, 10) is null and 
                    src:oamt_rpc1 is not null) THEN
                    'oamt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:oamt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt_rpc2), '0'), 38, 10) is null and 
                    src:oamt_rpc2 is not null) THEN
                    'oamt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:oamt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odis), '0'), 38, 10) is null and 
                    src:odis is not null) THEN
                    'odis contains a non-numeric value : \'' || AS_VARCHAR(src:odis) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odpr), '0'), 38, 10) is null and 
                    src:odpr is not null) THEN
                    'odpr contains a non-numeric value : \'' || AS_VARCHAR(src:odpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) THEN
                    'ofad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) THEN
                    'ofcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orac), '0'), 38, 10) is null and 
                    src:orac is not null) THEN
                    'orac contains a non-numeric value : \'' || AS_VARCHAR(src:orac) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ordt), '1900-01-01')) is null and 
                    src:ordt is not null) THEN
                    'ordt contains a non-timestamp value : \'' || AS_VARCHAR(src:ordt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ordu), '0'), 38, 10) is null and 
                    src:ordu is not null) THEN
                    'ordu contains a non-numeric value : \'' || AS_VARCHAR(src:ordu) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:orrt), '1900-01-01')) is null and 
                    src:orrt is not null) THEN
                    'orrt contains a non-timestamp value : \'' || AS_VARCHAR(src:orrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osta), '0'), 38, 10) is null and 
                    src:osta is not null) THEN
                    'osta contains a non-numeric value : \'' || AS_VARCHAR(src:osta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otim), '0'), 38, 10) is null and 
                    src:otim is not null) THEN
                    'otim contains a non-numeric value : \'' || AS_VARCHAR(src:otim) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is null and 
                    src:pfad_ref_compnr is not null) THEN
                    'pfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) THEN
                    'pfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:pfcn_ref_compnr is not null) THEN
                    'pfcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pftm), '1900-01-01')) is null and 
                    src:pftm is not null) THEN
                    'pftm contains a non-timestamp value : \'' || AS_VARCHAR(src:pftm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pldd_ref_compnr), '0'), 38, 10) is null and 
                    src:pldd_ref_compnr is not null) THEN
                    'pldd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pldd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmtd), '0'), 38, 10) is null and 
                    src:pmtd is not null) THEN
                    'pmtd contains a non-numeric value : \'' || AS_VARCHAR(src:pmtd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:porg), '0'), 38, 10) is null and 
                    src:porg is not null) THEN
                    'porg contains a non-numeric value : \'' || AS_VARCHAR(src:porg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppeg), '0'), 38, 10) is null and 
                    src:ppeg is not null) THEN
                    'ppeg contains a non-numeric value : \'' || AS_VARCHAR(src:ppeg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prac), '0'), 38, 10) is null and 
                    src:prac is not null) THEN
                    'prac contains a non-numeric value : \'' || AS_VARCHAR(src:prac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) THEN
                    'pris contains a non-numeric value : \'' || AS_VARCHAR(src:pris) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pstm), '1900-01-01')) is null and 
                    src:pstm is not null) THEN
                    'pstm contains a non-timestamp value : \'' || AS_VARCHAR(src:pstm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ptft), '1900-01-01')) is null and 
                    src:ptft is not null) THEN
                    'ptft contains a non-timestamp value : \'' || AS_VARCHAR(src:ptft) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ptst), '1900-01-01')) is null and 
                    src:ptst is not null) THEN
                    'ptst contains a non-timestamp value : \'' || AS_VARCHAR(src:ptst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtno_revn_ref_compnr), '0'), 38, 10) is null and 
                    src:qtno_revn_ref_compnr is not null) THEN
                    'qtno_revn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:qtno_revn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ract), '1900-01-01')) is null and 
                    src:ract is not null) THEN
                    'ract contains a non-timestamp value : \'' || AS_VARCHAR(src:ract) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) THEN
                    'ratf_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) THEN
                    'ratf_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) THEN
                    'ratf_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_1), '0'), 38, 10) is null and 
                    src:rats_1 is not null) THEN
                    'rats_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rats_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_2), '0'), 38, 10) is null and 
                    src:rats_2 is not null) THEN
                    'rats_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rats_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_3), '0'), 38, 10) is null and 
                    src:rats_3 is not null) THEN
                    'rats_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rats_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcde_ref_compnr), '0'), 38, 10) is null and 
                    src:rcde_ref_compnr is not null) THEN
                    'rcde_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcde_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) THEN
                    'revn contains a non-numeric value : \'' || AS_VARCHAR(src:revn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rptp_ref_compnr), '0'), 38, 10) is null and 
                    src:rptp_ref_compnr is not null) THEN
                    'rptp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rptp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsrp), '0'), 38, 10) is null and 
                    src:rsrp is not null) THEN
                    'rsrp contains a non-numeric value : \'' || AS_VARCHAR(src:rsrp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rtdt), '1900-01-01')) is null and 
                    src:rtdt is not null) THEN
                    'rtdt contains a non-timestamp value : \'' || AS_VARCHAR(src:rtdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) THEN
                    'rtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smto), '0'), 38, 10) is null and 
                    src:smto is not null) THEN
                    'smto contains a non-numeric value : \'' || AS_VARCHAR(src:smto) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) THEN
                    'stad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) THEN
                    'stbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is null and 
                    src:stcn_ref_compnr is not null) THEN
                    'stcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is null and 
                    src:styp_ref_compnr is not null) THEN
                    'styp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:styp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tinv), '0'), 38, 10) is null and 
                    src:tinv is not null) THEN
                    'tinv contains a non-numeric value : \'' || AS_VARCHAR(src:tinv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdi), '0'), 38, 10) is null and 
                    src:trdi is not null) THEN
                    'trdi contains a non-numeric value : \'' || AS_VARCHAR(src:trdi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdi_buln), '0'), 38, 10) is null and 
                    src:trdi_buln is not null) THEN
                    'trdi_buln contains a non-numeric value : \'' || AS_VARCHAR(src:trdi_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trpm), '0'), 38, 10) is null and 
                    src:trpm is not null) THEN
                    'trpm contains a non-numeric value : \'' || AS_VARCHAR(src:trpm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:tstm), '1900-01-01')) is null and 
                    src:tstm is not null) THEN
                    'tstm contains a non-timestamp value : \'' || AS_VARCHAR(src:tstm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tvdu), '0'), 38, 10) is null and 
                    src:tvdu is not null) THEN
                    'tvdu contains a non-numeric value : \'' || AS_VARCHAR(src:tvdu) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txyn), '0'), 38, 10) is null and 
                    src:txyn is not null) THEN
                    'txyn contains a non-numeric value : \'' || AS_VARCHAR(src:txyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucce), '0'), 38, 10) is null and 
                    src:ucce is not null) THEN
                    'ucce contains a non-numeric value : \'' || AS_VARCHAR(src:ucce) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:utce), '0'), 38, 10) is null and 
                    src:utce is not null) THEN
                    'utce contains a non-numeric value : \'' || AS_VARCHAR(src:utce) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vtbo), '0'), 38, 10) is null and 
                    src:vtbo is not null) THEN
                    'vtbo contains a non-numeric value : \'' || AS_VARCHAR(src:vtbo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wcyn), '0'), 38, 10) is null and 
                    src:wcyn is not null) THEN
                    'wcyn contains a non-numeric value : \'' || AS_VARCHAR(src:wcyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wrtp), '0'), 38, 10) is null and 
                    src:wrtp is not null) THEN
                    'wrtp contains a non-numeric value : \'' || AS_VARCHAR(src:wrtp) || '\'' WHEN 
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
                                    
                src:orno,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSSOC200 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:acdt), '1900-01-01')) is null and 
                    src:acdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:aftm), '1900-01-01')) is null and 
                    src:aftm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ahpt), '0'), 38, 10) is null and 
                    src:ahpt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_dwhc), '0'), 38, 10) is null and 
                    src:amnt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_homc), '0'), 38, 10) is null and 
                    src:amnt_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_refc), '0'), 38, 10) is null and 
                    src:amnt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc1), '0'), 38, 10) is null and 
                    src:amnt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc2), '0'), 38, 10) is null and 
                    src:amnt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appo), '0'), 38, 10) is null and 
                    src:appo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aril), '0'), 38, 10) is null and 
                    src:aril is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:astm), '1900-01-01')) is null and 
                    src:astm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atpc), '0'), 38, 10) is null and 
                    src:atpc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:atpd), '1900-01-01')) is null and 
                    src:atpd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blyn), '0'), 38, 10) is null and 
                    src:blyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bppr_ref_compnr), '0'), 38, 10) is null and 
                    src:bppr_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cadt), '1900-01-01')) is null and 
                    src:cadt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr_ref_compnr), '0'), 38, 10) is null and 
                    src:camr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:caro_ref_compnr), '0'), 38, 10) is null and 
                    src:caro_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccar_ref_compnr), '0'), 38, 10) is null and 
                    src:ccar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctp_ref_compnr), '0'), 38, 10) is null and 
                    src:cctp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdis_ref_compnr), '0'), 38, 10) is null and 
                    src:cdis_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clmt), '0'), 38, 10) is null and 
                    src:clmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clst_ref_compnr), '0'), 38, 10) is null and 
                    src:clst_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cplt_ref_compnr), '0'), 38, 10) is null and 
                    src:cplt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpor), '0'), 38, 10) is null and 
                    src:cpor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is null and 
                    src:crep_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crit_ref_compnr), '0'), 38, 10) is null and 
                    src:crit_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csar_ref_compnr), '0'), 38, 10) is null and 
                    src:csar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csqu_ref_compnr), '0'), 38, 10) is null and 
                    src:csqu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstp_ref_compnr), '0'), 38, 10) is null and 
                    src:cstp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctcs), '0'), 38, 10) is null and 
                    src:ctcs is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cvtm), '1900-01-01')) is null and 
                    src:cvtm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfpb), '0'), 38, 10) is null and 
                    src:dfpb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dipy), '0'), 38, 10) is null and 
                    src:dipy is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:espr), '0'), 38, 10) is null and 
                    src:espr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:estm), '1900-01-01')) is null and 
                    src:estm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcrt), '0'), 38, 10) is null and 
                    src:fcrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:fdpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inby), '0'), 38, 10) is null and 
                    src:inby is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpl_ref_compnr), '0'), 38, 10) is null and 
                    src:inpl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insm), '0'), 38, 10) is null and 
                    src:insm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itad_ref_compnr), '0'), 38, 10) is null and 
                    src:itad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itcn_ref_compnr), '0'), 38, 10) is null and 
                    src:itcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_sern_ref_compnr), '0'), 38, 10) is null and 
                    src:item_sern_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_site_ref_compnr), '0'), 38, 10) is null and 
                    src:item_site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ityn), '0'), 38, 10) is null and 
                    src:ityn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcad_ref_compnr), '0'), 38, 10) is null and 
                    src:lcad_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lftm), '1900-01-01')) is null and 
                    src:lftm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lutd), '1900-01-01')) is null and 
                    src:lutd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:micn), '0'), 38, 10) is null and 
                    src:micn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:npeg), '0'), 38, 10) is null and 
                    src:npeg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt), '0'), 38, 10) is null and 
                    src:oamt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt_dwhc), '0'), 38, 10) is null and 
                    src:oamt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt_homc), '0'), 38, 10) is null and 
                    src:oamt_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt_refc), '0'), 38, 10) is null and 
                    src:oamt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt_rpc1), '0'), 38, 10) is null and 
                    src:oamt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt_rpc2), '0'), 38, 10) is null and 
                    src:oamt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odis), '0'), 38, 10) is null and 
                    src:odis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odpr), '0'), 38, 10) is null and 
                    src:odpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orac), '0'), 38, 10) is null and 
                    src:orac is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ordt), '1900-01-01')) is null and 
                    src:ordt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ordu), '0'), 38, 10) is null and 
                    src:ordu is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:orrt), '1900-01-01')) is null and 
                    src:orrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osta), '0'), 38, 10) is null and 
                    src:osta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otim), '0'), 38, 10) is null and 
                    src:otim is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is null and 
                    src:pfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:pfcn_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pftm), '1900-01-01')) is null and 
                    src:pftm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pldd_ref_compnr), '0'), 38, 10) is null and 
                    src:pldd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmtd), '0'), 38, 10) is null and 
                    src:pmtd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:porg), '0'), 38, 10) is null and 
                    src:porg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppeg), '0'), 38, 10) is null and 
                    src:ppeg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prac), '0'), 38, 10) is null and 
                    src:prac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pstm), '1900-01-01')) is null and 
                    src:pstm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ptft), '1900-01-01')) is null and 
                    src:ptft is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ptst), '1900-01-01')) is null and 
                    src:ptst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtno_revn_ref_compnr), '0'), 38, 10) is null and 
                    src:qtno_revn_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ract), '1900-01-01')) is null and 
                    src:ract is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_1), '0'), 38, 10) is null and 
                    src:rats_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_2), '0'), 38, 10) is null and 
                    src:rats_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_3), '0'), 38, 10) is null and 
                    src:rats_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcde_ref_compnr), '0'), 38, 10) is null and 
                    src:rcde_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rptp_ref_compnr), '0'), 38, 10) is null and 
                    src:rptp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsrp), '0'), 38, 10) is null and 
                    src:rsrp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rtdt), '1900-01-01')) is null and 
                    src:rtdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smto), '0'), 38, 10) is null and 
                    src:smto is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is null and 
                    src:stcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is null and 
                    src:styp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tinv), '0'), 38, 10) is null and 
                    src:tinv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdi), '0'), 38, 10) is null and 
                    src:trdi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdi_buln), '0'), 38, 10) is null and 
                    src:trdi_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trpm), '0'), 38, 10) is null and 
                    src:trpm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:tstm), '1900-01-01')) is null and 
                    src:tstm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tvdu), '0'), 38, 10) is null and 
                    src:tvdu is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txyn), '0'), 38, 10) is null and 
                    src:txyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucce), '0'), 38, 10) is null and 
                    src:ucce is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:utce), '0'), 38, 10) is null and 
                    src:utce is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vtbo), '0'), 38, 10) is null and 
                    src:vtbo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wcyn), '0'), 38, 10) is null and 
                    src:wcyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wrtp), '0'), 38, 10) is null and 
                    src:wrtp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)