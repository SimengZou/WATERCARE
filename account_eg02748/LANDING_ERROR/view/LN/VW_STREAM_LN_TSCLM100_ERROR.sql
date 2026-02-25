CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSCLM100_ERROR AS SELECT src, 'LN_TSCLM100' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:acct), '1900-01-01')) is null and 
                    src:acct is not null) THEN
                    'acct contains a non-timestamp value : \'' || AS_VARCHAR(src:acct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:actt_ref_compnr), '0'), 38, 10) is null and 
                    src:actt_ref_compnr is not null) THEN
                    'actt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:actt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adtm), '0'), 38, 10) is null and 
                    src:adtm is not null) THEN
                    'adtm contains a non-numeric value : \'' || AS_VARCHAR(src:adtm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adur), '0'), 38, 10) is null and 
                    src:adur is not null) THEN
                    'adur contains a non-numeric value : \'' || AS_VARCHAR(src:adur) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appo), '0'), 38, 10) is null and 
                    src:appo is not null) THEN
                    'appo contains a non-numeric value : \'' || AS_VARCHAR(src:appo) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:arct), '1900-01-01')) is null and 
                    src:arct is not null) THEN
                    'arct contains a non-timestamp value : \'' || AS_VARCHAR(src:arct) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:asct), '1900-01-01')) is null and 
                    src:asct is not null) THEN
                    'asct contains a non-timestamp value : \'' || AS_VARCHAR(src:asct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atml), '0'), 38, 10) is null and 
                    src:atml is not null) THEN
                    'atml contains a non-numeric value : \'' || AS_VARCHAR(src:atml) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bdfx), '0'), 38, 10) is null and 
                    src:bdfx is not null) THEN
                    'bdfx contains a non-numeric value : \'' || AS_VARCHAR(src:bdfx) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:bfbp_ref_compnr is not null) THEN
                    'bfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:bldt), '1900-01-01')) is null and 
                    src:bldt is not null) THEN
                    'bldt contains a non-timestamp value : \'' || AS_VARCHAR(src:bldt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blyn), '0'), 38, 10) is null and 
                    src:blyn is not null) THEN
                    'blyn contains a non-numeric value : \'' || AS_VARCHAR(src:blyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr_ref_compnr), '0'), 38, 10) is null and 
                    src:camr_ref_compnr is not null) THEN
                    'camr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:camr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctp_ref_compnr), '0'), 38, 10) is null and 
                    src:cctp_ref_compnr is not null) THEN
                    'cctp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cctp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cgrp_ref_compnr), '0'), 38, 10) is null and 
                    src:cgrp_ref_compnr is not null) THEN
                    'cgrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cgrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinv_ref_compnr), '0'), 38, 10) is null and 
                    src:cinv_ref_compnr is not null) THEN
                    'cinv_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cinv_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citm_cser_ref_compnr), '0'), 38, 10) is null and 
                    src:citm_cser_ref_compnr is not null) THEN
                    'citm_cser_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:citm_cser_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citm_ref_compnr), '0'), 38, 10) is null and 
                    src:citm_ref_compnr is not null) THEN
                    'citm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:citm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) THEN
                    'clan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cllo), '0'), 38, 10) is null and 
                    src:cllo is not null) THEN
                    'cllo contains a non-numeric value : \'' || AS_VARCHAR(src:cllo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clst_ref_compnr), '0'), 38, 10) is null and 
                    src:clst_ref_compnr is not null) THEN
                    'clst_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clst_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtc_ref_compnr), '0'), 38, 10) is null and 
                    src:crtc_ref_compnr is not null) THEN
                    'crtc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crtc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) THEN
                    'csco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csgr_ref_compnr), '0'), 38, 10) is null and 
                    src:csgr_ref_compnr is not null) THEN
                    'csgr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csgr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspc_ref_compnr), '0'), 38, 10) is null and 
                    src:cspc_ref_compnr is not null) THEN
                    'cspc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cspc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupr), '0'), 38, 10) is null and 
                    src:cupr is not null) THEN
                    'cupr contains a non-numeric value : \'' || AS_VARCHAR(src:cupr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupr_ref_compnr), '0'), 38, 10) is null and 
                    src:cupr_ref_compnr is not null) THEN
                    'cupr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cupr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cvtm), '1900-01-01')) is null and 
                    src:cvtm is not null) THEN
                    'cvtm contains a non-timestamp value : \'' || AS_VARCHAR(src:cvtm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) THEN
                    'cwoc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwoc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cxsc_ref_compnr), '0'), 38, 10) is null and 
                    src:cxsc_ref_compnr is not null) THEN
                    'cxsc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cxsc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:docn_ref_compnr), '0'), 38, 10) is null and 
                    src:docn_ref_compnr is not null) THEN
                    'docn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:docn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:egct), '1900-01-01')) is null and 
                    src:egct is not null) THEN
                    'egct contains a non-timestamp value : \'' || AS_VARCHAR(src:egct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emer), '0'), 38, 10) is null and 
                    src:emer is not null) THEN
                    'emer contains a non-numeric value : \'' || AS_VARCHAR(src:emer) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) THEN
                    'emno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:emno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:encl), '0'), 38, 10) is null and 
                    src:encl is not null) THEN
                    'encl contains a non-numeric value : \'' || AS_VARCHAR(src:encl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:espr_ref_compnr), '0'), 38, 10) is null and 
                    src:espr_ref_compnr is not null) THEN
                    'espr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:espr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:expr_ref_compnr), '0'), 38, 10) is null and 
                    src:expr_ref_compnr is not null) THEN
                    'expr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:expr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exsl_ref_compnr), '0'), 38, 10) is null and 
                    src:exsl_ref_compnr is not null) THEN
                    'exsl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:exsl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcll_ref_compnr), '0'), 38, 10) is null and 
                    src:fcll_ref_compnr is not null) THEN
                    'fcll_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fcll_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inby), '0'), 38, 10) is null and 
                    src:inby is not null) THEN
                    'inby contains a non-numeric value : \'' || AS_VARCHAR(src:inby) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inrt), '0'), 38, 10) is null and 
                    src:inrt is not null) THEN
                    'inrt contains a non-numeric value : \'' || AS_VARCHAR(src:inrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inyn), '0'), 38, 10) is null and 
                    src:inyn is not null) THEN
                    'inyn contains a non-numeric value : \'' || AS_VARCHAR(src:inyn) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:minu), '0'), 38, 10) is null and 
                    src:minu is not null) THEN
                    'minu contains a non-numeric value : \'' || AS_VARCHAR(src:minu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obpr), '0'), 38, 10) is null and 
                    src:obpr is not null) THEN
                    'obpr contains a non-numeric value : \'' || AS_VARCHAR(src:obpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obpr_ref_compnr), '0'), 38, 10) is null and 
                    src:obpr_ref_compnr is not null) THEN
                    'obpr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:obpr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oemn_ref_compnr), '0'), 38, 10) is null and 
                    src:oemn_ref_compnr is not null) THEN
                    'oemn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:oemn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) THEN
                    'ofad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) THEN
                    'ofcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbsm), '0'), 38, 10) is null and 
                    src:pbsm is not null) THEN
                    'pbsm contains a non-numeric value : \'' || AS_VARCHAR(src:pbsm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcch), '0'), 38, 10) is null and 
                    src:pcch is not null) THEN
                    'pcch contains a non-numeric value : \'' || AS_VARCHAR(src:pcch) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcll_ref_compnr), '0'), 38, 10) is null and 
                    src:pcll_ref_compnr is not null) THEN
                    'pcll_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pcll_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcln), '0'), 38, 10) is null and 
                    src:pcln is not null) THEN
                    'pcln contains a non-numeric value : \'' || AS_VARCHAR(src:pcln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcon_ref_compnr), '0'), 38, 10) is null and 
                    src:pcon_ref_compnr is not null) THEN
                    'pcon_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pcon_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pect), '1900-01-01')) is null and 
                    src:pect is not null) THEN
                    'pect contains a non-timestamp value : \'' || AS_VARCHAR(src:pect) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is null and 
                    src:pfad_ref_compnr is not null) THEN
                    'pfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) THEN
                    'pfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:pfcn_ref_compnr is not null) THEN
                    'pfcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pfct), '1900-01-01')) is null and 
                    src:pfct is not null) THEN
                    'pfct contains a non-timestamp value : \'' || AS_VARCHAR(src:pfct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prpr), '0'), 38, 10) is null and 
                    src:prpr is not null) THEN
                    'prpr contains a non-numeric value : \'' || AS_VARCHAR(src:prpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prpr_ref_compnr), '0'), 38, 10) is null and 
                    src:prpr_ref_compnr is not null) THEN
                    'prpr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prpr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:repr_wrty), '0'), 38, 10) is null and 
                    src:repr_wrty is not null) THEN
                    'repr_wrty contains a non-numeric value : \'' || AS_VARCHAR(src:repr_wrty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfcl), '0'), 38, 10) is null and 
                    src:rfcl is not null) THEN
                    'rfcl contains a non-numeric value : \'' || AS_VARCHAR(src:rfcl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfco_ref_compnr), '0'), 38, 10) is null and 
                    src:rfco_ref_compnr is not null) THEN
                    'rfco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rfco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfco_rfcl_ref_compnr), '0'), 38, 10) is null and 
                    src:rfco_rfcl_ref_compnr is not null) THEN
                    'rfco_rfcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rfco_rfcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rlct), '1900-01-01')) is null and 
                    src:rlct is not null) THEN
                    'rlct contains a non-timestamp value : \'' || AS_VARCHAR(src:rlct) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rpct), '1900-01-01')) is null and 
                    src:rpct is not null) THEN
                    'rpct contains a non-timestamp value : \'' || AS_VARCHAR(src:rpct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rprl_ref_compnr), '0'), 38, 10) is null and 
                    src:rprl_ref_compnr is not null) THEN
                    'rprl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rprl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rqct_ref_compnr), '0'), 38, 10) is null and 
                    src:rqct_ref_compnr is not null) THEN
                    'rqct_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rqct_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rtct), '1900-01-01')) is null and 
                    src:rtct is not null) THEN
                    'rtct contains a non-timestamp value : \'' || AS_VARCHAR(src:rtct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbct_ref_compnr), '0'), 38, 10) is null and 
                    src:sbct_ref_compnr is not null) THEN
                    'sbct_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sbct_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scgr_ref_compnr), '0'), 38, 10) is null and 
                    src:scgr_ref_compnr is not null) THEN
                    'scgr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:scgr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfad_ref_compnr), '0'), 38, 10) is null and 
                    src:sfad_ref_compnr is not null) THEN
                    'sfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) THEN
                    'sfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:slct), '1900-01-01')) is null and 
                    src:slct is not null) THEN
                    'slct contains a non-timestamp value : \'' || AS_VARCHAR(src:slct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sltn_ref_compnr), '0'), 38, 10) is null and 
                    src:sltn_ref_compnr is not null) THEN
                    'sltn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sltn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:soad_ref_compnr), '0'), 38, 10) is null and 
                    src:soad_ref_compnr is not null) THEN
                    'soad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:soad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:socn_ref_compnr), '0'), 38, 10) is null and 
                    src:socn_ref_compnr is not null) THEN
                    'socn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:socn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:susr_ref_compnr), '0'), 38, 10) is null and 
                    src:susr_ref_compnr is not null) THEN
                    'susr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:susr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:susr_susn_ref_compnr), '0'), 38, 10) is null and 
                    src:susr_susn_ref_compnr is not null) THEN
                    'susr_susn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:susr_susn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:svct), '1900-01-01')) is null and 
                    src:svct is not null) THEN
                    'svct contains a non-timestamp value : \'' || AS_VARCHAR(src:svct) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtd), '0'), 38, 10) is null and 
                    src:txtd is not null) THEN
                    'txtd contains a non-numeric value : \'' || AS_VARCHAR(src:txtd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtd_ref_compnr), '0'), 38, 10) is null and 
                    src:txtd_ref_compnr is not null) THEN
                    'txtd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txte), '0'), 38, 10) is null and 
                    src:txte is not null) THEN
                    'txte contains a non-numeric value : \'' || AS_VARCHAR(src:txte) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txte_ref_compnr), '0'), 38, 10) is null and 
                    src:txte_ref_compnr is not null) THEN
                    'txte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtf), '0'), 38, 10) is null and 
                    src:txtf is not null) THEN
                    'txtf contains a non-numeric value : \'' || AS_VARCHAR(src:txtf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtf_ref_compnr), '0'), 38, 10) is null and 
                    src:txtf_ref_compnr is not null) THEN
                    'txtf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtg), '0'), 38, 10) is null and 
                    src:txtg is not null) THEN
                    'txtg contains a non-numeric value : \'' || AS_VARCHAR(src:txtg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtg_ref_compnr), '0'), 38, 10) is null and 
                    src:txtg_ref_compnr is not null) THEN
                    'txtg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txth), '0'), 38, 10) is null and 
                    src:txth is not null) THEN
                    'txth contains a non-numeric value : \'' || AS_VARCHAR(src:txth) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txth_ref_compnr), '0'), 38, 10) is null and 
                    src:txth_ref_compnr is not null) THEN
                    'txth_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txth_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtk), '0'), 38, 10) is null and 
                    src:txtk is not null) THEN
                    'txtk contains a non-numeric value : \'' || AS_VARCHAR(src:txtk) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtk_ref_compnr), '0'), 38, 10) is null and 
                    src:txtk_ref_compnr is not null) THEN
                    'txtk_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtk_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubad_ref_compnr), '0'), 38, 10) is null and 
                    src:ubad_ref_compnr is not null) THEN
                    'ubad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ubad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ubbp_ref_compnr is not null) THEN
                    'ubbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ubbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ubcn_ref_compnr is not null) THEN
                    'ubcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ubcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubop_ref_compnr), '0'), 38, 10) is null and 
                    src:ubop_ref_compnr is not null) THEN
                    'ubop_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ubop_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ustm), '0'), 38, 10) is null and 
                    src:ustm is not null) THEN
                    'ustm contains a non-numeric value : \'' || AS_VARCHAR(src:ustm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wait), '0'), 38, 10) is null and 
                    src:wait is not null) THEN
                    'wait contains a non-numeric value : \'' || AS_VARCHAR(src:wait) || '\'' WHEN 
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
                                    
                src:ccll,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSCLM100 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:acct), '1900-01-01')) is null and 
                    src:acct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:actt_ref_compnr), '0'), 38, 10) is null and 
                    src:actt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adtm), '0'), 38, 10) is null and 
                    src:adtm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adur), '0'), 38, 10) is null and 
                    src:adur is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appo), '0'), 38, 10) is null and 
                    src:appo is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:arct), '1900-01-01')) is null and 
                    src:arct is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:asct), '1900-01-01')) is null and 
                    src:asct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atml), '0'), 38, 10) is null and 
                    src:atml is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bdfx), '0'), 38, 10) is null and 
                    src:bdfx is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:bfbp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:bldt), '1900-01-01')) is null and 
                    src:bldt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blyn), '0'), 38, 10) is null and 
                    src:blyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr_ref_compnr), '0'), 38, 10) is null and 
                    src:camr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctp_ref_compnr), '0'), 38, 10) is null and 
                    src:cctp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cgrp_ref_compnr), '0'), 38, 10) is null and 
                    src:cgrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinv_ref_compnr), '0'), 38, 10) is null and 
                    src:cinv_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citm_cser_ref_compnr), '0'), 38, 10) is null and 
                    src:citm_cser_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citm_ref_compnr), '0'), 38, 10) is null and 
                    src:citm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cllo), '0'), 38, 10) is null and 
                    src:cllo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clst_ref_compnr), '0'), 38, 10) is null and 
                    src:clst_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtc_ref_compnr), '0'), 38, 10) is null and 
                    src:crtc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csgr_ref_compnr), '0'), 38, 10) is null and 
                    src:csgr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspc_ref_compnr), '0'), 38, 10) is null and 
                    src:cspc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupr), '0'), 38, 10) is null and 
                    src:cupr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupr_ref_compnr), '0'), 38, 10) is null and 
                    src:cupr_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cvtm), '1900-01-01')) is null and 
                    src:cvtm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cxsc_ref_compnr), '0'), 38, 10) is null and 
                    src:cxsc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:docn_ref_compnr), '0'), 38, 10) is null and 
                    src:docn_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:egct), '1900-01-01')) is null and 
                    src:egct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emer), '0'), 38, 10) is null and 
                    src:emer is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:encl), '0'), 38, 10) is null and 
                    src:encl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:espr_ref_compnr), '0'), 38, 10) is null and 
                    src:espr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:expr_ref_compnr), '0'), 38, 10) is null and 
                    src:expr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exsl_ref_compnr), '0'), 38, 10) is null and 
                    src:exsl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcll_ref_compnr), '0'), 38, 10) is null and 
                    src:fcll_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inby), '0'), 38, 10) is null and 
                    src:inby is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inrt), '0'), 38, 10) is null and 
                    src:inrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inyn), '0'), 38, 10) is null and 
                    src:inyn is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:minu), '0'), 38, 10) is null and 
                    src:minu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obpr), '0'), 38, 10) is null and 
                    src:obpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obpr_ref_compnr), '0'), 38, 10) is null and 
                    src:obpr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oemn_ref_compnr), '0'), 38, 10) is null and 
                    src:oemn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbsm), '0'), 38, 10) is null and 
                    src:pbsm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcch), '0'), 38, 10) is null and 
                    src:pcch is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcll_ref_compnr), '0'), 38, 10) is null and 
                    src:pcll_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcln), '0'), 38, 10) is null and 
                    src:pcln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcon_ref_compnr), '0'), 38, 10) is null and 
                    src:pcon_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pect), '1900-01-01')) is null and 
                    src:pect is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is null and 
                    src:pfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:pfcn_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pfct), '1900-01-01')) is null and 
                    src:pfct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prpr), '0'), 38, 10) is null and 
                    src:prpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prpr_ref_compnr), '0'), 38, 10) is null and 
                    src:prpr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:repr_wrty), '0'), 38, 10) is null and 
                    src:repr_wrty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfcl), '0'), 38, 10) is null and 
                    src:rfcl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfco_ref_compnr), '0'), 38, 10) is null and 
                    src:rfco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfco_rfcl_ref_compnr), '0'), 38, 10) is null and 
                    src:rfco_rfcl_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rlct), '1900-01-01')) is null and 
                    src:rlct is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rpct), '1900-01-01')) is null and 
                    src:rpct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rprl_ref_compnr), '0'), 38, 10) is null and 
                    src:rprl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rqct_ref_compnr), '0'), 38, 10) is null and 
                    src:rqct_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rtct), '1900-01-01')) is null and 
                    src:rtct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbct_ref_compnr), '0'), 38, 10) is null and 
                    src:sbct_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scgr_ref_compnr), '0'), 38, 10) is null and 
                    src:scgr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfad_ref_compnr), '0'), 38, 10) is null and 
                    src:sfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:slct), '1900-01-01')) is null and 
                    src:slct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sltn_ref_compnr), '0'), 38, 10) is null and 
                    src:sltn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:soad_ref_compnr), '0'), 38, 10) is null and 
                    src:soad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:socn_ref_compnr), '0'), 38, 10) is null and 
                    src:socn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:susr_ref_compnr), '0'), 38, 10) is null and 
                    src:susr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:susr_susn_ref_compnr), '0'), 38, 10) is null and 
                    src:susr_susn_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:svct), '1900-01-01')) is null and 
                    src:svct is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtd), '0'), 38, 10) is null and 
                    src:txtd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtd_ref_compnr), '0'), 38, 10) is null and 
                    src:txtd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txte), '0'), 38, 10) is null and 
                    src:txte is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txte_ref_compnr), '0'), 38, 10) is null and 
                    src:txte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtf), '0'), 38, 10) is null and 
                    src:txtf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtf_ref_compnr), '0'), 38, 10) is null and 
                    src:txtf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtg), '0'), 38, 10) is null and 
                    src:txtg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtg_ref_compnr), '0'), 38, 10) is null and 
                    src:txtg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txth), '0'), 38, 10) is null and 
                    src:txth is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txth_ref_compnr), '0'), 38, 10) is null and 
                    src:txth_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtk), '0'), 38, 10) is null and 
                    src:txtk is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtk_ref_compnr), '0'), 38, 10) is null and 
                    src:txtk_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubad_ref_compnr), '0'), 38, 10) is null and 
                    src:ubad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ubbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ubcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubop_ref_compnr), '0'), 38, 10) is null and 
                    src:ubop_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ustm), '0'), 38, 10) is null and 
                    src:ustm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wait), '0'), 38, 10) is null and 
                    src:wait is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)