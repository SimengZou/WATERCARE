CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHINH220_ERROR AS SELECT src, 'LN_WHINH220' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acpn), '0'), 38, 10) is null and 
                    src:acpn is not null) THEN
                    'acpn contains a non-numeric value : \'' || AS_VARCHAR(src:acpn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acvt), '0'), 38, 10) is null and 
                    src:acvt is not null) THEN
                    'acvt contains a non-numeric value : \'' || AS_VARCHAR(src:acvt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:addt), '1900-01-01')) is null and 
                    src:addt is not null) THEN
                    'addt contains a non-timestamp value : \'' || AS_VARCHAR(src:addt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is null and 
                    src:atse_ref_compnr is not null) THEN
                    'atse_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:atse_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bcko), '0'), 38, 10) is null and 
                    src:bcko is not null) THEN
                    'bcko contains a non-numeric value : \'' || AS_VARCHAR(src:bcko) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bflh), '0'), 38, 10) is null and 
                    src:bflh is not null) THEN
                    'bflh contains a non-numeric value : \'' || AS_VARCHAR(src:bflh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgxc), '0'), 38, 10) is null and 
                    src:bgxc is not null) THEN
                    'bgxc contains a non-numeric value : \'' || AS_VARCHAR(src:bgxc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blck), '0'), 38, 10) is null and 
                    src:blck is not null) THEN
                    'blck contains a non-numeric value : \'' || AS_VARCHAR(src:blck) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casi_ref_compnr), '0'), 38, 10) is null and 
                    src:casi_ref_compnr is not null) THEN
                    'casi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:casi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdck), '0'), 38, 10) is null and 
                    src:cdck is not null) THEN
                    'cdck contains a non-numeric value : \'' || AS_VARCHAR(src:cdck) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cncl), '0'), 38, 10) is null and 
                    src:cncl is not null) THEN
                    'cncl contains a non-numeric value : \'' || AS_VARCHAR(src:cncl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comp), '0'), 38, 10) is null and 
                    src:comp is not null) THEN
                    'comp contains a non-numeric value : \'' || AS_VARCHAR(src:comp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csvc_ref_compnr), '0'), 38, 10) is null and 
                    src:csvc_ref_compnr is not null) THEN
                    'csvc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csvc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csvl), '0'), 38, 10) is null and 
                    src:csvl is not null) THEN
                    'csvl contains a non-numeric value : \'' || AS_VARCHAR(src:csvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) THEN
                    'effn contains a non-numeric value : \'' || AS_VARCHAR(src:effn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) THEN
                    'effn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:effn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcco_ref_compnr), '0'), 38, 10) is null and 
                    src:fcco_ref_compnr is not null) THEN
                    'fcco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fcco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fisc), '0'), 38, 10) is null and 
                    src:fisc is not null) THEN
                    'fisc contains a non-numeric value : \'' || AS_VARCHAR(src:fisc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fmln), '0'), 38, 10) is null and 
                    src:fmln is not null) THEN
                    'fmln contains a non-numeric value : \'' || AS_VARCHAR(src:fmln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fprj_ref_compnr), '0'), 38, 10) is null and 
                    src:fprj_ref_compnr is not null) THEN
                    'fprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gefo), '0'), 38, 10) is null and 
                    src:gefo is not null) THEN
                    'gefo contains a non-numeric value : \'' || AS_VARCHAR(src:gefo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hstq), '0'), 38, 10) is null and 
                    src:hstq is not null) THEN
                    'hstq contains a non-numeric value : \'' || AS_VARCHAR(src:hstq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:huwt), '0'), 38, 10) is null and 
                    src:huwt is not null) THEN
                    'huwt contains a non-numeric value : \'' || AS_VARCHAR(src:huwt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) THEN
                    'ifbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ifbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inup), '0'), 38, 10) is null and 
                    src:inup is not null) THEN
                    'inup contains a non-numeric value : \'' || AS_VARCHAR(src:inup) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iown), '0'), 38, 10) is null and 
                    src:iown is not null) THEN
                    'iown contains a non-numeric value : \'' || AS_VARCHAR(src:iown) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipay), '0'), 38, 10) is null and 
                    src:ipay is not null) THEN
                    'ipay contains a non-numeric value : \'' || AS_VARCHAR(src:ipay) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iscn), '0'), 38, 10) is null and 
                    src:iscn is not null) THEN
                    'iscn contains a non-numeric value : \'' || AS_VARCHAR(src:iscn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:istr), '0'), 38, 10) is null and 
                    src:istr is not null) THEN
                    'istr contains a non-numeric value : \'' || AS_VARCHAR(src:istr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_atse_ref_compnr), '0'), 38, 10) is null and 
                    src:item_atse_ref_compnr is not null) THEN
                    'item_atse_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_atse_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_clot_ref_compnr), '0'), 38, 10) is null and 
                    src:item_clot_ref_compnr is not null) THEN
                    'item_clot_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_clot_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iubw), '0'), 38, 10) is null and 
                    src:iubw is not null) THEN
                    'iubw contains a non-numeric value : \'' || AS_VARCHAR(src:iubw) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsel), '0'), 38, 10) is null and 
                    src:lsel is not null) THEN
                    'lsel contains a non-numeric value : \'' || AS_VARCHAR(src:lsel) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsta), '0'), 38, 10) is null and 
                    src:lsta is not null) THEN
                    'lsta contains a non-numeric value : \'' || AS_VARCHAR(src:lsta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:masp), '0'), 38, 10) is null and 
                    src:masp is not null) THEN
                    'masp contains a non-numeric value : \'' || AS_VARCHAR(src:masp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oarq), '0'), 38, 10) is null and 
                    src:oarq is not null) THEN
                    'oarq contains a non-numeric value : \'' || AS_VARCHAR(src:oarq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocur_ref_compnr), '0'), 38, 10) is null and 
                    src:ocur_ref_compnr is not null) THEN
                    'ocur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ocur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oorg), '0'), 38, 10) is null and 
                    src:oorg is not null) THEN
                    'oorg contains a non-numeric value : \'' || AS_VARCHAR(src:oorg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oorg_orno_oset_ref_compnr), '0'), 38, 10) is null and 
                    src:oorg_orno_oset_ref_compnr is not null) THEN
                    'oorg_orno_oset_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:oorg_orno_oset_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orpr), '0'), 38, 10) is null and 
                    src:orpr is not null) THEN
                    'orpr contains a non-numeric value : \'' || AS_VARCHAR(src:orpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orun_ref_compnr), '0'), 38, 10) is null and 
                    src:orun_ref_compnr is not null) THEN
                    'orun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:orun_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oset), '0'), 38, 10) is null and 
                    src:oset is not null) THEN
                    'oset contains a non-numeric value : \'' || AS_VARCHAR(src:oset) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ovfp), '0'), 38, 10) is null and 
                    src:ovfp is not null) THEN
                    'ovfp contains a non-numeric value : \'' || AS_VARCHAR(src:ovfp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ovlp), '0'), 38, 10) is null and 
                    src:ovlp is not null) THEN
                    'ovlp contains a non-numeric value : \'' || AS_VARCHAR(src:ovlp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owns), '0'), 38, 10) is null and 
                    src:owns is not null) THEN
                    'owns contains a non-numeric value : \'' || AS_VARCHAR(src:owns) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paym), '0'), 38, 10) is null and 
                    src:paym is not null) THEN
                    'paym contains a non-numeric value : \'' || AS_VARCHAR(src:paym) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pddt), '1900-01-01')) is null and 
                    src:pddt is not null) THEN
                    'pddt contains a non-timestamp value : \'' || AS_VARCHAR(src:pddt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pkdb), '0'), 38, 10) is null and 
                    src:pkdb is not null) THEN
                    'pkdb contains a non-numeric value : \'' || AS_VARCHAR(src:pkdb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pkdf_ref_compnr), '0'), 38, 10) is null and 
                    src:pkdf_ref_compnr is not null) THEN
                    'pkdf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pkdf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) THEN
                    'prdt contains a non-timestamp value : \'' || AS_VARCHAR(src:prdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio), '0'), 38, 10) is null and 
                    src:prio is not null) THEN
                    'prio contains a non-numeric value : \'' || AS_VARCHAR(src:prio) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prjp), '0'), 38, 10) is null and 
                    src:prjp is not null) THEN
                    'prjp contains a non-numeric value : \'' || AS_VARCHAR(src:prjp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qacp), '0'), 38, 10) is null and 
                    src:qacp is not null) THEN
                    'qacp contains a non-numeric value : \'' || AS_VARCHAR(src:qacp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qact), '0'), 38, 10) is null and 
                    src:qact is not null) THEN
                    'qact contains a non-numeric value : \'' || AS_VARCHAR(src:qact) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadp), '0'), 38, 10) is null and 
                    src:qadp is not null) THEN
                    'qadp contains a non-numeric value : \'' || AS_VARCHAR(src:qadp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadv), '0'), 38, 10) is null and 
                    src:qadv is not null) THEN
                    'qadv contains a non-numeric value : \'' || AS_VARCHAR(src:qadv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapp), '0'), 38, 10) is null and 
                    src:qapp is not null) THEN
                    'qapp contains a non-numeric value : \'' || AS_VARCHAR(src:qapp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapr), '0'), 38, 10) is null and 
                    src:qapr is not null) THEN
                    'qapr contains a non-numeric value : \'' || AS_VARCHAR(src:qapr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcad), '0'), 38, 10) is null and 
                    src:qcad is not null) THEN
                    'qcad contains a non-numeric value : \'' || AS_VARCHAR(src:qcad) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcap), '0'), 38, 10) is null and 
                    src:qcap is not null) THEN
                    'qcap contains a non-numeric value : \'' || AS_VARCHAR(src:qcap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcnl), '0'), 38, 10) is null and 
                    src:qcnl is not null) THEN
                    'qcnl contains a non-numeric value : \'' || AS_VARCHAR(src:qcnl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcnp), '0'), 38, 10) is null and 
                    src:qcnp is not null) THEN
                    'qcnp contains a non-numeric value : \'' || AS_VARCHAR(src:qcnp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnpu), '0'), 38, 10) is null and 
                    src:qnpu is not null) THEN
                    'qnpu contains a non-numeric value : \'' || AS_VARCHAR(src:qnpu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnse), '0'), 38, 10) is null and 
                    src:qnse is not null) THEN
                    'qnse contains a non-numeric value : \'' || AS_VARCHAR(src:qnse) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnsh), '0'), 38, 10) is null and 
                    src:qnsh is not null) THEN
                    'qnsh contains a non-numeric value : \'' || AS_VARCHAR(src:qnsh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnsp), '0'), 38, 10) is null and 
                    src:qnsp is not null) THEN
                    'qnsp contains a non-numeric value : \'' || AS_VARCHAR(src:qnsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoop), '0'), 38, 10) is null and 
                    src:qoop is not null) THEN
                    'qoop contains a non-numeric value : \'' || AS_VARCHAR(src:qoop) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor), '0'), 38, 10) is null and 
                    src:qoor is not null) THEN
                    'qoor contains a non-numeric value : \'' || AS_VARCHAR(src:qoor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qopu), '0'), 38, 10) is null and 
                    src:qopu is not null) THEN
                    'qopu contains a non-numeric value : \'' || AS_VARCHAR(src:qopu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qord), '0'), 38, 10) is null and 
                    src:qord is not null) THEN
                    'qord contains a non-numeric value : \'' || AS_VARCHAR(src:qord) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoro), '0'), 38, 10) is null and 
                    src:qoro is not null) THEN
                    'qoro contains a non-numeric value : \'' || AS_VARCHAR(src:qoro) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qorp), '0'), 38, 10) is null and 
                    src:qorp is not null) THEN
                    'qorp contains a non-numeric value : \'' || AS_VARCHAR(src:qorp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qova), '0'), 38, 10) is null and 
                    src:qova is not null) THEN
                    'qova contains a non-numeric value : \'' || AS_VARCHAR(src:qova) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qovd), '0'), 38, 10) is null and 
                    src:qovd is not null) THEN
                    'qovd contains a non-numeric value : \'' || AS_VARCHAR(src:qovd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qovp), '0'), 38, 10) is null and 
                    src:qovp is not null) THEN
                    'qovp contains a non-numeric value : \'' || AS_VARCHAR(src:qovp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpic), '0'), 38, 10) is null and 
                    src:qpic is not null) THEN
                    'qpic contains a non-numeric value : \'' || AS_VARCHAR(src:qpic) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpip), '0'), 38, 10) is null and 
                    src:qpip is not null) THEN
                    'qpip contains a non-numeric value : \'' || AS_VARCHAR(src:qpip) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qprv), '0'), 38, 10) is null and 
                    src:qprv is not null) THEN
                    'qprv contains a non-numeric value : \'' || AS_VARCHAR(src:qprv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpss), '0'), 38, 10) is null and 
                    src:qpss is not null) THEN
                    'qpss contains a non-numeric value : \'' || AS_VARCHAR(src:qpss) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpsv), '0'), 38, 10) is null and 
                    src:qpsv is not null) THEN
                    'qpsv contains a non-numeric value : \'' || AS_VARCHAR(src:qpsv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrej), '0'), 38, 10) is null and 
                    src:qrej is not null) THEN
                    'qrej contains a non-numeric value : \'' || AS_VARCHAR(src:qrej) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrel), '0'), 38, 10) is null and 
                    src:qrel is not null) THEN
                    'qrel contains a non-numeric value : \'' || AS_VARCHAR(src:qrel) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrep), '0'), 38, 10) is null and 
                    src:qrep is not null) THEN
                    'qrep contains a non-numeric value : \'' || AS_VARCHAR(src:qrep) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qreq), '0'), 38, 10) is null and 
                    src:qreq is not null) THEN
                    'qreq contains a non-numeric value : \'' || AS_VARCHAR(src:qreq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrpu), '0'), 38, 10) is null and 
                    src:qrpu is not null) THEN
                    'qrpu contains a non-numeric value : \'' || AS_VARCHAR(src:qrpu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrsc), '0'), 38, 10) is null and 
                    src:qrsc is not null) THEN
                    'qrsc contains a non-numeric value : \'' || AS_VARCHAR(src:qrsc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrsp), '0'), 38, 10) is null and 
                    src:qrsp is not null) THEN
                    'qrsp contains a non-numeric value : \'' || AS_VARCHAR(src:qrsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscl), '0'), 38, 10) is null and 
                    src:qscl is not null) THEN
                    'qscl contains a non-numeric value : \'' || AS_VARCHAR(src:qscl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscp), '0'), 38, 10) is null and 
                    src:qscp is not null) THEN
                    'qscp contains a non-numeric value : \'' || AS_VARCHAR(src:qscp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qshp), '0'), 38, 10) is null and 
                    src:qshp is not null) THEN
                    'qshp contains a non-numeric value : \'' || AS_VARCHAR(src:qshp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qspu), '0'), 38, 10) is null and 
                    src:qspu is not null) THEN
                    'qspu contains a non-numeric value : \'' || AS_VARCHAR(src:qspu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rorg), '0'), 38, 10) is null and 
                    src:rorg is not null) THEN
                    'rorg contains a non-numeric value : \'' || AS_VARCHAR(src:rorg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc), '0'), 38, 10) is null and 
                    src:rowc is not null) THEN
                    'rowc contains a non-numeric value : \'' || AS_VARCHAR(src:rowc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is null and 
                    src:rowc_ref_compnr is not null) THEN
                    'rowc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rowc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is null and 
                    src:rown_ref_compnr is not null) THEN
                    'rown_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rown_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpon), '0'), 38, 10) is null and 
                    src:rpon is not null) THEN
                    'rpon contains a non-numeric value : \'' || AS_VARCHAR(src:rpon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rqpu), '0'), 38, 10) is null and 
                    src:rqpu is not null) THEN
                    'rqpu contains a non-numeric value : \'' || AS_VARCHAR(src:rqpu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) THEN
                    'rseq contains a non-numeric value : \'' || AS_VARCHAR(src:rseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rush), '0'), 38, 10) is null and 
                    src:rush is not null) THEN
                    'rush contains a non-numeric value : \'' || AS_VARCHAR(src:rush) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scon), '0'), 38, 10) is null and 
                    src:scon is not null) THEN
                    'scon contains a non-numeric value : \'' || AS_VARCHAR(src:scon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) THEN
                    'seqn contains a non-numeric value : \'' || AS_VARCHAR(src:seqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shrt), '0'), 38, 10) is null and 
                    src:shrt is not null) THEN
                    'shrt contains a non-numeric value : \'' || AS_VARCHAR(src:shrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssts), '0'), 38, 10) is null and 
                    src:ssts is not null) THEN
                    'ssts contains a non-numeric value : \'' || AS_VARCHAR(src:ssts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_dlpt_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_dlpt_ref_compnr is not null) THEN
                    'stad_dlpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_dlpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sups), '0'), 38, 10) is null and 
                    src:sups is not null) THEN
                    'sups contains a non-numeric value : \'' || AS_VARCHAR(src:sups) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcco_ref_compnr), '0'), 38, 10) is null and 
                    src:tcco_ref_compnr is not null) THEN
                    'tcco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tcco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprj_ref_compnr), '0'), 38, 10) is null and 
                    src:tprj_ref_compnr is not null) THEN
                    'tprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtn), '0'), 38, 10) is null and 
                    src:txtn is not null) THEN
                    'txtn contains a non-numeric value : \'' || AS_VARCHAR(src:txtn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtn_ref_compnr), '0'), 38, 10) is null and 
                    src:txtn_ref_compnr is not null) THEN
                    'txtn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubin), '0'), 38, 10) is null and 
                    src:ubin is not null) THEN
                    'ubin contains a non-numeric value : \'' || AS_VARCHAR(src:ubin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uwop), '0'), 38, 10) is null and 
                    src:uwop is not null) THEN
                    'uwop contains a non-numeric value : \'' || AS_VARCHAR(src:uwop) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wmss), '0'), 38, 10) is null and 
                    src:wmss is not null) THEN
                    'wmss contains a non-numeric value : \'' || AS_VARCHAR(src:wmss) || '\'' WHEN 
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
                src:seqn,
                src:pono,
                src:compnr,
                src:oorg  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINH220 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acpn), '0'), 38, 10) is null and 
                    src:acpn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acvt), '0'), 38, 10) is null and 
                    src:acvt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:addt), '1900-01-01')) is null and 
                    src:addt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is null and 
                    src:atse_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bcko), '0'), 38, 10) is null and 
                    src:bcko is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bflh), '0'), 38, 10) is null and 
                    src:bflh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgxc), '0'), 38, 10) is null and 
                    src:bgxc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blck), '0'), 38, 10) is null and 
                    src:blck is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casi_ref_compnr), '0'), 38, 10) is null and 
                    src:casi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdck), '0'), 38, 10) is null and 
                    src:cdck is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cncl), '0'), 38, 10) is null and 
                    src:cncl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comp), '0'), 38, 10) is null and 
                    src:comp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csvc_ref_compnr), '0'), 38, 10) is null and 
                    src:csvc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csvl), '0'), 38, 10) is null and 
                    src:csvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcco_ref_compnr), '0'), 38, 10) is null and 
                    src:fcco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fisc), '0'), 38, 10) is null and 
                    src:fisc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fmln), '0'), 38, 10) is null and 
                    src:fmln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fprj_ref_compnr), '0'), 38, 10) is null and 
                    src:fprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gefo), '0'), 38, 10) is null and 
                    src:gefo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hstq), '0'), 38, 10) is null and 
                    src:hstq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:huwt), '0'), 38, 10) is null and 
                    src:huwt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inup), '0'), 38, 10) is null and 
                    src:inup is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iown), '0'), 38, 10) is null and 
                    src:iown is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipay), '0'), 38, 10) is null and 
                    src:ipay is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iscn), '0'), 38, 10) is null and 
                    src:iscn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:istr), '0'), 38, 10) is null and 
                    src:istr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_atse_ref_compnr), '0'), 38, 10) is null and 
                    src:item_atse_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_clot_ref_compnr), '0'), 38, 10) is null and 
                    src:item_clot_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iubw), '0'), 38, 10) is null and 
                    src:iubw is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsel), '0'), 38, 10) is null and 
                    src:lsel is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsta), '0'), 38, 10) is null and 
                    src:lsta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:masp), '0'), 38, 10) is null and 
                    src:masp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oarq), '0'), 38, 10) is null and 
                    src:oarq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocur_ref_compnr), '0'), 38, 10) is null and 
                    src:ocur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oorg), '0'), 38, 10) is null and 
                    src:oorg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oorg_orno_oset_ref_compnr), '0'), 38, 10) is null and 
                    src:oorg_orno_oset_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orpr), '0'), 38, 10) is null and 
                    src:orpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orun_ref_compnr), '0'), 38, 10) is null and 
                    src:orun_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oset), '0'), 38, 10) is null and 
                    src:oset is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ovfp), '0'), 38, 10) is null and 
                    src:ovfp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ovlp), '0'), 38, 10) is null and 
                    src:ovlp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owns), '0'), 38, 10) is null and 
                    src:owns is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paym), '0'), 38, 10) is null and 
                    src:paym is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pddt), '1900-01-01')) is null and 
                    src:pddt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pkdb), '0'), 38, 10) is null and 
                    src:pkdb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pkdf_ref_compnr), '0'), 38, 10) is null and 
                    src:pkdf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio), '0'), 38, 10) is null and 
                    src:prio is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prjp), '0'), 38, 10) is null and 
                    src:prjp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qacp), '0'), 38, 10) is null and 
                    src:qacp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qact), '0'), 38, 10) is null and 
                    src:qact is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadp), '0'), 38, 10) is null and 
                    src:qadp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadv), '0'), 38, 10) is null and 
                    src:qadv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapp), '0'), 38, 10) is null and 
                    src:qapp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapr), '0'), 38, 10) is null and 
                    src:qapr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcad), '0'), 38, 10) is null and 
                    src:qcad is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcap), '0'), 38, 10) is null and 
                    src:qcap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcnl), '0'), 38, 10) is null and 
                    src:qcnl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcnp), '0'), 38, 10) is null and 
                    src:qcnp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnpu), '0'), 38, 10) is null and 
                    src:qnpu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnse), '0'), 38, 10) is null and 
                    src:qnse is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnsh), '0'), 38, 10) is null and 
                    src:qnsh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnsp), '0'), 38, 10) is null and 
                    src:qnsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoop), '0'), 38, 10) is null and 
                    src:qoop is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor), '0'), 38, 10) is null and 
                    src:qoor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qopu), '0'), 38, 10) is null and 
                    src:qopu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qord), '0'), 38, 10) is null and 
                    src:qord is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoro), '0'), 38, 10) is null and 
                    src:qoro is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qorp), '0'), 38, 10) is null and 
                    src:qorp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qova), '0'), 38, 10) is null and 
                    src:qova is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qovd), '0'), 38, 10) is null and 
                    src:qovd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qovp), '0'), 38, 10) is null and 
                    src:qovp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpic), '0'), 38, 10) is null and 
                    src:qpic is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpip), '0'), 38, 10) is null and 
                    src:qpip is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qprv), '0'), 38, 10) is null and 
                    src:qprv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpss), '0'), 38, 10) is null and 
                    src:qpss is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpsv), '0'), 38, 10) is null and 
                    src:qpsv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrej), '0'), 38, 10) is null and 
                    src:qrej is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrel), '0'), 38, 10) is null and 
                    src:qrel is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrep), '0'), 38, 10) is null and 
                    src:qrep is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qreq), '0'), 38, 10) is null and 
                    src:qreq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrpu), '0'), 38, 10) is null and 
                    src:qrpu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrsc), '0'), 38, 10) is null and 
                    src:qrsc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrsp), '0'), 38, 10) is null and 
                    src:qrsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscl), '0'), 38, 10) is null and 
                    src:qscl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qscp), '0'), 38, 10) is null and 
                    src:qscp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qshp), '0'), 38, 10) is null and 
                    src:qshp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qspu), '0'), 38, 10) is null and 
                    src:qspu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rorg), '0'), 38, 10) is null and 
                    src:rorg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc), '0'), 38, 10) is null and 
                    src:rowc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is null and 
                    src:rowc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is null and 
                    src:rown_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpon), '0'), 38, 10) is null and 
                    src:rpon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rqpu), '0'), 38, 10) is null and 
                    src:rqpu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rush), '0'), 38, 10) is null and 
                    src:rush is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scon), '0'), 38, 10) is null and 
                    src:scon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shrt), '0'), 38, 10) is null and 
                    src:shrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssts), '0'), 38, 10) is null and 
                    src:ssts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_dlpt_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_dlpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sups), '0'), 38, 10) is null and 
                    src:sups is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcco_ref_compnr), '0'), 38, 10) is null and 
                    src:tcco_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprj_ref_compnr), '0'), 38, 10) is null and 
                    src:tprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtn), '0'), 38, 10) is null and 
                    src:txtn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtn_ref_compnr), '0'), 38, 10) is null and 
                    src:txtn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubin), '0'), 38, 10) is null and 
                    src:ubin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uwop), '0'), 38, 10) is null and 
                    src:uwop is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wmss), '0'), 38, 10) is null and 
                    src:wmss is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)