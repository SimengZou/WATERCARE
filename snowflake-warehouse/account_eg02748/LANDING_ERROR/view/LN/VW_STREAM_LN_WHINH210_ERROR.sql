CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHINH210_ERROR AS SELECT src, 'LN_WHINH210' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acvt), '0'), 38, 10) is null and 
                    src:acvt is not null) THEN
                    'acvt contains a non-numeric value : \'' || AS_VARCHAR(src:acvt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is null and 
                    src:atse_ref_compnr is not null) THEN
                    'atse_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:atse_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) THEN
                    'cmnf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmnf_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fire), '0'), 38, 10) is null and 
                    src:fire is not null) THEN
                    'fire contains a non-numeric value : \'' || AS_VARCHAR(src:fire) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fmln), '0'), 38, 10) is null and 
                    src:fmln is not null) THEN
                    'fmln contains a non-numeric value : \'' || AS_VARCHAR(src:fmln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fprj_ref_compnr), '0'), 38, 10) is null and 
                    src:fprj_ref_compnr is not null) THEN
                    'fprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fshl), '0'), 38, 10) is null and 
                    src:fshl is not null) THEN
                    'fshl contains a non-numeric value : \'' || AS_VARCHAR(src:fshl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gefo), '0'), 38, 10) is null and 
                    src:gefo is not null) THEN
                    'gefo contains a non-numeric value : \'' || AS_VARCHAR(src:gefo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hstd), '0'), 38, 10) is null and 
                    src:hstd is not null) THEN
                    'hstd contains a non-numeric value : \'' || AS_VARCHAR(src:hstd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hstq), '0'), 38, 10) is null and 
                    src:hstq is not null) THEN
                    'hstq contains a non-numeric value : \'' || AS_VARCHAR(src:hstq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insp), '0'), 38, 10) is null and 
                    src:insp is not null) THEN
                    'insp contains a non-numeric value : \'' || AS_VARCHAR(src:insp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inup), '0'), 38, 10) is null and 
                    src:inup is not null) THEN
                    'inup contains a non-numeric value : \'' || AS_VARCHAR(src:inup) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipay), '0'), 38, 10) is null and 
                    src:ipay is not null) THEN
                    'ipay contains a non-numeric value : \'' || AS_VARCHAR(src:ipay) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_atse_ref_compnr), '0'), 38, 10) is null and 
                    src:item_atse_ref_compnr is not null) THEN
                    'item_atse_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_atse_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_clot_ref_compnr), '0'), 38, 10) is null and 
                    src:item_clot_ref_compnr is not null) THEN
                    'item_clot_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_clot_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_pkdf_ref_compnr), '0'), 38, 10) is null and 
                    src:item_pkdf_ref_compnr is not null) THEN
                    'item_pkdf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_pkdf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iubw), '0'), 38, 10) is null and 
                    src:iubw is not null) THEN
                    'iubw contains a non-numeric value : \'' || AS_VARCHAR(src:iubw) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kbpr), '0'), 38, 10) is null and 
                    src:kbpr is not null) THEN
                    'kbpr contains a non-numeric value : \'' || AS_VARCHAR(src:kbpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lccl_ref_compnr), '0'), 38, 10) is null and 
                    src:lccl_ref_compnr is not null) THEN
                    'lccl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lccl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsel), '0'), 38, 10) is null and 
                    src:lsel is not null) THEN
                    'lsel contains a non-numeric value : \'' || AS_VARCHAR(src:lsel) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsta), '0'), 38, 10) is null and 
                    src:lsta is not null) THEN
                    'lsta contains a non-numeric value : \'' || AS_VARCHAR(src:lsta) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ownr_ref_compnr), '0'), 38, 10) is null and 
                    src:ownr_ref_compnr is not null) THEN
                    'ownr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ownr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owns), '0'), 38, 10) is null and 
                    src:owns is not null) THEN
                    'owns contains a non-numeric value : \'' || AS_VARCHAR(src:owns) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paym), '0'), 38, 10) is null and 
                    src:paym is not null) THEN
                    'paym contains a non-numeric value : \'' || AS_VARCHAR(src:paym) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pkdf_ref_compnr), '0'), 38, 10) is null and 
                    src:pkdf_ref_compnr is not null) THEN
                    'pkdf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pkdf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) THEN
                    'prdt contains a non-timestamp value : \'' || AS_VARCHAR(src:prdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prjp), '0'), 38, 10) is null and 
                    src:prjp is not null) THEN
                    'prjp contains a non-numeric value : \'' || AS_VARCHAR(src:prjp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psqp), '0'), 38, 10) is null and 
                    src:psqp is not null) THEN
                    'psqp contains a non-numeric value : \'' || AS_VARCHAR(src:psqp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psqu), '0'), 38, 10) is null and 
                    src:psqu is not null) THEN
                    'psqu contains a non-numeric value : \'' || AS_VARCHAR(src:psqu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadi), '0'), 38, 10) is null and 
                    src:qadi is not null) THEN
                    'qadi contains a non-numeric value : \'' || AS_VARCHAR(src:qadi) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapu), '0'), 38, 10) is null and 
                    src:qapu is not null) THEN
                    'qapu contains a non-numeric value : \'' || AS_VARCHAR(src:qapu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdep), '0'), 38, 10) is null and 
                    src:qdep is not null) THEN
                    'qdep contains a non-numeric value : \'' || AS_VARCHAR(src:qdep) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdes), '0'), 38, 10) is null and 
                    src:qdes is not null) THEN
                    'qdes contains a non-numeric value : \'' || AS_VARCHAR(src:qdes) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qgip), '0'), 38, 10) is null and 
                    src:qgip is not null) THEN
                    'qgip contains a non-numeric value : \'' || AS_VARCHAR(src:qgip) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qgit), '0'), 38, 10) is null and 
                    src:qgit is not null) THEN
                    'qgit contains a non-numeric value : \'' || AS_VARCHAR(src:qgit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiip), '0'), 38, 10) is null and 
                    src:qiip is not null) THEN
                    'qiip contains a non-numeric value : \'' || AS_VARCHAR(src:qiip) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiit), '0'), 38, 10) is null and 
                    src:qiit is not null) THEN
                    'qiit contains a non-numeric value : \'' || AS_VARCHAR(src:qiit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qopu), '0'), 38, 10) is null and 
                    src:qopu is not null) THEN
                    'qopu contains a non-numeric value : \'' || AS_VARCHAR(src:qopu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qorc), '0'), 38, 10) is null and 
                    src:qorc is not null) THEN
                    'qorc contains a non-numeric value : \'' || AS_VARCHAR(src:qorc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qord), '0'), 38, 10) is null and 
                    src:qord is not null) THEN
                    'qord contains a non-numeric value : \'' || AS_VARCHAR(src:qord) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoro), '0'), 38, 10) is null and 
                    src:qoro is not null) THEN
                    'qoro contains a non-numeric value : \'' || AS_VARCHAR(src:qoro) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qorp), '0'), 38, 10) is null and 
                    src:qorp is not null) THEN
                    'qorp contains a non-numeric value : \'' || AS_VARCHAR(src:qorp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qppu), '0'), 38, 10) is null and 
                    src:qppu is not null) THEN
                    'qppu contains a non-numeric value : \'' || AS_VARCHAR(src:qppu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpui), '0'), 38, 10) is null and 
                    src:qpui is not null) THEN
                    'qpui contains a non-numeric value : \'' || AS_VARCHAR(src:qpui) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpup), '0'), 38, 10) is null and 
                    src:qpup is not null) THEN
                    'qpup contains a non-numeric value : \'' || AS_VARCHAR(src:qpup) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qput), '0'), 38, 10) is null and 
                    src:qput is not null) THEN
                    'qput contains a non-numeric value : \'' || AS_VARCHAR(src:qput) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrec), '0'), 38, 10) is null and 
                    src:qrec is not null) THEN
                    'qrec contains a non-numeric value : \'' || AS_VARCHAR(src:qrec) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrej), '0'), 38, 10) is null and 
                    src:qrej is not null) THEN
                    'qrej contains a non-numeric value : \'' || AS_VARCHAR(src:qrej) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrep), '0'), 38, 10) is null and 
                    src:qrep is not null) THEN
                    'qrep contains a non-numeric value : \'' || AS_VARCHAR(src:qrep) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrpu), '0'), 38, 10) is null and 
                    src:qrpu is not null) THEN
                    'qrpu contains a non-numeric value : \'' || AS_VARCHAR(src:qrpu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrsc), '0'), 38, 10) is null and 
                    src:qrsc is not null) THEN
                    'qrsc contains a non-numeric value : \'' || AS_VARCHAR(src:qrsc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrsp), '0'), 38, 10) is null and 
                    src:qrsp is not null) THEN
                    'qrsp contains a non-numeric value : \'' || AS_VARCHAR(src:qrsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbp), '0'), 38, 10) is null and 
                    src:qtbp is not null) THEN
                    'qtbp contains a non-numeric value : \'' || AS_VARCHAR(src:qtbp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbr), '0'), 38, 10) is null and 
                    src:qtbr is not null) THEN
                    'qtbr contains a non-numeric value : \'' || AS_VARCHAR(src:qtbr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) THEN
                    'rseq contains a non-numeric value : \'' || AS_VARCHAR(src:rseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) THEN
                    'seqn contains a non-numeric value : \'' || AS_VARCHAR(src:seqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) THEN
                    'sfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfbp_ref_compnr) || '\'' WHEN 
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
                                    
                src:compnr,
                src:pono,
                src:orno,
                src:oorg,
                src:seqn  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINH210 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acvt), '0'), 38, 10) is null and 
                    src:acvt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is null and 
                    src:atse_ref_compnr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fire), '0'), 38, 10) is null and 
                    src:fire is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fmln), '0'), 38, 10) is null and 
                    src:fmln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fprj_ref_compnr), '0'), 38, 10) is null and 
                    src:fprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fshl), '0'), 38, 10) is null and 
                    src:fshl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gefo), '0'), 38, 10) is null and 
                    src:gefo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hstd), '0'), 38, 10) is null and 
                    src:hstd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hstq), '0'), 38, 10) is null and 
                    src:hstq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insp), '0'), 38, 10) is null and 
                    src:insp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inup), '0'), 38, 10) is null and 
                    src:inup is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipay), '0'), 38, 10) is null and 
                    src:ipay is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_atse_ref_compnr), '0'), 38, 10) is null and 
                    src:item_atse_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_clot_ref_compnr), '0'), 38, 10) is null and 
                    src:item_clot_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_pkdf_ref_compnr), '0'), 38, 10) is null and 
                    src:item_pkdf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iubw), '0'), 38, 10) is null and 
                    src:iubw is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kbpr), '0'), 38, 10) is null and 
                    src:kbpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lccl_ref_compnr), '0'), 38, 10) is null and 
                    src:lccl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsel), '0'), 38, 10) is null and 
                    src:lsel is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsta), '0'), 38, 10) is null and 
                    src:lsta is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ownr_ref_compnr), '0'), 38, 10) is null and 
                    src:ownr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owns), '0'), 38, 10) is null and 
                    src:owns is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paym), '0'), 38, 10) is null and 
                    src:paym is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pkdf_ref_compnr), '0'), 38, 10) is null and 
                    src:pkdf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prjp), '0'), 38, 10) is null and 
                    src:prjp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psqp), '0'), 38, 10) is null and 
                    src:psqp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psqu), '0'), 38, 10) is null and 
                    src:psqu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadi), '0'), 38, 10) is null and 
                    src:qadi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadp), '0'), 38, 10) is null and 
                    src:qadp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadv), '0'), 38, 10) is null and 
                    src:qadv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapp), '0'), 38, 10) is null and 
                    src:qapp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapr), '0'), 38, 10) is null and 
                    src:qapr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapu), '0'), 38, 10) is null and 
                    src:qapu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdep), '0'), 38, 10) is null and 
                    src:qdep is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdes), '0'), 38, 10) is null and 
                    src:qdes is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qgip), '0'), 38, 10) is null and 
                    src:qgip is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qgit), '0'), 38, 10) is null and 
                    src:qgit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiip), '0'), 38, 10) is null and 
                    src:qiip is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiit), '0'), 38, 10) is null and 
                    src:qiit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qopu), '0'), 38, 10) is null and 
                    src:qopu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qorc), '0'), 38, 10) is null and 
                    src:qorc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qord), '0'), 38, 10) is null and 
                    src:qord is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoro), '0'), 38, 10) is null and 
                    src:qoro is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qorp), '0'), 38, 10) is null and 
                    src:qorp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qppu), '0'), 38, 10) is null and 
                    src:qppu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpui), '0'), 38, 10) is null and 
                    src:qpui is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpup), '0'), 38, 10) is null and 
                    src:qpup is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qput), '0'), 38, 10) is null and 
                    src:qput is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrec), '0'), 38, 10) is null and 
                    src:qrec is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrej), '0'), 38, 10) is null and 
                    src:qrej is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrep), '0'), 38, 10) is null and 
                    src:qrep is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrpu), '0'), 38, 10) is null and 
                    src:qrpu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrsc), '0'), 38, 10) is null and 
                    src:qrsc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrsp), '0'), 38, 10) is null and 
                    src:qrsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbp), '0'), 38, 10) is null and 
                    src:qtbp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbr), '0'), 38, 10) is null and 
                    src:qtbr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uwop), '0'), 38, 10) is null and 
                    src:uwop is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wmss), '0'), 38, 10) is null and 
                    src:wmss is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)