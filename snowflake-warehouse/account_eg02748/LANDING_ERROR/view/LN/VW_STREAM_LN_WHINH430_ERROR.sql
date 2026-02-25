CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHINH430_ERROR AS SELECT src, 'LN_WHINH430' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aclr), '0'), 38, 10) is null and 
                    src:aclr is not null) THEN
                    'aclr contains a non-numeric value : \'' || AS_VARCHAR(src:aclr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bolp), '0'), 38, 10) is null and 
                    src:bolp is not null) THEN
                    'bolp contains a non-numeric value : \'' || AS_VARCHAR(src:bolp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) THEN
                    'bptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:carr_ref_compnr), '0'), 38, 10) is null and 
                    src:carr_ref_compnr is not null) THEN
                    'carr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:carr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdat), '1900-01-01')) is null and 
                    src:cdat is not null) THEN
                    'cdat contains a non-timestamp value : \'' || AS_VARCHAR(src:cdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdun_ref_compnr), '0'), 38, 10) is null and 
                    src:cdun_ref_compnr is not null) THEN
                    'cdun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdun_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cham), '0'), 38, 10) is null and 
                    src:cham is not null) THEN
                    'cham contains a non-numeric value : \'' || AS_VARCHAR(src:cham) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) THEN
                    'crte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:curr_ref_compnr), '0'), 38, 10) is null and 
                    src:curr_ref_compnr is not null) THEN
                    'curr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:curr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwun_ref_compnr), '0'), 38, 10) is null and 
                    src:cwun_ref_compnr is not null) THEN
                    'cwun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwun_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:daby_ref_compnr), '0'), 38, 10) is null and 
                    src:daby_ref_compnr is not null) THEN
                    'daby_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:daby_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dacc), '0'), 38, 10) is null and 
                    src:dacc is not null) THEN
                    'dacc contains a non-numeric value : \'' || AS_VARCHAR(src:dacc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dadt), '1900-01-01')) is null and 
                    src:dadt is not null) THEN
                    'dadt contains a non-timestamp value : \'' || AS_VARCHAR(src:dadt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:darq), '0'), 38, 10) is null and 
                    src:darq is not null) THEN
                    'darq contains a non-numeric value : \'' || AS_VARCHAR(src:darq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delc_ref_compnr), '0'), 38, 10) is null and 
                    src:delc_ref_compnr is not null) THEN
                    'delc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:delc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delp), '0'), 38, 10) is null and 
                    src:delp is not null) THEN
                    'delp contains a non-numeric value : \'' || AS_VARCHAR(src:delp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depc), '0'), 38, 10) is null and 
                    src:depc is not null) THEN
                    'depc contains a non-numeric value : \'' || AS_VARCHAR(src:depc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depc_ref_compnr), '0'), 38, 10) is null and 
                    src:depc_ref_compnr is not null) THEN
                    'depc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:depc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpth), '0'), 38, 10) is null and 
                    src:dpth is not null) THEN
                    'dpth contains a non-numeric value : \'' || AS_VARCHAR(src:dpth) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt), '0'), 38, 10) is null and 
                    src:eamt is not null) THEN
                    'eamt contains a non-numeric value : \'' || AS_VARCHAR(src:eamt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:erdt), '1900-01-01')) is null and 
                    src:erdt is not null) THEN
                    'erdt contains a non-timestamp value : \'' || AS_VARCHAR(src:erdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flsp), '0'), 38, 10) is null and 
                    src:flsp is not null) THEN
                    'flsp contains a non-numeric value : \'' || AS_VARCHAR(src:flsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frsm), '0'), 38, 10) is null and 
                    src:frsm is not null) THEN
                    'frsm contains a non-numeric value : \'' || AS_VARCHAR(src:frsm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fval), '0'), 38, 10) is null and 
                    src:fval is not null) THEN
                    'fval contains a non-numeric value : \'' || AS_VARCHAR(src:fval) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fxwt), '0'), 38, 10) is null and 
                    src:fxwt is not null) THEN
                    'fxwt contains a non-numeric value : \'' || AS_VARCHAR(src:fxwt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hazm), '0'), 38, 10) is null and 
                    src:hazm is not null) THEN
                    'hazm contains a non-numeric value : \'' || AS_VARCHAR(src:hazm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hght), '0'), 38, 10) is null and 
                    src:hght is not null) THEN
                    'hght contains a non-numeric value : \'' || AS_VARCHAR(src:hght) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hubc), '0'), 38, 10) is null and 
                    src:hubc is not null) THEN
                    'hubc contains a non-numeric value : \'' || AS_VARCHAR(src:hubc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:huid_ref_compnr), '0'), 38, 10) is null and 
                    src:huid_ref_compnr is not null) THEN
                    'huid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:huid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ibpd), '0'), 38, 10) is null and 
                    src:ibpd is not null) THEN
                    'ibpd contains a non-numeric value : \'' || AS_VARCHAR(src:ibpd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iedi), '0'), 38, 10) is null and 
                    src:iedi is not null) THEN
                    'iedi contains a non-numeric value : \'' || AS_VARCHAR(src:iedi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iedi_ref_compnr), '0'), 38, 10) is null and 
                    src:iedi_ref_compnr is not null) THEN
                    'iedi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:iedi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incp), '0'), 38, 10) is null and 
                    src:incp is not null) THEN
                    'incp contains a non-numeric value : \'' || AS_VARCHAR(src:incp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:invd), '1900-01-01')) is null and 
                    src:invd is not null) THEN
                    'invd contains a non-timestamp value : \'' || AS_VARCHAR(src:invd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invn), '0'), 38, 10) is null and 
                    src:invn is not null) THEN
                    'invn contains a non-numeric value : \'' || AS_VARCHAR(src:invn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) THEN
                    'itbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:koch), '0'), 38, 10) is null and 
                    src:koch is not null) THEN
                    'koch contains a non-numeric value : \'' || AS_VARCHAR(src:koch) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:llsq), '0'), 38, 10) is null and 
                    src:llsq is not null) THEN
                    'llsq contains a non-numeric value : \'' || AS_VARCHAR(src:llsq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:load_cntr_ref_compnr), '0'), 38, 10) is null and 
                    src:load_cntr_ref_compnr is not null) THEN
                    'load_cntr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:load_cntr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:load_ref_compnr), '0'), 38, 10) is null and 
                    src:load_ref_compnr is not null) THEN
                    'load_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:load_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:manl), '0'), 38, 10) is null and 
                    src:manl is not null) THEN
                    'manl contains a non-numeric value : \'' || AS_VARCHAR(src:manl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mapr), '0'), 38, 10) is null and 
                    src:mapr is not null) THEN
                    'mapr contains a non-numeric value : \'' || AS_VARCHAR(src:mapr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:minv), '0'), 38, 10) is null and 
                    src:minv is not null) THEN
                    'minv contains a non-numeric value : \'' || AS_VARCHAR(src:minv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:motv_ref_compnr), '0'), 38, 10) is null and 
                    src:motv_ref_compnr is not null) THEN
                    'motv_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:motv_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ntwt), '0'), 38, 10) is null and 
                    src:ntwt is not null) THEN
                    'ntwt contains a non-numeric value : \'' || AS_VARCHAR(src:ntwt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oshc), '0'), 38, 10) is null and 
                    src:oshc is not null) THEN
                    'oshc contains a non-numeric value : \'' || AS_VARCHAR(src:oshc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pclp), '0'), 38, 10) is null and 
                    src:pclp is not null) THEN
                    'pclp contains a non-numeric value : \'' || AS_VARCHAR(src:pclp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcsp), '0'), 38, 10) is null and 
                    src:pcsp is not null) THEN
                    'pcsp contains a non-numeric value : \'' || AS_VARCHAR(src:pcsp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pdat), '1900-01-01')) is null and 
                    src:pdat is not null) THEN
                    'pdat contains a non-timestamp value : \'' || AS_VARCHAR(src:pdat) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pddt), '1900-01-01')) is null and 
                    src:pddt is not null) THEN
                    'pddt contains a non-timestamp value : \'' || AS_VARCHAR(src:pddt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfpr), '0'), 38, 10) is null and 
                    src:pfpr is not null) THEN
                    'pfpr contains a non-numeric value : \'' || AS_VARCHAR(src:pfpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:picm), '0'), 38, 10) is null and 
                    src:picm is not null) THEN
                    'picm contains a non-numeric value : \'' || AS_VARCHAR(src:picm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pitm_ref_compnr), '0'), 38, 10) is null and 
                    src:pitm_ref_compnr is not null) THEN
                    'pitm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pitm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prdn_ref_compnr), '0'), 38, 10) is null and 
                    src:prdn_ref_compnr is not null) THEN
                    'prdn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prdn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) THEN
                    'prdt contains a non-timestamp value : \'' || AS_VARCHAR(src:prdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psdd), '0'), 38, 10) is null and 
                    src:psdd is not null) THEN
                    'psdd contains a non-numeric value : \'' || AS_VARCHAR(src:psdd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psde), '0'), 38, 10) is null and 
                    src:psde is not null) THEN
                    'psde contains a non-numeric value : \'' || AS_VARCHAR(src:psde) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psts), '0'), 38, 10) is null and 
                    src:psts is not null) THEN
                    'psts contains a non-numeric value : \'' || AS_VARCHAR(src:psts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpac), '0'), 38, 10) is null and 
                    src:qpac is not null) THEN
                    'qpac contains a non-numeric value : \'' || AS_VARCHAR(src:qpac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rule_ref_compnr), '0'), 38, 10) is null and 
                    src:rule_ref_compnr is not null) THEN
                    'rule_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rule_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:saby_ref_compnr), '0'), 38, 10) is null and 
                    src:saby_ref_compnr is not null) THEN
                    'saby_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:saby_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sacc), '0'), 38, 10) is null and 
                    src:sacc is not null) THEN
                    'sacc contains a non-numeric value : \'' || AS_VARCHAR(src:sacc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sadt), '1900-01-01')) is null and 
                    src:sadt is not null) THEN
                    'sadt contains a non-timestamp value : \'' || AS_VARCHAR(src:sadt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sanl), '0'), 38, 10) is null and 
                    src:sanl is not null) THEN
                    'sanl contains a non-numeric value : \'' || AS_VARCHAR(src:sanl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sarq), '0'), 38, 10) is null and 
                    src:sarq is not null) THEN
                    'sarq contains a non-numeric value : \'' || AS_VARCHAR(src:sarq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scsh), '0'), 38, 10) is null and 
                    src:scsh is not null) THEN
                    'scsh contains a non-numeric value : \'' || AS_VARCHAR(src:scsh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdsh), '0'), 38, 10) is null and 
                    src:sdsh is not null) THEN
                    'sdsh contains a non-numeric value : \'' || AS_VARCHAR(src:sdsh) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdtf), '1900-01-01')) is null and 
                    src:sdtf is not null) THEN
                    'sdtf contains a non-timestamp value : \'' || AS_VARCHAR(src:sdtf) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdtt), '1900-01-01')) is null and 
                    src:sdtt is not null) THEN
                    'sdtt contains a non-timestamp value : \'' || AS_VARCHAR(src:sdtt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfad_ref_compnr), '0'), 38, 10) is null and 
                    src:sfad_ref_compnr is not null) THEN
                    'sfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfcp), '0'), 38, 10) is null and 
                    src:sfcp is not null) THEN
                    'sfcp contains a non-numeric value : \'' || AS_VARCHAR(src:sfcp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sflp), '0'), 38, 10) is null and 
                    src:sflp is not null) THEN
                    'sflp contains a non-numeric value : \'' || AS_VARCHAR(src:sflp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfsi_ref_compnr), '0'), 38, 10) is null and 
                    src:sfsi_ref_compnr is not null) THEN
                    'sfsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfty), '0'), 38, 10) is null and 
                    src:sfty is not null) THEN
                    'sfty contains a non-numeric value : \'' || AS_VARCHAR(src:sfty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shst), '0'), 38, 10) is null and 
                    src:shst is not null) THEN
                    'shst contains a non-numeric value : \'' || AS_VARCHAR(src:shst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shvc), '0'), 38, 10) is null and 
                    src:shvc is not null) THEN
                    'shvc contains a non-numeric value : \'' || AS_VARCHAR(src:shvc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:shvd), '1900-01-01')) is null and 
                    src:shvd is not null) THEN
                    'shvd contains a non-timestamp value : \'' || AS_VARCHAR(src:shvd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shvl), '0'), 38, 10) is null and 
                    src:shvl is not null) THEN
                    'shvl contains a non-numeric value : \'' || AS_VARCHAR(src:shvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shvo), '0'), 38, 10) is null and 
                    src:shvo is not null) THEN
                    'shvo contains a non-numeric value : \'' || AS_VARCHAR(src:shvo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:slsh), '0'), 38, 10) is null and 
                    src:slsh is not null) THEN
                    'slsh contains a non-numeric value : \'' || AS_VARCHAR(src:slsh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqdl), '0'), 38, 10) is null and 
                    src:sqdl is not null) THEN
                    'sqdl contains a non-numeric value : \'' || AS_VARCHAR(src:sqdl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqdp), '0'), 38, 10) is null and 
                    src:sqdp is not null) THEN
                    'sqdp contains a non-numeric value : \'' || AS_VARCHAR(src:sqdp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srsh), '0'), 38, 10) is null and 
                    src:srsh is not null) THEN
                    'srsh contains a non-numeric value : \'' || AS_VARCHAR(src:srsh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_dlpt_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_dlpt_ref_compnr is not null) THEN
                    'stad_dlpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_dlpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) THEN
                    'stad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcp), '0'), 38, 10) is null and 
                    src:stcp is not null) THEN
                    'stcp contains a non-numeric value : \'' || AS_VARCHAR(src:stcp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:stsi_ref_compnr is not null) THEN
                    'stsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stty), '0'), 38, 10) is null and 
                    src:stty is not null) THEN
                    'stty contains a non-numeric value : \'' || AS_VARCHAR(src:stty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:suba), '0'), 38, 10) is null and 
                    src:suba is not null) THEN
                    'suba contains a non-numeric value : \'' || AS_VARCHAR(src:suba) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcap), '0'), 38, 10) is null and 
                    src:tcap is not null) THEN
                    'tcap contains a non-numeric value : \'' || AS_VARCHAR(src:tcap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tccp), '0'), 38, 10) is null and 
                    src:tccp is not null) THEN
                    'tccp contains a non-numeric value : \'' || AS_VARCHAR(src:tccp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) THEN
                    'text contains a non-numeric value : \'' || AS_VARCHAR(src:text) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) THEN
                    'text_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:text_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:volm), '0'), 38, 10) is null and 
                    src:volm is not null) THEN
                    'volm contains a non-numeric value : \'' || AS_VARCHAR(src:volm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wdth), '0'), 38, 10) is null and 
                    src:wdth is not null) THEN
                    'wdth contains a non-numeric value : \'' || AS_VARCHAR(src:wdth) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wght), '0'), 38, 10) is null and 
                    src:wght is not null) THEN
                    'wght contains a non-numeric value : \'' || AS_VARCHAR(src:wght) || '\'' WHEN 
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
                                    
                src:shpm,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINH430 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aclr), '0'), 38, 10) is null and 
                    src:aclr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bolp), '0'), 38, 10) is null and 
                    src:bolp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:carr_ref_compnr), '0'), 38, 10) is null and 
                    src:carr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdat), '1900-01-01')) is null and 
                    src:cdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdun_ref_compnr), '0'), 38, 10) is null and 
                    src:cdun_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cham), '0'), 38, 10) is null and 
                    src:cham is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:curr_ref_compnr), '0'), 38, 10) is null and 
                    src:curr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwun_ref_compnr), '0'), 38, 10) is null and 
                    src:cwun_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:daby_ref_compnr), '0'), 38, 10) is null and 
                    src:daby_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dacc), '0'), 38, 10) is null and 
                    src:dacc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dadt), '1900-01-01')) is null and 
                    src:dadt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:darq), '0'), 38, 10) is null and 
                    src:darq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delc_ref_compnr), '0'), 38, 10) is null and 
                    src:delc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delp), '0'), 38, 10) is null and 
                    src:delp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depc), '0'), 38, 10) is null and 
                    src:depc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depc_ref_compnr), '0'), 38, 10) is null and 
                    src:depc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpth), '0'), 38, 10) is null and 
                    src:dpth is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt), '0'), 38, 10) is null and 
                    src:eamt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:erdt), '1900-01-01')) is null and 
                    src:erdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flsp), '0'), 38, 10) is null and 
                    src:flsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frsm), '0'), 38, 10) is null and 
                    src:frsm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fval), '0'), 38, 10) is null and 
                    src:fval is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fxwt), '0'), 38, 10) is null and 
                    src:fxwt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hazm), '0'), 38, 10) is null and 
                    src:hazm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hght), '0'), 38, 10) is null and 
                    src:hght is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hubc), '0'), 38, 10) is null and 
                    src:hubc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:huid_ref_compnr), '0'), 38, 10) is null and 
                    src:huid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ibpd), '0'), 38, 10) is null and 
                    src:ibpd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iedi), '0'), 38, 10) is null and 
                    src:iedi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iedi_ref_compnr), '0'), 38, 10) is null and 
                    src:iedi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incp), '0'), 38, 10) is null and 
                    src:incp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:invd), '1900-01-01')) is null and 
                    src:invd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invn), '0'), 38, 10) is null and 
                    src:invn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:koch), '0'), 38, 10) is null and 
                    src:koch is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:llsq), '0'), 38, 10) is null and 
                    src:llsq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:load_cntr_ref_compnr), '0'), 38, 10) is null and 
                    src:load_cntr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:load_ref_compnr), '0'), 38, 10) is null and 
                    src:load_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:manl), '0'), 38, 10) is null and 
                    src:manl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mapr), '0'), 38, 10) is null and 
                    src:mapr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:minv), '0'), 38, 10) is null and 
                    src:minv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:motv_ref_compnr), '0'), 38, 10) is null and 
                    src:motv_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ntwt), '0'), 38, 10) is null and 
                    src:ntwt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oshc), '0'), 38, 10) is null and 
                    src:oshc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pclp), '0'), 38, 10) is null and 
                    src:pclp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcsp), '0'), 38, 10) is null and 
                    src:pcsp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pdat), '1900-01-01')) is null and 
                    src:pdat is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pddt), '1900-01-01')) is null and 
                    src:pddt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfpr), '0'), 38, 10) is null and 
                    src:pfpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:picm), '0'), 38, 10) is null and 
                    src:picm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pitm_ref_compnr), '0'), 38, 10) is null and 
                    src:pitm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prdn_ref_compnr), '0'), 38, 10) is null and 
                    src:prdn_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psdd), '0'), 38, 10) is null and 
                    src:psdd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psde), '0'), 38, 10) is null and 
                    src:psde is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psts), '0'), 38, 10) is null and 
                    src:psts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpac), '0'), 38, 10) is null and 
                    src:qpac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rule_ref_compnr), '0'), 38, 10) is null and 
                    src:rule_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:saby_ref_compnr), '0'), 38, 10) is null and 
                    src:saby_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sacc), '0'), 38, 10) is null and 
                    src:sacc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sadt), '1900-01-01')) is null and 
                    src:sadt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sanl), '0'), 38, 10) is null and 
                    src:sanl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sarq), '0'), 38, 10) is null and 
                    src:sarq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scsh), '0'), 38, 10) is null and 
                    src:scsh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdsh), '0'), 38, 10) is null and 
                    src:sdsh is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdtf), '1900-01-01')) is null and 
                    src:sdtf is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdtt), '1900-01-01')) is null and 
                    src:sdtt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfad_ref_compnr), '0'), 38, 10) is null and 
                    src:sfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfcp), '0'), 38, 10) is null and 
                    src:sfcp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sflp), '0'), 38, 10) is null and 
                    src:sflp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfsi_ref_compnr), '0'), 38, 10) is null and 
                    src:sfsi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfty), '0'), 38, 10) is null and 
                    src:sfty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shst), '0'), 38, 10) is null and 
                    src:shst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shvc), '0'), 38, 10) is null and 
                    src:shvc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:shvd), '1900-01-01')) is null and 
                    src:shvd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shvl), '0'), 38, 10) is null and 
                    src:shvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shvo), '0'), 38, 10) is null and 
                    src:shvo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:slsh), '0'), 38, 10) is null and 
                    src:slsh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqdl), '0'), 38, 10) is null and 
                    src:sqdl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqdp), '0'), 38, 10) is null and 
                    src:sqdp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srsh), '0'), 38, 10) is null and 
                    src:srsh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_dlpt_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_dlpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcp), '0'), 38, 10) is null and 
                    src:stcp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:stsi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stty), '0'), 38, 10) is null and 
                    src:stty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:suba), '0'), 38, 10) is null and 
                    src:suba is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcap), '0'), 38, 10) is null and 
                    src:tcap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tccp), '0'), 38, 10) is null and 
                    src:tccp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:volm), '0'), 38, 10) is null and 
                    src:volm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wdth), '0'), 38, 10) is null and 
                    src:wdth is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wght), '0'), 38, 10) is null and 
                    src:wght is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)