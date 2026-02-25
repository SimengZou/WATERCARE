CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TTAAD200_ERROR AS SELECT src, 'LN_TTAAD200' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:allp), '0'), 38, 10) is null and 
                    src:allp is not null) THEN
                    'allp contains a non-numeric value : \'' || AS_VARCHAR(src:allp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apud), '0'), 38, 10) is null and 
                    src:apud is not null) THEN
                    'apud contains a non-numeric value : \'' || AS_VARCHAR(src:apud) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) THEN
                    'clan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comp), '0'), 38, 10) is null and 
                    src:comp is not null) THEN
                    'comp contains a non-numeric value : \'' || AS_VARCHAR(src:comp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comp_ref_compnr), '0'), 38, 10) is null and 
                    src:comp_ref_compnr is not null) THEN
                    'comp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:comp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpac_ref_compnr), '0'), 38, 10) is null and 
                    src:cpac_ref_compnr is not null) THEN
                    'cpac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csbh), '0'), 38, 10) is null and 
                    src:csbh is not null) THEN
                    'csbh contains a non-numeric value : \'' || AS_VARCHAR(src:csbh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csbn), '0'), 38, 10) is null and 
                    src:csbn is not null) THEN
                    'csbn contains a non-numeric value : \'' || AS_VARCHAR(src:csbn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:defa), '0'), 38, 10) is null and 
                    src:defa is not null) THEN
                    'defa contains a non-numeric value : \'' || AS_VARCHAR(src:defa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delh), '0'), 38, 10) is null and 
                    src:delh is not null) THEN
                    'delh contains a non-numeric value : \'' || AS_VARCHAR(src:delh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:edtm), '0'), 38, 10) is null and 
                    src:edtm is not null) THEN
                    'edtm contains a non-numeric value : \'' || AS_VARCHAR(src:edtm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hist), '0'), 38, 10) is null and 
                    src:hist is not null) THEN
                    'hist contains a non-numeric value : \'' || AS_VARCHAR(src:hist) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifss), '0'), 38, 10) is null and 
                    src:ifss is not null) THEN
                    'ifss contains a non-numeric value : \'' || AS_VARCHAR(src:ifss) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifsu), '0'), 38, 10) is null and 
                    src:ifsu is not null) THEN
                    'ifsu contains a non-numeric value : \'' || AS_VARCHAR(src:ifsu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intt), '0'), 38, 10) is null and 
                    src:intt is not null) THEN
                    'intt contains a non-numeric value : \'' || AS_VARCHAR(src:intt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loca_ref_compnr), '0'), 38, 10) is null and 
                    src:loca_ref_compnr is not null) THEN
                    'loca_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:loca_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maxp), '0'), 38, 10) is null and 
                    src:maxp is not null) THEN
                    'maxp contains a non-numeric value : \'' || AS_VARCHAR(src:maxp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtyp), '0'), 38, 10) is null and 
                    src:mtyp is not null) THEN
                    'mtyp contains a non-numeric value : \'' || AS_VARCHAR(src:mtyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pacc_ref_compnr), '0'), 38, 10) is null and 
                    src:pacc_ref_compnr is not null) THEN
                    'pacc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pacc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:palp), '0'), 38, 10) is null and 
                    src:palp is not null) THEN
                    'palp contains a non-numeric value : \'' || AS_VARCHAR(src:palp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prrt), '0'), 38, 10) is null and 
                    src:prrt is not null) THEN
                    'prrt contains a non-numeric value : \'' || AS_VARCHAR(src:prrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pwyn), '0'), 38, 10) is null and 
                    src:pwyn is not null) THEN
                    'pwyn contains a non-numeric value : \'' || AS_VARCHAR(src:pwyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s132), '0'), 38, 10) is null and 
                    src:s132 is not null) THEN
                    's132 contains a non-numeric value : \'' || AS_VARCHAR(src:s132) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scdh), '0'), 38, 10) is null and 
                    src:scdh is not null) THEN
                    'scdh contains a non-numeric value : \'' || AS_VARCHAR(src:scdh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shtp), '0'), 38, 10) is null and 
                    src:shtp is not null) THEN
                    'shtp contains a non-numeric value : \'' || AS_VARCHAR(src:shtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stdp), '0'), 38, 10) is null and 
                    src:stdp is not null) THEN
                    'stdp contains a non-numeric value : \'' || AS_VARCHAR(src:stdp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stpr), '0'), 38, 10) is null and 
                    src:stpr is not null) THEN
                    'stpr contains a non-numeric value : \'' || AS_VARCHAR(src:stpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sttm), '0'), 38, 10) is null and 
                    src:sttm is not null) THEN
                    'sttm contains a non-numeric value : \'' || AS_VARCHAR(src:sttm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tdir), '0'), 38, 10) is null and 
                    src:tdir is not null) THEN
                    'tdir contains a non-numeric value : \'' || AS_VARCHAR(src:tdir) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tdvp_ref_compnr), '0'), 38, 10) is null and 
                    src:tdvp_ref_compnr is not null) THEN
                    'tdvp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tdvp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tlb1), '0'), 38, 10) is null and 
                    src:tlb1 is not null) THEN
                    'tlb1 contains a non-numeric value : \'' || AS_VARCHAR(src:tlb1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tlb2), '0'), 38, 10) is null and 
                    src:tlb2 is not null) THEN
                    'tlb2 contains a non-numeric value : \'' || AS_VARCHAR(src:tlb2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tusd_ref_compnr), '0'), 38, 10) is null and 
                    src:tusd_ref_compnr is not null) THEN
                    'tusd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tusd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucln), '0'), 38, 10) is null and 
                    src:ucln is not null) THEN
                    'ucln contains a non-numeric value : \'' || AS_VARCHAR(src:ucln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uhmd), '0'), 38, 10) is null and 
                    src:uhmd is not null) THEN
                    'uhmd contains a non-numeric value : \'' || AS_VARCHAR(src:uhmd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uico), '0'), 38, 10) is null and 
                    src:uico is not null) THEN
                    'uico contains a non-numeric value : \'' || AS_VARCHAR(src:uico) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uids), '0'), 38, 10) is null and 
                    src:uids is not null) THEN
                    'uids contains a non-numeric value : \'' || AS_VARCHAR(src:uids) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uoos), '0'), 38, 10) is null and 
                    src:uoos is not null) THEN
                    'uoos contains a non-numeric value : \'' || AS_VARCHAR(src:uoos) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:user_role_ref_compnr), '0'), 38, 10) is null and 
                    src:user_role_ref_compnr is not null) THEN
                    'user_role_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:user_role_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uspr_ref_compnr), '0'), 38, 10) is null and 
                    src:uspr_ref_compnr is not null) THEN
                    'uspr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:uspr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:utyp), '0'), 38, 10) is null and 
                    src:utyp is not null) THEN
                    'utyp contains a non-numeric value : \'' || AS_VARCHAR(src:utyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wait), '0'), 38, 10) is null and 
                    src:wait is not null) THEN
                    'wait contains a non-numeric value : \'' || AS_VARCHAR(src:wait) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:z_mmode), '0'), 38, 10) is null and 
                    src:z_mmode is not null) THEN
                    'z_mmode contains a non-numeric value : \'' || AS_VARCHAR(src:z_mmode) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:zb_ugsu), '0'), 38, 10) is null and 
                    src:zb_ugsu is not null) THEN
                    'zb_ugsu contains a non-numeric value : \'' || AS_VARCHAR(src:zb_ugsu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:zc_fcom), '0'), 38, 10) is null and 
                    src:zc_fcom is not null) THEN
                    'zc_fcom contains a non-numeric value : \'' || AS_VARCHAR(src:zc_fcom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:zc_lcom), '0'), 38, 10) is null and 
                    src:zc_lcom is not null) THEN
                    'zc_lcom contains a non-numeric value : \'' || AS_VARCHAR(src:zc_lcom) || '\'' WHEN 
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
                                    
                src:user,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TTAAD200 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:allp), '0'), 38, 10) is null and 
                    src:allp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apud), '0'), 38, 10) is null and 
                    src:apud is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comp), '0'), 38, 10) is null and 
                    src:comp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comp_ref_compnr), '0'), 38, 10) is null and 
                    src:comp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpac_ref_compnr), '0'), 38, 10) is null and 
                    src:cpac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csbh), '0'), 38, 10) is null and 
                    src:csbh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csbn), '0'), 38, 10) is null and 
                    src:csbn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:defa), '0'), 38, 10) is null and 
                    src:defa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delh), '0'), 38, 10) is null and 
                    src:delh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:edtm), '0'), 38, 10) is null and 
                    src:edtm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hist), '0'), 38, 10) is null and 
                    src:hist is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifss), '0'), 38, 10) is null and 
                    src:ifss is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifsu), '0'), 38, 10) is null and 
                    src:ifsu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intt), '0'), 38, 10) is null and 
                    src:intt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loca_ref_compnr), '0'), 38, 10) is null and 
                    src:loca_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maxp), '0'), 38, 10) is null and 
                    src:maxp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtyp), '0'), 38, 10) is null and 
                    src:mtyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pacc_ref_compnr), '0'), 38, 10) is null and 
                    src:pacc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:palp), '0'), 38, 10) is null and 
                    src:palp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prrt), '0'), 38, 10) is null and 
                    src:prrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pwyn), '0'), 38, 10) is null and 
                    src:pwyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s132), '0'), 38, 10) is null and 
                    src:s132 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scdh), '0'), 38, 10) is null and 
                    src:scdh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shtp), '0'), 38, 10) is null and 
                    src:shtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stdp), '0'), 38, 10) is null and 
                    src:stdp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stpr), '0'), 38, 10) is null and 
                    src:stpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sttm), '0'), 38, 10) is null and 
                    src:sttm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tdir), '0'), 38, 10) is null and 
                    src:tdir is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tdvp_ref_compnr), '0'), 38, 10) is null and 
                    src:tdvp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tlb1), '0'), 38, 10) is null and 
                    src:tlb1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tlb2), '0'), 38, 10) is null and 
                    src:tlb2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tusd_ref_compnr), '0'), 38, 10) is null and 
                    src:tusd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucln), '0'), 38, 10) is null and 
                    src:ucln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uhmd), '0'), 38, 10) is null and 
                    src:uhmd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uico), '0'), 38, 10) is null and 
                    src:uico is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uids), '0'), 38, 10) is null and 
                    src:uids is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uoos), '0'), 38, 10) is null and 
                    src:uoos is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:user_role_ref_compnr), '0'), 38, 10) is null and 
                    src:user_role_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uspr_ref_compnr), '0'), 38, 10) is null and 
                    src:uspr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:utyp), '0'), 38, 10) is null and 
                    src:utyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wait), '0'), 38, 10) is null and 
                    src:wait is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:z_mmode), '0'), 38, 10) is null and 
                    src:z_mmode is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:zb_ugsu), '0'), 38, 10) is null and 
                    src:zb_ugsu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:zc_fcom), '0'), 38, 10) is null and 
                    src:zc_fcom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:zc_lcom), '0'), 38, 10) is null and 
                    src:zc_lcom is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)