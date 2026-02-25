CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCCOM100_ERROR AS SELECT src, 'LN_TCCOM100' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bprl), '0'), 38, 10) is null and 
                    src:bprl is not null) THEN
                    'bprl contains a non-numeric value : \'' || AS_VARCHAR(src:bprl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptx_ref_compnr), '0'), 38, 10) is null and 
                    src:bptx_ref_compnr is not null) THEN
                    'bptx_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptx_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btbv), '0'), 38, 10) is null and 
                    src:btbv is not null) THEN
                    'btbv contains a non-numeric value : \'' || AS_VARCHAR(src:btbv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) THEN
                    'cadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbcl_ref_compnr), '0'), 38, 10) is null and 
                    src:cbcl_ref_compnr is not null) THEN
                    'cbcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccnt_ref_compnr), '0'), 38, 10) is null and 
                    src:ccnt_ref_compnr is not null) THEN
                    'ccnt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccnt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_bsrm), '0'), 38, 10) is null and 
                    src:cdf_bsrm is not null) THEN
                    'cdf_bsrm contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_bsrm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdf_cond), '1900-01-01')) is null and 
                    src:cdf_cond is not null) THEN
                    'cdf_cond contains a non-timestamp value : \'' || AS_VARCHAR(src:cdf_cond) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_crtl), '0'), 38, 10) is null and 
                    src:cdf_crtl is not null) THEN
                    'cdf_crtl contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_crtl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_emno), '0'), 38, 10) is null and 
                    src:cdf_emno is not null) THEN
                    'cdf_emno contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_emno) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdf_exdt), '1900-01-01')) is null and 
                    src:cdf_exdt is not null) THEN
                    'cdf_exdt contains a non-timestamp value : \'' || AS_VARCHAR(src:cdf_exdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_hsst), '0'), 38, 10) is null and 
                    src:cdf_hsst is not null) THEN
                    'cdf_hsst contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_hsst) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdf_rrdt), '1900-01-01')) is null and 
                    src:cdf_rrdt is not null) THEN
                    'cdf_rrdt contains a non-timestamp value : \'' || AS_VARCHAR(src:cdf_rrdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_wfst), '0'), 38, 10) is null and 
                    src:cdf_wfst is not null) THEN
                    'cdf_wfst contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_wfst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cint), '0'), 38, 10) is null and 
                    src:cint is not null) THEN
                    'cint contains a non-numeric value : \'' || AS_VARCHAR(src:cint) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) THEN
                    'clan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clcd), '0'), 38, 10) is null and 
                    src:clcd is not null) THEN
                    'clcd contains a non-numeric value : \'' || AS_VARCHAR(src:clcd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coff), '0'), 38, 10) is null and 
                    src:coff is not null) THEN
                    'coff contains a non-numeric value : \'' || AS_VARCHAR(src:coff) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crid), '0'), 38, 10) is null and 
                    src:crid is not null) THEN
                    'crid contains a non-numeric value : \'' || AS_VARCHAR(src:crid) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csto), '0'), 38, 10) is null and 
                    src:csto is not null) THEN
                    'csto contains a non-numeric value : \'' || AS_VARCHAR(src:csto) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctit_ref_compnr), '0'), 38, 10) is null and 
                    src:ctit_ref_compnr is not null) THEN
                    'ctit_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctit_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ecmp), '0'), 38, 10) is null and 
                    src:ecmp is not null) THEN
                    'ecmp contains a non-numeric value : \'' || AS_VARCHAR(src:ecmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:edyn), '0'), 38, 10) is null and 
                    src:edyn is not null) THEN
                    'edyn contains a non-numeric value : \'' || AS_VARCHAR(src:edyn) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:endt), '1900-01-01')) is null and 
                    src:endt is not null) THEN
                    'endt contains a non-timestamp value : \'' || AS_VARCHAR(src:endt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fact), '0'), 38, 10) is null and 
                    src:fact is not null) THEN
                    'fact contains a non-numeric value : \'' || AS_VARCHAR(src:fact) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:foid), '0'), 38, 10) is null and 
                    src:foid is not null) THEN
                    'foid contains a non-numeric value : \'' || AS_VARCHAR(src:foid) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icod), '0'), 38, 10) is null and 
                    src:icod is not null) THEN
                    'icod contains a non-numeric value : \'' || AS_VARCHAR(src:icod) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icst), '0'), 38, 10) is null and 
                    src:icst is not null) THEN
                    'icst contains a non-numeric value : \'' || AS_VARCHAR(src:icst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inrl), '0'), 38, 10) is null and 
                    src:inrl is not null) THEN
                    'inrl contains a non-numeric value : \'' || AS_VARCHAR(src:inrl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iscn), '0'), 38, 10) is null and 
                    src:iscn is not null) THEN
                    'iscn contains a non-numeric value : \'' || AS_VARCHAR(src:iscn) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) THEN
                    'lmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lmdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lvdt), '1900-01-01')) is null and 
                    src:lvdt is not null) THEN
                    'lvdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lvdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prbp_ref_compnr), '0'), 38, 10) is null and 
                    src:prbp_ref_compnr is not null) THEN
                    'prbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prst), '0'), 38, 10) is null and 
                    src:prst is not null) THEN
                    'prst contains a non-numeric value : \'' || AS_VARCHAR(src:prst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smt1), '0'), 38, 10) is null and 
                    src:smt1 is not null) THEN
                    'smt1 contains a non-numeric value : \'' || AS_VARCHAR(src:smt1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smt2), '0'), 38, 10) is null and 
                    src:smt2 is not null) THEN
                    'smt2 contains a non-numeric value : \'' || AS_VARCHAR(src:smt2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smt3), '0'), 38, 10) is null and 
                    src:smt3 is not null) THEN
                    'smt3 contains a non-numeric value : \'' || AS_VARCHAR(src:smt3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smt4), '0'), 38, 10) is null and 
                    src:smt4 is not null) THEN
                    'smt4 contains a non-numeric value : \'' || AS_VARCHAR(src:smt4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smt5), '0'), 38, 10) is null and 
                    src:smt5 is not null) THEN
                    'smt5 contains a non-numeric value : \'' || AS_VARCHAR(src:smt5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sndr), '0'), 38, 10) is null and 
                    src:sndr is not null) THEN
                    'sndr contains a non-numeric value : \'' || AS_VARCHAR(src:sndr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) THEN
                    'stdt contains a non-timestamp value : \'' || AS_VARCHAR(src:stdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
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
                                    
                src:compnr,
                src:bpid  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCCOM100 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bprl), '0'), 38, 10) is null and 
                    src:bprl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptx_ref_compnr), '0'), 38, 10) is null and 
                    src:bptx_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btbv), '0'), 38, 10) is null and 
                    src:btbv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbcl_ref_compnr), '0'), 38, 10) is null and 
                    src:cbcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccnt_ref_compnr), '0'), 38, 10) is null and 
                    src:ccnt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_bsrm), '0'), 38, 10) is null and 
                    src:cdf_bsrm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdf_cond), '1900-01-01')) is null and 
                    src:cdf_cond is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_crtl), '0'), 38, 10) is null and 
                    src:cdf_crtl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_emno), '0'), 38, 10) is null and 
                    src:cdf_emno is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdf_exdt), '1900-01-01')) is null and 
                    src:cdf_exdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_hsst), '0'), 38, 10) is null and 
                    src:cdf_hsst is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdf_rrdt), '1900-01-01')) is null and 
                    src:cdf_rrdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_wfst), '0'), 38, 10) is null and 
                    src:cdf_wfst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cint), '0'), 38, 10) is null and 
                    src:cint is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clcd), '0'), 38, 10) is null and 
                    src:clcd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coff), '0'), 38, 10) is null and 
                    src:coff is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crid), '0'), 38, 10) is null and 
                    src:crid is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csto), '0'), 38, 10) is null and 
                    src:csto is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctit_ref_compnr), '0'), 38, 10) is null and 
                    src:ctit_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ecmp), '0'), 38, 10) is null and 
                    src:ecmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:edyn), '0'), 38, 10) is null and 
                    src:edyn is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:endt), '1900-01-01')) is null and 
                    src:endt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fact), '0'), 38, 10) is null and 
                    src:fact is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:foid), '0'), 38, 10) is null and 
                    src:foid is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icod), '0'), 38, 10) is null and 
                    src:icod is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icst), '0'), 38, 10) is null and 
                    src:icst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inrl), '0'), 38, 10) is null and 
                    src:inrl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iscn), '0'), 38, 10) is null and 
                    src:iscn is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lvdt), '1900-01-01')) is null and 
                    src:lvdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prbp_ref_compnr), '0'), 38, 10) is null and 
                    src:prbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prst), '0'), 38, 10) is null and 
                    src:prst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smt1), '0'), 38, 10) is null and 
                    src:smt1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smt2), '0'), 38, 10) is null and 
                    src:smt2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smt3), '0'), 38, 10) is null and 
                    src:smt3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smt4), '0'), 38, 10) is null and 
                    src:smt4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smt5), '0'), 38, 10) is null and 
                    src:smt5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sndr), '0'), 38, 10) is null and 
                    src:sndr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)