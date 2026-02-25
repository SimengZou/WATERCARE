CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHINH435_ERROR AS SELECT src, 'LN_WHINH435' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) THEN
                    'bptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) THEN
                    'crte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwun_ref_compnr), '0'), 38, 10) is null and 
                    src:cwun_ref_compnr is not null) THEN
                    'cwun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwun_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dcdt), '1900-01-01')) is null and 
                    src:dcdt is not null) THEN
                    'dcdt contains a non-timestamp value : \'' || AS_VARCHAR(src:dcdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delc_ref_compnr), '0'), 38, 10) is null and 
                    src:delc_ref_compnr is not null) THEN
                    'delc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:delc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depc), '0'), 38, 10) is null and 
                    src:depc is not null) THEN
                    'depc contains a non-numeric value : \'' || AS_VARCHAR(src:depc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depc_ref_compnr), '0'), 38, 10) is null and 
                    src:depc_ref_compnr is not null) THEN
                    'depc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:depc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dnst), '0'), 38, 10) is null and 
                    src:dnst is not null) THEN
                    'dnst contains a non-numeric value : \'' || AS_VARCHAR(src:dnst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:finp), '0'), 38, 10) is null and 
                    src:finp is not null) THEN
                    'finp contains a non-numeric value : \'' || AS_VARCHAR(src:finp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fsyr), '0'), 38, 10) is null and 
                    src:fsyr is not null) THEN
                    'fsyr contains a non-numeric value : \'' || AS_VARCHAR(src:fsyr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grwt), '0'), 38, 10) is null and 
                    src:grwt is not null) THEN
                    'grwt contains a non-numeric value : \'' || AS_VARCHAR(src:grwt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) THEN
                    'itbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:load_ref_compnr), '0'), 38, 10) is null and 
                    src:load_ref_compnr is not null) THEN
                    'load_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:load_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:manl), '0'), 38, 10) is null and 
                    src:manl is not null) THEN
                    'manl contains a non-numeric value : \'' || AS_VARCHAR(src:manl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:motv_ref_compnr), '0'), 38, 10) is null and 
                    src:motv_ref_compnr is not null) THEN
                    'motv_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:motv_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) THEN
                    'prdt contains a non-timestamp value : \'' || AS_VARCHAR(src:prdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpac), '0'), 38, 10) is null and 
                    src:qpac is not null) THEN
                    'qpac contains a non-numeric value : \'' || AS_VARCHAR(src:qpac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfad_ref_compnr), '0'), 38, 10) is null and 
                    src:sfad_ref_compnr is not null) THEN
                    'sfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfcp), '0'), 38, 10) is null and 
                    src:sfcp is not null) THEN
                    'sfcp contains a non-numeric value : \'' || AS_VARCHAR(src:sfcp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfty), '0'), 38, 10) is null and 
                    src:sfty is not null) THEN
                    'sfty contains a non-numeric value : \'' || AS_VARCHAR(src:sfty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) THEN
                    'stad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcp), '0'), 38, 10) is null and 
                    src:stcp is not null) THEN
                    'stcp contains a non-numeric value : \'' || AS_VARCHAR(src:stcp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stty), '0'), 38, 10) is null and 
                    src:stty is not null) THEN
                    'stty contains a non-numeric value : \'' || AS_VARCHAR(src:stty) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tsad_ref_compnr), '0'), 38, 10) is null and 
                    src:tsad_ref_compnr is not null) THEN
                    'tsad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tsad_ref_compnr) || '\'' WHEN 
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
                src:prdn  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINH435 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwun_ref_compnr), '0'), 38, 10) is null and 
                    src:cwun_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dcdt), '1900-01-01')) is null and 
                    src:dcdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delc_ref_compnr), '0'), 38, 10) is null and 
                    src:delc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depc), '0'), 38, 10) is null and 
                    src:depc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depc_ref_compnr), '0'), 38, 10) is null and 
                    src:depc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dnst), '0'), 38, 10) is null and 
                    src:dnst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:finp), '0'), 38, 10) is null and 
                    src:finp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fsyr), '0'), 38, 10) is null and 
                    src:fsyr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grwt), '0'), 38, 10) is null and 
                    src:grwt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:load_ref_compnr), '0'), 38, 10) is null and 
                    src:load_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:manl), '0'), 38, 10) is null and 
                    src:manl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:motv_ref_compnr), '0'), 38, 10) is null and 
                    src:motv_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpac), '0'), 38, 10) is null and 
                    src:qpac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfad_ref_compnr), '0'), 38, 10) is null and 
                    src:sfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfcp), '0'), 38, 10) is null and 
                    src:sfcp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfty), '0'), 38, 10) is null and 
                    src:sfty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcp), '0'), 38, 10) is null and 
                    src:stcp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stty), '0'), 38, 10) is null and 
                    src:stty is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tsad_ref_compnr), '0'), 38, 10) is null and 
                    src:tsad_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)