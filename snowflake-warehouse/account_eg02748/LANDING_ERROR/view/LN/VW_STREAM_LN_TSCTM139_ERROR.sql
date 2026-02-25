CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSCTM139_ERROR AS SELECT src, 'LN_TSCTM139' as TABLE_OBJECT, CASE WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:avtm), '0'), 38, 10) is null and 
                    src:avtm is not null) THEN
                    'avtm contains a non-numeric value : \'' || AS_VARCHAR(src:avtm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:avtm_butm), '0'), 38, 10) is null and 
                    src:avtm_butm is not null) THEN
                    'avtm_butm contains a non-numeric value : \'' || AS_VARCHAR(src:avtm_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_1), '0'), 38, 10) is null and 
                    src:camt_1 is not null) THEN
                    'camt_1 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_2), '0'), 38, 10) is null and 
                    src:camt_2 is not null) THEN
                    'camt_2 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_3), '0'), 38, 10) is null and 
                    src:camt_3 is not null) THEN
                    'camt_3 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_cntc), '0'), 38, 10) is null and 
                    src:camt_cntc is not null) THEN
                    'camt_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_dwhc), '0'), 38, 10) is null and 
                    src:camt_dwhc is not null) THEN
                    'camt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_refc), '0'), 38, 10) is null and 
                    src:camt_refc is not null) THEN
                    'camt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctp_ref_compnr), '0'), 38, 10) is null and 
                    src:cctp_ref_compnr is not null) THEN
                    'cctp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cctp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfln), '0'), 38, 10) is null and 
                    src:cfln is not null) THEN
                    'cfln contains a non-numeric value : \'' || AS_VARCHAR(src:cfln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp), '0'), 38, 10) is null and 
                    src:cotp is not null) THEN
                    'cotp contains a non-numeric value : \'' || AS_VARCHAR(src:cotp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crmt), '0'), 38, 10) is null and 
                    src:crmt is not null) THEN
                    'crmt contains a non-numeric value : \'' || AS_VARCHAR(src:crmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cseq), '0'), 38, 10) is null and 
                    src:cseq is not null) THEN
                    'cseq contains a non-numeric value : \'' || AS_VARCHAR(src:cseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstm), '0'), 38, 10) is null and 
                    src:cstm is not null) THEN
                    'cstm contains a non-numeric value : \'' || AS_VARCHAR(src:cstm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstm_butm), '0'), 38, 10) is null and 
                    src:cstm_butm is not null) THEN
                    'cstm_butm contains a non-numeric value : \'' || AS_VARCHAR(src:cstm_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrbt), '0'), 38, 10) is null and 
                    src:nrbt is not null) THEN
                    'nrbt contains a non-numeric value : \'' || AS_VARCHAR(src:nrbt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proc), '0'), 38, 10) is null and 
                    src:proc is not null) THEN
                    'proc contains a non-numeric value : \'' || AS_VARCHAR(src:proc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sptm), '0'), 38, 10) is null and 
                    src:sptm is not null) THEN
                    'sptm contains a non-numeric value : \'' || AS_VARCHAR(src:sptm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sptm_butm), '0'), 38, 10) is null and 
                    src:sptm_butm is not null) THEN
                    'sptm_butm contains a non-numeric value : \'' || AS_VARCHAR(src:sptm_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term), '0'), 38, 10) is null and 
                    src:term is not null) THEN
                    'term contains a non-numeric value : \'' || AS_VARCHAR(src:term) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term_cfln_cctp_cotp_nrbt_ref_compnr), '0'), 38, 10) is null and 
                    src:term_cfln_cctp_cotp_nrbt_ref_compnr is not null) THEN
                    'term_cfln_cctp_cotp_nrbt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:term_cfln_cctp_cotp_nrbt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term_cfln_ref_compnr), '0'), 38, 10) is null and 
                    src:term_cfln_ref_compnr is not null) THEN
                    'term_cfln_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:term_cfln_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:utpc), '0'), 38, 10) is null and 
                    src:utpc is not null) THEN
                    'utpc contains a non-numeric value : \'' || AS_VARCHAR(src:utpc) || '\'' WHEN 
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
                                    
                src:term,
                src:cotp,
                src:cfln,
                src:nrbt,
                src:cseq,
                src:cctp,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSCTM139 as strm)
                WHERE
                ROWNUMBER=1) where 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:avtm), '0'), 38, 10) is null and 
                    src:avtm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:avtm_butm), '0'), 38, 10) is null and 
                    src:avtm_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_1), '0'), 38, 10) is null and 
                    src:camt_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_2), '0'), 38, 10) is null and 
                    src:camt_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_3), '0'), 38, 10) is null and 
                    src:camt_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_cntc), '0'), 38, 10) is null and 
                    src:camt_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_dwhc), '0'), 38, 10) is null and 
                    src:camt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_refc), '0'), 38, 10) is null and 
                    src:camt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctp_ref_compnr), '0'), 38, 10) is null and 
                    src:cctp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfln), '0'), 38, 10) is null and 
                    src:cfln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp), '0'), 38, 10) is null and 
                    src:cotp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crmt), '0'), 38, 10) is null and 
                    src:crmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cseq), '0'), 38, 10) is null and 
                    src:cseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstm), '0'), 38, 10) is null and 
                    src:cstm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstm_butm), '0'), 38, 10) is null and 
                    src:cstm_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrbt), '0'), 38, 10) is null and 
                    src:nrbt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proc), '0'), 38, 10) is null and 
                    src:proc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sptm), '0'), 38, 10) is null and 
                    src:sptm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sptm_butm), '0'), 38, 10) is null and 
                    src:sptm_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term), '0'), 38, 10) is null and 
                    src:term is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term_cfln_cctp_cotp_nrbt_ref_compnr), '0'), 38, 10) is null and 
                    src:term_cfln_cctp_cotp_nrbt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term_cfln_ref_compnr), '0'), 38, 10) is null and 
                    src:term_cfln_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:utpc), '0'), 38, 10) is null and 
                    src:utpc is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)