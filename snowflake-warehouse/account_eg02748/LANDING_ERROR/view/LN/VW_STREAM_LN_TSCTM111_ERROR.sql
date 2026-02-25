CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSCTM111_ERROR AS SELECT src, 'LN_TSCTM111' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cact_ref_compnr is not null) THEN
                    'cact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr_ref_compnr), '0'), 38, 10) is null and 
                    src:camr_ref_compnr is not null) THEN
                    'camr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:camr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_cntc), '0'), 38, 10) is null and 
                    src:camt_cntc is not null) THEN
                    'camt_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_dwhc), '0'), 38, 10) is null and 
                    src:camt_dwhc is not null) THEN
                    'camt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_homc), '0'), 38, 10) is null and 
                    src:camt_homc is not null) THEN
                    'camt_homc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_refc), '0'), 38, 10) is null and 
                    src:camt_refc is not null) THEN
                    'camt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_rpc1), '0'), 38, 10) is null and 
                    src:camt_rpc1 is not null) THEN
                    'camt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_rpc2), '0'), 38, 10) is null and 
                    src:camt_rpc2 is not null) THEN
                    'camt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:caro_ref_compnr), '0'), 38, 10) is null and 
                    src:caro_ref_compnr is not null) THEN
                    'caro_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:caro_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfln), '0'), 38, 10) is null and 
                    src:cfln is not null) THEN
                    'cfln contains a non-numeric value : \'' || AS_VARCHAR(src:cfln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cseq), '0'), 38, 10) is null and 
                    src:cseq is not null) THEN
                    'cseq contains a non-numeric value : \'' || AS_VARCHAR(src:cseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fplv), '0'), 38, 10) is null and 
                    src:fplv is not null) THEN
                    'fplv contains a non-numeric value : \'' || AS_VARCHAR(src:fplv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) THEN
                    'pris contains a non-numeric value : \'' || AS_VARCHAR(src:pris) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_dwhc), '0'), 38, 10) is null and 
                    src:pris_dwhc is not null) THEN
                    'pris_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:pris_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_homc), '0'), 38, 10) is null and 
                    src:pris_homc is not null) THEN
                    'pris_homc contains a non-numeric value : \'' || AS_VARCHAR(src:pris_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_refc), '0'), 38, 10) is null and 
                    src:pris_refc is not null) THEN
                    'pris_refc contains a non-numeric value : \'' || AS_VARCHAR(src:pris_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_rpc1), '0'), 38, 10) is null and 
                    src:pris_rpc1 is not null) THEN
                    'pris_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:pris_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_rpc2), '0'), 38, 10) is null and 
                    src:pris_rpc2 is not null) THEN
                    'pris_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:pris_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term), '0'), 38, 10) is null and 
                    src:term is not null) THEN
                    'term contains a non-numeric value : \'' || AS_VARCHAR(src:term) || '\'' WHEN 
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
                                    
                src:cseq,
                src:compnr,
                src:term,
                src:cfln  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSCTM111 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camr_ref_compnr), '0'), 38, 10) is null and 
                    src:camr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_cntc), '0'), 38, 10) is null and 
                    src:camt_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_dwhc), '0'), 38, 10) is null and 
                    src:camt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_homc), '0'), 38, 10) is null and 
                    src:camt_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_refc), '0'), 38, 10) is null and 
                    src:camt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_rpc1), '0'), 38, 10) is null and 
                    src:camt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_rpc2), '0'), 38, 10) is null and 
                    src:camt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:caro_ref_compnr), '0'), 38, 10) is null and 
                    src:caro_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfln), '0'), 38, 10) is null and 
                    src:cfln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cseq), '0'), 38, 10) is null and 
                    src:cseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fplv), '0'), 38, 10) is null and 
                    src:fplv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_dwhc), '0'), 38, 10) is null and 
                    src:pris_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_homc), '0'), 38, 10) is null and 
                    src:pris_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_refc), '0'), 38, 10) is null and 
                    src:pris_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_rpc1), '0'), 38, 10) is null and 
                    src:pris_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_rpc2), '0'), 38, 10) is null and 
                    src:pris_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term), '0'), 38, 10) is null and 
                    src:term is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term_cfln_ref_compnr), '0'), 38, 10) is null and 
                    src:term_cfln_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)