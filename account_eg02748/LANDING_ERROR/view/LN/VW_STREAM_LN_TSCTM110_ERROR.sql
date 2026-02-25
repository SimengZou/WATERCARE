CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSCTM110_ERROR AS SELECT src, 'LN_TSCTM110' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amdy), '0'), 38, 10) is null and 
                    src:amdy is not null) THEN
                    'amdy contains a non-numeric value : \'' || AS_VARCHAR(src:amdy) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccds_ref_compnr), '0'), 38, 10) is null and 
                    src:ccds_ref_compnr is not null) THEN
                    'ccds_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccds_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cchn), '0'), 38, 10) is null and 
                    src:cchn is not null) THEN
                    'cchn contains a non-numeric value : \'' || AS_VARCHAR(src:cchn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccmt_1), '0'), 38, 10) is null and 
                    src:ccmt_1 is not null) THEN
                    'ccmt_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ccmt_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccmt_2), '0'), 38, 10) is null and 
                    src:ccmt_2 is not null) THEN
                    'ccmt_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ccmt_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccmt_3), '0'), 38, 10) is null and 
                    src:ccmt_3 is not null) THEN
                    'ccmt_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ccmt_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccte_ref_compnr), '0'), 38, 10) is null and 
                    src:ccte_ref_compnr is not null) THEN
                    'ccte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctp_ref_compnr), '0'), 38, 10) is null and 
                    src:cctp_ref_compnr is not null) THEN
                    'cctp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cctp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cddt), '1900-01-01')) is null and 
                    src:cddt is not null) THEN
                    'cddt contains a non-timestamp value : \'' || AS_VARCHAR(src:cddt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdmt), '0'), 38, 10) is null and 
                    src:cdmt is not null) THEN
                    'cdmt contains a non-numeric value : \'' || AS_VARCHAR(src:cdmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfln), '0'), 38, 10) is null and 
                    src:cfln is not null) THEN
                    'cfln contains a non-numeric value : \'' || AS_VARCHAR(src:cfln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clst_ref_compnr), '0'), 38, 10) is null and 
                    src:clst_ref_compnr is not null) THEN
                    'clst_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clst_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cltp), '0'), 38, 10) is null and 
                    src:cltp is not null) THEN
                    'cltp contains a non-numeric value : \'' || AS_VARCHAR(src:cltp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnrp), '0'), 38, 10) is null and 
                    src:cnrp is not null) THEN
                    'cnrp contains a non-numeric value : \'' || AS_VARCHAR(src:cnrp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cody_1), '0'), 38, 10) is null and 
                    src:cody_1 is not null) THEN
                    'cody_1 contains a non-numeric value : \'' || AS_VARCHAR(src:cody_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cody_2), '0'), 38, 10) is null and 
                    src:cody_2 is not null) THEN
                    'cody_2 contains a non-numeric value : \'' || AS_VARCHAR(src:cody_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cody_3), '0'), 38, 10) is null and 
                    src:cody_3 is not null) THEN
                    'cody_3 contains a non-numeric value : \'' || AS_VARCHAR(src:cody_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpru), '0'), 38, 10) is null and 
                    src:cpru is not null) THEN
                    'cpru contains a non-numeric value : \'' || AS_VARCHAR(src:cpru) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptp), '0'), 38, 10) is null and 
                    src:cptp is not null) THEN
                    'cptp contains a non-numeric value : \'' || AS_VARCHAR(src:cptp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crac_ref_compnr), '0'), 38, 10) is null and 
                    src:crac_ref_compnr is not null) THEN
                    'crac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csar_ref_compnr), '0'), 38, 10) is null and 
                    src:csar_ref_compnr is not null) THEN
                    'csar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_cchn_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_cchn_ref_compnr is not null) THEN
                    'csco_cchn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csco_cchn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) THEN
                    'csco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csmt), '0'), 38, 10) is null and 
                    src:csmt is not null) THEN
                    'csmt contains a non-numeric value : \'' || AS_VARCHAR(src:csmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspr), '0'), 38, 10) is null and 
                    src:cspr is not null) THEN
                    'cspr contains a non-numeric value : \'' || AS_VARCHAR(src:cspr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctin_ref_compnr), '0'), 38, 10) is null and 
                    src:ctin_ref_compnr is not null) THEN
                    'ctin_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctin_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctrm), '0'), 38, 10) is null and 
                    src:ctrm is not null) THEN
                    'ctrm contains a non-numeric value : \'' || AS_VARCHAR(src:ctrm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dimt), '0'), 38, 10) is null and 
                    src:dimt is not null) THEN
                    'dimt contains a non-numeric value : \'' || AS_VARCHAR(src:dimt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dipc), '0'), 38, 10) is null and 
                    src:dipc is not null) THEN
                    'dipc contains a non-numeric value : \'' || AS_VARCHAR(src:dipc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:docn_ref_compnr), '0'), 38, 10) is null and 
                    src:docn_ref_compnr is not null) THEN
                    'docn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:docn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) THEN
                    'efdt contains a non-timestamp value : \'' || AS_VARCHAR(src:efdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erfa), '0'), 38, 10) is null and 
                    src:erfa is not null) THEN
                    'erfa contains a non-numeric value : \'' || AS_VARCHAR(src:erfa) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) THEN
                    'exdt contains a non-timestamp value : \'' || AS_VARCHAR(src:exdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frpr), '0'), 38, 10) is null and 
                    src:frpr is not null) THEN
                    'frpr contains a non-numeric value : \'' || AS_VARCHAR(src:frpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fxpr), '0'), 38, 10) is null and 
                    src:fxpr is not null) THEN
                    'fxpr contains a non-numeric value : \'' || AS_VARCHAR(src:fxpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gsmt), '0'), 38, 10) is null and 
                    src:gsmt is not null) THEN
                    'gsmt contains a non-numeric value : \'' || AS_VARCHAR(src:gsmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_sern_ref_compnr), '0'), 38, 10) is null and 
                    src:item_sern_ref_compnr is not null) THEN
                    'item_sern_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_sern_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mexp), '0'), 38, 10) is null and 
                    src:mexp is not null) THEN
                    'mexp contains a non-numeric value : \'' || AS_VARCHAR(src:mexp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nitm), '0'), 38, 10) is null and 
                    src:nitm is not null) THEN
                    'nitm contains a non-numeric value : \'' || AS_VARCHAR(src:nitm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrac), '0'), 38, 10) is null and 
                    src:nrac is not null) THEN
                    'nrac contains a non-numeric value : \'' || AS_VARCHAR(src:nrac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrpe), '0'), 38, 10) is null and 
                    src:nrpe is not null) THEN
                    'nrpe contains a non-numeric value : \'' || AS_VARCHAR(src:nrpe) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nsmt), '0'), 38, 10) is null and 
                    src:nsmt is not null) THEN
                    'nsmt contains a non-numeric value : \'' || AS_VARCHAR(src:nsmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ortp), '0'), 38, 10) is null and 
                    src:ortp is not null) THEN
                    'ortp contains a non-numeric value : \'' || AS_VARCHAR(src:ortp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbsm), '0'), 38, 10) is null and 
                    src:pbsm is not null) THEN
                    'pbsm contains a non-numeric value : \'' || AS_VARCHAR(src:pbsm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:perc), '0'), 38, 10) is null and 
                    src:perc is not null) THEN
                    'perc contains a non-numeric value : \'' || AS_VARCHAR(src:perc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peru), '0'), 38, 10) is null and 
                    src:peru is not null) THEN
                    'peru contains a non-numeric value : \'' || AS_VARCHAR(src:peru) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prmt), '0'), 38, 10) is null and 
                    src:prmt is not null) THEN
                    'prmt contains a non-numeric value : \'' || AS_VARCHAR(src:prmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptrm), '0'), 38, 10) is null and 
                    src:ptrm is not null) THEN
                    'ptrm contains a non-numeric value : \'' || AS_VARCHAR(src:ptrm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt), '0'), 38, 10) is null and 
                    src:rsmt is not null) THEN
                    'rsmt contains a non-numeric value : \'' || AS_VARCHAR(src:rsmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_dwhc), '0'), 38, 10) is null and 
                    src:rsmt_dwhc is not null) THEN
                    'rsmt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:rsmt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_homc), '0'), 38, 10) is null and 
                    src:rsmt_homc is not null) THEN
                    'rsmt_homc contains a non-numeric value : \'' || AS_VARCHAR(src:rsmt_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_refc), '0'), 38, 10) is null and 
                    src:rsmt_refc is not null) THEN
                    'rsmt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:rsmt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_rpc1), '0'), 38, 10) is null and 
                    src:rsmt_rpc1 is not null) THEN
                    'rsmt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:rsmt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_rpc2), '0'), 38, 10) is null and 
                    src:rsmt_rpc2 is not null) THEN
                    'rsmt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:rsmt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rspr), '0'), 38, 10) is null and 
                    src:rspr is not null) THEN
                    'rspr contains a non-numeric value : \'' || AS_VARCHAR(src:rspr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdno), '0'), 38, 10) is null and 
                    src:sdno is not null) THEN
                    'sdno contains a non-numeric value : \'' || AS_VARCHAR(src:sdno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spno), '0'), 38, 10) is null and 
                    src:spno is not null) THEN
                    'spno contains a non-numeric value : \'' || AS_VARCHAR(src:spno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term), '0'), 38, 10) is null and 
                    src:term is not null) THEN
                    'term contains a non-numeric value : \'' || AS_VARCHAR(src:term) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmdu), '0'), 38, 10) is null and 
                    src:tmdu is not null) THEN
                    'tmdu contains a non-numeric value : \'' || AS_VARCHAR(src:tmdu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmpr), '0'), 38, 10) is null and 
                    src:tmpr is not null) THEN
                    'tmpr contains a non-numeric value : \'' || AS_VARCHAR(src:tmpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upmt), '0'), 38, 10) is null and 
                    src:upmt is not null) THEN
                    'upmt contains a non-numeric value : \'' || AS_VARCHAR(src:upmt) || '\'' WHEN 
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
                                    
                src:cfln,
                src:compnr,
                src:term  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSCTM110 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amdy), '0'), 38, 10) is null and 
                    src:amdy is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccds_ref_compnr), '0'), 38, 10) is null and 
                    src:ccds_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cchn), '0'), 38, 10) is null and 
                    src:cchn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccmt_1), '0'), 38, 10) is null and 
                    src:ccmt_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccmt_2), '0'), 38, 10) is null and 
                    src:ccmt_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccmt_3), '0'), 38, 10) is null and 
                    src:ccmt_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccte_ref_compnr), '0'), 38, 10) is null and 
                    src:ccte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctp_ref_compnr), '0'), 38, 10) is null and 
                    src:cctp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cddt), '1900-01-01')) is null and 
                    src:cddt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdmt), '0'), 38, 10) is null and 
                    src:cdmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfln), '0'), 38, 10) is null and 
                    src:cfln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clst_ref_compnr), '0'), 38, 10) is null and 
                    src:clst_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cltp), '0'), 38, 10) is null and 
                    src:cltp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnrp), '0'), 38, 10) is null and 
                    src:cnrp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cody_1), '0'), 38, 10) is null and 
                    src:cody_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cody_2), '0'), 38, 10) is null and 
                    src:cody_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cody_3), '0'), 38, 10) is null and 
                    src:cody_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpru), '0'), 38, 10) is null and 
                    src:cpru is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptp), '0'), 38, 10) is null and 
                    src:cptp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crac_ref_compnr), '0'), 38, 10) is null and 
                    src:crac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csar_ref_compnr), '0'), 38, 10) is null and 
                    src:csar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_cchn_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_cchn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csmt), '0'), 38, 10) is null and 
                    src:csmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspr), '0'), 38, 10) is null and 
                    src:cspr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctin_ref_compnr), '0'), 38, 10) is null and 
                    src:ctin_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctrm), '0'), 38, 10) is null and 
                    src:ctrm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dimt), '0'), 38, 10) is null and 
                    src:dimt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dipc), '0'), 38, 10) is null and 
                    src:dipc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:docn_ref_compnr), '0'), 38, 10) is null and 
                    src:docn_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erfa), '0'), 38, 10) is null and 
                    src:erfa is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frpr), '0'), 38, 10) is null and 
                    src:frpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fxpr), '0'), 38, 10) is null and 
                    src:fxpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gsmt), '0'), 38, 10) is null and 
                    src:gsmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_sern_ref_compnr), '0'), 38, 10) is null and 
                    src:item_sern_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mexp), '0'), 38, 10) is null and 
                    src:mexp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nitm), '0'), 38, 10) is null and 
                    src:nitm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrac), '0'), 38, 10) is null and 
                    src:nrac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrpe), '0'), 38, 10) is null and 
                    src:nrpe is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nsmt), '0'), 38, 10) is null and 
                    src:nsmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ortp), '0'), 38, 10) is null and 
                    src:ortp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbsm), '0'), 38, 10) is null and 
                    src:pbsm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:perc), '0'), 38, 10) is null and 
                    src:perc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peru), '0'), 38, 10) is null and 
                    src:peru is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prmt), '0'), 38, 10) is null and 
                    src:prmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptrm), '0'), 38, 10) is null and 
                    src:ptrm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt), '0'), 38, 10) is null and 
                    src:rsmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_dwhc), '0'), 38, 10) is null and 
                    src:rsmt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_homc), '0'), 38, 10) is null and 
                    src:rsmt_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_refc), '0'), 38, 10) is null and 
                    src:rsmt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_rpc1), '0'), 38, 10) is null and 
                    src:rsmt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_rpc2), '0'), 38, 10) is null and 
                    src:rsmt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rspr), '0'), 38, 10) is null and 
                    src:rspr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdno), '0'), 38, 10) is null and 
                    src:sdno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spno), '0'), 38, 10) is null and 
                    src:spno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term), '0'), 38, 10) is null and 
                    src:term is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmdu), '0'), 38, 10) is null and 
                    src:tmdu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmpr), '0'), 38, 10) is null and 
                    src:tmpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upmt), '0'), 38, 10) is null and 
                    src:upmt is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)