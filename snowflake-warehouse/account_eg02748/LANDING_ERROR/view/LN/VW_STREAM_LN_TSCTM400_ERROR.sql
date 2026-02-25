CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSCTM400_ERROR AS SELECT src, 'LN_TSCTM400' as TABLE_OBJECT, CASE WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cchn), '0'), 38, 10) is null and 
                    src:cchn is not null) THEN
                    'cchn contains a non-numeric value : \'' || AS_VARCHAR(src:cchn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfln), '0'), 38, 10) is null and 
                    src:cfln is not null) THEN
                    'cfln contains a non-numeric value : \'' || AS_VARCHAR(src:cfln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_cchn_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_cchn_ref_compnr is not null) THEN
                    'csco_cchn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csco_cchn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) THEN
                    'csco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dimt), '0'), 38, 10) is null and 
                    src:dimt is not null) THEN
                    'dimt contains a non-numeric value : \'' || AS_VARCHAR(src:dimt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dimt_dwhc), '0'), 38, 10) is null and 
                    src:dimt_dwhc is not null) THEN
                    'dimt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:dimt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dimt_homc), '0'), 38, 10) is null and 
                    src:dimt_homc is not null) THEN
                    'dimt_homc contains a non-numeric value : \'' || AS_VARCHAR(src:dimt_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dimt_refc), '0'), 38, 10) is null and 
                    src:dimt_refc is not null) THEN
                    'dimt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:dimt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dimt_rpc1), '0'), 38, 10) is null and 
                    src:dimt_rpc1 is not null) THEN
                    'dimt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:dimt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dimt_rpc2), '0'), 38, 10) is null and 
                    src:dimt_rpc2 is not null) THEN
                    'dimt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:dimt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc), '0'), 38, 10) is null and 
                    src:disc is not null) THEN
                    'disc contains a non-numeric value : \'' || AS_VARCHAR(src:disc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) THEN
                    'efdt contains a non-timestamp value : \'' || AS_VARCHAR(src:efdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) THEN
                    'exdt contains a non-timestamp value : \'' || AS_VARCHAR(src:exdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmt_1), '0'), 38, 10) is null and 
                    src:fcmt_1 is not null) THEN
                    'fcmt_1 contains a non-numeric value : \'' || AS_VARCHAR(src:fcmt_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmt_2), '0'), 38, 10) is null and 
                    src:fcmt_2 is not null) THEN
                    'fcmt_2 contains a non-numeric value : \'' || AS_VARCHAR(src:fcmt_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmt_3), '0'), 38, 10) is null and 
                    src:fcmt_3 is not null) THEN
                    'fcmt_3 contains a non-numeric value : \'' || AS_VARCHAR(src:fcmt_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcpc), '0'), 38, 10) is null and 
                    src:fcpc is not null) THEN
                    'fcpc contains a non-numeric value : \'' || AS_VARCHAR(src:fcpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fper), '0'), 38, 10) is null and 
                    src:fper is not null) THEN
                    'fper contains a non-numeric value : \'' || AS_VARCHAR(src:fper) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fyer), '0'), 38, 10) is null and 
                    src:fyer is not null) THEN
                    'fyer contains a non-numeric value : \'' || AS_VARCHAR(src:fyer) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icmp), '0'), 38, 10) is null and 
                    src:icmp is not null) THEN
                    'icmp contains a non-numeric value : \'' || AS_VARCHAR(src:icmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icmt), '0'), 38, 10) is null and 
                    src:icmt is not null) THEN
                    'icmt contains a non-numeric value : \'' || AS_VARCHAR(src:icmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) THEN
                    'idoc contains a non-numeric value : \'' || AS_VARCHAR(src:idoc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) THEN
                    'indt contains a non-timestamp value : \'' || AS_VARCHAR(src:indt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inmt), '0'), 38, 10) is null and 
                    src:inmt is not null) THEN
                    'inmt contains a non-numeric value : \'' || AS_VARCHAR(src:inmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inst), '0'), 38, 10) is null and 
                    src:inst is not null) THEN
                    'inst contains a non-numeric value : \'' || AS_VARCHAR(src:inst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:isst), '0'), 38, 10) is null and 
                    src:isst is not null) THEN
                    'isst contains a non-numeric value : \'' || AS_VARCHAR(src:isst) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:istd), '1900-01-01')) is null and 
                    src:istd is not null) THEN
                    'istd contains a non-timestamp value : \'' || AS_VARCHAR(src:istd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrpf), '0'), 38, 10) is null and 
                    src:nrpf is not null) THEN
                    'nrpf contains a non-numeric value : \'' || AS_VARCHAR(src:nrpf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ntmt), '0'), 38, 10) is null and 
                    src:ntmt is not null) THEN
                    'ntmt contains a non-numeric value : \'' || AS_VARCHAR(src:ntmt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pidt), '1900-01-01')) is null and 
                    src:pidt is not null) THEN
                    'pidt contains a non-timestamp value : \'' || AS_VARCHAR(src:pidt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) THEN
                    'prdt contains a non-timestamp value : \'' || AS_VARCHAR(src:prdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_1), '0'), 38, 10) is null and 
                    src:rahc_1 is not null) THEN
                    'rahc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rahc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_2), '0'), 38, 10) is null and 
                    src:rahc_2 is not null) THEN
                    'rahc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rahc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_3), '0'), 38, 10) is null and 
                    src:rahc_3 is not null) THEN
                    'rahc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rahc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_cntc), '0'), 38, 10) is null and 
                    src:rahc_cntc is not null) THEN
                    'rahc_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:rahc_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_dwhc), '0'), 38, 10) is null and 
                    src:rahc_dwhc is not null) THEN
                    'rahc_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:rahc_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_refc), '0'), 38, 10) is null and 
                    src:rahc_refc is not null) THEN
                    'rahc_refc contains a non-numeric value : \'' || AS_VARCHAR(src:rahc_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_1), '0'), 38, 10) is null and 
                    src:ratc_1 is not null) THEN
                    'ratc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_2), '0'), 38, 10) is null and 
                    src:ratc_2 is not null) THEN
                    'ratc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_3), '0'), 38, 10) is null and 
                    src:ratc_3 is not null) THEN
                    'ratc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ratd), '1900-01-01')) is null and 
                    src:ratd is not null) THEN
                    'ratd contains a non-timestamp value : \'' || AS_VARCHAR(src:ratd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) THEN
                    'ratf_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) THEN
                    'ratf_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) THEN
                    'ratf_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stin), '0'), 38, 10) is null and 
                    src:stin is not null) THEN
                    'stin contains a non-numeric value : \'' || AS_VARCHAR(src:stin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term), '0'), 38, 10) is null and 
                    src:term is not null) THEN
                    'term contains a non-numeric value : \'' || AS_VARCHAR(src:term) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term_cfln_ref_compnr), '0'), 38, 10) is null and 
                    src:term_cfln_ref_compnr is not null) THEN
                    'term_cfln_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:term_cfln_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txmt), '0'), 38, 10) is null and 
                    src:txmt is not null) THEN
                    'txmt contains a non-numeric value : \'' || AS_VARCHAR(src:txmt) || '\'' WHEN 
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
                                    
                src:inst,
                src:compnr,
                src:csco  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSCTM400 as strm)
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cchn), '0'), 38, 10) is null and 
                    src:cchn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfln), '0'), 38, 10) is null and 
                    src:cfln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_cchn_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_cchn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dimt), '0'), 38, 10) is null and 
                    src:dimt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dimt_dwhc), '0'), 38, 10) is null and 
                    src:dimt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dimt_homc), '0'), 38, 10) is null and 
                    src:dimt_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dimt_refc), '0'), 38, 10) is null and 
                    src:dimt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dimt_rpc1), '0'), 38, 10) is null and 
                    src:dimt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dimt_rpc2), '0'), 38, 10) is null and 
                    src:dimt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc), '0'), 38, 10) is null and 
                    src:disc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmt_1), '0'), 38, 10) is null and 
                    src:fcmt_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmt_2), '0'), 38, 10) is null and 
                    src:fcmt_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmt_3), '0'), 38, 10) is null and 
                    src:fcmt_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcpc), '0'), 38, 10) is null and 
                    src:fcpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fper), '0'), 38, 10) is null and 
                    src:fper is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fyer), '0'), 38, 10) is null and 
                    src:fyer is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icmp), '0'), 38, 10) is null and 
                    src:icmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icmt), '0'), 38, 10) is null and 
                    src:icmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inmt), '0'), 38, 10) is null and 
                    src:inmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inst), '0'), 38, 10) is null and 
                    src:inst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:isst), '0'), 38, 10) is null and 
                    src:isst is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:istd), '1900-01-01')) is null and 
                    src:istd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrpf), '0'), 38, 10) is null and 
                    src:nrpf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ntmt), '0'), 38, 10) is null and 
                    src:ntmt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pidt), '1900-01-01')) is null and 
                    src:pidt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_1), '0'), 38, 10) is null and 
                    src:rahc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_2), '0'), 38, 10) is null and 
                    src:rahc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_3), '0'), 38, 10) is null and 
                    src:rahc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_cntc), '0'), 38, 10) is null and 
                    src:rahc_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_dwhc), '0'), 38, 10) is null and 
                    src:rahc_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_refc), '0'), 38, 10) is null and 
                    src:rahc_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_1), '0'), 38, 10) is null and 
                    src:ratc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_2), '0'), 38, 10) is null and 
                    src:ratc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_3), '0'), 38, 10) is null and 
                    src:ratc_3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ratd), '1900-01-01')) is null and 
                    src:ratd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stin), '0'), 38, 10) is null and 
                    src:stin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term), '0'), 38, 10) is null and 
                    src:term is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term_cfln_ref_compnr), '0'), 38, 10) is null and 
                    src:term_cfln_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txmt), '0'), 38, 10) is null and 
                    src:txmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)