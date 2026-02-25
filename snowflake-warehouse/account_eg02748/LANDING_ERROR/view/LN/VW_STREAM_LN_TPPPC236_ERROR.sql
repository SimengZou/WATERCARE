CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPPC236_ERROR AS SELECT src, 'LN_TPPPC236' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc), '0'), 38, 10) is null and 
                    src:amoc is not null) THEN
                    'amoc contains a non-numeric value : \'' || AS_VARCHAR(src:amoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_cntc), '0'), 38, 10) is null and 
                    src:amoc_cntc is not null) THEN
                    'amoc_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:amoc_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_dwhc), '0'), 38, 10) is null and 
                    src:amoc_dwhc is not null) THEN
                    'amoc_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:amoc_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_homc), '0'), 38, 10) is null and 
                    src:amoc_homc is not null) THEN
                    'amoc_homc contains a non-numeric value : \'' || AS_VARCHAR(src:amoc_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_prjc), '0'), 38, 10) is null and 
                    src:amoc_prjc is not null) THEN
                    'amoc_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:amoc_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_refc), '0'), 38, 10) is null and 
                    src:amoc_refc is not null) THEN
                    'amoc_refc contains a non-numeric value : \'' || AS_VARCHAR(src:amoc_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_rpc1), '0'), 38, 10) is null and 
                    src:amoc_rpc1 is not null) THEN
                    'amoc_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:amoc_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_rpc2), '0'), 38, 10) is null and 
                    src:amoc_rpc2 is not null) THEN
                    'amoc_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:amoc_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appr), '0'), 38, 10) is null and 
                    src:appr is not null) THEN
                    'appr contains a non-numeric value : \'' || AS_VARCHAR(src:appr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) THEN
                    'ccco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) THEN
                    'cprj_cpla_cact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_cact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) THEN
                    'cprj_cspa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cspa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_fcmp), '0'), 38, 10) is null and 
                    src:cprj_fcmp is not null) THEN
                    'cprj_fcmp contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_fcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwgt_ref_compnr), '0'), 38, 10) is null and 
                    src:cwgt_ref_compnr is not null) THEN
                    'cwgt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwgt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_cntc), '0'), 38, 10) is null and 
                    src:feac_cntc is not null) THEN
                    'feac_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:feac_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_cuni_base), '0'), 38, 10) is null and 
                    src:feac_cuni_base is not null) THEN
                    'feac_cuni_base contains a non-numeric value : \'' || AS_VARCHAR(src:feac_cuni_base) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_dwhc), '0'), 38, 10) is null and 
                    src:feac_dwhc is not null) THEN
                    'feac_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:feac_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_homc), '0'), 38, 10) is null and 
                    src:feac_homc is not null) THEN
                    'feac_homc contains a non-numeric value : \'' || AS_VARCHAR(src:feac_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_prjc), '0'), 38, 10) is null and 
                    src:feac_prjc is not null) THEN
                    'feac_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:feac_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_refc), '0'), 38, 10) is null and 
                    src:feac_refc is not null) THEN
                    'feac_refc contains a non-numeric value : \'' || AS_VARCHAR(src:feac_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_rpc1), '0'), 38, 10) is null and 
                    src:feac_rpc1 is not null) THEN
                    'feac_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:feac_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_rpc2), '0'), 38, 10) is null and 
                    src:feac_rpc2 is not null) THEN
                    'feac_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:feac_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_trnc), '0'), 38, 10) is null and 
                    src:feac_trnc is not null) THEN
                    'feac_trnc contains a non-numeric value : \'' || AS_VARCHAR(src:feac_trnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frat), '0'), 38, 10) is null and 
                    src:frat is not null) THEN
                    'frat contains a non-numeric value : \'' || AS_VARCHAR(src:frat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_cntc), '0'), 38, 10) is null and 
                    src:pric_cntc is not null) THEN
                    'pric_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:pric_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_dwhc), '0'), 38, 10) is null and 
                    src:pric_dwhc is not null) THEN
                    'pric_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:pric_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_homc), '0'), 38, 10) is null and 
                    src:pric_homc is not null) THEN
                    'pric_homc contains a non-numeric value : \'' || AS_VARCHAR(src:pric_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_prjc), '0'), 38, 10) is null and 
                    src:pric_prjc is not null) THEN
                    'pric_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:pric_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_refc), '0'), 38, 10) is null and 
                    src:pric_refc is not null) THEN
                    'pric_refc contains a non-numeric value : \'' || AS_VARCHAR(src:pric_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_rpc1), '0'), 38, 10) is null and 
                    src:pric_rpc1 is not null) THEN
                    'pric_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:pric_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_rpc2), '0'), 38, 10) is null and 
                    src:pric_rpc2 is not null) THEN
                    'pric_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:pric_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan), '0'), 38, 10) is null and 
                    src:quan is not null) THEN
                    'quan contains a non-numeric value : \'' || AS_VARCHAR(src:quan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buar), '0'), 38, 10) is null and 
                    src:quan_buar is not null) THEN
                    'quan_buar contains a non-numeric value : \'' || AS_VARCHAR(src:quan_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buln), '0'), 38, 10) is null and 
                    src:quan_buln is not null) THEN
                    'quan_buln contains a non-numeric value : \'' || AS_VARCHAR(src:quan_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_bupc), '0'), 38, 10) is null and 
                    src:quan_bupc is not null) THEN
                    'quan_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:quan_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buvl), '0'), 38, 10) is null and 
                    src:quan_buvl is not null) THEN
                    'quan_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:quan_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buwg), '0'), 38, 10) is null and 
                    src:quan_buwg is not null) THEN
                    'quan_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:quan_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc), '0'), 38, 10) is null and 
                    src:ratc is not null) THEN
                    'ratc contains a non-numeric value : \'' || AS_VARCHAR(src:ratc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) THEN
                    'rdat contains a non-timestamp value : \'' || AS_VARCHAR(src:rdat) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rgdt), '1900-01-01')) is null and 
                    src:rgdt is not null) THEN
                    'rgdt contains a non-timestamp value : \'' || AS_VARCHAR(src:rgdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_1), '0'), 38, 10) is null and 
                    src:rtcc_1 is not null) THEN
                    'rtcc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_2), '0'), 38, 10) is null and 
                    src:rtcc_2 is not null) THEN
                    'rtcc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_3), '0'), 38, 10) is null and 
                    src:rtcc_3 is not null) THEN
                    'rtcc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_1), '0'), 38, 10) is null and 
                    src:rtfc_1 is not null) THEN
                    'rtfc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_2), '0'), 38, 10) is null and 
                    src:rtfc_2 is not null) THEN
                    'rtfc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_3), '0'), 38, 10) is null and 
                    src:rtfc_3 is not null) THEN
                    'rtfc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:task_rcmp), '0'), 38, 10) is null and 
                    src:task_rcmp is not null) THEN
                    'task_rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:task_rcmp) || '\'' WHEN 
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
                                    
                src:rgdt,
                src:compnr,
                src:task,
                src:cact,
                src:cprj,
                src:cspa  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPPC236 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc), '0'), 38, 10) is null and 
                    src:amoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_cntc), '0'), 38, 10) is null and 
                    src:amoc_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_dwhc), '0'), 38, 10) is null and 
                    src:amoc_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_homc), '0'), 38, 10) is null and 
                    src:amoc_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_prjc), '0'), 38, 10) is null and 
                    src:amoc_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_refc), '0'), 38, 10) is null and 
                    src:amoc_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_rpc1), '0'), 38, 10) is null and 
                    src:amoc_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc_rpc2), '0'), 38, 10) is null and 
                    src:amoc_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appr), '0'), 38, 10) is null and 
                    src:appr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_fcmp), '0'), 38, 10) is null and 
                    src:cprj_fcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwgt_ref_compnr), '0'), 38, 10) is null and 
                    src:cwgt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_cntc), '0'), 38, 10) is null and 
                    src:feac_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_cuni_base), '0'), 38, 10) is null and 
                    src:feac_cuni_base is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_dwhc), '0'), 38, 10) is null and 
                    src:feac_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_homc), '0'), 38, 10) is null and 
                    src:feac_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_prjc), '0'), 38, 10) is null and 
                    src:feac_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_refc), '0'), 38, 10) is null and 
                    src:feac_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_rpc1), '0'), 38, 10) is null and 
                    src:feac_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_rpc2), '0'), 38, 10) is null and 
                    src:feac_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feac_trnc), '0'), 38, 10) is null and 
                    src:feac_trnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frat), '0'), 38, 10) is null and 
                    src:frat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_cntc), '0'), 38, 10) is null and 
                    src:pric_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_dwhc), '0'), 38, 10) is null and 
                    src:pric_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_homc), '0'), 38, 10) is null and 
                    src:pric_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_prjc), '0'), 38, 10) is null and 
                    src:pric_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_refc), '0'), 38, 10) is null and 
                    src:pric_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_rpc1), '0'), 38, 10) is null and 
                    src:pric_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric_rpc2), '0'), 38, 10) is null and 
                    src:pric_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan), '0'), 38, 10) is null and 
                    src:quan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buar), '0'), 38, 10) is null and 
                    src:quan_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buln), '0'), 38, 10) is null and 
                    src:quan_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_bupc), '0'), 38, 10) is null and 
                    src:quan_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buvl), '0'), 38, 10) is null and 
                    src:quan_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buwg), '0'), 38, 10) is null and 
                    src:quan_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc), '0'), 38, 10) is null and 
                    src:ratc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rgdt), '1900-01-01')) is null and 
                    src:rgdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_1), '0'), 38, 10) is null and 
                    src:rtcc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_2), '0'), 38, 10) is null and 
                    src:rtcc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_3), '0'), 38, 10) is null and 
                    src:rtcc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_1), '0'), 38, 10) is null and 
                    src:rtfc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_2), '0'), 38, 10) is null and 
                    src:rtfc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_3), '0'), 38, 10) is null and 
                    src:rtfc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:task_rcmp), '0'), 38, 10) is null and 
                    src:task_rcmp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)