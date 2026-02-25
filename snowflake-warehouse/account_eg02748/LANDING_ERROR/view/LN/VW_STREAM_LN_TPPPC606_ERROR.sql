CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPPC606_ERROR AS SELECT src, 'LN_TPPPC606' as TABLE_OBJECT, CASE WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eatc_cntc), '0'), 38, 10) is null and 
                    src:eatc_cntc is not null) THEN
                    'eatc_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:eatc_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eatc_dwhc), '0'), 38, 10) is null and 
                    src:eatc_dwhc is not null) THEN
                    'eatc_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:eatc_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eatc_homc), '0'), 38, 10) is null and 
                    src:eatc_homc is not null) THEN
                    'eatc_homc contains a non-numeric value : \'' || AS_VARCHAR(src:eatc_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eatc_prjc), '0'), 38, 10) is null and 
                    src:eatc_prjc is not null) THEN
                    'eatc_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:eatc_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eatc_refc), '0'), 38, 10) is null and 
                    src:eatc_refc is not null) THEN
                    'eatc_refc contains a non-numeric value : \'' || AS_VARCHAR(src:eatc_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eatc_rpc1), '0'), 38, 10) is null and 
                    src:eatc_rpc1 is not null) THEN
                    'eatc_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:eatc_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eatc_rpc2), '0'), 38, 10) is null and 
                    src:eatc_rpc2 is not null) THEN
                    'eatc_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:eatc_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eatc_trnc), '0'), 38, 10) is null and 
                    src:eatc_trnc is not null) THEN
                    'eatc_trnc contains a non-numeric value : \'' || AS_VARCHAR(src:eatc_trnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etoc_cntc), '0'), 38, 10) is null and 
                    src:etoc_cntc is not null) THEN
                    'etoc_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:etoc_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etoc_dwhc), '0'), 38, 10) is null and 
                    src:etoc_dwhc is not null) THEN
                    'etoc_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:etoc_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etoc_homc), '0'), 38, 10) is null and 
                    src:etoc_homc is not null) THEN
                    'etoc_homc contains a non-numeric value : \'' || AS_VARCHAR(src:etoc_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etoc_prjc), '0'), 38, 10) is null and 
                    src:etoc_prjc is not null) THEN
                    'etoc_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:etoc_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etoc_refc), '0'), 38, 10) is null and 
                    src:etoc_refc is not null) THEN
                    'etoc_refc contains a non-numeric value : \'' || AS_VARCHAR(src:etoc_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etoc_rpc1), '0'), 38, 10) is null and 
                    src:etoc_rpc1 is not null) THEN
                    'etoc_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:etoc_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etoc_rpc2), '0'), 38, 10) is null and 
                    src:etoc_rpc2 is not null) THEN
                    'etoc_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:etoc_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etoc_trnc), '0'), 38, 10) is null and 
                    src:etoc_trnc is not null) THEN
                    'etoc_trnc contains a non-numeric value : \'' || AS_VARCHAR(src:etoc_trnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ovhd_ref_compnr), '0'), 38, 10) is null and 
                    src:ovhd_ref_compnr is not null) THEN
                    'ovhd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ovhd_ref_compnr) || '\'' WHEN 
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
                                    
                src:cact,
                src:cprj,
                src:ovhd,
                src:cspa,
                src:rgdt,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPPC606 as strm)
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eatc_cntc), '0'), 38, 10) is null and 
                    src:eatc_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eatc_dwhc), '0'), 38, 10) is null and 
                    src:eatc_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eatc_homc), '0'), 38, 10) is null and 
                    src:eatc_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eatc_prjc), '0'), 38, 10) is null and 
                    src:eatc_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eatc_refc), '0'), 38, 10) is null and 
                    src:eatc_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eatc_rpc1), '0'), 38, 10) is null and 
                    src:eatc_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eatc_rpc2), '0'), 38, 10) is null and 
                    src:eatc_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eatc_trnc), '0'), 38, 10) is null and 
                    src:eatc_trnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etoc_cntc), '0'), 38, 10) is null and 
                    src:etoc_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etoc_dwhc), '0'), 38, 10) is null and 
                    src:etoc_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etoc_homc), '0'), 38, 10) is null and 
                    src:etoc_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etoc_prjc), '0'), 38, 10) is null and 
                    src:etoc_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etoc_refc), '0'), 38, 10) is null and 
                    src:etoc_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etoc_rpc1), '0'), 38, 10) is null and 
                    src:etoc_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etoc_rpc2), '0'), 38, 10) is null and 
                    src:etoc_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etoc_trnc), '0'), 38, 10) is null and 
                    src:etoc_trnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ovhd_ref_compnr), '0'), 38, 10) is null and 
                    src:ovhd_ref_compnr is not null) or 
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)