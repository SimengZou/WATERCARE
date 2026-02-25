CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPTC316_ERROR AS SELECT src, 'LN_TPPTC316' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aeqc_1), '0'), 38, 10) is null and 
                    src:aeqc_1 is not null) THEN
                    'aeqc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:aeqc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aeqc_2), '0'), 38, 10) is null and 
                    src:aeqc_2 is not null) THEN
                    'aeqc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:aeqc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aeqc_3), '0'), 38, 10) is null and 
                    src:aeqc_3 is not null) THEN
                    'aeqc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:aeqc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aeqc_4), '0'), 38, 10) is null and 
                    src:aeqc_4 is not null) THEN
                    'aeqc_4 contains a non-numeric value : \'' || AS_VARCHAR(src:aeqc_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aicc_1), '0'), 38, 10) is null and 
                    src:aicc_1 is not null) THEN
                    'aicc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:aicc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aicc_2), '0'), 38, 10) is null and 
                    src:aicc_2 is not null) THEN
                    'aicc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:aicc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aicc_3), '0'), 38, 10) is null and 
                    src:aicc_3 is not null) THEN
                    'aicc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:aicc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aicc_4), '0'), 38, 10) is null and 
                    src:aicc_4 is not null) THEN
                    'aicc_4 contains a non-numeric value : \'' || AS_VARCHAR(src:aicc_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aicl_1), '0'), 38, 10) is null and 
                    src:aicl_1 is not null) THEN
                    'aicl_1 contains a non-numeric value : \'' || AS_VARCHAR(src:aicl_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aicl_2), '0'), 38, 10) is null and 
                    src:aicl_2 is not null) THEN
                    'aicl_2 contains a non-numeric value : \'' || AS_VARCHAR(src:aicl_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aicl_3), '0'), 38, 10) is null and 
                    src:aicl_3 is not null) THEN
                    'aicl_3 contains a non-numeric value : \'' || AS_VARCHAR(src:aicl_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aicl_4), '0'), 38, 10) is null and 
                    src:aicl_4 is not null) THEN
                    'aicl_4 contains a non-numeric value : \'' || AS_VARCHAR(src:aicl_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_1), '0'), 38, 10) is null and 
                    src:amac_1 is not null) THEN
                    'amac_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amac_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_2), '0'), 38, 10) is null and 
                    src:amac_2 is not null) THEN
                    'amac_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amac_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_3), '0'), 38, 10) is null and 
                    src:amac_3 is not null) THEN
                    'amac_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amac_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_4), '0'), 38, 10) is null and 
                    src:amac_4 is not null) THEN
                    'amac_4 contains a non-numeric value : \'' || AS_VARCHAR(src:amac_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asbc_1), '0'), 38, 10) is null and 
                    src:asbc_1 is not null) THEN
                    'asbc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:asbc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asbc_2), '0'), 38, 10) is null and 
                    src:asbc_2 is not null) THEN
                    'asbc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:asbc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asbc_3), '0'), 38, 10) is null and 
                    src:asbc_3 is not null) THEN
                    'asbc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:asbc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asbc_4), '0'), 38, 10) is null and 
                    src:asbc_4 is not null) THEN
                    'asbc_4 contains a non-numeric value : \'' || AS_VARCHAR(src:asbc_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atac_1), '0'), 38, 10) is null and 
                    src:atac_1 is not null) THEN
                    'atac_1 contains a non-numeric value : \'' || AS_VARCHAR(src:atac_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atac_2), '0'), 38, 10) is null and 
                    src:atac_2 is not null) THEN
                    'atac_2 contains a non-numeric value : \'' || AS_VARCHAR(src:atac_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atac_3), '0'), 38, 10) is null and 
                    src:atac_3 is not null) THEN
                    'atac_3 contains a non-numeric value : \'' || AS_VARCHAR(src:atac_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atac_4), '0'), 38, 10) is null and 
                    src:atac_4 is not null) THEN
                    'atac_4 contains a non-numeric value : \'' || AS_VARCHAR(src:atac_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) THEN
                    'ccco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ccal_ref_compnr is not null) THEN
                    'cprj_ccal_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ccal_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) THEN
                    'cprj_cpla_cact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_cact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) THEN
                    'cprj_cpla_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtah), '0'), 38, 10) is null and 
                    src:qtah is not null) THEN
                    'qtah contains a non-numeric value : \'' || AS_VARCHAR(src:qtah) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
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
                                    
                src:cpla,
                src:cprj,
                src:cact,
                src:compnr,
                src:ccco,
                src:ccal  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPTC316 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aeqc_1), '0'), 38, 10) is null and 
                    src:aeqc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aeqc_2), '0'), 38, 10) is null and 
                    src:aeqc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aeqc_3), '0'), 38, 10) is null and 
                    src:aeqc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aeqc_4), '0'), 38, 10) is null and 
                    src:aeqc_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aicc_1), '0'), 38, 10) is null and 
                    src:aicc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aicc_2), '0'), 38, 10) is null and 
                    src:aicc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aicc_3), '0'), 38, 10) is null and 
                    src:aicc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aicc_4), '0'), 38, 10) is null and 
                    src:aicc_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aicl_1), '0'), 38, 10) is null and 
                    src:aicl_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aicl_2), '0'), 38, 10) is null and 
                    src:aicl_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aicl_3), '0'), 38, 10) is null and 
                    src:aicl_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aicl_4), '0'), 38, 10) is null and 
                    src:aicl_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_1), '0'), 38, 10) is null and 
                    src:amac_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_2), '0'), 38, 10) is null and 
                    src:amac_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_3), '0'), 38, 10) is null and 
                    src:amac_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_4), '0'), 38, 10) is null and 
                    src:amac_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asbc_1), '0'), 38, 10) is null and 
                    src:asbc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asbc_2), '0'), 38, 10) is null and 
                    src:asbc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asbc_3), '0'), 38, 10) is null and 
                    src:asbc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asbc_4), '0'), 38, 10) is null and 
                    src:asbc_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atac_1), '0'), 38, 10) is null and 
                    src:atac_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atac_2), '0'), 38, 10) is null and 
                    src:atac_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atac_3), '0'), 38, 10) is null and 
                    src:atac_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atac_4), '0'), 38, 10) is null and 
                    src:atac_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ccal_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtah), '0'), 38, 10) is null and 
                    src:qtah is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)