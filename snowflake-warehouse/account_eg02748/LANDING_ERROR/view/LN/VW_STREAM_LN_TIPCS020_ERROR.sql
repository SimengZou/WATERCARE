CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TIPCS020_ERROR AS SELECT src, 'LN_TIPCS020' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is null and 
                    src:bpid_ref_compnr is not null) THEN
                    'bpid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccgr_ref_compnr), '0'), 38, 10) is null and 
                    src:ccgr_ref_compnr is not null) THEN
                    'ccgr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccgr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cfdt), '1900-01-01')) is null and 
                    src:cfdt is not null) THEN
                    'cfdt contains a non-timestamp value : \'' || AS_VARCHAR(src:cfdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clco_ref_compnr), '0'), 38, 10) is null and 
                    src:clco_ref_compnr is not null) THEN
                    'clco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcc_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcc_ref_compnr is not null) THEN
                    'cpcc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpcc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtfs), '0'), 38, 10) is null and 
                    src:dtfs is not null) THEN
                    'dtfs contains a non-numeric value : \'' || AS_VARCHAR(src:dtfs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ffci), '0'), 38, 10) is null and 
                    src:ffci is not null) THEN
                    'ffci contains a non-numeric value : \'' || AS_VARCHAR(src:ffci) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kopr), '0'), 38, 10) is null and 
                    src:kopr is not null) THEN
                    'kopr contains a non-numeric value : \'' || AS_VARCHAR(src:kopr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pemp_ref_compnr), '0'), 38, 10) is null and 
                    src:pemp_ref_compnr is not null) THEN
                    'pemp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pemp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peng), '0'), 38, 10) is null and 
                    src:peng is not null) THEN
                    'peng contains a non-numeric value : \'' || AS_VARCHAR(src:peng) || '\'' WHEN 
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
                                    
                src:cprj,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TIPCS020 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is null and 
                    src:bpid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccgr_ref_compnr), '0'), 38, 10) is null and 
                    src:ccgr_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cfdt), '1900-01-01')) is null and 
                    src:cfdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clco_ref_compnr), '0'), 38, 10) is null and 
                    src:clco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcc_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtfs), '0'), 38, 10) is null and 
                    src:dtfs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ffci), '0'), 38, 10) is null and 
                    src:ffci is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kopr), '0'), 38, 10) is null and 
                    src:kopr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pemp_ref_compnr), '0'), 38, 10) is null and 
                    src:pemp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peng), '0'), 38, 10) is null and 
                    src:peng is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)