CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPSS210_ERROR AS SELECT src, 'LN_TPPSS210' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_pact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_pact_ref_compnr is not null) THEN
                    'cprj_cpla_pact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_pact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) THEN
                    'cprj_cpla_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_sact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_sact_ref_compnr is not null) THEN
                    'cprj_cpla_sact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_sact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lead), '0'), 38, 10) is null and 
                    src:lead is not null) THEN
                    'lead contains a non-numeric value : \'' || AS_VARCHAR(src:lead) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:llpc), '0'), 38, 10) is null and 
                    src:llpc is not null) THEN
                    'llpc contains a non-numeric value : \'' || AS_VARCHAR(src:llpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcot), '0'), 38, 10) is null and 
                    src:pcot is not null) THEN
                    'pcot contains a non-numeric value : \'' || AS_VARCHAR(src:pcot) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psen), '0'), 38, 10) is null and 
                    src:psen is not null) THEN
                    'psen contains a non-numeric value : \'' || AS_VARCHAR(src:psen) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rest), '0'), 38, 10) is null and 
                    src:rest is not null) THEN
                    'rest contains a non-numeric value : \'' || AS_VARCHAR(src:rest) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rltp), '0'), 38, 10) is null and 
                    src:rltp is not null) THEN
                    'rltp contains a non-numeric value : \'' || AS_VARCHAR(src:rltp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scot), '0'), 38, 10) is null and 
                    src:scot is not null) THEN
                    'scot contains a non-numeric value : \'' || AS_VARCHAR(src:scot) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssen), '0'), 38, 10) is null and 
                    src:ssen is not null) THEN
                    'ssen contains a non-numeric value : \'' || AS_VARCHAR(src:ssen) || '\'' WHEN 
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
                src:ffno,
                src:cpla,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPSS210 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_pact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_pact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_sact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_sact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lead), '0'), 38, 10) is null and 
                    src:lead is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:llpc), '0'), 38, 10) is null and 
                    src:llpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcot), '0'), 38, 10) is null and 
                    src:pcot is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psen), '0'), 38, 10) is null and 
                    src:psen is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rest), '0'), 38, 10) is null and 
                    src:rest is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rltp), '0'), 38, 10) is null and 
                    src:rltp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scot), '0'), 38, 10) is null and 
                    src:scot is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssen), '0'), 38, 10) is null and 
                    src:ssen is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)