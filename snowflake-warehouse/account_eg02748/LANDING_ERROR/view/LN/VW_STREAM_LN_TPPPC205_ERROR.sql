CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPPC205_ERROR AS SELECT src, 'LN_TPPPC205' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) THEN
                    'cprj_cpla_cact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_cact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) THEN
                    'cprj_cspa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cspa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) THEN
                    'cprj_cstl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cstl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) THEN
                    'emno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:emno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:frdt), '1900-01-01')) is null and 
                    src:frdt is not null) THEN
                    'frdt contains a non-timestamp value : \'' || AS_VARCHAR(src:frdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hper), '0'), 38, 10) is null and 
                    src:hper is not null) THEN
                    'hper contains a non-numeric value : \'' || AS_VARCHAR(src:hper) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hyea), '0'), 38, 10) is null and 
                    src:hyea is not null) THEN
                    'hyea contains a non-numeric value : \'' || AS_VARCHAR(src:hyea) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peri), '0'), 38, 10) is null and 
                    src:peri is not null) THEN
                    'peri contains a non-numeric value : \'' || AS_VARCHAR(src:peri) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rgdt), '1900-01-01')) is null and 
                    src:rgdt is not null) THEN
                    'rgdt contains a non-timestamp value : \'' || AS_VARCHAR(src:rgdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) THEN
                    'year contains a non-numeric value : \'' || AS_VARCHAR(src:year) || '\'' WHEN 
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
                src:cprj,
                src:user  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPPC205 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:frdt), '1900-01-01')) is null and 
                    src:frdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hper), '0'), 38, 10) is null and 
                    src:hper is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hyea), '0'), 38, 10) is null and 
                    src:hyea is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peri), '0'), 38, 10) is null and 
                    src:peri is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rgdt), '1900-01-01')) is null and 
                    src:rgdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)