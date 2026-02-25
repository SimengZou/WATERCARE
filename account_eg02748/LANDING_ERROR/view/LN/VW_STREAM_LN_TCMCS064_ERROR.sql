CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCMCS064_ERROR AS SELECT src, 'LN_TCMCS064' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:actn), '0'), 38, 10) is null and 
                    src:actn is not null) THEN
                    'actn contains a non-numeric value : \'' || AS_VARCHAR(src:actn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ddcc), '0'), 38, 10) is null and 
                    src:ddcc is not null) THEN
                    'ddcc contains a non-numeric value : \'' || AS_VARCHAR(src:ddcc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ioso), '0'), 38, 10) is null and 
                    src:ioso is not null) THEN
                    'ioso contains a non-numeric value : \'' || AS_VARCHAR(src:ioso) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phs1), '0'), 38, 10) is null and 
                    src:phs1 is not null) THEN
                    'phs1 contains a non-numeric value : \'' || AS_VARCHAR(src:phs1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phs2), '0'), 38, 10) is null and 
                    src:phs2 is not null) THEN
                    'phs2 contains a non-numeric value : \'' || AS_VARCHAR(src:phs2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phs3), '0'), 38, 10) is null and 
                    src:phs3 is not null) THEN
                    'phs3 contains a non-numeric value : \'' || AS_VARCHAR(src:phs3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phs4), '0'), 38, 10) is null and 
                    src:phs4 is not null) THEN
                    'phs4 contains a non-numeric value : \'' || AS_VARCHAR(src:phs4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phs5), '0'), 38, 10) is null and 
                    src:phs5 is not null) THEN
                    'phs5 contains a non-numeric value : \'' || AS_VARCHAR(src:phs5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phs6), '0'), 38, 10) is null and 
                    src:phs6 is not null) THEN
                    'phs6 contains a non-numeric value : \'' || AS_VARCHAR(src:phs6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phs7), '0'), 38, 10) is null and 
                    src:phs7 is not null) THEN
                    'phs7 contains a non-numeric value : \'' || AS_VARCHAR(src:phs7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revp), '0'), 38, 10) is null and 
                    src:revp is not null) THEN
                    'revp contains a non-numeric value : \'' || AS_VARCHAR(src:revp) || '\'' WHEN 
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
                                    
                src:crat,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCMCS064 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:actn), '0'), 38, 10) is null and 
                    src:actn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ddcc), '0'), 38, 10) is null and 
                    src:ddcc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ioso), '0'), 38, 10) is null and 
                    src:ioso is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phs1), '0'), 38, 10) is null and 
                    src:phs1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phs2), '0'), 38, 10) is null and 
                    src:phs2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phs3), '0'), 38, 10) is null and 
                    src:phs3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phs4), '0'), 38, 10) is null and 
                    src:phs4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phs5), '0'), 38, 10) is null and 
                    src:phs5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phs6), '0'), 38, 10) is null and 
                    src:phs6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phs7), '0'), 38, 10) is null and 
                    src:phs7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revp), '0'), 38, 10) is null and 
                    src:revp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)